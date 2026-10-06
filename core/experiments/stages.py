"""Runner-owned DAG attempts through the common participant call protocol."""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

import psutil

from core.experiments.journal import RunnerJournal
from core.experiments.launch import ModuleLauncher
from core.experiments.results import (
    MissingConditionalDataError,
    _normalize_conditional_result,
    read_result,
)
from core.experiments.state import (
    RunnerState,
    RunnerStateStore,
    ServiceInstance,
    StageAttempt,
    StageOutcome,
    _attempt_result_identity,
    _process_identity_document,
    _process_identity_pid,
    _retain_process_identity,
    state_to_document,
)
from core.journal.events import LoggingError
from core.models.experiment_template import (
    ModuleReference,
    ServiceCallDefinition,
    StageDefinition,
)
from core.models.module_manifest import ModuleManifest
from core.models.participant_identity import ParticipantIdentity
from core.models.participant_launch import (
    AttemptContextObservation,
    ModulePreparation,
    PreparedLaunch,
)
from core.models.participant_observations import (
    ExecutorCommandState,
    ExecutorCommandStateResponse,
    RetainedExecutorStatus,
)
from core.models.participant_protocol import StageOutcomeResult
from core.models.process_identity import ProcessIdentity
from core.models.runner_state import (
    ExecutorStatus,
    PendingInput,
    ServiceCallExecutorStatus,
)
from core.models.updates import _update_model
from core.participants.connection import ParticipantConnection
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import JsonObject, JsonValue
from core.primitives.paths import repository_root
from core.primitives.processes import process_identity

if TYPE_CHECKING:
    from core.journal.logger import OperationLogger


def _record_runner_checkpoint(logger: OperationLogger, state: RunnerState) -> None:
    logger.record_event(
        "runner.checkpoint",
        state_to_document(state),
        context={"experiment_id": state.experiment_id, "run_id": state.run_id},
    )


def _finish_executor_status(status: ExecutorStatus | JsonValue) -> ExecutorStatus:
    if isinstance(
        status,
        (ExecutorCommandState, RetainedExecutorStatus, ServiceCallExecutorStatus),
    ):
        return _update_model(status, finished=True, current=None, pending=[])
    if isinstance(status, dict):
        values = dict(status)
    elif not status:
        values = {}
    else:
        raise TypeError("Executor status must be an object to accept a result.")
    values.update(finished=True, current=None, pending=[])
    return RetainedExecutorStatus.model_validate(values)


class StageRunner:
    """Execute/recover stage attempts and accept outcomes while the runner owns DAG movement."""
    def __init__(
        self,
        launcher: ModuleLauncher,
        journal: RunnerJournal,
        state_store: RunnerStateStore,
        *,
        services=None,
        notify_resources: Callable[[], None] | None = None,
    ) -> None:
        """Bind attempt preparation, journal, state storage, and optional service control.

        Args:
            launcher: Module launch preparation service.
            journal: Open runner journal used for intents and accepted results.
            state_store: Runner checkpoint persistence.
            services: Optional service manager for DAG service-call nodes.
            notify_resources: Optional callback publishing changed process targets.
        """
        self._launcher = launcher
        self._journal = journal
        self._state_store = state_store
        self._services = services
        self._connection = None
        self._call_future = None
        self._process = None
        self._process_attempt_id = None
        self._unstarted_attempt_id = None
        self._executor_processes = []
        self._executor_identities: dict[str, ProcessIdentity | JsonObject | None] = {}
        self._notify_resources = notify_resources

    def bind_services(self, services) -> None:
        """Set the service manager used for queued DAG calls and interruption.

        Args:
            services: Caller-owned ServiceManager handling queued DAG calls.
        """
        self._services = services

    async def execute(
        self,
        state: RunnerState,
        *,
        manual: bool = False,
        wait_services: Callable[
            [RunnerState], Awaitable[Literal["ready", "pause", "stop"]]
        ]
        | None = None,
        recovered: StageAttempt | None = None,
    ) -> StageOutcome:
        """Execute the selected stage visit, applying retries without advancing the DAG.

        Args:
            state: Mutable runner state at the selected one-based stage position.
            manual: Whether to invalidate the selected stage's current result before work.
            wait_services: Optional readiness callback run before automatic retries.
            recovered: Existing attempt to reconcile before starting any new work.

        Returns:
            Final attempt/result and the requested advance, pause, or stop action.
        """
        definition = state.template.stages[state.stage_position - 1]
        stage_id = definition.stage_id
        input_data = (
            self.select_input(state) if recovered is None else recovered.input_data
        )
        execution_id = recovered.stage_execution_id if recovered else str(uuid4())
        if manual:
            state.stage_result_ids.pop(stage_id, None)
            state.stage_result_origins.pop(stage_id, None)
            state.last_result = state.last_result_id = None
        policy = definition.errors
        while True:
            attempt = (
                recovered
                if recovered is not None and recovered.outcome != "unknown_stopped"
                else await self._start_attempt(
                    state, input_data, execution_id=execution_id
                )
            )
            recovered = None
            response = await self._collect_result(state, attempt)
            self._journal.client.record_event(
                "stage.finished",
                {
                    "result": response.model_dump(exclude_unset=True),
                    "outcome": attempt.outcome,
                    "result_request_id": attempt.result_request_id,
                },
                context=self._context(state, attempt),
            )
            self._save_state(state)
            if attempt.outcome == "unknown":
                return StageOutcome(attempt, response, "stop")
            if response is not None and response.result == "success":
                return StageOutcome(attempt, response, "advance")
            if state.pause_requested:
                return StageOutcome(attempt, response, "pause")
            used = state.stage_retry_counts.get(stage_id, 0)
            if used >= policy.retries:
                return StageOutcome(
                    attempt, response, self._exhausted_action(definition, response)
                )
            await asyncio.sleep(policy.retry_delay_seconds)
            if state.pause_requested:
                return StageOutcome(attempt, response, "pause")
            if wait_services is not None:
                action = await wait_services(state)
                if action != "ready":
                    return StageOutcome(attempt, response, action)
                if state.pause_requested:
                    return StageOutcome(attempt, response, "pause")
            state.stage_retry_counts[stage_id] = used + 1
            self._save_state(state)

    def _exhausted_action(
        self,
        definition: StageDefinition | ServiceCallDefinition,
        response: StageOutcomeResult | None,
    ) -> Literal["advance", "pause", "stop"]:
        """Select failure policy separately from attempt execution and retries.

        Args:
            definition: Validated stage/service definition with assigned stable
                identity.
            response: Participant/controller result envelope being processed.

        Returns:
            Advance for skip, otherwise the configured pause/stop action. Missing
            required conditional data always stops after retries are exhausted.
        """
        if (
            isinstance(definition, StageDefinition)
            and definition.returns_data is True
            and response is not None
            and (response.error or {}).get("code") == "conditional_missing_data"
        ):
            return "stop"
        action = definition.errors.on_exhausted
        return "advance" if action == "skip" else action

    def select_input(self, state: RunnerState) -> JsonValue:
        """Read accepted predecessor or conditional-transfer data for the selected stage.

        Args:
            state: Runner cursor, accepted result references, and optional pending move input.

        Returns:
            Accepted application input, or None for no successful predecessor.

        Raises:
            ValueError: A saved result reference is missing or a move transfer is inconsistent.
        """
        if state.pending_input is not None:
            transfer = state.pending_input
            return self._read_move_input(state, transfer)
        if state.stage_position == 1:
            return None
        previous = state.template.stages[state.stage_position - 2].stage_id
        request_id = state.stage_result_ids.get(previous)
        if request_id is None:
            return None
        record = read_result(
            self._journal.client,
            request_id,
            expected={
                "experiment_id": state.stage_result_origins.get(
                    previous, state.experiment_id
                ),
                "stage_id": previous,
            },
            accepted=True,
        )
        if record is None:
            raise ValueError("Predecessor result is missing from the journal.")
        return (
            record["response"]["data"]
            if record["outcome"] == "succeeded"
            and record["response"]["result"] == "success"
            else None
        )

    def _read_move_input(self, state: RunnerState, transfer: PendingInput) -> JsonValue:
        """Require a successful accepted move to the current target and return its payload.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            transfer: Accepted conditional result reference assigned to the current
                target stage.

        Returns:
            Application data from the accepted successful move result that names the
            current target.

        Raises:
            ValueError: The transfer targets another stage or lacks a matching
                accepted successful move result.
        """
        if (
            transfer.stage_id
            != state.template.stages[state.stage_position - 1].stage_id
        ):
            raise ValueError("Pending input belongs to another stage.")
        record = read_result(
            self._journal.client,
            transfer.request_id,
            expected={
                "experiment_id": transfer.experiment_id,
                "stage_id": transfer.source_stage_id,
            },
            accepted=True,
        )
        if (
            record is None
            or record["outcome"] != "succeeded"
            or record["response"]["result"] != "success"
            or record["response"].get("execution", {}).get("dag_decision")
            != {"command": "move", "stage_id": transfer.stage_id}
        ):
            raise ValueError("Move input has no accepted successful journal result.")
        return record["response"]["data"]

    def _context(self, state: RunnerState, attempt: StageAttempt) -> JsonObject:
        """Build journal coordinates for an attempt using its current template module reference.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.

        Returns:
            Journal context combining experiment/run/template, module identity, and
            fixed attempt/participant coordinates.
        """
        definition = next(
            item for item in state.template.stages if item.stage_id == attempt.stage_id
        )
        module = self._launcher._assembler._module_reference(state.template, definition)
        return {
            "experiment_id": state.experiment_id,
            "run_id": state.run_id,
            "stage_id": attempt.stage_id,
            "attempt_id": attempt.attempt_id,
            "stage_execution_id": attempt.stage_execution_id,
            "request_id": attempt.request_id,
            "service_id": attempt.service_id,
            "service_instance_id": None
            if attempt.service_id is None
            else attempt.participant.participant_instance_id,
            "cycle_number": attempt.cycle_number,
            "attempt_number": attempt.attempt_number,
            "module_name": module.name,
            "module_version": module.version,
            "module_hash": module.hash,
            "template_revision_id": state.template_revision_id,
            **(
                {}
                if attempt.participant is None
                else attempt.participant.model_dump(exclude_unset=True)
            ),
        }

    async def _start_attempt(
        self,
        state: RunnerState,
        input_data: JsonValue,
        *,
        execution_id: str | None = None,
    ) -> StageAttempt:
        """Prepare and journal a new attempt before enqueueing service work or spawning an executor.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            input_data: Accepted JSON input for the new stage visit.
            execution_id: Stage visit UUID shared by retries, or None to allocate a
                fresh visit.

        Returns:
            Newly admitted attempt, including attempts whose deadline expired before
            execution could start.
        """
        definition = state.template.stages[state.stage_position - 1]
        module = self._launcher._assembler._module_reference(state.template, definition)
        attempt, instance = self._new_stage_attempt(
            state, definition, module, input_data, execution_id
        )
        manifest = await self._launcher._assembler._check_module_async(
            state, module, "returns_data" in definition.model_fields_set
        )
        launch, context = self._write_attempt_context(
            state, attempt, definition, module, manifest
        )
        await self._bind_attempt_intent(state, attempt, instance, launch, context)
        if state.pending_input is not None or attempt.attempt_number > 1:
            # A self/backwards jump may reuse an attempt directory's parent
            # immediately after the old executor published its result.
            await self.close(state)
        self._prune_artifacts(state, attempt)
        if self._notify_resources is not None:
            self._notify_resources()
        deadline = self._deadline(attempt)
        if deadline is not None and time.monotonic() >= deadline:
            return attempt
        if instance is not None:
            return await self._start_service_call(
                state, attempt, instance, launch, deadline
            )
        return await self._start_executor(
            attempt, attempt.attempt_id, attempt.artifacts_directory, launch, deadline
        )

    def _new_stage_attempt(
        self,
        state: RunnerState,
        definition: StageDefinition | ServiceCallDefinition,
        module: ModuleReference,
        input_data: JsonValue,
        execution_id: str | None,
    ) -> tuple[StageAttempt, ServiceInstance | None]:
        """Allocate fresh attempt identity, artifact path, participant identity, and queue time.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            definition: Validated stage/service definition with assigned stable
                identity.
            module: Validated name/version/hash reference used to locate the node's
                module.
            input_data: Accepted JSON input for the new stage visit.
            execution_id: Stage visit UUID shared by retries, or None to allocate a
                fresh visit.

        Returns:
            Fresh StageAttempt and its existing ServiceInstance for service-call
            nodes, otherwise None.
        """
        stage_id = definition.stage_id
        number = state.stage_attempt_numbers.get(stage_id, 0) + 1
        attempt_id = str(uuid4())
        directory = (
            state.experiment_directory
            / "shared_artifacts"
            / f"epoch_{state.cycle_number}"
            / module.name
            / stage_id
            / f"attempt_{number}"
        )
        attempt = StageAttempt(
            attempt_id,
            stage_id,
            execution_id or str(uuid4()),
            state.cycle_number,
            number,
            directory,
            input_data,
            {},
            definition.timeout_seconds,
        )
        attempt.service_id = (
            definition.service_id
            if isinstance(definition, ServiceCallDefinition)
            else None
        )
        instance = (
            None if attempt.service_id is None else state.services[attempt.service_id]
        )
        attempt.participant = ParticipantIdentity(
            experiment_id=state.experiment_id,
            participant_id=stage_id if instance is None else instance.service_id,
            participant_instance_id=attempt_id
            if instance is None
            else instance.service_instance_id,
        )
        attempt.queued_at = datetime.now(UTC).isoformat()
        attempt.queued_monotonic = time.monotonic()
        return attempt, instance

    def _write_attempt_context(
        self,
        state: RunnerState,
        attempt: StageAttempt,
        definition: StageDefinition | ServiceCallDefinition,
        module: ModuleReference,
        manifest: ModuleManifest | None = None,
    ) -> tuple[PreparedLaunch, JsonObject]:
        """Prepare fixed launch inputs and persist the attempt's context.json.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.
            definition: Validated stage/service definition with assigned stable
                identity.
            module: Validated name/version/hash reference used to locate the node's
                module.
            manifest: Already checked module manifest, or None to use the public
                preparation hook.

        Returns:
            Prepared launch and journal context after persisting fixed runtime
            inputs to context.json.
        """
        context = {
            **self._context(state, attempt),
            "template_revision_id": state.template_revision_id,
            "module_name": module.name,
            "module_version": module.version,
            "module_hash": module.hash,
        }
        if (
            manifest is None
            or getattr(self._launcher.prepare, "__func__", None) is not ModuleLauncher.prepare
        ):
            # Preserve the public synchronous preparation hook for library callers.
            launch = PreparedLaunch.model_validate(
                self._launcher.prepare(
                    state, definition.model_dump(exclude_unset=True), context,
                    attempt.artifacts_directory, attempt.input_data,
                )
            )
        else:
            inputs = ModulePreparation(
                context=ParticipantIdentity.model_validate(context),
                artifacts_directory=attempt.artifacts_directory,
                input_data=attempt.input_data,
            )
            launch = self._launcher._prepare_checked(state, definition, inputs, manifest)
        attempt.endpoint_path = launch.endpoint_path
        attempt.effective_settings = launch.effective_settings
        runtime_context = _update_model(
            launch.runtime_context,
            queued_at=attempt.queued_at,
            queued_monotonic=attempt.queued_monotonic,
            service_id=attempt.service_id,
        )
        launch = _update_model(launch, runtime_context=runtime_context)
        write_json(
            attempt.artifacts_directory / "context.json",
            runtime_context.model_dump(mode="json", exclude_unset=True),
        )
        return launch, context

    async def _bind_attempt_intent(
        self,
        state: RunnerState,
        attempt: StageAttempt,
        instance: ServiceInstance | None,
        launch: PreparedLaunch,
        context: JsonObject,
    ) -> None:
        """Bind the active attempt and persist parameters/start intent before external execution.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            launch: Validated fixed participant launch inputs and runtime paths.
            context: Journal/participant coordinates associated with this operation.
        """
        state.active_attempt = attempt
        state.stage_attempt_numbers[attempt.stage_id] = attempt.attempt_number
        if attempt.request_id in state.used_request_ids:
            raise RuntimeError("Attempt request ID collision.")
        if instance is None:
            state.used_request_ids.add(attempt.request_id)
        self._unstarted_attempt_id = attempt.attempt_id
        self._process_attempt_id = attempt.attempt_id
        self._process = None
        self._call_future = None
        if self._connection is not None:
            await self._connection.close()
        self._connection = None
        self._journal.client.record_attempt_parameters(
            state.template.model_dump(exclude_unset=True),
            attempt.effective_settings,
            template_yaml=state.template_yaml,
            context=context,
        )
        self._journal.client.record_event(
            "control.intent",
            {
                "action": "start_stage",
                "argv": None if instance is not None else launch.argv,
                "service_id": attempt.service_id,
                "queued_monotonic": attempt.queued_monotonic,
                "queued_at": attempt.queued_at,
            },
            context=context,
        )
        self._save_state(state)

    async def _start_service_call(
        self,
        state: RunnerState,
        attempt: StageAttempt,
        instance: ServiceInstance,
        launch: PreparedLaunch,
        deadline: float | None,
    ) -> StageAttempt:
        """Enqueue the fixed call on the unchanged service instance and retain its result future.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            launch: Validated fixed participant launch inputs and runtime paths.
            deadline: Absolute monotonic deadline in seconds, or None when
                unbounded.

        Returns:
            The supplied attempt after attaching a service-result future, or
            unchanged if the service instance was replaced.
        """
        if self._services is None:
            raise RuntimeError("Service attempts require a bound ServiceManager.")
        if state.services[instance.service_id] is not instance:
            return attempt
        self._call_future = self._services.enqueue(
            state,
            instance.service_id,
            attempt.request_id,
            "execute",
            launch.call.model_dump(mode="json", exclude_unset=True),
            deadline=deadline,
        )
        self._unstarted_attempt_id = None
        return attempt

    async def _start_executor(
        self,
        attempt: StageAttempt,
        attempt_id: str,
        directory: Path,
        launch: PreparedLaunch,
        deadline: float | None,
    ) -> StageAttempt:
        """Write launch.json, spawn the executor, and await its endpoint or journal outcome.

        Args:
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.
            attempt_id: UUID of the stage attempt being launched or reconciled.
            directory: Absolute attempt artifact directory receiving launch.json.
            launch: Validated fixed participant launch inputs and runtime paths.
            deadline: Absolute monotonic deadline in seconds, or None when
                unbounded.

        Returns:
            The admitted attempt after spawning and endpoint/result startup
            reconciliation.
        """
        launch_path = directory / "launch.json"
        write_json(launch_path, launch.model_dump(mode="json", exclude_unset=True))
        await self._spawn_executor(attempt, attempt_id, launch_path)
        return await self._wait_executor_startup(attempt, attempt_id, launch, deadline)

    async def _spawn_executor(
        self, attempt: StageAttempt, attempt_id: str, launch_path: Path
    ) -> None:
        """Start a stage executor through the project uv environment and retain process ownership.

        Args:
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.
            attempt_id: UUID of the stage attempt being launched or reconciled.
            launch_path: Absolute JSON path containing executor launch inputs.
        """
        library_root = repository_root()
        self._unstarted_attempt_id = None
        spawn = asyncio.create_task(
            asyncio.to_thread(
                subprocess.Popen,
                [
                    "uv",
                    "run",
                    "--project",
                    str(library_root),
                    "--no-sync",
                    "python",
                    "-B",
                    "-m",
                    "core.participants.executor",
                    "--launch",
                    str(launch_path),
                ],
                cwd=library_root,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        )
        try:
            self._process = await asyncio.shield(spawn)
        except asyncio.CancelledError:
            self._process = await spawn
            self._executor_processes.append((self._process, attempt.request_id))
            raise
        except OSError:
            self._unstarted_attempt_id = attempt_id
            raise
        self._executor_processes.append((self._process, attempt.request_id))

    async def _wait_executor_startup(
        self,
        attempt: StageAttempt,
        attempt_id: str,
        launch: PreparedLaunch,
        deadline: float | None,
    ) -> StageAttempt:
        """Wait for the matching endpoint/result and send execute within the startup deadline.

        Args:
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.
            attempt_id: UUID of the stage attempt being launched or reconciled.
            launch: Validated fixed participant launch inputs and runtime paths.
            deadline: Absolute monotonic deadline in seconds, or None when
                unbounded.

        Returns:
            The supplied attempt once execute was sent or a matching journal result
            is already available.
        """
        startup_deadline = time.monotonic() + launch.control_timeout_seconds
        if deadline is not None:
            startup_deadline = min(startup_deadline, deadline)
        while time.monotonic() < startup_deadline:
            if self._journal.client.read_command_result(attempt.request_id) is not None:
                return attempt
            if attempt.endpoint_path.is_file():
                try:
                    endpoint = read_json(attempt.endpoint_path)
                    if endpoint.get("participant_instance_id") != attempt_id:
                        await asyncio.sleep(0.02)
                        continue
                    await self._connect(
                        attempt, max(0.001, startup_deadline - time.monotonic())
                    )
                    self._call_future = asyncio.create_task(
                        self._connection.request(
                            attempt.request_id,
                            "execute",
                            launch.call.model_dump(mode="json", exclude_unset=True),
                            deadline_monotonic=deadline,
                        )
                    )
                    return attempt
                except (OSError, EOFError):
                    if (
                        self._journal.client.read_command_result(attempt.request_id)
                        is not None
                    ):
                        return attempt
                    if self._process.poll() is not None:
                        raise
            if self._process.poll() is not None:
                raise RuntimeError(
                    "Executor exited before publishing its endpoint or journal result."
                )
            await asyncio.sleep(0.02)
        if deadline is not None and time.monotonic() >= deadline:
            return attempt
        raise TimeoutError("Executor startup deadline expired.")

    def _deadline(self, attempt: StageAttempt) -> float | None:
        """Return the attempt's monotonic deadline from queue time, or None when unbounded."""
        return (
            None
            if attempt.timeout_seconds is None
            else attempt.queued_monotonic + attempt.timeout_seconds
        )

    async def _connect(self, attempt: StageAttempt, timeout: float) -> None:
        """Replace the participant connection and verify identity within timeout seconds.

        Args:
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.
            timeout: Timeout in seconds for this operation.
        """
        if self._connection is not None:
            await self._connection.close()
        self._connection = ParticipantConnection(
            attempt.endpoint_path, attempt.participant
        )
        await self._connection.connect(timeout_seconds=timeout)

    def _accept(
        self,
        state: RunnerState,
        attempt: StageAttempt,
        response: JsonObject,
        outcome: str,
    ) -> StageOutcomeResult:
        """Journal the runner's normalized outcome and persist attempt/result observations.

        Args:
            state: Runner state receiving the accepted request and checkpoint.
            attempt: Attempt whose result is being accepted.
            response: Participant or runner-generated result document.
            outcome: Journal outcome before conditional-result normalization.

        Returns:
            Accepted typed response; invalid conditional output becomes failure.
        """
        result, outcome = self._normalize_accepted_response(
            state, attempt, StageOutcomeResult.model_validate(response), outcome
        )
        state.used_request_ids.add(attempt.request_id)
        self._journal.client.record_command_result(
            attempt.request_id,
            result.model_dump(exclude_unset=True),
            author="runner",
            outcome=outcome,
            context=self._context(state, attempt),
        )
        attempt.result_request_id = attempt.request_id
        attempt.outcome = outcome
        execution = result.execution if "execution" in result.model_fields_set else {}
        if execution:
            attempt.executor_status = RetainedExecutorStatus.model_validate(execution)
            attempt.process_identity = _retain_process_identity(
                attempt.executor_status.process
            )
            attempt.started_at = attempt.executor_status.started_at
        attempt.executor_status = _finish_executor_status(attempt.executor_status)
        self._save_state(state)
        if self._notify_resources is not None:
            self._notify_resources()
        return result

    def _normalize_accepted_response(
        self,
        state: RunnerState,
        attempt: StageAttempt,
        response: StageOutcomeResult,
        outcome: str,
    ) -> tuple[StageOutcomeResult, str]:
        """Validate successful conditional output and separate application data from DAG commands.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.
            response: Participant/controller result envelope being processed.
            outcome: Accepted stage/command outcome that determines the next policy
                action.

        Returns:
            Normalized application response and final outcome. Invalid conditional
            decisions become failures; ignored disabled payloads are journaled.
        """
        definition = state.template.stages[state.stage_position - 1]
        if (
            isinstance(definition, StageDefinition)
            and "returns_data" in definition.model_fields_set
            and response.result == "success"
        ):
            decision = response.data
            try:
                response = _normalize_conditional_result(
                    response, attempt.input_data, definition, state.template
                )
            except (ValueError, TypeError) as error:
                code = (
                    "conditional_missing_data"
                    if isinstance(error, MissingConditionalDataError)
                    else "invalid_conditional_result"
                )
                response = _update_model(
                    response,
                    result="fail",
                    data={
                        "reason": code,
                        "message": str(error),
                    },
                    error={"code": code, "message": str(error)},
                )
                outcome = "failed"
            else:
                if (
                    not definition.returns_data
                    and isinstance(decision, dict)
                    and "data" in decision
                ):
                    self._journal.client.record_error(
                        ValueError(
                            "Conditional stage returned data with returns_data=false; "
                            "the payload was ignored."
                        ),
                        error_code="conditional_unexpected_data",
                        context=self._context(state, attempt),
                    )
        return response, outcome

    async def _collect_result(
        self, state: RunnerState, attempt: StageAttempt
    ) -> StageOutcomeResult:
        """Reconcile journal evidence and live observations until an outcome is accepted.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.

        Returns:
            Authoritative accepted result, reusing runner evidence when available
            and preserving timeout precedence over late participant replies.
        """
        deadline = self._deadline(attempt)
        try:
            while True:
                record = read_result(
                    self._journal.client,
                    attempt.request_id,
                    expected=_attempt_result_identity(attempt),
                )
                if record is not None and record["author"] == "runner":
                    attempt.result_request_id = attempt.request_id
                    attempt.outcome = record["outcome"]
                    execution = record["response"].get("execution", {})
                    attempt.executor_status = _finish_executor_status(execution)
                    if execution:
                        attempt.process_identity = _retain_process_identity(
                            execution.get("process")
                        )
                        attempt.started_at = execution.get("started_at")
                    finished = any(
                        item["author"] == "participant"
                        for item in record["observations"]
                    )
                    if (
                        attempt.service_id is None
                        and not finished
                        and not await self.interrupt(state, "recovery")
                    ):
                        attempt.outcome = "unknown"
                    return StageOutcomeResult.model_validate(record["response"])
                if deadline is not None and time.monotonic() >= deadline:
                    response = self._accept(
                        state,
                        attempt,
                        {"result": "fail", "data": {"reason": "timeout"}},
                        "timed_out",
                    )
                    if attempt.service_id is not None:
                        await self._services.cancel_request(
                            state, attempt.service_id, attempt.request_id
                        )
                    elif record is None and not await self.interrupt(state, "timeout"):
                        attempt.outcome = "unknown"
                    return response
                if record is not None:
                    return self._accept(
                        state,
                        attempt,
                        record["response"],
                        "succeeded"
                        if record["response"]["result"] == "success"
                        else "failed",
                    )
                if attempt.service_id is not None:
                    instance = state.services.get(attempt.service_id)
                    if (
                        instance is None
                        or instance.stopped
                        or instance.service_instance_id
                        != attempt.participant.participant_instance_id
                    ):
                        return self._accept(
                            state,
                            attempt,
                            {
                                "result": "fail",
                                "data": {"reason": "service_instance_changed"},
                            },
                            "invalidated",
                        )
                    attempt.process_identity = instance.process_identity
                    attempt.executor_status = ServiceCallExecutorStatus(
                        participant=attempt.participant,
                        request_id=attempt.request_id,
                        process=instance.process_identity,
                        finished=False,
                        current=instance.active_request,
                    )
                    if (
                        self._call_future is not None
                        and self._call_future.done()
                        and not self._call_future.cancelled()
                    ):
                        response = self._call_future.result()
                        if response["result"] == "fail":
                            return self._accept(
                                state,
                                attempt,
                                {"result": "fail", "data": response["data"]},
                                "failed",
                            )
                else:
                    if await self._observe_executor(state, attempt, deadline):
                        continue
                await asyncio.sleep(0.05)
        finally:
            if self._call_future is not None:
                if not self._call_future.done():
                    self._call_future.cancel()
                await asyncio.gather(self._call_future, return_exceptions=True)
                self._call_future = None
            if self._connection is not None:
                await self._connection.close()
                self._connection = None

    async def _observe_executor(
        self, state: RunnerState, attempt: StageAttempt, deadline: float | None
    ) -> bool:
        """Return True when the result loop must recheck its journal and deadline.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.
            deadline: Absolute monotonic deadline in seconds, or None when
                unbounded.

        Returns:
            True when newly discovered evidence requires immediately rechecking the
            journal; False when polling should continue normally.
        """
        if self._call_future is not None and self._call_future.done():
            if not self._call_future.cancelled():
                self._call_future.exception()
            self._call_future = None
        timeout = state.template.unknown_state.timeout_seconds
        if deadline is not None:
            timeout = min(timeout, max(0.001, deadline - time.monotonic()))
        try:
            if self._connection is None:
                await self._connect(attempt, timeout)
            request_id = str(uuid4())
            state.used_request_ids.add(request_id)
            reply = ExecutorCommandStateResponse.model_validate(
                await self._connection.query_command_state(
                    request_id, timeout_seconds=timeout
                )
            )
            first_observation = (
                attempt.process_identity is None and reply.data.process is not None
            )
            attempt.executor_status = reply.data
            attempt.process_identity = reply.data.process
            attempt.started_at = reply.data.started_at
            self._save_state(state, checkpoint=first_observation)
            if first_observation and self._notify_resources is not None:
                self._notify_resources()
        except (OSError, EOFError):
            if self._journal.client.read_command_result(attempt.request_id) is not None:
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return True
            try:
                await self._connect(attempt, timeout)
            except (OSError, EOFError):
                # Completion can commit and remove the endpoint while
                # reconnecting. Reenter normal result validation and
                # deadline handling before declaring the attempt unknown.
                if (
                    self._journal.client.read_command_result(attempt.request_id)
                    is not None
                ):
                    return True
                if deadline is not None and time.monotonic() >= deadline:
                    return True
                raise
        return False

    async def interrupt(self, state: RunnerState, reason: str) -> bool:
        """Cancel the active stage/service call and seek confirmation that its work stopped.

        Args:
            state: Runner state containing the active attempt and ownership evidence.
            reason: Reason journaled and sent to the participant.

        Returns:
            True when work is unstarted or termination is confirmed; False when the
            available observations cannot establish termination within the deadline.
        """
        attempt = state.active_attempt
        if attempt is None or self._unstarted_attempt_id == attempt.attempt_id:
            return True
        try:
            existing = self._journal.client.read_command_result(attempt.request_id)
            if existing is None or existing["author"] != "runner":
                self._accept(
                    state,
                    attempt,
                    {"result": "fail", "data": {"reason": reason}},
                    "cancelled",
                )
        except LoggingError:
            # Mandatory journal failure does not disable emergency interruption.
            pass
        if attempt.service_id is not None:
            instance = state.services.get(attempt.service_id)
            if instance is None:
                return False
            if (
                instance.stopped
                or instance.service_instance_id
                != attempt.participant.participant_instance_id
            ):
                return True
            return await self._services.cancel_request(
                state, attempt.service_id, attempt.request_id, interrupt=True
            )
        process_path = attempt.artifacts_directory / "process.json"
        if process_path.is_file():
            saved = read_json(process_path)
            if saved.get("attempt_id") != attempt.attempt_id:
                raise ValueError("Process record belongs to another attempt.")
            attempt.process_identity = _retain_process_identity(saved.get("stage"))
            if saved.get("executor") is not None:
                self._executor_identities[attempt.request_id] = (
                    _retain_process_identity(saved["executor"])
                )
        deadline = time.monotonic() + state.template.unknown_state.timeout_seconds
        while time.monotonic() < deadline:
            try:
                await self._connect(attempt, max(0.001, deadline - time.monotonic()))
                request_id = str(uuid4())
                state.used_request_ids.add(request_id)
                intent = {
                    "action": "interrupt_stage",
                    "request_id": request_id,
                    "target_request_id": attempt.request_id,
                    "reason": reason,
                }
                try:
                    self._journal.client.record_event(
                        "control.intent",
                        intent,
                        context={
                            **self._context(state, attempt),
                            "request_id": request_id,
                        },
                    )
                except LoggingError as error:
                    write_json(
                        attempt.artifacts_directory / "stop.emergency.json",
                        {**intent, "error": str(error)},
                    )
                reply = await self._connection.request(
                    request_id,
                    "interrupt",
                    {"request_id": attempt.request_id, "reason": reason},
                    timeout_seconds=state.template.start_timeout
                    + state.template.runner_timeout_margin_seconds,
                )
                if reply["result"] == "success" and reply["data"].get("stopped"):
                    return True
            except (OSError, EOFError):
                pass
            finally:
                if self._connection is not None:
                    await self._connection.close()
                    self._connection = None
            if attempt.process_identity is not None:
                try:
                    if process_identity(
                        _process_identity_pid(attempt.process_identity)
                    ) != _process_identity_document(attempt.process_identity):
                        return True
                    psutil.Process(
                        _process_identity_pid(attempt.process_identity)
                    ).wait(timeout=0)
                    return True
                except psutil.TimeoutExpired:
                    pass
                except (FileNotFoundError, ProcessLookupError, psutil.NoSuchProcess):
                    return True
                except OSError as error:
                    if getattr(error, "winerror", None) in (87, 1168):
                        return True
                    raise
            await asyncio.sleep(0.05)
        return False

    async def recover(
        self,
        state: RunnerState,
        *,
        wait_services: Callable[
            [RunnerState], Awaitable[Literal["ready", "pause", "stop"]]
        ]
        | None = None,
    ) -> StageOutcome | None:
        """Reconcile the saved active attempt with its original inputs and participant evidence.

        Args:
            state: Restored runner state with an optional active attempt.
            wait_services: Optional readiness callback used before a recovered retry.

        Returns:
            Recovered outcome/action, or None when there is no active attempt.

        Raises:
            ValueError: Saved attempt coordinates or inputs differ from their fixed context.
        """
        attempt = state.active_attempt
        if attempt is None:
            return None
        definition = state.template.stages[state.stage_position - 1]
        if (
            definition.stage_id != attempt.stage_id
            or attempt.cycle_number != state.cycle_number
        ):
            raise ValueError("Saved attempt does not match the DAG cursor.")
        saved = AttemptContextObservation.model_validate(
            read_json(attempt.artifacts_directory / "context.json")
        )
        if (
            saved.context.attempt_id != attempt.attempt_id
            or json.dumps(saved.input_data, sort_keys=True)
            != json.dumps(attempt.input_data, sort_keys=True)
            or json.dumps(saved.settings, sort_keys=True)
            != json.dumps(attempt.effective_settings, sort_keys=True)
        ):
            raise ValueError("Attempt differs from its original context.")
        self._process = None
        self._process_attempt_id = attempt.attempt_id
        self._unstarted_attempt_id = None
        try:
            record = read_result(
                self._journal.client, attempt.request_id, expected=attempt.participant
            )
            if record is None and attempt.service_id is None:
                try:
                    await self._connect(
                        attempt, state.template.unknown_state.timeout_seconds
                    )
                except (OSError, EOFError):
                    # The executor may finish between the journal read and connect.
                    # A matching committed result no longer needs a live endpoint.
                    if (
                        read_result(
                            self._journal.client,
                            attempt.request_id,
                            expected=attempt.participant,
                        )
                        is None
                    ):
                        raise
            self._journal.client.record_event(
                "stage.reconnected", {}, context=self._context(state, attempt)
            )
            return await self.execute(
                state, wait_services=wait_services, recovered=attempt
            )
        except (OSError, EOFError, ValueError, RuntimeError) as error:
            return await self._recover_unknown_attempt(
                state, attempt, error, wait_services
            )

    async def _recover_unknown_attempt(
        self,
        state: RunnerState,
        attempt: StageAttempt,
        error: Exception,
        wait_services: Callable[
            [RunnerState], Awaitable[Literal["ready", "pause", "stop"]]
        ]
        | None,
    ) -> StageOutcome:
        """Apply unknown-state policy, requiring confirmed interruption before skip or rerun.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.
            error: Primary failure retained while cleanup or error translation
                proceeds.
            wait_services: Optional readiness callback consulted before starting an
                automatic/recovered retry.

        Returns:
            Recovered/rerun attempt outcome and the selected advance, pause, or stop
            action; skip/rerun require confirmed termination.
        """
        state.unknown_state_recovery_count += 1
        policy = state.template.unknown_state
        action = (
            policy.on_recovery_limit
            if state.unknown_state_recovery_count >= policy.recovery_limit
            else policy.on_timeout
        )
        self._journal.client.record_event(
            "stage.recovery_failed",
            {
                "action": action,
                "error": str(error),
                "count": state.unknown_state_recovery_count,
            },
            context=self._context(state, attempt),
        )
        if action == "pause":
            attempt.outcome = "unknown"
            self._save_state(state)
            return StageOutcome(attempt, None, "pause")
        if not await self.interrupt(state, "unknown_state"):
            attempt.outcome = "unknown"
            self._save_state(state)
            return StageOutcome(attempt, None, "stop")
        attempt.outcome = "unknown_stopped"
        if action == "rerun":
            return await self.execute(
                state, wait_services=wait_services, recovered=attempt
            )
        return StageOutcome(
            attempt,
            {"result": "fail", "data": {"reason": "unknown_state"}},
            "advance" if action == "skip" else "stop",
        )

    def _prune_artifacts(self, state: RunnerState, attempt: StageAttempt) -> None:
        """Delete expired attempt directories while preserving conditional result artifacts.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            attempt: Stage attempt carrying fixed inputs, identity, timing, and
                execution observations.

        Raises:
            ValueError: An expired attempt directory resolves outside the
                experiment.
            OSError: An eligible old attempt directory cannot be removed.
        """
        import shutil

        parent = attempt.artifacts_directory.parent
        keep = state.template.keep_attempts
        for path in parent.iterdir():
            if (
                path == attempt.artifacts_directory
                or not path.is_dir()
                or not path.name.startswith("attempt_")
            ):
                continue
            suffix = path.name.removeprefix("attempt_")
            if suffix.isdigit() and int(suffix) <= attempt.attempt_number - keep:
                resolved = path.resolve()
                if any(
                    (state.experiment_directory / retained)
                    .resolve()
                    .is_relative_to(resolved)
                    or resolved.is_relative_to(
                        (state.experiment_directory / retained).resolve()
                    )
                    for retained in state.retained_artifacts
                ):
                    continue
                if not path.resolve().is_relative_to(
                    state.experiment_directory.resolve()
                ):
                    raise ValueError("Attempt directory escapes the experiment.")
                shutil.rmtree(path)

    def termination_confirmed(self, state: RunnerState) -> bool:
        """Return whether current evidence confirms the active attempt is unstarted or stopped.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.

        Returns:
            Whether current evidence confirms the active attempt is unstarted or
            stopped.
        """
        attempt = state.active_attempt
        if attempt is None or self._unstarted_attempt_id == attempt.attempt_id:
            return True
        if attempt.service_id is not None:
            instance = state.services.get(attempt.service_id)
            return instance is not None and (
                instance.stopped
                or instance.service_instance_id
                != attempt.participant.participant_instance_id
            )
        identity = attempt.process_identity
        if identity is None:
            return False
        try:
            if process_identity(
                _process_identity_pid(identity)
            ) != _process_identity_document(identity):
                return True
            psutil.Process(_process_identity_pid(identity)).wait(timeout=0)
            return True
        except psutil.TimeoutExpired:
            return False
        except (FileNotFoundError, ProcessLookupError, psutil.NoSuchProcess):
            return True
        except OSError as error:
            if getattr(error, "winerror", None) in (87, 1168):
                return True
            raise

    def _save_state(self, state: RunnerState, *, checkpoint: bool = True) -> None:
        # Retry/input ownership must survive loss of the optional state.json
        # copy, including a crash between acceptance and the DAG transition.
        """Journal an optional authoritative checkpoint and publish state.json with I/O diagnostics.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            checkpoint: Whether to journal a new authoritative runner checkpoint
                before the optional state file.
        """
        if checkpoint:
            state.checkpoint_id = str(uuid4())
            _record_runner_checkpoint(self._journal.client, state)
        try:
            self._state_store.save(state)
        except OSError as error:
            self._journal.client.record_error(
                error, context={"experiment_id": state.experiment_id}
            )

    async def close(self, state: RunnerState | None = None) -> None:
        """Close attempt communication and reap completed executor processes.

        Args:
            state: Optional runner state used to distinguish active work and confirm
                accepted executors exited before snapshot or artifact operations.

        Active executor processes remain tracked; interruption is a separate operation.
        """
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        if self._call_future is not None:
            self._call_future.cancel()
            await asyncio.gather(self._call_future, return_exceptions=True)
            self._call_future = None
        remaining = []
        for process, request_id in self._executor_processes:
            active = (
                state is None
                or state.active_attempt is not None
                and state.active_attempt.request_id == request_id
            )
            if active and process.poll() is None:
                remaining.append((process, request_id))
            else:
                await asyncio.to_thread(
                    process.wait,
                    timeout=30 if state is None else state.template.start_timeout,
                )
        self._executor_processes = remaining
        if state is not None:
            await self._wait_accepted_executors(state)

    async def _wait_accepted_executors(self, state: RunnerState) -> None:
        """Recover accepted-result executor identities and wait for remaining writers to exit.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
        """
        request_ids = set(state.stage_result_ids.values())
        for transfer in (state.pending_input, state.last_dag_decision):
            if transfer is not None:
                request_ids.add(transfer.request_id)
        for request_id in request_ids:
            record = self._journal.client.read_command_result(request_id)
            if record is None:
                raise ValueError("An accepted result is missing from the journal.")
            context = record["event"]["context"]
            if context.get("participant_id") != context.get("stage_id"):
                continue
            # The result context identifies its writer without a per-call result file.
            module_name = context.get("module_name")
            if module_name is None:
                continue
            directory = (
                state.experiment_directory
                / "shared_artifacts"
                / f"epoch_{context['cycle_number']}"
                / module_name
                / context["stage_id"]
                / f"attempt_{context['attempt_number']}"
            )
            process_path = directory / "process.json"
            if process_path.is_file():
                process_record = read_json(process_path)
                if process_record.get("experiment_id") == state.experiment_id:
                    self._executor_identities[request_id] = _retain_process_identity(
                        process_record["executor"]
                    )
        for request_id, identity in list(self._executor_identities.items()):
            try:
                if process_identity(
                    _process_identity_pid(identity)
                ) == _process_identity_document(identity):
                    await asyncio.to_thread(
                        psutil.Process(_process_identity_pid(identity)).wait,
                        state.template.start_timeout,
                    )
            except (FileNotFoundError, ProcessLookupError, psutil.NoSuchProcess):
                pass
            except OSError as error:
                if getattr(error, "winerror", None) not in (87, 1168):
                    raise
            self._executor_identities.pop(request_id, None)
