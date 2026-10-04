"""High-level orchestration of sequential stages and experiment services."""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

import psutil
import yaml

from core.experiments.archiver import ExperimentArchiver
from core.experiments.assembler import ExperimentAssembler, find_experiment
from core.experiments.journal import (
    RunnerJournal,
    _read_recovery_checkpoint,
    _read_recovery_evidence,
    _read_reload_progress,
)
from core.experiments.launch import ModuleLauncher
from core.experiments.reload import (
    ReloadApplication,
    _definition_change,
    _definition_fingerprint,
    _prepare_reload_candidate,
    _reload_cursor,
    _reload_layout,
    _service_state,
    _template_fingerprint,
)
from core.experiments.service_inputs import _service_definition
from core.experiments.services import ServiceManager
from core.experiments.snapshots import ExperimentSnapshots
from core.experiments.stages import StageRunner, _record_runner_checkpoint
from core.experiments.state import (
    ModuleRole,
    RunnerState,
    RunnerStateStore,
    ServiceInstance,
    StageOutcome,
    _apply_recovered_attempt_context,
    _attempt_from_launch,
    _executor_status_document,
    state_from_document,
)
from core.journal.events import LoggingError, encode_event
from core.journal.logger import Operation, OperationLogger
from core.models.experiment_registry import RegistryEntry
from core.models.experiment_template import (
    ExperimentTemplate,
    ServiceDefinition,
)
from core.models.participant_observations import (
    ExecutorCommandState,
    RetainedExecutorStatus,
)
from core.models.process_identity import ProcessIdentity
from core.models.runner_state import (
    DagDecision,
    LastDecision,
    PendingInput,
    PendingRebuild,
    ServiceCallExecutorStatus,
)
from core.modules.manager import ModuleManager
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import (
    JsonObject,
    JsonValue,
    copy_json_object,
    require_text,
)
from core.primitives.processes import process_identity


class ExperimentRunner:
    def __init__(
        self,
        project_root: Path,
        module_manager: ModuleManager,
        *,
        notify: Callable[[JsonObject], None] | None = None,
        archive_config_path: Path | None = None,
        control_logger: OperationLogger | None = None,
    ) -> None:
        self._project_root = Path(project_root)
        if not self._project_root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._assembler = ExperimentAssembler(self._project_root, module_manager)
        self._archiver = ExperimentArchiver(
            self._project_root, module_manager, config_path=archive_config_path
        )
        self._hash_module = module_manager.module_hash
        self._journal = RunnerJournal()
        self._control_logger = control_logger
        self._state_store = RunnerStateStore()
        self._resource_observer: Callable[[JsonObject], None] | None = None
        self._resource_error: str | None = None
        self._suspend_resources: Callable[[], Awaitable[None]] | None = None
        self._resume_resources: Callable[[], None] | None = None
        self._launcher = ModuleLauncher(self._assembler, self._journal)
        self._services = ServiceManager(
            self._launcher,
            self._journal,
            self._state_store,
            notify_resources=self._publish_resources,
        )
        self._stages = StageRunner(
            self._launcher,
            self._journal,
            self._state_store,
            notify_resources=self._publish_resources,
        )
        self._snapshots = ExperimentSnapshots(
            self._project_root,
            self._stages,
            self._services,
            self._journal,
            self._state_store,
            assembler=self._assembler,
            hash_module=self._hash_module,
            notify_resources=self._publish_resources,
        )
        self._maintenance = False
        self._maintenance_task = None
        self._continue_source: Path | None = None
        self._recover_live = False
        self._state: RunnerState | None = None
        self._task = None
        self._stage_task = None
        self._service_task = None
        self._stages.bind_services(self._services)
        self._service_retrying = False
        self._service_control_task: asyncio.Task | None = None
        self._step_future = None
        self._last_attempt = None
        self._last_response = None
        self._last_snapshot = None
        self._error = None
        self._notify = notify
        self._closed = False
        self._stop_requested = False
        self._termination_confirmed = True
        self._wake = asyncio.Event()
        self._ready = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._pending_advance = False
        self._manual = False
        self._desired_mode = "running"
        self._requested_id = None
        self._reload_operation = None
        self._command_context: JsonObject = {}

    async def run(
        self,
        template_path: Path | None = None,
        experiment_id: str | None = None,
        *,
        continue_run: bool = False,
        delayed_start: bool = False,
    ) -> JsonObject:
        if self._closed:
            raise RuntimeError("Runner is closed.")
        if self._state is not None and self._state.pending_rebuild is not None:
            raise RuntimeError(
                "Recover the unfinished template reload before starting a run."
            )
        if self._maintenance:
            raise RuntimeError(
                "Wait for the current snapshot or restoration operation."
            )
        if not self._termination_confirmed:
            raise RuntimeError(
                "Previous participant termination is unconfirmed; no new experiment may start."
            )
        if type(delayed_start) is not bool or type(continue_run) is not bool:
            raise TypeError("Run flags must be booleans.")
        if continue_run and (experiment_id is None or template_path is not None):
            raise ValueError(
                "continue requires a source experiment_id and no replacement template."
            )
        if self._task is not None and not self._task.done():
            return await self._signal_existing_run(continue_run, experiment_id)
        source = (
            find_experiment(self._project_root, experiment_id) if continue_run else None
        )
        experiment_id = (
            f"{uuid4()}:{experiment_id}"
            if continue_run
            else (str(uuid4()) if experiment_id is None else experiment_id)
        )
        try:
            find_experiment(self._project_root, experiment_id)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError(f"Experiment ID already exists: {experiment_id}")
        if not continue_run and (
            template_path is None or not Path(template_path).is_absolute()
        ):
            raise ValueError("A new experiment requires an absolute template_path.")
        await self._reset_run_components()
        self._bind_new_run(experiment_id, template_path, source, delayed_start or continue_run)
        return {"experiment_id": experiment_id, "signaled": True}

    async def _reset_run_components(self) -> None:
        await self._services.close()
        self._services = ServiceManager(
            self._launcher,
            self._journal,
            self._state_store,
            notify_resources=self._publish_resources,
        )
        self._service_task = None
        self._stages.bind_services(self._services)
        self._snapshots = ExperimentSnapshots(
            self._project_root,
            self._stages,
            self._services,
            self._journal,
            self._state_store,
            assembler=self._assembler,
            hash_module=self._hash_module,
            notify_resources=self._publish_resources,
        )
        self._journal.close()

    def _bind_new_run(
        self, experiment_id: str, template_path: Path | None, source: Path | None, paused: bool
    ) -> None:
        self._requested_id = experiment_id
        self._template_path = None if template_path is None else Path(template_path)
        self._continue_source = source
        self._recover_live = False
        self._desired_mode = "paused" if paused else "running"
        self._state = None
        self._publish_resources()
        self._error = None
        self._last_attempt = None
        self._last_response = None
        self._last_snapshot = None
        self._stop_requested = False
        self._pending_advance = False
        self._manual = False
        self._ready.clear()
        self._idle.clear()
        self._wake.set()
        self._task = asyncio.create_task(
            self._advance_dag(), name=f"dag:{experiment_id}"
        )

    async def _signal_existing_run(
        self, continue_run: bool, experiment_id: str | None
    ) -> JsonObject:
        if continue_run:
            raise RuntimeError(
                "Stop the selected experiment before continuing another instance."
            )
        if experiment_id != self._requested_id:
            raise RuntimeError("Stop the active experiment before creating another.")
        if self._state is not None and self._state.mode == "paused":
            await self.resume()
        return {"experiment_id": self._requested_id, "signaled": True}

    async def _advance_dag(self) -> None:
        wake_task = None
        readiness_task = None
        try:
            state = await self._prepare_dag_state()
            if state is None:
                return
            wake_task = asyncio.create_task(self._wake.wait())
            if state.template.services:
                self._service_task = asyncio.create_task(self._services.monitor(state))

            while not self._stop_requested:
                waiting = [
                    wake_task,
                    self._service_task,
                    self._stage_task,
                    readiness_task,
                ]
                await asyncio.wait(
                    [task for task in waiting if task is not None],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                # Apply service decisions even while a stage runs or the DAG is paused.
                if self._service_task is not None and self._service_task.done():
                    self._apply_supervision_result(state)

                if self._stage_task is not None and self._stage_task.done():
                    stage_action = await self._complete_stage_task(state)
                    if stage_action == "unknown":
                        continue
                    if stage_action == "stopped":
                        return

                if wake_task.done():
                    self._wake.clear()
                    wake_task = asyncio.create_task(self._wake.wait())
                    final = self._at_dag_end()
                    if (
                        self._stage_task is None
                        and readiness_task is None
                        and (
                            state.mode == "running"
                            or self._step_future is not None
                            or final
                        )
                    ):
                        readiness_task = asyncio.create_task(
                            self._services.wait_ready(state)
                        )

                if readiness_task is None or not readiness_task.done():
                    continue
                action = readiness_task.result()
                readiness_task = None
                if not self._apply_readiness_result(state, action):
                    continue
                # Manual recovery must finish before a new stage or final shutdown.
                if self._service_retrying or self._maintenance:
                    continue
                final = self._at_dag_end()
                if final:
                    await self._complete_dag(state)
                    break
                if state.mode == "paused" and self._step_future is None:
                    continue
                self._launch_next_stage(state)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - Background failures become explicit experiment state.
            await self._fail(error, {})
        finally:
            tasks = [wake_task, readiness_task, self._stage_task, self._service_task]
            tasks = [task for task in tasks if task is not None]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._stage_task = self._service_task = None
            self._publish_resources()
            self._ready.set()
            self._idle.set()

    async def _prepare_dag_state(self) -> RunnerState | None:
        if (
            self._state is not None
            and self._state.last_dag_decision is not None
            and self._state.last_dag_decision.decision.command == "stop"
        ):
            await self._stop_from_stage(recovering=True)
            return
        if self._state is None and self._continue_source is not None:
            state, action = await self._prepare_continuation()
        elif self._state is None:
            state, action = await self._prepare_initial_run()
        else:
            state, action = await self._prepare_live_run()
        if action == "stop":
            raise RuntimeError("Service startup or recovery requires experiment stop.")
        if action == "pause":
            self._desired_mode = "paused"
        state.mode = self._desired_mode
        if (
            state.mode == "running"
            and state.last_dag_decision is not None
            and state.last_dag_decision.decision.command == "pause"
        ):
            state.last_dag_decision = None
        state.phase = (
            "stage_running"
            if state.active_attempt is not None
            else ("waiting" if state.mode == "paused" else "starting")
        )
        self._save_state()
        self._ready.set()
        if state.active_attempt is not None:
            self._idle.clear()
            self._stage_task = asyncio.create_task(
                self._stages.recover(state, wait_services=self._services.wait_ready)
            )
        else:
            self._idle.set()
        return state

    def _apply_supervision_result(self, state: RunnerState) -> None:
        action = self._service_task.result()
        if action == "stop":
            raise RuntimeError(
                "Service supervision requires experiment stop."
            )
        state.mode = self._desired_mode = "paused"
        state.pause_requested = True
        if self._stage_task is None:
            state.phase = "waiting"
        self._save_state()
        if self._step_future is not None:
            future, self._step_future = self._step_future, None
            if not future.done():
                future.set_exception(
                    RuntimeError("A service requires a pause.")
                )
        self._service_task = asyncio.create_task(
            self._services.monitor(state)
        )

    async def _complete_stage_task(
        self, state: RunnerState
    ) -> Literal["unknown", "stopped", "ready"]:
        outcome = self._stage_task.result()
        self._stage_task = None
        self._last_attempt = outcome.attempt
        self._last_response = outcome.result
        if outcome.attempt.outcome == "unknown":
            self._pause_unknown_stage(state, outcome)
            return "unknown"
        if outcome.action == "stop":
            state.last_result = None
            state.last_result_id = None
            state.stage_result_ids.pop(outcome.attempt.stage_id, None)
            state.stage_result_origins.pop(outcome.attempt.stage_id, None)
            raise RuntimeError("Stage execution requires experiment stop.")
        response = outcome.result
        self._apply_stage_outcome(outcome)
        final = self._at_dag_end()
        state.phase = "waiting"
        self._save_state()
        if (
            state.last_dag_decision is not None
            and state.last_dag_decision.decision.command == "stop"
        ):
            await self._stop_from_stage()
            return "stopped"
        await self._snapshot_stage_boundary(state, outcome, final)
        self._idle.set()
        self._finish_stage_step(state, outcome, response, final)
        if state.mode == "running" or final:
            self._wake.set()
        return "ready"

    def _pause_unknown_stage(self, state: RunnerState, outcome: StageOutcome) -> None:
        state.mode = self._desired_mode = "paused"
        state.pause_requested = True
        state.phase = "waiting"
        self._idle.clear()
        self._pending_advance = False
        self._save_state()
        if outcome.action == "stop":
            raise RuntimeError(
                "Previous stage ownership or termination is unconfirmed."
            )
        if self._step_future is not None:
            future, self._step_future = self._step_future, None
            if not future.done():
                future.set_exception(
                    RuntimeError(
                        "Stage state is unknown; recover or stop it first."
                    )
                )

    async def _snapshot_stage_boundary(
        self, state: RunnerState, outcome: StageOutcome, final: bool
    ) -> None:
        mode = state.template.snapshots.mode
        if (
            outcome.action == "advance"
            and not final
            and (
                mode == "after_stage"
                or mode == "after_epoch"
                and self._pending_advance
                and state.stage_position == len(state.template.stages)
            )
        ):
            self._maintenance = True
            state.phase = "snapshotting"
            try:
                await self._snapshots.create(state)
            finally:
                self._maintenance = False
                if state.phase == "snapshotting":
                    state.phase = "waiting"
            self._save_state()

    def _finish_stage_step(
        self, state: RunnerState, outcome: StageOutcome,
        response: JsonObject | None, final: bool,
    ) -> None:
        # The final step also waits for confirmed service shutdown.
        if self._step_future is not None and not final:
            future, self._step_future = self._step_future, None
            if not future.done():
                if outcome.action == "advance":
                    future.set_result(
                        {
                            "attempt_id": outcome.attempt.attempt_id,
                            "result": response,
                            "phase": state.phase,
                        }
                    )
                else:
                    future.set_exception(
                        RuntimeError(
                            "Stage did not complete or skip under its policy."
                        )
                    )

    def _apply_readiness_result(self, state: RunnerState, action: str) -> bool:
        if action == "stop":
            raise RuntimeError("Service readiness requires experiment stop.")
        if action == "pause":
            state.mode = self._desired_mode = "paused"
            state.pause_requested = True
            state.phase = "waiting"
            self._save_state()
            if self._step_future is not None:
                future, self._step_future = self._step_future, None
                if not future.done():
                    future.set_exception(
                        RuntimeError("Service readiness requires a pause.")
                    )
            return False
        return True

    async def _complete_dag(self, state: RunnerState) -> None:
        state.pending_advance = self._pending_advance
        self._maintenance = True
        try:
            self._last_snapshot = await self._snapshots.finalize(
                state, terminal_phase="completed"
            )
            if not self._last_snapshot["valid"]:
                raise RuntimeError(
                    f"Final snapshot is invalid: {self._last_snapshot['error']}"
                )
        finally:
            self._maintenance = False
        self._termination_confirmed = all(
            item.stopped for item in state.services.values()
        )
        await self._services.close()
        state.phase = "completed"
        self._save_state()
        if self._step_future is not None:
            future, self._step_future = self._step_future, None
            if not future.done():
                future.set_result(
                    {
                        "attempt_id": None
                        if self._last_attempt is None
                        else self._last_attempt.attempt_id,
                        "result": self._last_response,
                        "phase": state.phase,
                    }
                )

    def _launch_next_stage(self, state: RunnerState) -> None:
        self._idle.clear()
        if self._pending_advance:
            state.pending_input = None
            state.stage_position += 1
            if state.stage_position > len(state.template.stages):
                state.cycle_number += 1
                state.stage_position = 1
                state.stage_result_ids.clear()
                state.stage_result_origins.clear()
                state.stage_attempt_numbers.clear()
                state.last_result = None
                state.last_result_id = None
                for instance in state.services.values():
                    instance.restart_count = 0
            self._pending_advance = False
        state.last_dag_decision = None
        state.pause_requested = False
        state.phase = "stage_running"
        self._save_state()
        self._stage_task = asyncio.create_task(
            self._stages.execute(
                state,
                manual=self._manual,
                wait_services=self._services.wait_ready,
            )
        )
        self._manual = False

    async def _prepare_continuation(self) -> tuple[RunnerState, str]:
        manifest = await asyncio.to_thread(
            self._snapshots.latest_valid, self._continue_source
        )
        saved = manifest["state"]
        if manifest["experiment_id"] != self._requested_id.split(":", 1)[1]:
            raise ValueError("Continuation snapshot belongs to another experiment.")
        if saved["phase"] == "completed" or (
            saved["pending_advance"]
            and saved["cycle_number"] == saved["template"]["cycles"]
            and saved["stage_position"] == len(saved["template"]["stages"])
            and not (
                saved.get("last_dag_decision") is not None
                and saved["last_dag_decision"]["decision"]["command"] == "pause"
            )
        ):
            raise RuntimeError(
                "The snapshot has no remaining cycles; use experiment rerun."
            )
        folder = str(uuid4())
        directory = self._project_root / "experiments" / folder
        directory.mkdir(parents=True, exist_ok=False)
        state = RunnerState(
            self._requested_id,
            directory,
            f"{uuid4()}:{saved['run_id']}",
            directory / "experiment.yaml",
            saved["template_revision_id"],
            saved["template_yaml"],
            saved["template"],
            "paused",
        )
        state.phase = "restoring"
        self._state = state
        self._state_store.save(state)
        registry = read_json(self._project_root / "experiments.json")
        if self._requested_id in registry:
            raise FileExistsError("Continuation experiment ID already exists.")
        registry[self._requested_id] = folder
        write_json(self._project_root / "experiments.json", registry)
        self._maintenance = True
        try:
            if self._resource_observer is not None and self._suspend_resources is None:
                raise RuntimeError(
                    "Resource observer must provide a restoration barrier."
                )
            await self._snapshots.restore(
                state,
                manifest["snapshot_id"],
                source_directory=self._continue_source,
                suspend_resources=self._suspend_resources,
            )
        finally:
            self._maintenance = False
            self._publish_resources()
            if self._resume_resources is not None:
                self._resume_resources()
        self._pending_advance = state.pending_advance
        action = "ready"
        return state, action

    async def _prepare_initial_run(self) -> tuple[RunnerState, str]:
        state = await self._assembler.assemble(self._template_path, self._requested_id)
        self._state = state
        self._journal.open(state, create=True)
        self._journal.record_template(
            state, state.template_yaml, state.template, "initial"
        )
        self._assembler.check_modules(state)
        self._assembler.check_resources(state)
        state.mode = self._desired_mode
        state.phase = "starting"
        self._save_state()
        action = await self._services.start_all(state)
        return state, action

    async def _prepare_live_run(self) -> tuple[RunnerState, str]:
        state = self._state
        self._pending_advance = state.pending_advance
        self._assembler.check_modules(state)
        action = "ready"
        if self._recover_live:
            action = await self._services.recover(state)
            barriers = {
                item.prepared_freeze_id or item.freeze_id
                for item in state.services.values()
            } - {None}
            if barriers and action != "stop":
                await self._services.unfreeze(state, barriers.pop())
            self._recover_live = False
        return state, action

    def _at_dag_end(self) -> bool:
        state = self._state
        conditional_pause = (
            state.last_dag_decision is not None
            and state.last_dag_decision.decision.command == "pause"
        )
        return (
            self._pending_advance
            and state.stage_position == len(state.template.stages)
            and state.cycle_number == state.template.cycles
            and not conditional_pause
        )

    def _apply_stage_outcome(self, outcome: StageOutcome) -> None:
        """Commit output and its cursor decision together at a stage boundary."""
        state = self._state
        response = outcome.result
        stage_id = outcome.attempt.stage_id
        state.active_attempt = None
        state.last_dag_decision = None
        self._pending_advance = outcome.action == "advance"
        if outcome.action == "pause":
            state.mode = self._desired_mode = "paused"
        if response is None or response["result"] != "success":
            state.last_result = state.last_result_id = None
            state.stage_result_ids.pop(stage_id, None)
            state.stage_result_origins.pop(stage_id, None)
            return
        # Keep a transferred input while the cursor still names its target:
        # manual rerun must receive the same input even after successful output.
        state.last_result = response["data"]
        state.last_result_id = outcome.attempt.result_request_id
        state.stage_result_ids[stage_id] = outcome.attempt.result_request_id
        state.stage_result_origins[stage_id] = state.experiment_id
        definition = state.template.stages[state.stage_position - 1]
        if "returns_data" not in definition.model_fields_set:
            return
        decision = DagDecision.model_validate(response["execution"]["dag_decision"])
        source = LastDecision.model_validate(
            {
                "request_id": outcome.attempt.result_request_id,
                "source_stage_id": stage_id,
                "experiment_id": state.experiment_id,
                "decision": decision,
            }
        )
        state.last_dag_decision = source
        self._retain_input_artifacts(response["data"])
        command = decision.command
        if command == "pause":
            state.mode = self._desired_mode = "paused"
            state.pause_requested = True
        elif command == "move":
            target = decision.stage_id
            if target is None:
                raise ValueError("Conditional move requires a target stage.")
            self._apply_conditional_move(state, source, target)

    def _apply_conditional_move(
        self, state: RunnerState, source: LastDecision, target: str
    ) -> None:
        position = next(
            index
            for index, item in enumerate(state.template.stages, 1)
            if item.stage_id == target
        )
        # The source remains in the journal even when a backwards jump
        # invalidates its current result reference.
        state.pending_input = PendingInput(
            request_id=source.request_id,
            source_stage_id=source.source_stage_id,
            experiment_id=source.experiment_id,
            stage_id=target,
        )
        for item in state.template.stages[position - 1 :]:
            state.stage_result_ids.pop(item.stage_id, None)
            state.stage_result_origins.pop(item.stage_id, None)
            state.stage_retry_counts.pop(item.stage_id, None)
        state.stage_position = position
        self._pending_advance = False

    def _retain_input_artifacts(self, data: JsonValue) -> None:
        """Protect experiment-relative artifact references carried through a jump."""
        state = self._state
        root = state.experiment_directory.resolve()
        artifacts = root / "shared_artifacts"
        pending = [data]
        retained = set(state.retained_artifacts)
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                pending.extend(value.keys())
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)
            elif isinstance(value, str):
                try:
                    relative = Path(value)
                    if relative.anchor:
                        continue
                    resolved = (root / relative).resolve()
                    if resolved.is_relative_to(artifacts) and resolved.exists():
                        retained.add(resolved.relative_to(root).as_posix())
                except (OSError, ValueError):
                    # Arbitrary application strings need not be filesystem paths.
                    continue
        state.retained_artifacts = sorted(retained)

    async def _stop_from_stage(self, *, recovering: bool = False) -> None:
        """Use ordinary shutdown without cancelling or awaiting this DAG task."""
        future, self._step_future = self._step_future, None
        try:
            if recovering:
                self._retire_accepted_service_requests(self._state)
                self._save_state()
            await self._stop(finalize_snapshot=not recovering)
        except BaseException as error:
            if future is not None and not future.done():
                future.set_exception(error)
            raise
        if future is not None and not future.done():
            future.set_result(
                {
                    "attempt_id": None
                    if self._last_attempt is None
                    else self._last_attempt.attempt_id,
                    "result": self._last_response,
                    "phase": self._state.phase,
                }
            )

    def _retire_accepted_service_requests(self, state: RunnerState) -> None:
        # A crash may leave completed snapshot calls in saved queues.
        # Preserve their accepted outcomes before stopping unresolved work.
        for instance in state.services.values():
            requests = list(instance.pending_requests)
            if instance.active_request is not None:
                requests.append(instance.active_request)
            for request in requests:
                record = self._journal.client.read_command_result(request.request_id)
                if record is None or record["author"] != "runner":
                    continue
                if request is instance.active_request:
                    instance.active_request = None
                else:
                    instance.pending_requests.remove(request)

    async def pause(self) -> None:
        if self._task is None or self._task.done():
            raise RuntimeError("There is no active experiment to pause.")
        self._desired_mode = "paused"
        if self._state is not None:
            self._state.mode = "paused"
            self._state.pause_requested = True
            self._save_state()
        await self._ready.wait()
        await self._idle.wait()
        self._require_active()
        self._state.mode = "paused"
        if self._state.phase == "starting":
            self._state.phase = "waiting"
        self._save_state()

    async def resume(self) -> None:
        if self._task is None:
            raise RuntimeError("There is no experiment to resume.")
        await self._ready.wait()
        state = self._require_active()
        if any(
            definition.service_id not in state.services
            for definition in state.template.services
        ):
            raise RuntimeError(
                "Start all declared services before resuming the experiment."
            )
        if (
            self._maintenance
            or state.active_attempt is not None
            and self._stage_task is None
        ):
            raise RuntimeError(
                "Resolve the current snapshot, restoration, or unknown stage first."
            )
        if (
            any(
                instance.blocked_action is not None or instance.manually_stopped
                for instance in state.services.values()
            )
            or self._service_retrying
        ):
            raise RuntimeError(
                "Resolve the blocked service before resuming the experiment."
            )
        state.mode = "running"
        self._desired_mode = "running"
        state.pause_requested = False
        if (
            state.last_dag_decision is not None
            and state.last_dag_decision.decision.command == "pause"
        ):
            state.last_dag_decision = None
        self._save_state()
        self._wake.set()

    async def step(self) -> JsonObject:
        if self._maintenance:
            raise RuntimeError("Wait for snapshot or restoration before stepping.")
        if self._task is None:
            raise RuntimeError("There is no experiment to step.")
        await self._ready.wait()
        state = self._require_active()
        if (
            state.mode != "paused"
            or not self._idle.is_set()
            or self._step_future is not None
            or self._service_retrying
        ):
            raise RuntimeError(
                "step requires a paused experiment without an active attempt."
            )
        if any(instance.manually_stopped for instance in state.services.values()):
            raise RuntimeError("Start manually stopped services before stepping.")
        if any(
            definition.service_id not in state.services
            for definition in state.template.services
        ):
            raise RuntimeError("Start all declared services before stepping.")
        if (
            state.last_dag_decision is not None
            and state.last_dag_decision.decision.command == "pause"
        ):
            state.last_dag_decision = None
            self._save_state()
        self._step_future = asyncio.get_running_loop().create_future()
        future = self._step_future
        self._wake.set()
        try:
            result = await asyncio.shield(future)
            if (
                result.get("phase") in ("completed", "stopped")
                and self._task is not None
            ):
                # A final step includes publishing its snapshot and closing the
                # DAG task, so a following command can use the completed state.
                await asyncio.shield(self._task)
            return result
        except asyncio.CancelledError:
            future.cancel()
            if self._step_future is future:
                self._step_future = None
            raise

    async def stop(self) -> JsonObject:
        return await self._stop(finalize_snapshot=True)

    async def _stop(self, *, finalize_snapshot: bool) -> JsonObject:
        self._stop_requested = True
        await self._cancel_control_tasks()
        if self._state is not None:
            failure = None
            try:
                stopped = await self._stages.interrupt(self._state, "stop")
                if not stopped:
                    failure = RuntimeError("Could not confirm stage termination.")
                else:
                    await self._stages.close(self._state)
            except Exception as error:  # noqa: BLE001 - Services must still receive stop after a stage error.
                stopped = False
                failure = error
            if stopped and self._state.active_attempt is not None:
                self._last_attempt = self._state.active_attempt
                self._state.active_attempt = None
                self._pending_advance = False
            try:
                if (
                    stopped
                    and finalize_snapshot
                    and self._state.pending_rebuild is None
                    and self._state.phase
                    not in (
                        "completed",
                        "failed",
                        "stopped",
                        "restoring",
                    )
                ):
                    self._state.pending_advance = self._pending_advance
                    self._last_snapshot = await self._snapshots.finalize(self._state)
                    stopped = all(
                        item.stopped for item in self._state.services.values()
                    )
                else:
                    results = await self._services.stop_all(self._state)
                    stopped = stopped and all(
                        result["stopped"] for result in results.values()
                    )
                    if any(
                        not result["stopped"] or result["error"]
                        for result in results.values()
                    ):
                        failure = failure or RuntimeError(
                            f"Service shutdown failed: {results}"
                        )
            except Exception as error:  # noqa: BLE001 - Preserve the first failure and report unconfirmed shutdown.
                stopped = stopped and all(
                    item.stopped for item in self._state.services.values()
                )
                failure = failure or error
            if (
                not stopped
                and self._state.active_attempt is not None
                and self._state.active_attempt.service_id is not None
                and all(item.stopped for item in self._state.services.values())
                and self._stages.termination_confirmed(self._state)
            ):
                stopped = True
                if (
                    isinstance(failure, RuntimeError)
                    and str(failure) == "Could not confirm stage termination."
                ):
                    failure = None
            self._termination_confirmed = stopped
            await self._services.close()
            if failure is not None:
                await self._fail(failure, {})
                raise RuntimeError("Experiment shutdown failed.") from failure
            if (
                self._state.phase == "restoring"
                or self._state.pending_rebuild is not None
            ):
                self._state.phase = "failed"
            elif self._state.phase not in ("completed", "failed"):
                self._state.phase = "stopped"
            if self._state.active_attempt is not None:
                self._last_attempt = self._state.active_attempt
            self._state.active_attempt = None
            self._save_state()
        if self._step_future is not None:
            if not self._step_future.done():
                self._step_future.set_exception(
                    RuntimeError("Step was cancelled by stop.")
                )
            self._step_future = None
        self._idle.set()
        return self.get_state()

    async def _cancel_control_tasks(self) -> None:
        if (
            self._service_control_task is not None
            and self._service_control_task is not asyncio.current_task()
            and not self._service_control_task.done()
        ):
            self._service_control_task.cancel()
            await asyncio.gather(self._service_control_task, return_exceptions=True)
        if (
            self._maintenance_task is not None
            and self._maintenance_task is not asyncio.current_task()
            and not self._maintenance_task.done()
        ):
            self._maintenance_task.cancel()
            await asyncio.gather(self._maintenance_task, return_exceptions=True)
        if (
            self._task is not None
            and self._task is not asyncio.current_task()
            and not self._task.done()
        ):
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def rerun(
        self,
        scope: Literal["stage", "experiment"],
        *,
        position: int | None = None,
        experiment_id: str | None = None,
    ) -> JsonObject:
        if self._maintenance:
            raise RuntimeError("Wait for snapshot or restoration before rerunning.")
        if scope != "stage":
            if scope != "experiment" or position is not None:
                raise ValueError("rerun scope must be stage or experiment.")
            source_id = self._requested_id if experiment_id is None else experiment_id
            if source_id is None:
                raise ValueError("Experiment rerun requires a source experiment.")
            source = find_experiment(self._project_root, source_id)
            if self._task is not None and not self._task.done():
                await self.stop()
            return await self.run(source / "experiment.yaml", delayed_start=True)
        state = self._require_active()
        if type(position) is not int:
            raise ValueError("rerun position must be an integer.")
        if (
            position != state.stage_position
            or state.mode != "paused"
            or not self._idle.is_set()
        ):
            raise RuntimeError("rerun requires the paused pointer's stage.")
        self._pending_advance = False
        self._manual = True
        return await self.step()

    def _require_idle_service_control(self, state: RunnerState, message: str) -> None:
        if (
            self._maintenance
            or self._service_retrying
            or self._stop_requested
            or self._task is None
            or self._task.done()
            or state.mode != "paused"
            or state.phase != "waiting"
            or not self._idle.is_set()
            or state.active_attempt is not None
            or self._step_future is not None
        ):
            raise RuntimeError(message)

    async def start_service(self, position: int) -> JsonObject:
        state = self._require_active()
        self._require_idle_service_control(
            state,
            "service start requires an idle pause without another service or maintenance operation.",
        )
        service_id = _service_definition(state, position).service_id
        self._service_retrying = True
        self._service_control_task = asyncio.current_task()
        try:
            instance = await self._services.start(state, service_id)
            self._save_state()
            return {
                "service_id": service_id,
                "service_instance_id": instance.service_instance_id,
                "ready": instance.ready,
                "manually_stopped": instance.manually_stopped,
            }
        finally:
            self._service_retrying = False
            self._service_control_task = None
            self._wake.set()

    async def stop_service(self, position: int) -> JsonObject:
        state = self._require_active()
        self._require_idle_service_control(
            state,
            "service stop requires an idle pause without another service or maintenance operation.",
        )
        definition = _service_definition(state, position)
        service_id = definition.service_id
        instance = state.services.get(service_id)
        if instance is None:
            instance = ServiceInstance(service_id, str(uuid4()), definition)
            instance.stopped = True
            state.services[service_id] = instance
        self._service_retrying = True
        self._service_control_task = asyncio.current_task()
        try:
            # Persist intent before touching the process so recovery can finish
            # an interrupted stop without restarting the selected service.
            instance.manually_stopped = True
            self._save_state()
            self._state_store.save(state)
            # A stopped process can still have a delayed automatic restart.
            # The manager must cancel that work before acknowledging manual stop.
            results = await self._services.stop_all(state, service_ids={service_id})
            if not results[service_id]["stopped"] or results[service_id]["error"]:
                self._save_state()
                raise RuntimeError(f"Service shutdown failed: {results[service_id]}")
            instance.ready = False
            instance.blocked_action = None
            self._save_state()
            return {
                "service_id": service_id,
                "service_instance_id": instance.service_instance_id,
                "stopped": instance.stopped,
                "manually_stopped": True,
            }
        finally:
            self._service_retrying = False
            self._service_control_task = None
            self._wake.set()

    async def retry(self, position: int) -> JsonObject:
        if self._maintenance:
            raise RuntimeError(
                "Wait for snapshot or restoration before retrying a service."
            )
        state = self._require_active()
        if state.mode != "paused" or self._service_retrying:
            raise RuntimeError(
                "retry requires a paused experiment without another service retry."
            )
        definition = _service_definition(state, position)
        service_id = definition.service_id
        instance = state.services.get(service_id)
        if instance is None:
            raise RuntimeError(
                "Retry the failed preceding service before starting later services."
            )
        if instance.manually_stopped:
            raise RuntimeError("Use service start to start a manually stopped service.")
        self._service_retrying = True
        try:
            action = await self._services.restart(state, service_id, automatic=False)
            if action == "ready":
                # Startup may have paused before reaching the rest of the template.
                action = await self._services.reconcile(state, state.template)
            if action == "stop":
                raise RuntimeError("Service retry requires experiment stop.")
            self._save_state()
            return {
                "service_id": service_id,
                "action": action,
                "service_instance_id": state.services[service_id].service_instance_id,
            }
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._fail(error, {"service_id": service_id})
            raise
        finally:
            self._service_retrying = False
            self._wake.set()

    def move(self, position: int) -> None:
        if self._maintenance:
            raise RuntimeError(
                "Wait for snapshot or restoration before moving the cursor."
            )
        state = self._require_active()
        if state.mode != "paused" or not self._idle.is_set():
            raise RuntimeError("move requires a paused experiment.")
        if type(position) is not int or not 1 <= position <= len(state.template.stages):
            raise ValueError("Position is outside the DAG.")
        state.stage_position = position
        state.pending_input = None
        state.last_dag_decision = None
        if state.last_result_id not in state.stage_result_ids.values():
            state.last_result = state.last_result_id = None
        self._pending_advance = False
        self._save_state()

    def reset_retries(self, kind: ModuleRole, position: int) -> JsonObject:
        if self._maintenance:
            raise RuntimeError(
                "Wait for snapshot or restoration before resetting retries."
            )
        state = self._require_active()
        if kind not in ("stage", "service"):
            raise ValueError("kind must be stage or service.")
        if state.mode != "paused":
            raise RuntimeError("reset_retries requires a paused experiment.")
        if kind == "service":
            if self._service_retrying:
                raise RuntimeError(
                    "Wait for the service retry before resetting counters."
                )
            if type(position) is not int or not 1 <= position <= len(
                state.template.services
            ):
                raise ValueError("Service position is outside the template.")
            service_id = state.template.services[position - 1].service_id
            instance = state.services.get(service_id)
            if instance is None:
                raise ValueError("A started socket service is required.")
            old = instance.restart_count
            instance.restart_count = 0
            result_key, identifier = "service_id", service_id
        else:
            if type(position) is not int or not 1 <= position <= len(
                state.template.stages
            ):
                raise ValueError("Position is outside the DAG.")
            stage_id = state.template.stages[position - 1].stage_id
            old = state.stage_retry_counts.get(stage_id, 0)
            state.stage_retry_counts[stage_id] = 0
            result_key, identifier = "stage_id", stage_id
        self._save_state()
        return {result_key: identifier, "previous": old, "current": 0}

    async def replace(
        self, kind: ModuleRole, position: int, module: JsonObject, settings: JsonObject
    ) -> JsonObject:
        raise NotImplementedError(
            "Module replacement requires protected rebuilds and snapshots."
        )

    async def reload_template(self, template_path: Path | None = None) -> JsonObject:
        logger = (
            self._journal.client
            if self._journal.reader_config_path is not None
            else self._control_logger
        )
        try:
            state = self._require_active()
            if (
                self._closed
                or self._stop_requested
                or self._maintenance
                or self._service_retrying
                or state.pending_rebuild is not None
                or state.mode != "paused"
                or state.phase != "waiting"
                or not self._idle.is_set()
                or state.active_attempt is not None
                or self._step_future is not None
                or any(
                    s.manually_stopped
                    or s.blocked_action
                    or not s.ready
                    or s.stopped
                    or s.stopping
                    for s in state.services.values()
                )
            ):
                raise RuntimeError(
                    "reload_template requires an idle pause with ready services."
                )
        except RuntimeError as error:
            if logger is not None:
                logger.record_event(
                    "reload.rejected",
                    {
                        "template_path": None
                        if template_path is None
                        else str(template_path),
                        "reason": str(error),
                    },
                    context=self._command_context,
                )
            raise
        operation = self._journal.client.start_operation(
            "rebuild",
            "reload_template",
            context={
                **self._command_context,
                "experiment_id": state.experiment_id,
                "run_id": state.run_id,
                "template_revision_id": state.template_revision_id,
            },
            attributes={"template_path": str(template_path or state.template_path)},
        )
        self._reload_operation = operation
        self._maintenance = True
        self._maintenance_task = asyncio.current_task()
        try:
            path = state.template_path if template_path is None else Path(template_path)
            try:
                _, template = await asyncio.to_thread(
                    self._assembler.load_template, path
                )
            except yaml.YAMLError as error:
                raise ValueError(f"Invalid template YAML: {error}") from error
            template = _prepare_reload_candidate(
                state, ExperimentTemplate.model_validate(template)
            )
            text = yaml.safe_dump(
                template.model_dump(exclude_unset=True),
                allow_unicode=True,
                sort_keys=False,
            )
            result = await self._apply_template(text, template)
            if self._reload_operation is not None:
                self._journal.client.finish_operation(operation, attributes=result)
                self._reload_operation = None
            return result
        except BaseException as error:
            if self._reload_operation is not None:
                try:
                    self._journal.client.record_error(
                        error,
                        operation=operation,
                        include_traceback=True,
                    )
                    self._journal.client.finish_operation(
                        operation,
                        status="cancelled"
                        if isinstance(error, asyncio.CancelledError)
                        else "failed",
                    )
                except Exception as logging_error:  # noqa: BLE001 - Preserve the original reload failure.
                    error.add_note(f"Reload audit failed: {logging_error}")
                self._reload_operation = None
            raise
        finally:
            self._maintenance = False
            self._maintenance_task = None

    async def _apply_template(
        self, template_yaml: str, template: ExperimentTemplate
    ) -> JsonObject:
        state = self._require_active()
        operation = self._reload_operation
        if operation is None or not self._maintenance:
            raise RuntimeError("Template application requires a reload operation.")
        logger = self._journal.client
        previous_revision = state.template_revision_id
        result = {
            "experiment_id": state.experiment_id,
            "previous_template_revision_id": previous_revision,
            "template_revision_id": previous_revision,
            "snapshot_id": None,
            "changed": _template_fingerprint(state.template)
            != _template_fingerprint(template),
            "stage_position": state.stage_position,
            "pending_advance": self._pending_advance,
            "mode": "paused",
            "changes": [],
        }
        if not result["changed"]:
            logger.record_event("reload.unchanged", result, operation=operation)
            return result
        application = await self._plan_reload_application(
            state, template_yaml, template, operation, result
        )
        self._audit_reload_definitions(application)
        self._audit_reload_candidate(state, application)
        self._audit_reload_progress(state, application)
        try:
            await self._prepare_reload_snapshot(state, application)
            self._isolate_reload_service_data(state, application)
            await self._publish_reload_dag(state, application)
            await self._transfer_reload_services(state, application)
            self._commit_reload(state, application)
            return result
        except BaseException as error:
            await self._handle_reload_failure(state, application, error)
            raise
        finally:
            await self._cleanup_reload_application(state, application)

    async def _plan_reload_application(
        self,
        state: RunnerState,
        template_yaml: str,
        template: ExperimentTemplate,
        operation: Operation,
        result: JsonObject,
    ) -> ReloadApplication:
        layout = _reload_layout(
            state.template, template, state.stage_position, self._pending_advance
        )
        completed = await _read_reload_progress(
            self._journal.client,
            state.experiment_id,
            state.template_revision_id,
            state.cycle_number,
            set(state.stage_result_ids),
        )
        cursor = _reload_cursor(layout, completed)
        return ReloadApplication(
            template=template,
            template_yaml=template_yaml,
            previous=state.template,
            layout=layout,
            cursor=cursor,
            completed=completed,
            previous_revision=state.template_revision_id,
            candidate_revision=str(uuid4()),
            candidate_run=f"{uuid4()}:{state.run_id}",
            workspace=state.experiment_directory
            / "runner/rebuilds"
            / operation.get_operation_id(),
            logger=self._journal.client,
            operation=operation,
            result=result,
            prior_instances={
                sid: item.service_instance_id for sid, item in state.services.items()
            },
            prior_service_retries={
                sid: item.restart_count for sid, item in state.services.items()
            },
        )

    def _audit_reload_definitions(self, application: ReloadApplication) -> None:
        for role, old_entries, new_entries in (
            ("stage", application.previous.stages, application.template.stages),
            ("service", application.previous.services, application.template.services),
        ):
            before = {
                (s.service_id if isinstance(s, ServiceDefinition) else s.stage_id): (
                    i + 1,
                    s,
                )
                for i, s in enumerate(old_entries)
            }
            after = {
                (s.service_id if isinstance(s, ServiceDefinition) else s.stage_id): (
                    i + 1,
                    s,
                )
                for i, s in enumerate(new_entries)
            }
            for identity in dict.fromkeys([*before, *after]):
                old = before.get(identity)
                new = after.get(identity)
                if (
                    old is not None
                    and new is not None
                    and old[0] == new[0]
                    and _definition_fingerprint(old[1])
                    == _definition_fingerprint(new[1])
                ):
                    continue
                change = _definition_change(role, identity, before, after)
                application.logger.record_event(
                    "reload.definition_changed", change, operation=application.operation
                )
                application.result["changes"].append(
                    {
                        k: v
                        for k, v in change.items()
                        if k not in ("before", "after", "fields")
                    }
                )

    def _audit_reload_candidate(
        self, state: RunnerState, application: ReloadApplication
    ) -> None:
        # Validate the dedicated full-template event before any filesystem/process effects.
        encode_event(
            {
                "schema_version": 2,
                "event_id": str(uuid4()),
                "producer_instance_id": str(uuid4()),
                "sequence_number": 1,
                "occurred_at": datetime.now(UTC).isoformat(),
                "event_type": "template.applied",
                "operation_id": None,
                "context": {
                    **application.logger.get_context(),
                    "run_id": application.candidate_run,
                    "previous_run_id": state.run_id,
                },
                "data": {
                    "template": application.template.model_dump(exclude_unset=True),
                    "template_yaml": application.template_yaml,
                    "template_revision_id": application.candidate_revision,
                    "previous_template_revision_id": application.previous_revision,
                    "reason": "reload_template",
                },
            },
            state.template.logging.max_event_bytes,
        )
        application.logger.record_event(
            "reload.previous_template",
            {
                "template": application.previous.model_dump(exclude_unset=True),
                "template_yaml": state.template_yaml,
            },
            operation=application.operation,
        )
        application.parameters = application.logger.record_event(
            "reload.candidate",
            {
                "template": application.template.model_dump(exclude_unset=True),
                "template_yaml": application.template_yaml,
                "run_id": application.candidate_run,
                "template_revision_id": application.candidate_revision,
            },
            operation=application.operation,
        )

    def _audit_reload_progress(self, state: RunnerState, application: ReloadApplication) -> None:
        application.logger.record_event(
            "reload.progress",
            {
                "template_revision_id": application.candidate_revision,
                "preserved_completed_stage_ids": sorted(application.completed & application.cursor.preserved),
                "cycle_number": state.cycle_number,
                "old_position": state.stage_position,
                "old_pending_advance": self._pending_advance,
                "new_position": application.cursor.stage_position,
                "new_pending_advance": application.cursor.pending_advance,
                "rewind": application.cursor.rewind,
                "unchanged_prefix_length": application.layout.prefix,
                "invalidated_results": {
                    k: v
                    for k, v in state.stage_result_ids.items()
                    if k not in application.cursor.preserved
                },
                "preserved_results": {
                    k: v for k, v in state.stage_result_ids.items() if k in application.cursor.preserved
                },
                "result_origins": state.stage_result_origins,
                "cleared_retries": {
                    k: v
                    for k, v in state.stage_retry_counts.items()
                    if k not in application.cursor.preserved
                },
                "preserved_attempt_numbers": state.stage_attempt_numbers,
            },
            operation=application.operation,
        )

    async def _prepare_reload_snapshot(
        self, state: RunnerState, application: ReloadApplication
    ) -> None:
        await self._services.prepare_rebuild(
            state, application.template, validate_only=True
        )
        await self._assembler.rebuild(
            state,
            application.template_yaml,
            application.template.model_dump(exclude_unset=True),
            workspace=application.workspace,
            prepare_only=True,
        )
        if self._resource_observer is not None and self._suspend_resources is None:
            raise RuntimeError("Reload requires a resource restoration barrier.")
        # Detach the paused scheduler, not the service manager. Its monitor must
        # still answer freeze, shutdown and readiness work during maintenance.
        if self._task is not None and not self._task.done():
            self._task.cancel()
            application.detached = True
            await asyncio.gather(self._task, return_exceptions=True)
        self._last_snapshot = await self._snapshots.create(
            state, "before reload_template"
        )
        application.snapshot_id = self._last_snapshot["snapshot_id"]
        application.result["snapshot_id"] = application.snapshot_id
        state.pending_rebuild = PendingRebuild.model_validate(
            {
                "operation_id": application.operation.get_operation_id(),
                "snapshot_id": application.snapshot_id,
                "template_revision_id": application.candidate_revision,
                "run_id": application.candidate_run,
            }
        )
        state.phase = "rebuilding"
        self._save_state()
        application.logger.record_event(
            "control.intent",
            {
                "action": "reload_template",
                "parameters_event_id": application.parameters,
                "snapshot_id": application.snapshot_id,
            },
            operation=application.operation,
        )
        await self._services.prepare_rebuild(state, application.template)
        self._save_state()

    def _isolate_reload_service_data(
        self, state: RunnerState, application: ReloadApplication
    ) -> None:
        for sid, definition in application.layout.new_services.items():
            previous = application.layout.old_services.get(sid)
            application.logger.record_event(
                "reload.service_plan",
                {
                    "service_id": sid,
                    "old_definition": None
                    if previous is None
                    else previous.model_dump(exclude_unset=True),
                    "new_definition": definition.model_dump(exclude_unset=True),
                    "previous_instance_id": application.prior_instances.get(sid),
                    "transfer_state": previous is not None
                    and sid in application.layout.changed_services
                    and previous.module.name == definition.module.name,
                },
                operation=application.operation,
            )
            # An absent definition cannot authorize reuse of files left by
            # a removed service, even when its stable ID is added again.
            if previous is None or previous.module.name != definition.module.name:
                data = state.experiment_directory / "module_data" / sid
                if data.exists():
                    if (
                        data.is_symlink()
                        or data.is_junction()
                        or not data.resolve().is_relative_to(
                            state.experiment_directory.resolve()
                        )
                    ):
                        raise ValueError("Service data escapes the experiment.")
                    displaced = application.workspace / "previous_data" / sid
                    displaced.parent.mkdir(parents=True, exist_ok=True)
                    data.replace(displaced)
                    application.logger.record_event(
                        "reload.service_data",
                        {
                            "service_id": sid,
                            "action": "isolate_previous_module_data",
                            "previous_module": None
                            if previous is None
                            else previous.module.model_dump(exclude_unset=True),
                            "new_module": definition.module.model_dump(
                                exclude_unset=True
                            ),
                            "reason": "no_applied_service"
                            if previous is None
                            else "module_name_changed",
                        },
                        operation=application.operation,
                    )

    async def _publish_reload_dag(
        self, state: RunnerState, application: ReloadApplication
    ) -> None:
        await self._assembler.rebuild(
            state,
            application.template_yaml,
            application.template.model_dump(exclude_unset=True),
            workspace=application.workspace,
        )
        application.logger.record_event(
            "control.observed",
            {"action": "reload_template", "state": "files_published"},
            operation=application.operation,
        )
        state.template = application.template
        state.template_yaml = application.template_yaml
        self._last_attempt = self._last_response = None
        state.run_id = application.candidate_run
        state.template_revision_id = application.candidate_revision
        state.stage_result_ids = {
            k: v
            for k, v in state.stage_result_ids.items()
            if k in application.cursor.preserved
        }
        state.stage_result_origins = {
            k: v
            for k, v in state.stage_result_origins.items()
            if k in application.cursor.preserved
        }
        state.stage_retry_counts = {
            k: v
            for k, v in state.stage_retry_counts.items()
            if k in application.cursor.preserved
        }
        state.stage_position = application.cursor.stage_position
        self._pending_advance = state.pending_advance = (
            application.cursor.pending_advance
        )
        if state.pending_input is not None and (
            state.pending_input.source_stage_id not in application.cursor.preserved
            or state.pending_input.stage_id not in application.cursor.preserved
            or state.pending_input.stage_id
            != application.layout.new_stages[
                application.cursor.stage_position - 1
            ].stage_id
        ):
            state.pending_input = None
        if state.last_dag_decision is not None and (
            state.last_dag_decision.source_stage_id not in application.cursor.preserved
            or state.last_dag_decision.decision.stage_id is not None
            and state.pending_input is None
        ):
            state.last_dag_decision = None
        state.last_result = state.last_result_id = None
        predecessor = application.cursor.next_position - 1
        if predecessor >= 0:
            request_id = state.stage_result_ids.get(
                application.layout.new_stages[predecessor].stage_id
            )
            if request_id is not None:
                record = application.logger.read_command_result(request_id)
                if record is None or record["author"] != "runner":
                    raise ValueError(
                        "Retained predecessor has no accepted journal result."
                    )
                state.last_result_id = request_id
                state.last_result = record["response"]["data"]
        self._save_state()

    async def _transfer_reload_services(
        self, state: RunnerState, application: ReloadApplication
    ) -> None:
        if await self._services.reconcile(state, application.template) != "ready":
            raise RuntimeError("Reloaded services did not become ready.")
        transfer = {
            sid
            for sid, definition in application.layout.new_services.items()
            if sid in application.layout.old_services
            and sid in application.layout.changed_services
            and definition.module.name
            == application.layout.old_services[sid].module.name
        }
        manifest = await asyncio.to_thread(
            self._snapshots._validate_snapshot,
            self._project_root
            / "snapshots"
            / state.experiment_directory.name
            / application.snapshot_id,
        )
        exports = {
            sid: Path(path)
            for sid, path in manifest["services"].items()
            if sid in transfer
        }
        await self._services.load_states(state, exports, service_ids=transfer)
        if any(
            state.services[sid].service_instance_id != application.prior_instances[sid]
            for sid in application.layout.new_services.keys()
            & application.layout.old_services.keys()
            if sid not in application.layout.changed_services
        ):
            raise RuntimeError("An unchanged service restarted during reload.")
        for sid in dict.fromkeys(
            [*application.layout.old_services, *application.layout.new_services]
        ):
            instance = state.services.get(sid)
            application.logger.record_event(
                "reload.service",
                {
                    "service_id": sid,
                    "state_loaded": sid in exports,
                    "state_path": None
                    if sid not in exports
                    else exports[sid].as_posix(),
                    "service_instance_id": None
                    if instance is None
                    else instance.service_instance_id,
                    "previous_instance_id": application.prior_instances.get(sid),
                    "previous_restart_count": application.prior_service_retries.get(
                        sid
                    ),
                    "restart_count": None
                    if instance is None
                    else instance.restart_count,
                    "ready": False if instance is None else instance.ready,
                    "definition": None
                    if sid not in application.layout.new_services
                    else application.layout.new_services[sid].model_dump(
                        exclude_unset=True
                    ),
                },
                operation=application.operation,
            )

    def _commit_reload(
        self, state: RunnerState, application: ReloadApplication
    ) -> None:
        application.logger.record_template_applied(
            application.template.model_dump(exclude_unset=True),
            template_yaml=application.template_yaml,
            template_revision_id=application.candidate_revision,
            previous_template_revision_id=application.previous_revision,
            reason="reload_template",
            context={
                "experiment_id": state.experiment_id,
                "run_id": application.candidate_run,
                "previous_run_id": application.candidate_run.split(":", 1)[1],
            },
        )
        state.phase, state.mode = "waiting", "paused"
        state.pause_requested = False
        pending = state.pending_rebuild
        state.pending_rebuild = None
        try:
            self._save_state()
        except BaseException:
            state.pending_rebuild = pending
            raise
        application.committed = True
        application.result.update(
            template_revision_id=application.candidate_revision,
            stage_position=application.cursor.stage_position,
            pending_advance=application.cursor.pending_advance,
        )

    async def _handle_reload_failure(
        self, state: RunnerState, application: ReloadApplication, error: BaseException
    ) -> None:
        if state.pending_rebuild is None or application.committed:
            await self._handle_detached_reload_failure(state, application, error)
            return
        failure_id = None
        try:
            failure_id = application.logger.record_error(
                error, operation=application.operation, include_traceback=True
            )
            application.logger.finish_operation(
                application.operation,
                status="cancelled"
                if isinstance(error, asyncio.CancelledError)
                else "failed",
            )
            self._reload_operation = None
            if isinstance(error, (asyncio.CancelledError, LoggingError)):
                # A priority stop must retain the recovery point, never finalize
                # the partially published tree as a valid experiment snapshot.
                raise error
            await self._snapshots.restore(
                state,
                state.pending_rebuild.snapshot_id,
                preserve_rebuild_diagnostics=True,
                suspend_resources=self._suspend_resources,
            )
            self._last_attempt = self._last_response = None
            self._pending_advance = state.pending_advance
            with self._journal.client.operation(
                "rebuild",
                "reload_template_rollback",
                context={
                    "experiment_id": state.experiment_id,
                    "run_id": state.run_id,
                    "parent_operation_id": application.operation.get_operation_id(),
                },
            ) as recovery:
                self._journal.client.record_event(
                    "control.reconciled",
                    {
                        "action": "reload_template",
                        "state": "rolled_back",
                        "error_id": failure_id,
                    },
                    operation=recovery,
                )
            self._save_state()
        except BaseException as rollback_error:  # noqa: BLE001 - Cancellation must preserve unfinished ownership.
            if rollback_error is not error:
                error.add_note(f"Reload rollback failed: {rollback_error}")
                try:
                    self._journal.client.record_error(
                        rollback_error,
                        caused_by_error_id=failure_id,
                        context={
                            "experiment_id": state.experiment_id,
                            "parent_operation_id": application.operation.get_operation_id(),
                        },
                        include_traceback=True,
                    )
                except (LoggingError, OSError) as logging_error:
                    error.add_note(f"Rollback error recording failed: {logging_error}")
            if not isinstance(rollback_error, asyncio.CancelledError):
                await self._fail(error, {})

    async def _handle_detached_reload_failure(self, state: RunnerState, application: ReloadApplication, error: BaseException) -> None:
        if (
            application.detached
            and not application.committed
            and not isinstance(error, asyncio.CancelledError)
            and not (
                # Snapshot rejection is recoverable only while the original
                # services are healthy and no freeze remains unconfirmed.
                not isinstance(error, LoggingError)
                and state.phase == "waiting"
                and self._services._snapshot_id is None
                and self._services._pending_action is None
                and state.services.keys() == application.prior_instances.keys()
                and all(
                    instance.service_instance_id == application.prior_instances[sid]
                    and instance.ready
                    and not instance.stopped
                    and not instance.stopping
                    and not instance.manually_stopped
                    and not instance.blocked_action
                    and not instance.failure
                    and instance.freeze_id is None
                    and instance.prepared_freeze_id is None
                    for sid, instance in state.services.items()
                )
            )
        ):
            await self._fail(error, {})

    async def _cleanup_reload_application(self, state: RunnerState, application: ReloadApplication) -> None:
        if state.pending_rebuild is None and state.phase == "waiting":
            self._desired_mode = "paused"
            self._recover_live = False
            self._wake.clear()
            if application.detached and not self._stop_requested and not self._closed:
                self._task = asyncio.create_task(
                    self._advance_dag(), name=f"dag:{state.experiment_id}"
                )
        if self._resume_resources is not None:
            self._resume_resources()
        self._publish_resources()
        # Keep failed rebuild work until recovery; only remove an owned tree.
        if (
            state.pending_rebuild is None
            and application.workspace.exists()
            and application.workspace.resolve().is_relative_to(
                state.experiment_directory.resolve() / "runner/rebuilds"
            )
            and not application.workspace.is_symlink()
            and not application.workspace.is_junction()
        ):
            try:
                await asyncio.to_thread(shutil.rmtree, application.workspace)
            except OSError as cleanup_error:
                self._journal.client.record_error(
                    cleanup_error,
                    context={
                        "experiment_id": state.experiment_id,
                        "parent_operation_id": application.operation.get_operation_id(),
                    },
                )


    async def snapshot(self, label: str | None = None) -> JsonObject:
        if label is not None:
            require_text(label, "snapshot label")
        state = self._require_active()
        if any(instance.manually_stopped for instance in state.services.values()):
            raise RuntimeError(
                "Start manually stopped services before creating a snapshot."
            )
        if (
            self._maintenance
            or self._service_retrying
            or state.mode != "paused"
            or not self._idle.is_set()
            or state.active_attempt is not None
            or self._step_future is not None
        ):
            raise RuntimeError(
                "snapshot requires a stage-free pause without another maintenance operation."
            )
        self._maintenance = True
        self._maintenance_task = asyncio.current_task()
        state.phase = "snapshotting"
        self._save_state()
        try:
            self._last_snapshot = await self._snapshots.create(state, label)
            state.phase = "waiting"
            self._save_state()
            return self._last_snapshot
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._fail(error, {})
            raise
        finally:
            self._maintenance = False
            self._maintenance_task = None

    async def create_archive(
        self,
        archive_path: Path,
        experiment_id: str | None = None,
    ) -> JsonObject:
        """Export a stopped selection or an explicitly identified stopped experiment."""
        state = self._state
        if experiment_id is not None:
            require_text(experiment_id, "experiment_id")
            if state is None or state.experiment_id != experiment_id:
                root = find_experiment(self._project_root, experiment_id)
                state = self._state_store.load(root)
                if state.owner_identity is not None:
                    raise RuntimeError(
                        "Recover and stop the experiment before archiving an owned state."
                    )
        if state is None:
            raise RuntimeError("Select a stopped experiment or provide experiment_id.")
        if state is self._state and not self._termination_confirmed:
            raise RuntimeError("Participant shutdown is unconfirmed.")
        return await self._run_archive(self._archiver.create(state, archive_path))

    async def inspect_archive(self, archive_path: Path) -> JsonObject:
        """Validate a portable archive without changing the selected experiment."""
        return await self._run_archive(self._archiver.inspect(archive_path))

    async def install_archive(
        self, archive_path: Path, destination: Path
    ) -> JsonObject:
        """Install checked inputs while no DAG owns the local module store."""
        return await self._run_archive(
            self._archiver.install(archive_path, destination)
        )

    async def _run_archive(
        self, operation: Coroutine[None, None, JsonObject]
    ) -> JsonObject:
        if (
            self._closed
            or self._maintenance
            or not self._termination_confirmed
            or (self._task is not None and not self._task.done())
            or (
                self._state is not None
                and self._state.phase not in ("stopped", "completed", "failed")
            )
        ):
            operation.close()
            raise RuntimeError(
                "Stop the experiment and finish maintenance before archive operations."
            )
        self._maintenance = True
        self._maintenance_task = asyncio.current_task()
        try:
            return await operation
        finally:
            self._maintenance = False
            self._maintenance_task = None
            self._wake.set()

    async def rollback(self, snapshot_id: str) -> JsonObject:
        snapshot_id = str(UUID(require_text(snapshot_id, "snapshot_id")))
        if self._closed or self._state is None:
            raise RuntimeError("Select an experiment before rollback.")
        state = self._state
        if state.pending_rebuild is not None:
            raise RuntimeError(
                "Use recover to restore the unfinished template reload and its audit."
            )
        pending = (
            self._project_root
            / "controller/restore_transactions"
            / f"{state.experiment_directory.name}.json"
        )
        if pending.exists() and read_json(pending).get("phase") not in (
            "complete",
            "failed",
        ):
            raise RuntimeError(
                "Use recover to finish the pending restoration transaction."
            )
        if (
            self._maintenance
            or self._service_retrying
            or state.active_attempt is not None
            or (
                state.phase not in ("completed", "stopped", "failed")
                and (state.mode != "paused" or not self._idle.is_set())
            )
        ):
            raise RuntimeError(
                "rollback requires a stage-free pause or a terminal experiment."
            )
        if self._resource_observer is not None and self._suspend_resources is None:
            raise RuntimeError("Resource observer must provide a restoration barrier.")
        self._maintenance = True
        self._maintenance_task = asyncio.current_task()
        previous_phase = state.phase
        state.phase = "restoring"
        self._publish_resources()
        try:
            # Reopening also permits rollback after an earlier journal writer failure.
            self._journal.close()
            self._journal.open(state, create=False)
            await self._snapshots.restore(
                state, snapshot_id, suspend_resources=self._suspend_resources
            )
            await self._activate_restored_state(state)
            return {
                "experiment_id": state.experiment_id,
                "snapshot_id": snapshot_id,
                "mode": "paused",
            }
        except (ValueError, FileNotFoundError):
            if (
                state.phase == "restoring"
                and not (
                    self._project_root
                    / "controller/restore_transactions"
                    / f"{state.experiment_directory.name}.json"
                ).exists()
            ):
                state.phase = previous_phase
            else:
                await self._fail(
                    RuntimeError("Snapshot restoration is incomplete."), {}
                )
            raise
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._fail(error, {})
            raise
        finally:
            self._maintenance = False
            self._maintenance_task = None
            self._publish_resources()
            if self._resume_resources is not None:
                self._resume_resources()

    async def _activate_restored_state(self, state: RunnerState) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._error = None
        self._stop_requested = False
        self._termination_confirmed = True
        self._pending_advance = state.pending_advance
        self._last_attempt = self._last_response = None
        self._desired_mode = "paused"
        self._continue_source = None
        self._recover_live = False
        self._wake.clear()
        self._ready.clear()
        self._save_state()
        self._task = asyncio.create_task(
            self._advance_dag(), name=f"dag:{state.experiment_id}"
        )

    async def recover(self, experiment_id: str) -> None:
        await self._prepare_recovery_selection(experiment_id)
        registry = read_json(self._project_root / "experiments.json")
        try:
            folder = RegistryEntry.model_validate(
                {"folder": registry.get(experiment_id)}
            ).folder
        except (ValueError, TypeError) as error:
            raise FileNotFoundError(f"Unknown experiment: {experiment_id}") from error
        root = (self._project_root / "experiments" / folder).resolve()
        if not root.is_relative_to((self._project_root / "experiments").resolve()):
            raise ValueError("Experiment registry path escapes the project.")
        marker = (
            self._project_root / "controller/restore_transactions" / f"{folder}.json"
        )
        transaction = read_json(marker) if marker.is_file() else None
        state = await self._load_recovery_state(root, experiment_id, transaction)
        await self._services.close()
        await self._stages.close()
        self._journal.close()
        self._state = state
        self._requested_id = experiment_id
        self._desired_mode = "paused"
        self._continue_source = None
        self._last_attempt = self._last_response = None
        self._error = None
        self._maintenance = True
        self._maintenance_task = asyncio.current_task()
        try:
            if transaction is not None and transaction["phase"] != "complete":
                if (
                    self._resource_observer is not None
                    and self._suspend_resources is None
                ):
                    raise RuntimeError(
                        "Resource observer must provide a restoration barrier."
                    )
                source = (
                    self._project_root / "experiments" / transaction["source_folder"]
                    if transaction["clone"]
                    else None
                )
                await self._snapshots.restore(
                    state,
                    transaction["snapshot_id"],
                    source_directory=source,
                    suspend_resources=self._suspend_resources,
                    resume_transaction=True,
                )
                self._recover_live = False
            else:
                self._journal.open(state, create=False)
                latest, launches = await _read_recovery_evidence(
                    self._journal.client, experiment_id
                )
                self._adopt_recovery_checkpoint(state, root, latest)
                rebuilding = state.pending_rebuild is not None
                if rebuilding:
                    await self._recover_interrupted_rebuild(state)
                    launches = []
                if launches:
                    self._recover_launched_attempt(state, root, launches[-1])
                self._journal.close()
                self._journal.open(state, create=False)
                self._recover_live = not rebuilding
            await self._start_recovered_dag(state, experiment_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._error is None:
                await self._fail(error, {})
            raise
        finally:
            self._maintenance = False
            self._maintenance_task = None
            self._publish_resources()
            if self._resume_resources is not None:
                self._resume_resources()

    async def _prepare_recovery_selection(self, experiment_id: str) -> None:
        if self._closed or self._maintenance:
            raise RuntimeError("Runner is closed or already restoring an experiment.")
        if (
            self._state is not None
            and self._state.pending_rebuild is not None
            and self._state.experiment_id != experiment_id
        ):
            raise RuntimeError(
                "Recover the selected experiment's pending rebuild before "
                "switching to another experiment."
            )
        if self._task is not None and not self._task.done():
            if (
                self._requested_id != experiment_id
                or self._state is None
                or self._state.mode != "paused"
                or self._state.active_attempt is None
                or self._state.active_attempt.outcome != "unknown"
            ):
                raise RuntimeError(
                    "Stop or detach the selected experiment before recovery."
                )
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _load_recovery_state(
        self, root: Path, experiment_id: str, transaction: JsonObject | None
    ) -> RunnerState:
        if transaction is not None and transaction.get("phase") != "complete":
            state = state_from_document(root, transaction["stopped_state"])
        else:
            try:
                state = self._state_store.load(root)
            except (FileNotFoundError, ValueError, TypeError, KeyError) as state_error:
                state = await _read_recovery_checkpoint(
                    root, experiment_id, state_error
                )
        if state.experiment_id != experiment_id:
            raise ValueError("Saved state belongs to a different experiment.")
        if state.template_path != root / "experiment.yaml":
            raise ValueError("Saved template path is outside the experiment.")
        _, applied = self._assembler.load_template(
            state.template_path, template_yaml=state.template_yaml
        )
        if applied != state.template.model_dump(exclude_unset=True):
            raise ValueError("Saved template JSON differs from its applied YAML.")
        if (
            transaction is None
            and state.pending_rebuild is None
            and set(state.services)
            != {item.service_id for item in state.template.services}
        ):
            raise RuntimeError(
                "Saved service ownership is incomplete; recover participant metadata before continuing."
            )
        owner = state.owner_identity
        if owner is not None and owner.pid != os.getpid():
            try:
                if process_identity(owner.pid) == owner.model_dump():
                    try:
                        psutil.Process(owner.pid).wait(timeout=0)
                    except psutil.TimeoutExpired as error:
                        raise RuntimeError(
                            "The previous experiment owner is still alive."
                        ) from error
            except psutil.NoSuchProcess:
                pass
            except OSError as error:
                if not isinstance(
                    error, (FileNotFoundError, ProcessLookupError)
                ) and getattr(error, "winerror", None) not in (87, 1168):
                    raise
        return state

    def _adopt_recovery_checkpoint(
        self, state: RunnerState, root: Path, latest: JsonObject | None
    ) -> None:
        if latest is not None and (
            latest["checkpoint_id"] != state.checkpoint_id
            or state.pending_rebuild is not None
            or latest["pending_rebuild"] is not None
        ):
            # The mandatory journal is authoritative for DAG progress; the
            # file may contain later participant observations with that cursor.
            committed = state_from_document(root, latest)
            if (
                state.run_id == committed.run_id
                and state.pending_rebuild is None
                and committed.pending_rebuild is None
            ):
                # A file saved during rebuild may still own the replaced
                # instances even when it already has the committed run ID.
                committed.services = state.services
            vars(state).update(vars(committed))

    async def _recover_interrupted_rebuild(self, state: RunnerState) -> None:
        if self._resource_observer is not None and self._suspend_resources is None:
            raise RuntimeError(
                "Reload recovery requires a resource restoration barrier."
            )
        # Startup may have published ownership after the last checkpoint.
        # Never assume that a missing PID proves no process was launched.
        for sid, instance in state.services.items():
            self._retire_rebuild_requests(state, sid, instance)
            if instance.stopped:
                continue
            await self._recover_service_process(state, sid, instance)
        operation_id = state.pending_rebuild.operation_id
        self._journal.client.record_event(
            "control.reconciled",
            {
                "action": "reload_template",
                "state": "interrupted",
            },
            context={
                "experiment_id": state.experiment_id,
                "run_id": state.run_id,
                "parent_operation_id": operation_id,
            },
        )
        await self._snapshots.restore(
            state,
            state.pending_rebuild.snapshot_id,
            preserve_rebuild_diagnostics=True,
            suspend_resources=self._suspend_resources,
        )
        self._journal.client.record_event(
            "control.reconciled",
            {
                "action": "reload_template",
                "state": "rolled_back",
            },
            context={
                "experiment_id": state.experiment_id,
                "run_id": state.run_id,
                "parent_operation_id": operation_id,
            },
        )
        self._pending_advance = state.pending_advance
        self._save_state()

    def _retire_rebuild_requests(
        self, state: RunnerState, sid: str, instance: ServiceInstance
    ) -> None:
        # The accepted result can be newer than the queue checkpoint.
        # Retire it without issuing a conflicting cancellation outcome.
        requests = [*instance.pending_requests]
        if instance.active_request is not None:
            requests.append(instance.active_request)
        for request in requests:
            accepted = self._journal.client.read_command_result(request.request_id)
            if accepted is None or accepted["author"] != "runner":
                continue
            context = accepted["event"]["context"]
            expected_instance = (
                request.service_instance_id
                or (request.model_extra or {}).get("expected_instance")
                or instance.service_instance_id
            )
            if (
                context.get("experiment_id") != state.experiment_id
                or context.get("participant_id") != sid
                or context.get("participant_instance_id") != expected_instance
            ):
                raise ValueError(
                    "Rebuild request result belongs to another participant."
                )
            if instance.active_request is request:
                instance.active_request = None
            else:
                instance.pending_requests.remove(request)
            self._journal.client.record_event(
                "control.reconciled",
                {
                    "action": "retire_rebuild_request",
                    "request_id": request.request_id,
                    "result_event_id": accepted["event_id"],
                    "outcome": accepted["outcome"],
                },
                context={
                    "experiment_id": state.experiment_id,
                    "run_id": state.run_id,
                    "parent_operation_id": state.pending_rebuild.operation_id,
                },
            )

    def _recover_launched_attempt(
        self, state: RunnerState, root: Path, launched: JsonObject
    ) -> None:
        if (
            state.active_attempt is not None
            and state.active_attempt.attempt_id == launched["attempt_id"]
        ):
            return
        definition = next(
            item
            for item in state.template.stages
            if item.stage_id == launched["stage_id"]
        )
        directory = (
            root
            / "shared_artifacts"
            / f"epoch_{launched['cycle_number']}"
            / self._assembler._module_reference(state.template, definition).name
            / launched["stage_id"]
            / f"attempt_{launched['attempt_number']}"
        )
        attempt = _attempt_from_launch(definition, launched, directory, root)
        state.active_attempt = attempt
        context = read_json(directory / "context.json")
        if context["context"]["attempt_id"] != launched["attempt_id"]:
            raise ValueError("Saved launch context has a different attempt identity.")
        _apply_recovered_attempt_context(attempt, context)
        record_path = directory / "process.json"
        if record_path.exists():
            record = read_json(record_path)
            if record.get("attempt_id") != attempt.attempt_id:
                raise ValueError(
                    "Saved process record has a different attempt identity."
                )
            attempt.process_identity = record["stage"]
            attempt.started_at = record["started_at"]
        state.active_attempt = attempt
        state.stage_attempt_numbers[attempt.stage_id] = attempt.attempt_number

    async def _start_recovered_dag(self, state: RunnerState, experiment_id: str) -> None:
        self._pending_advance = state.pending_advance
        self._stop_requested = False
        self._termination_confirmed = True
        state.mode = "paused"
        state.pause_requested = True
        self._wake.clear()
        self._ready.clear()
        if (
            state.phase in ("completed", "stopped", "failed")
            and state.active_attempt is None
            and all(item.stopped for item in state.services.values())
        ):
            self._ready.set()
            self._idle.set()
            return
        # The DAG now owns startup failure handling. It must not cancel the
        # recovery caller that is waiting for its readiness notification.
        self._maintenance_task = None
        self._task = asyncio.create_task(
            self._advance_dag(), name=f"dag:{experiment_id}"
        )
        await self._ready.wait()
        if self._error is not None:
            raise RuntimeError(self._error["message"])

    async def _recover_service_process(
        self, state: RunnerState, sid: str, instance: ServiceInstance
    ) -> None:
        process_file = (
            state.experiment_directory
            / "shared_artifacts/services"
            / sid
            / instance.service_instance_id
            / "process.json"
        )
        endpoint = instance.endpoint_path
        announced = (
            read_json(endpoint) if endpoint is not None and endpoint.is_file() else None
        )
        record = (
            read_json(process_file)
            if process_file.is_file()
            else announced
            if instance.process_identity is None
            else None
        )
        if record is not None:
            if (
                record.get("experiment_id") != state.experiment_id
                or record.get("participant_id") != sid
                or record.get("participant_instance_id") != instance.service_instance_id
            ):
                raise RuntimeError("Rebuild participant ownership cannot be verified.")
            instance.process_identity = record["process"]
        if (
            announced is not None
            and announced.get("participant_instance_id") == instance.service_instance_id
            and announced.get("process") != instance.process_identity
        ):
            await self._recover_service_child(
                state, sid, instance, process_file, announced, record
            )
        if instance.process_identity is not None:
            try:
                instance.stopped = (
                    process_identity(instance.process_identity["pid"])
                    != instance.process_identity
                )
                if not instance.stopped:
                    process = psutil.Process(instance.process_identity["pid"])
                    instance.stopped = process.status() == psutil.STATUS_ZOMBIE
                    if not instance.stopped:
                        try:
                            process.wait(timeout=0)
                            instance.stopped = True
                        except psutil.TimeoutExpired:
                            pass
            except (
                FileNotFoundError,
                ProcessLookupError,
                psutil.NoSuchProcess,
            ):
                instance.stopped = True
            except OSError as error:
                if getattr(error, "winerror", None) not in (87, 1168):
                    raise
                instance.stopped = True

    async def _recover_service_child(
        self,
        state: RunnerState,
        sid: str,
        instance: ServiceInstance,
        process_file: Path,
        announced: JsonObject,
        record: JsonObject | None,
    ) -> None:
        # Before readiness, process.json still names the
        # launcher; the endpoint may name its child service.
        launched = instance.process_identity
        declared = announced.get("process")
        if (
            announced.get("experiment_id") != state.experiment_id
            or announced.get("participant_id") != sid
            or launched is None
            or not isinstance(declared, dict)
        ):
            raise RuntimeError("Rebuild endpoint and saved process ownership disagree.")
        child_alive = False
        try:
            if process_identity(declared["pid"]) != declared:
                raise RuntimeError("Rebuild endpoint process identity has changed.")
            child = psutil.Process(declared["pid"])
            if child.status() != psutil.STATUS_ZOMBIE:
                try:
                    child.wait(timeout=0)
                except psutil.TimeoutExpired:
                    child_alive = True
            if child_alive:
                try:
                    ancestors = await asyncio.to_thread(child.parents)
                except psutil.NoSuchProcess as error:
                    if error.pid != declared["pid"]:
                        raise RuntimeError(
                            "Rebuild endpoint ancestry cannot be verified."
                        ) from error
                    raise
                except OSError as error:
                    raise RuntimeError(
                        "Rebuild endpoint ancestry cannot be verified."
                    ) from error
                if process_identity(declared["pid"]) != declared:
                    raise RuntimeError("Rebuild endpoint process identity has changed.")
        except (
            FileNotFoundError,
            ProcessLookupError,
            psutil.NoSuchProcess,
        ):
            child_alive = False
        except OSError as error:
            if getattr(error, "winerror", None) not in (87, 1168):
                raise
            child_alive = False
        # A departed child needs no ancestry proof. Keep the
        # launcher identity so its shutdown is still required.
        if child_alive:
            if (
                launched["pid"] not in {p.pid for p in ancestors}
                or process_identity(launched["pid"]) != launched
            ):
                raise RuntimeError(
                    "Rebuild endpoint is not owned by the launched process."
                )
            # Persist both identities before shutdown: a
            # second recovery must still wait for launcher
            # cleanup without needing live ancestry again.
            write_json(
                process_file,
                {
                    **(record or announced),
                    "process": declared,
                    "launcher_process": (record or {}).get(
                        "launcher_process", launched
                    ),
                },
            )
            instance.process_identity = declared
            self._journal.client.record_event(
                "control.reconciled",
                {
                    "action": "rebuild_service_process",
                    "launched_process": launched,
                    "participant_process": declared,
                },
                context={
                    "experiment_id": state.experiment_id,
                    "run_id": state.run_id,
                    "participant_id": sid,
                    "participant_instance_id": instance.service_instance_id,
                    "parent_operation_id": state.pending_rebuild.operation_id,
                },
            )

    async def _fail(self, error: Exception, context: JsonObject) -> None:
        self._error = {"type": type(error).__name__, "message": str(error)}
        self._stop_requested = True
        await self._cancel_control_tasks()
        if self._state is not None:
            if self._stage_task is not None and not self._stage_task.done():
                self._stage_task.cancel()
                await asyncio.gather(self._stage_task, return_exceptions=True)
            try:
                self._termination_confirmed = await self._stages.interrupt(
                    self._state, "failure"
                )
                if not self._termination_confirmed:
                    self._error["interruption_error"] = (
                        "Stage termination is unconfirmed."
                    )
            except Exception as interruption_error:  # noqa: BLE001 - Preserve both original and shutdown failures.
                self._termination_confirmed = False
                self._error["interruption_error"] = str(interruption_error)
            try:
                results = await self._services.stop_all(self._state)
                self._termination_confirmed = all(
                    result["stopped"] for result in results.values()
                ) and (
                    self._termination_confirmed
                    or (
                        self._state.active_attempt is not None
                        and self._state.active_attempt.service_id is not None
                        and self._stages.termination_confirmed(self._state)
                    )
                )
                if any(
                    not result["stopped"] or result["error"]
                    for result in results.values()
                ):
                    self._error["service_shutdown"] = results
            except Exception as service_error:  # noqa: BLE001 - Shutdown failures must not hide the experiment failure.
                self._termination_confirmed = False
                self._error["service_shutdown_error"] = (
                    f"{type(service_error).__name__}: {service_error}"
                )
            finally:
                await self._services.close()
            self._state.phase = "failed"
            self._publish_resources()
            try:
                self._journal.client.record_error(
                    error, context={"experiment_id": self._state.experiment_id}
                )
                self._save_state()
            except Exception as logging_error:  # noqa: BLE001 - Emergency storage must not recurse into the logger.
                write_json(
                    self._project_root / "controller" / f"failure-{uuid4()}.json",
                    {**self._error, "logging_error": str(logging_error)},
                )
        else:
            write_json(
                self._project_root / "controller" / f"failure-{uuid4()}.json",
                {**self._error, "experiment_id": self._requested_id},
            )
        if self._step_future is not None:
            if not self._step_future.done():
                self._step_future.set_exception(error)
            self._step_future = None
        if self._notify is not None:
            self._notify(self.get_state())

    def _require_active(self) -> RunnerState:
        if self._state is not None and self._state.pending_rebuild is not None:
            raise RuntimeError(
                "Recover the unfinished template reload before controlling the DAG."
            )
        if self._state is None or self._state.phase in (
            "stopped",
            "completed",
            "failed",
        ):
            raise RuntimeError("The experiment is not active.")
        return self._state

    def _save_state(self) -> None:
        self._state.pending_advance = self._pending_advance
        self._state.checkpoint_id = str(uuid4())
        self._state.owner_identity = ProcessIdentity.model_validate(
            process_identity(os.getpid())
        )
        _record_runner_checkpoint(self._journal.client, self._state)
        self._publish_resources()
        try:
            self._state_store.save(self._state)
        except OSError as error:
            self._journal.client.record_error(
                error, context={"experiment_id": self._state.experiment_id}
            )
        self._journal.client.record_event(
            "experiment.state",
            self.get_state(),
            context={
                "experiment_id": self._state.experiment_id,
                "run_id": self._state.run_id,
            },
        )
        if self._notify is not None:
            self._notify(self.get_state())

    def set_resource_observer(
        self,
        observer: Callable[[JsonObject], None] | None,
        *,
        suspend: Callable[[], Awaitable[None]] | None = None,
        resume: Callable[[], None] | None = None,
    ) -> None:
        """Report target changes; the application controller owns their observation."""
        self._resource_observer = observer
        self._suspend_resources = suspend
        self._resume_resources = resume
        self._publish_resources()

    def _publish_resources(self) -> None:
        if self._resource_observer is None:
            return
        try:
            self._resource_observer(self.get_resource_snapshot())
            self._resource_error = None
        except Exception as error:  # noqa: BLE001 - Optional observers cannot change execution policy.
            self._resource_error = f"{type(error).__name__}: {error}"

    def get_resource_snapshot(self) -> JsonObject:
        """Describe actual targets without sampling the OS or exposing mutable state."""
        state = self._state
        path = self._journal.reader_config_path
        if (
            state is None
            or path is None
            or self._closed
            or state.phase in ("completed", "stopped", "failed", "restoring")
        ):
            return {"context": {}, "logging_config_path": None, "targets": []}
        context = {
            "experiment_id": state.experiment_id,
            "run_id": state.run_id,
            "cycle_number": state.cycle_number,
            "template_revision_id": state.template_revision_id,
        }
        targets = []
        attempt = state.active_attempt
        if (
            attempt is not None
            and attempt.service_id is None
            and attempt.process_identity is not None
            and not (
                attempt.executor_status.finished
                if isinstance(
                    attempt.executor_status,
                    (
                        ExecutorCommandState,
                        RetainedExecutorStatus,
                        ServiceCallExecutorStatus,
                    ),
                )
                else (attempt.executor_status or {}).get("finished", False)
            )
        ):
            definition = next(
                item
                for item in state.template.stages
                if item.stage_id == attempt.stage_id
            )
            module = self._assembler._module_reference(state.template, definition)
            targets.append(
                {
                    "series_id": attempt.attempt_id,
                    "identity": dict(attempt.process_identity),
                    "context": {
                        **context,
                        "stage_id": attempt.stage_id,
                        "stage_execution_id": attempt.stage_execution_id,
                        "attempt_id": attempt.attempt_id,
                        "attempt_number": attempt.attempt_number,
                        "module_name": module.name,
                        "module_version": module.version,
                        "module_hash": module.hash,
                    },
                }
            )
        for instance in state.services.values():
            if instance.stopped or instance.process_identity is None:
                continue
            module = instance.definition.module
            targets.append(
                {
                    "series_id": instance.service_instance_id,
                    "identity": dict(instance.process_identity),
                    "context": {
                        **context,
                        "service_id": instance.service_id,
                        "service_instance_id": instance.service_instance_id,
                        "module_name": module.name,
                        "module_version": module.version,
                        "module_hash": module.hash,
                    },
                }
            )
        return {
            "context": context,
            "logging_config_path": str(path),
            "targets": targets,
        }

    def get_state(self) -> JsonObject:
        state = self._state
        attempt = (
            state.active_attempt
            if state is not None and state.active_attempt is not None
            else self._last_attempt
        )
        if state is not None:
            phase = state.phase
            if (
                phase == "completed"
                and self._task is not None
                and not self._task.done()
            ):
                # finalize serializes a terminal checkpoint before publishing its
                # snapshot. Public completion must wait for publication/teardown.
                phase = "snapshotting"
        elif self._error is not None:
            phase = "failed"
        elif self._stop_requested and self._requested_id is not None:
            phase = "stopped"
        elif self._task is not None and not self._task.done():
            phase = "starting"
        else:
            phase = "idle"
        services = []
        if state is not None:
            for position, definition in enumerate(state.template.services, 1):
                instance = state.services.get(definition.service_id)
                services.append(_service_state(position, definition, instance))
        return {
            "experiment_id": self._requested_id,
            "phase": phase,
            "mode": state.mode if state is not None else self._desired_mode,
            "cycle_number": None if state is None else state.cycle_number,
            "stage_position": None if state is None else state.stage_position,
            "stage_result_ids": {} if state is None else dict(state.stage_result_ids),
            "pending_advance": self._pending_advance if state is not None else False,
            "active_attempt_id": None
            if state is None or state.active_attempt is None
            else state.active_attempt.attempt_id,
            "pending_input": None
            if state is None or state.pending_input is None
            else state.pending_input.model_dump(exclude_unset=True),
            "dag_decision": None
            if state is None or state.last_dag_decision is None
            else state.last_dag_decision.model_dump(exclude_unset=True),
            "attempt_id": None if attempt is None else attempt.attempt_id,
            "executor": None
            if attempt is None
            else _executor_status_document(attempt.executor_status),
            "result": None if state is None else state.last_result,
            "error": self._error,
            "source": "runner",
            "fresh": not self._closed,
            "observed_at": datetime.now(UTC).isoformat(),
            "services": copy_json_object({"items": services}, "service state")["items"],
            "termination_confirmed": self._termination_confirmed,
            "resource_observer_error": self._resource_error,
            "stable_snapshot_id": None if state is None else state.stable_snapshot_id,
            "pending_rebuild": None
            if state is None or state.pending_rebuild is None
            else state.pending_rebuild.model_dump(exclude_unset=True),
            "template_revision_id": None
            if state is None
            else state.template_revision_id,
            "run_id": None if state is None else state.run_id,
            "snapshot": self._last_snapshot,
        }

    async def read_events(
        self,
        experiment_id: str,
        checkpoint: JsonObject | None = None,
        *,
        limit: int = 100,
    ) -> JsonObject:
        if self._state is None or experiment_id != self._state.experiment_id:
            raise FileNotFoundError(
                "The selected experiment journal is not open in this runner."
            )
        return await asyncio.to_thread(
            self._journal.client.read_events, checkpoint, limit=limit
        )

    async def close(self) -> None:
        self._closed = True
        self._publish_resources()
        if (
            self._maintenance_task is not None
            and self._maintenance_task is not asyncio.current_task()
            and not self._maintenance_task.done()
        ):
            self._maintenance_task.cancel()
            await asyncio.gather(self._maintenance_task, return_exceptions=True)
        if self._task is not None and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        await self._stages.close()
        await self._services.close()
        if self._step_future is not None:
            self._step_future.cancel()
            self._step_future = None
        if self._state is not None and self._journal.reader_config_path is not None:
            marker = (
                self._project_root
                / "controller/restore_transactions"
                / f"{self._state.experiment_directory.name}.json"
            )
            if not marker.exists() or read_json(marker).get("phase") == "complete":
                self._state.owner_identity = None
                self._state.checkpoint_id = str(uuid4())
                try:
                    _record_runner_checkpoint(self._journal.client, self._state)
                    self._state_store.save(self._state)
                except (LoggingError, OSError) as error:
                    write_json(
                        self._project_root
                        / "controller"
                        / f"owner-release-{uuid4()}.json",
                        {
                            "experiment_id": self._state.experiment_id,
                            "error": str(error),
                        },
                    )
        await asyncio.to_thread(self._journal.close)
