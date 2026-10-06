"""Runner-side service supervision and persistent working-request queues.

External service internals
belong to their Python proxies. Global pause/stop decisions return to the runner."""

from __future__ import annotations

import asyncio
import subprocess
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

import psutil

from core.experiments.journal import RunnerJournal
from core.experiments.launch import ModuleLauncher
from core.experiments.reload import _definition_fingerprint
from core.experiments.results import read_result
from core.experiments.service_inputs import _context, _load_state_paths
from core.experiments.state import (
    RunnerState,
    RunnerStateStore,
    ServiceInstance,
    _process_identity_document,
    state_to_document,
)
from core.journal.events import LoggingError
from core.models.experiment_template import (
    ErrorPolicy,
    ExperimentTemplate,
    ServiceCallDefinition,
    ServiceDefinition,
)
from core.models.module_manifest import ModuleManifest
from core.models.participant_identity import ParticipantIdentity
from core.models.participant_launch import ModulePreparation, PreparedLaunch
from core.models.participant_observations import (
    CommandState,
    CommandStateResponse,
    RetainedServiceStatus,
    ServiceObservation,
    ServiceStateExport,
)
from core.models.participant_protocol import ParticipantResult
from core.models.process_identity import ProcessIdentity
from core.models.runner_state import ServiceFailureDetails, WorkingServiceRequest
from core.models.updates import _update_model
from core.participants.connection import ParticipantConnection
from core.participants.protocol import PROTOCOL_VERSION
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import JsonObject, copy_json_object, require_text
from core.primitives.processes import module_process_arguments, process_identity

type ServiceAction = Literal["ready", "pause", "stop"]


class ServiceManager:
    """Own service connections and mutate only the service slice of runner state.

    start_all starts observation during readiness checks. The owner awaits monitor
    for a policy action and schedules it again after applying pause/stop. Pausing
    the DAG must not suspend this observation. close detaches; stop_all shuts down.
    The owner resets restart counts at cycle boundaries and coordinates snapshots.
    """

    _launcher: ModuleLauncher
    _journal: RunnerJournal
    _state_store: RunnerStateStore
    _connections: dict[str, ParticipantConnection]

    def __init__(
        self,
        launcher: ModuleLauncher,
        journal: RunnerJournal,
        state_store: RunnerStateStore,
        *,
        notify_resources: Callable[[], None] | None = None,
    ) -> None:
        """Initialize supervision queues and bind caller-owned launch/journal/state services.

        Args:
            launcher: Module preparation service.
            journal: Runner journal for service intents and accepted outcomes.
            state_store: Runner checkpoint persistence.
            notify_resources: Optional callback publishing changed process targets.
        """
        self._launcher = launcher
        self._journal = journal
        self._state_store = state_store
        self._notify_resources = notify_resources
        self._connections = {}
        self._processes: dict[str, subprocess.Popen] = {}
        self._launch_processes: list[subprocess.Popen] = []
        self._restarts: dict[str, asyncio.Task] = {}
        self._connecting: dict[str, asyncio.Task] = {}
        self._probes: dict[str, JsonObject] = {}
        self._next_probe: dict[str, float] = {}
        self._bad_replies: dict[str, int] = {}
        self._waiters: dict[str, asyncio.Future] = {}
        self._sends: dict[str, tuple[str, asyncio.Task]] = {}
        self._pending_action: Literal["pause", "stop"] | None = None
        self._starting: set[str] = set()
        self._changed = asyncio.Event()
        self._monitor_task: asyncio.Task | None = None
        self._snapshot_id: str | None = None
        self._frozen_instances: dict[str, str] = {}
        self._closed = False

    async def start_all(self, state: RunnerState) -> ServiceAction:
        """Start declared services in order and wait for readiness at each startup boundary.

        Args:
            state: Runner state without existing service instances.

        Returns:
            Ready when all services start, or a pause/stop policy action.

        Raises:
            RuntimeError: The manager is closed, already monitoring, or already owns services.

        Startup exceptions trigger service shutdown before propagating.
        """
        if (
            self._closed
            or state.services
            or self._monitor_task is not None
            and not self._monitor_task.done()
        ):
            raise RuntimeError(
                "start_all requires an open manager and no existing services."
            )
        definitions = state.template.services
        if type(definitions) is not list:
            raise TypeError("services must be an array.")
        identifiers = [item.service_id for item in definitions]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("Service definition IDs must be unique.")
        if not definitions:
            return "ready"
        self._monitor_task = asyncio.create_task(self.monitor(state))
        try:
            for definition in definitions:
                action = await self._start_with_recovery(state, definition)
                if action != "ready":
                    return action
                # Later definitions are intentionally not launched yet. Only
                # this startup prefix participates in the intermediate barrier.
                action = await self.wait_ready(state, service_ids=set(state.services))
                if action != "ready":
                    return action
        except BaseException as error:
            try:
                stopped = await self.stop_all(state)
                if any(not result["stopped"] for result in stopped.values()):
                    error.add_note(
                        "Some services could not be confirmed stopped after startup failure."
                    )
            except Exception as cleanup_error:  # noqa: BLE001 - Keep the startup error and cleanup diagnostics.
                error.add_note(f"Service startup cleanup failed: {cleanup_error}")
            self._monitor_task.cancel()
            await asyncio.gather(self._monitor_task, return_exceptions=True)
            raise
        return "ready"

    async def _start_with_recovery(
        self, state: RunnerState, definition: ServiceDefinition
    ) -> ServiceAction:
        """Start one definition, applying automatic restart policy to launch/connection failure.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            definition: Validated stage/service definition with assigned stable
                identity.

        Returns:
            Ready after startup, or pause/stop from automatic restart policy.
        """
        try:
            await self._start(state, definition)
        except (OSError, ConnectionError) as error:
            instance = state.services.get(definition.service_id)
            if instance is None:
                raise
            instance.failure = ServiceFailureDetails(
                code="service_failure", message=f"{type(error).__name__}: {error}"
            )
            action = await self.restart(state, instance.service_id, automatic=True)
            return action
        return "ready"

    async def start(self, state: RunnerState, service_id: str) -> ServiceInstance:
        """Start only the selected service and wait for its first ready heartbeat.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.

        Returns:
            The selected ready ServiceInstance. An already-ready active instance is
            reused.

        Raises:
            RuntimeError: A snapshot barrier, concurrent startup, pending work, or
                unconfirmed previous shutdown blocks the launch.
            ValueError: The service ID is not declared.
        """
        if self._closed or self._snapshot_id is not None:
            raise RuntimeError(
                "Service start requires an open manager without a snapshot barrier."
            )
        definition = next(
            (item for item in state.template.services if item.service_id == service_id),
            None,
        )
        if definition is None:
            raise ValueError("Unknown service definition.")
        if service_id in self._starting or service_id in self._restarts:
            raise RuntimeError("Wait for the current service startup or restart.")
        instance = state.services.get(service_id)
        if instance is not None and not instance.stopped:
            if (
                instance.ready
                and not instance.stopping
                and not instance.manually_stopped
                and instance.blocked_action is None
            ):
                return instance
            raise RuntimeError(
                "Confirm service shutdown before starting another instance."
            )
        if instance is not None and (
            instance.active_request or instance.pending_requests
        ):
            raise RuntimeError("Resolve pending service work before starting it.")
        try:
            instance = await self._start(state, definition)
            if self._monitor_task is None or self._monitor_task.done():
                self._monitor_task = asyncio.create_task(self.monitor(state))
            return instance
        except BaseException as error:
            # A failed or cancelled launch must not leave an unsupervised process
            # or allow the monitor to undo the caller's cleanup.
            instance = state.services.get(service_id)
            if instance is not None:
                instance.manually_stopped = True
                try:
                    results = await self.stop_all(state, service_ids={service_id})
                    if (
                        not results[service_id]["stopped"]
                        or results[service_id]["error"]
                    ):
                        error.add_note(
                            f"Service startup cleanup failed: {results[service_id]}"
                        )
                    self._state_store.save(state)
                except Exception as cleanup_error:  # noqa: BLE001 - Preserve the launch failure and its cleanup diagnostics.
                    error.add_note(f"Service startup cleanup failed: {cleanup_error}")
            raise

    async def _start(
        self, state: RunnerState, definition: ServiceDefinition
    ) -> ServiceInstance:
        """Spawn a fresh service instance and require its first successful readiness heartbeat.

        Persists launch intent before spawning and retains process handles even if
        startup is cancelled. Endpoint processes may be verified children of the
        launcher. The original startup deadline bounds handshake and readiness
        together.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            definition: Validated stage/service definition with assigned stable
                identity.

        Returns:
            New service instance after endpoint ownership and the first readiness
            heartbeat are confirmed.
        """
        manifest = await self._launcher._assembler._check_module_async(
            state, definition.module, False
        )
        instance, directory, launch, context = self._prepare_service_start(
            state, definition, manifest
        )
        service_id = instance.service_id
        instance_id = instance.service_instance_id
        self._starting.add(service_id)
        instance.start_deadline = time.monotonic() + state.template.start_timeout
        spawn = None
        try:
            self._publish_service_start_intent(state, definition, launch, context)
            with (
                (directory / "stdout.log").open("ab") as stdout,
                (directory / "stderr.log").open("ab") as stderr,
            ):
                argv, environment = module_process_arguments(launch.argv)
                spawn = asyncio.create_task(
                    asyncio.to_thread(
                        subprocess.Popen,
                        argv,
                        cwd=launch.code_directory,
                        env=environment,
                        stdin=subprocess.DEVNULL,
                        stdout=stdout,
                        stderr=stderr,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                )
                try:
                    process = await asyncio.shield(spawn)
                except asyncio.CancelledError:
                    process = await spawn
                    self._processes[service_id] = process
                    self._launch_processes.append(process)
                    instance.process_identity = (
                        ProcessIdentity.model_validate(process_identity(process.pid))
                        if process.poll() is None
                        else None
                    )
                    raise
            self._processes[service_id] = process
            self._launch_processes.append(process)
            instance.started_at = datetime.now(UTC).isoformat()
            instance.process_identity = (
                ProcessIdentity.model_validate(process_identity(process.pid))
                if process.poll() is None
                else None
            )
            launcher_identity = instance.process_identity
            self._write_service_process(directory, instance, context, launcher_identity)
            self._save(state)
            await self._confirm_service_readiness(
                state, service_id, instance_id, instance, process, context
            )
            self._publish_service_start(
                state, directory, instance, context, launcher_identity
            )
            return instance
        except BaseException:
            if (
                spawn is None
                or spawn.done()
                and not spawn.cancelled()
                and spawn.exception() is not None
            ):
                instance.stopped = True
            raise
        finally:
            self._starting.discard(service_id)
            if self._notify_resources is not None:
                self._notify_resources()
            self._changed.set()

    def _prepare_service_start(
        self, state: RunnerState, definition: ServiceDefinition,
        manifest: ModuleManifest | None = None,
    ) -> tuple[ServiceInstance, Path, PreparedLaunch, JsonObject]:
        """Prepare a fresh instance/launch and inherit eligible requests from a stopped predecessor.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            definition: Validated stage/service definition with assigned stable
                identity.
            manifest: Already checked module manifest, or None to invoke the public
                preparation hook.

        Returns:
            New instance, artifact directory, prepared launch, and journal context,
            in that order.
        """
        service_id = require_text(definition.service_id, "service_id")
        old = state.services.get(service_id)
        if old is not None and not old.stopped:
            raise RuntimeError(
                "The previous service must be confirmed stopped before launch."
            )
        instance_id = str(uuid4())
        context = _context(state, service_id, instance_id)
        directory = (
            state.experiment_directory
            / "shared_artifacts/services"
            / service_id
            / instance_id
        )
        if (
            manifest is None
            or getattr(self._launcher.prepare, "__func__", None) is not ModuleLauncher.prepare
        ):
            launch = PreparedLaunch.model_validate(
                self._launcher.prepare(
                    state, definition.model_dump(exclude_unset=True), context,
                    directory, None,
                )
            )
        else:
            inputs = ModulePreparation(
                context=ParticipantIdentity.model_validate(context),
                artifacts_directory=directory, input_data=None,
            )
            launch = self._launcher._prepare_checked(state, definition, inputs, manifest)
        instance = ServiceInstance(service_id, instance_id, definition)
        instance.implementation = launch.module.implementation
        instance.artifacts_directory = directory
        instance.endpoint_path = launch.endpoint_path
        if old is not None:
            self._inherit_service_requests(state, old, instance)
        state.services[service_id] = instance
        return instance, directory, launch, context

    def _inherit_service_requests(
        self, state: RunnerState, old: ServiceInstance, instance: ServiceInstance
    ) -> None:
        """Transfer retries and unsent requests, invalidating work pinned to the old instance.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            old: Previously stopped service instance supplying retries and eligible
                unsent requests.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
        """
        instance.restart_count = old.restart_count
        for entry in old.pending_requests:
            if (entry.model_extra or {}).get("expected_instance") is not None:
                self._finish_request(
                    state,
                    old,
                    entry,
                    {
                        "result": "fail",
                        "data": {"reason": "service_instance_changed"},
                    },
                    "invalidated",
                )
            else:
                instance.pending_requests.append(entry)

    def _publish_service_start_intent(
        self,
        state: RunnerState,
        definition: ServiceDefinition,
        launch: PreparedLaunch,
        context: JsonObject,
    ) -> None:
        """Journal effective service parameters and start intent before spawning the process.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            definition: Validated stage/service definition with assigned stable
                identity.
            launch: Validated fixed participant launch inputs and runtime paths.
            context: Journal/participant coordinates associated with this operation.
        """
        self._journal.client.record_event(
            "service.parameters",
            {
                "definition": definition.model_dump(exclude_unset=True),
                "effective_settings": launch.effective_settings,
                "template_revision_id": state.template_revision_id,
            },
            context=context,
        )
        self._journal.client.record_event(
            "control.intent",
            {"action": "start_service", "argv": launch.argv},
            context=context,
        )
        if state.pending_rebuild is not None:
            # Retain the prospective instance even if the owner dies during spawn.
            self._save(state)

    def _write_service_process(
        self,
        directory: Path,
        instance: ServiceInstance,
        context: JsonObject,
        launcher_identity: ProcessIdentity | None,
    ) -> None:
        """Persist participant and launcher identities beside the instance's runtime artifacts.

        Args:
            directory: Writable artifact directory assigned to this service
                instance.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            context: Journal/participant coordinates associated with this operation.
            launcher_identity: Verified OS identity of the process that launched the
                service, or None before it is known.
        """
        write_json(
            directory / "process.json",
            {
                **context,
                "process": _process_identity_document(instance.process_identity),
                "launcher_process": _process_identity_document(launcher_identity),
                "started_at": instance.started_at,
            },
        )

    def _publish_service_start(
        self,
        state: RunnerState,
        directory: Path,
        instance: ServiceInstance,
        context: JsonObject,
        launcher_identity: ProcessIdentity | None,
    ) -> None:
        """Publish confirmed process metadata, journal startup, and save service state.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            directory: Writable service-instance artifact directory containing
                process.json.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            context: Journal/participant coordinates associated with this operation.
            launcher_identity: Verified OS identity of the process that launched the
                service, or None before it is known.
        """
        self._write_service_process(directory, instance, context, launcher_identity)
        self._journal.client.record_event(
            "service.started",
            {
                "process": _process_identity_document(instance.process_identity),
                "launcher_process": _process_identity_document(launcher_identity),
                "implementation": instance.implementation,
            },
            context=context,
        )
        self._save(state)

    async def _confirm_service_readiness(
        self,
        state: RunnerState,
        service_id: str,
        instance_id: str,
        instance: ServiceInstance,
        process: subprocess.Popen,
        context: JsonObject,
    ) -> None:
        """Verify endpoint ancestry, connect, and require readiness before the original deadline.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance_id: UUID of the particular service process launch.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            process: Owned process handle used to observe exit without trusting a
                bare PID.
            context: Journal/participant coordinates associated with this operation.

        Raises:
            ConnectionError: The launched process exits or the first heartbeat does
                not confirm readiness.
            TimeoutError: Startup exceeds its original deadline.
            ValueError: The endpoint process is not owned by the launched process.
        """
        while time.monotonic() < instance.start_deadline:
            if process.poll() is not None:
                raise ConnectionError("Service process exited before readiness.")
            try:
                endpoint = read_json(instance.endpoint_path)
            except (FileNotFoundError, PermissionError):
                await asyncio.sleep(0.05)
                continue
            if endpoint.get("participant_instance_id") != instance_id:
                await asyncio.sleep(0.05)
                continue
            declared = endpoint["process"]
            if declared != _process_identity_document(instance.process_identity):
                ancestors = await asyncio.to_thread(
                    psutil.Process(declared["pid"]).parents
                )
                if process.poll() is not None or process.pid not in {
                    parent.pid for parent in ancestors
                }:
                    raise ValueError(
                        "Service endpoint is not owned by the launched process."
                    )
            connection = ParticipantConnection(instance.endpoint_path, context)
            self._connections[service_id] = connection
            remaining = max(0.001, instance.start_deadline - time.monotonic())
            await connection.connect(timeout_seconds=remaining)
            remaining = max(0.001, instance.start_deadline - time.monotonic())
            instance.process_identity = ProcessIdentity.model_validate(declared)
            request_id = str(uuid4())
            state.used_request_ids.add(request_id)
            self._probes[service_id] = {
                "request_id": request_id,
                "sent_monotonic": time.monotonic(),
            }
            self._journal.client.record_event(
                "control.intent",
                {"action": "heartbeat", "request_id": request_id},
                context=context,
            )
            reply = await self._exchange(
                service_id, request_id, "heartbeat", {}, timeout=remaining
            )
            await self._handle_message(state, service_id, reply)
            if not instance.ready:
                raise ConnectionError(
                    instance.failure.message
                    if instance.failure
                    else "Service did not confirm full readiness."
                )
            break
        if not instance.ready:
            raise TimeoutError(
                f"Service {service_id} readiness exceeded start_timeout."
            )

    async def wait_ready(
        self, state: RunnerState, *, service_ids: set[str] | None = None
    ) -> ServiceAction:
        """Observe the readiness barrier while keeping service supervision active.

        Args:
            state: Runner state containing current service instances.
            service_ids: Required startup prefix, or None for all declared services.

        Returns:
            Ready, pause, or stop according to current readiness and service policies.

        Raises:
            RuntimeError: The manager closes before the barrier completes.
        """
        required = (
            {definition.service_id for definition in state.template.services}
            if service_ids is None
            else service_ids
        )
        while not self._closed:
            if self._pending_action is not None:
                action, self._pending_action = self._pending_action, None
                return action
            action = self._readiness_action(state, required)
            if action is not None:
                return action
            if self._monitor_task is None or self._monitor_task.done():
                if self._monitor_task is not None:
                    action = self._monitor_task.result()
                    if action in ("pause", "stop"):
                        return action
                self._monitor_task = asyncio.create_task(self.monitor(state))
            self._changed.clear()
            changed = asyncio.create_task(self._changed.wait())
            try:
                await asyncio.wait(
                    (changed, self._monitor_task), return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                changed.cancel()
                await asyncio.gather(changed, return_exceptions=True)
        raise RuntimeError("Service manager is closed.")

    def _readiness_action(
        self, state: RunnerState, required: set[str]
    ) -> ServiceAction | None:
        """Return the current ready/pause/stop decision, or None while readiness is unresolved.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            required: Service IDs that must exist for the requested readiness
                barrier.

        Returns:
            Ready if all required services are present and usable, pause/stop for a
            blocking policy, or None while startup is unresolved.
        """
        for instance in state.services.values():
            if instance.manually_stopped:
                continue
            if instance.blocked_action is not None:
                return instance.blocked_action
        if required - state.services.keys():
            return "pause"
        if all(
            instance.ready and not instance.stopping and not instance.stopped
            for instance in state.services.values()
            if not instance.manually_stopped
        ):
            return (
                "pause"
                if any(
                    instance.manually_stopped for instance in state.services.values()
                )
                else "ready"
            )
        return None

    async def monitor(self, state: RunnerState) -> Literal["pause", "stop"]:
        """Supervise heartbeats, queued work, and restarts until a DAG policy action is needed.

        Args:
            state: Mutable runner state whose service slice is supervised.

        Returns:
            Pause or stop requested by service policy; ordinary healthy monitoring continues.

        An existing monitor is shared. The owner must resume observation after handling
        its returned action, including while the DAG remains paused.
        """
        current = asyncio.current_task()
        if (
            self._monitor_task is not None
            and not self._monitor_task.done()
            and self._monitor_task is not current
        ):
            return await asyncio.shield(self._monitor_task)
        self._monitor_task = current
        while not self._closed:
            if self._pending_action is not None:
                action, self._pending_action = self._pending_action, None
                return action
            for request_id, (service_id, task) in list(self._sends.items()):
                if not task.done():
                    continue
                await self._handle_completed_send(state, request_id, service_id, task)
            for service_id, instance in list(state.services.items()):
                restart = self._restarts.get(service_id)
                if restart is not None:
                    if not restart.done():
                        continue
                    self._restarts.pop(service_id, None)
                    action = restart.result()
                    self._changed.set()
                    if action != "ready":
                        return action
                    continue
                if (
                    instance.manually_stopped
                    or instance.stopped
                    or instance.stopping
                    or service_id in self._starting
                ):
                    continue
                connecting = self._connecting.get(service_id)
                if connecting is not None and connecting.done():
                    self._connecting.pop(service_id, None)
                    try:
                        connecting.result()
                    except (OSError, EOFError, ValueError) as error:
                        instance.failure = ServiceFailureDetails(
                            code="connection_failed",
                            message=f"Reconnect failed: {error}",
                        )
                await self._poll_result(state, service_id)
                if (
                    not instance.ever_ready
                    and instance.start_deadline is not None
                    and time.monotonic() >= instance.start_deadline
                ):
                    instance.failure = ServiceFailureDetails(
                        code="startup_timeout",
                        message="The original service startup deadline expired.",
                    )
                process = self._processes.get(service_id)
                if (
                    process is not None
                    and instance.process_identity is not None
                    and process.pid == instance.process_identity.pid
                    and process.poll() is not None
                ):
                    instance.failure = ServiceFailureDetails(
                        code="process_exited",
                        message="Service process exited unexpectedly.",
                    )
                self._check_heartbeat_deadline(service_id, instance)
                if instance.failure is not None:
                    instance.ready = False
                    if instance.blocked_action is None:
                        self._restarts[service_id] = asyncio.create_task(
                            self.restart(state, service_id, automatic=True)
                        )
                    continue
                active = instance.active_request
                if (
                    active is not None
                    and active.owner != "caller"
                    and not active.timed_out
                    and time.monotonic() - active.sent_monotonic
                    >= instance.definition.command_timeout_seconds
                ):
                    action = await self._handle_timeout(
                        state, service_id, active.request_id
                    )
                    if action in ("wait", "stop"):
                        self._pending_action = None
                        return "pause" if action == "wait" else "stop"
                    self._restarts[service_id] = asyncio.create_task(
                        self.restart(state, service_id, automatic=True)
                    )
                    continue
                self._schedule_heartbeat(state, service_id, instance)
                if instance.ready and instance.blocked_action is None:
                    await self._send_next(state, service_id)
            await asyncio.sleep(0.05)
        raise asyncio.CancelledError

    async def _handle_completed_send(
        self, state: RunnerState, request_id: str, service_id: str, task: asyncio.Task
    ) -> None:
        """Process a reply or reconnect after a first communication failure.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            request_id: UUID correlating the admitted request and its eventual
                outcome.
            service_id: Stable service definition UUID.
            task: Completed send task whose result or communication failure is being
                consumed.
        """
        self._sends.pop(request_id, None)
        instance = state.services[service_id]
        try:
            await self._handle_message(state, service_id, task.result())
        except (OSError, EOFError, ValueError, TypeError, KeyError) as error:
            if service_id in self._connecting or instance.stopping or instance.stopped:
                return
            instance.ready = False
            self._probes.pop(service_id, None)
            self._bad_replies[service_id] = self._bad_replies.get(service_id, 0) + 1
            self._journal.client.record_error(
                error,
                context=_context(state, service_id, instance.service_instance_id),
            )
            if self._bad_replies[service_id] >= 2:
                instance.failure = ServiceFailureDetails(
                    code="connection_failed",
                    message=f"Participant communication failed: {error}",
                )
            else:
                self._connecting[service_id] = asyncio.create_task(
                    self._connections[service_id].connect(
                        timeout_seconds=instance.definition.heartbeat.grace_seconds
                    )
                )
                self._next_probe[service_id] = 0

    def _check_heartbeat_deadline(
        self, service_id: str, instance: ServiceInstance
    ) -> None:
        """Mark an already-ready service failed when its outstanding probe exceeds grace.

        Args:
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
        """
        probe = self._probes.get(service_id)
        if (
            probe is not None
            and instance.ever_ready
            and time.monotonic() - probe["sent_monotonic"]
            >= instance.definition.heartbeat.grace_seconds
        ):
            instance.failure = ServiceFailureDetails(
                code="heartbeat_timeout",
                message="Service heartbeat grace period expired.",
            )

    def _schedule_heartbeat(
        self, state: RunnerState, service_id: str, instance: ServiceInstance
    ) -> None:
        """Journal and send a due heartbeat using startup or steady-state timing limits.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
        """
        if (
            service_id not in self._connecting
            and service_id not in self._probes
            and time.monotonic() >= self._next_probe.get(service_id, 0)
        ):
            request_id = str(uuid4())
            state.used_request_ids.add(request_id)
            self._journal.client.record_event(
                "control.intent",
                {"action": "heartbeat", "request_id": request_id},
                context=_context(state, service_id, instance.service_instance_id),
            )
            self._probes[service_id] = {
                "request_id": request_id,
                "sent_monotonic": time.monotonic(),
            }
            self._sends[request_id] = (
                service_id,
                asyncio.create_task(
                    self._exchange(
                        service_id,
                        request_id,
                        "heartbeat",
                        {},
                        timeout=(
                            max(
                                0.001,
                                instance.start_deadline - time.monotonic(),
                            )
                            if not instance.ever_ready
                            and instance.start_deadline is not None
                            else instance.definition.heartbeat.grace_seconds
                        ),
                    )
                ),
            )

    async def restart(
        self, state: RunnerState, service_id: str, *, automatic: bool
    ) -> ServiceAction:
        """Stop the previous instance and restart it under manual or automatic retry policy.

        Args:
            state: Runner state containing the selected service.
            service_id: Service definition UUID.
            automatic: Whether to consume automatic restart budget and retry delays.

        Returns:
            Ready after startup, or pause/stop when policy or shutdown prevents restart.

        Eligible service-owned timed-out requests receive a new request ID after restart;
        caller-owned DAG calls are not silently replayed.
        """
        instance = state.services[service_id]
        if instance.manually_stopped:
            if automatic:
                return "pause"
            raise RuntimeError("Use service start to start a manually stopped service.")
        retry = instance.active_request
        retry = (
            retry
            if retry is not None
            and retry.timed_out
            and retry.owner != "caller"
            and instance.definition.on_command_timeout == "restart"
            else None
        )
        waiter = None if retry is None else self._waiters.pop(retry.request_id, None)
        while not self._closed:
            policy = instance.definition.errors
            exhausted = (
                automatic
                and instance.restart_count >= policy.retries
                and policy.on_exhausted != "skip"
            )
            try:
                stopped = await self.stop_all(
                    state, service_ids={service_id}, preserve_pending=True
                )
            except asyncio.CancelledError:
                if waiter is not None and not waiter.done():
                    waiter.cancel()
                raise
            if not stopped[service_id]["stopped"]:
                if waiter is not None and not waiter.done():
                    waiter.set_exception(
                        RuntimeError("Previous service termination is unconfirmed.")
                    )
                return "stop"
            if exhausted:
                return self._finish_exhausted_restart(
                    state, service_id, instance, policy, retry, waiter
                )
            if automatic:
                instance.restart_count += 1
                try:
                    await asyncio.sleep(policy.retry_delay_seconds)
                except asyncio.CancelledError:
                    if waiter is not None and not waiter.done():
                        waiter.cancel()
                    raise
            if state.services[service_id].manually_stopped:
                if waiter is not None and not waiter.done():
                    waiter.cancel()
                return "pause"
            try:
                instance = await self._start(state, instance.definition)
            except asyncio.CancelledError:
                if waiter is not None and not waiter.done():
                    waiter.cancel()
                raise
            except (OSError, ConnectionError) as error:
                instance = state.services[service_id]
                instance.failure = ServiceFailureDetails(
                    code="service_failure", message=f"{type(error).__name__}: {error}"
                )
                if not automatic:
                    if waiter is not None and not waiter.done():
                        waiter.set_exception(error)
                    raise
                continue
            if retry is not None:
                self._requeue_timed_out_request(
                    state, service_id, instance, retry, waiter
                )
            self._changed.set()
            return "ready"
        raise RuntimeError("Service manager is closed.")

    def _finish_exhausted_restart(
        self,
        state: RunnerState,
        service_id: str,
        instance: ServiceInstance,
        policy: ErrorPolicy,
        retry: WorkingServiceRequest | None,
        waiter: asyncio.Future | None,
    ) -> ServiceAction:
        """Publish retry exhaustion, resolve any timed-out waiter, and return the blocked action.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            policy: Validated automatic retry and exhaustion policy.
            retry: Timed-out service-owned request eligible for a fresh request ID
                after restart.
            waiter: Existing result future transferred or completed by the restart
                policy.

        Returns:
            The exhaustion action retained on the service instance, pause or stop.
        """
        instance.blocked_action = policy.on_exhausted
        if waiter is not None and not waiter.done():
            waiter.set_result(
                {
                    "request_id": retry.request_id,
                    "result": "fail",
                    "data": {"reason": "command_timeout"},
                }
            )
        try:
            self._state_store.save(state)
        except OSError as error:
            self._journal.client.record_error(
                error,
                context={
                    "experiment_id": state.experiment_id,
                    "service_id": service_id,
                },
            )
        self._changed.set()
        return instance.blocked_action

    def _requeue_timed_out_request(
        self,
        state: RunnerState,
        service_id: str,
        instance: ServiceInstance,
        retry: WorkingServiceRequest,
        waiter: asyncio.Future | None,
    ) -> None:
        """Queue a fresh request ID after restart and transfer the original waiter.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            retry: Timed-out service-owned request eligible for a fresh request ID
                after restart.
            waiter: Existing result future transferred or completed by the restart
                policy.
        """
        replacement = _update_model(
            retry,
            request_id=str(uuid4()),
            sent_monotonic=None,
            sent_at=None,
            service_instance_id=None,
            timed_out=False,
            retry_of=retry.request_id,
        )
        if replacement.request_id in state.used_request_ids:
            raise RuntimeError("Request ID collision.")
        state.used_request_ids.add(replacement.request_id)
        instance.pending_requests.insert(0, replacement)
        if waiter is not None:
            self._waiters[replacement.request_id] = waiter
        self._journal.client.record_event(
            "service.request_queued",
            replacement.model_dump(exclude_unset=True),
            context={
                "experiment_id": state.experiment_id,
                "run_id": state.run_id,
                "service_id": service_id,
            },
        )
        try:
            self._state_store.save(state)
        except OSError as error:
            self._journal.client.record_error(
                error,
                context={
                    "experiment_id": state.experiment_id,
                    "service_id": service_id,
                },
            )

    async def request(
        self, state: RunnerState, service_id: str, command: str, args: JsonObject
    ) -> JsonObject:
        """Queue service-owned work and wait for its result under active supervision.

        Args:
            state: Runner state containing the service queue.
            service_id: Target service UUID.
            command: Working command name.
            args: JSON command arguments.

        Returns:
            Correlated result of the admitted command.
        """
        request_id = str(uuid4())
        future = self.enqueue(
            state, service_id, request_id, command, args, owner="service"
        )
        monitor = self._monitor_task
        await asyncio.wait((future, monitor), return_when=asyncio.FIRST_COMPLETED)
        if not future.done():
            monitor.result()
        return await asyncio.shield(future)

    async def _send_next(self, state: RunnerState, service_id: str) -> None:
        """Expire queued requests, persist send ownership, and transmit the next ready request.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
        """
        instance = state.services[service_id]
        if (
            not instance.ready
            or instance.stopping
            or instance.active_request is not None
        ):
            return
        while instance.pending_requests:
            entry = instance.pending_requests[0]
            if (
                entry.deadline_monotonic is None
                or time.monotonic() < entry.deadline_monotonic
            ):
                break
            instance.pending_requests.pop(0)
            self._finish_request(
                state,
                instance,
                entry,
                {"result": "fail", "data": {"reason": "queue_timeout"}},
                "timed_out",
            )
            self._save(state)
        if not instance.pending_requests:
            return
        entry = instance.pending_requests[0]
        context = _context(state, service_id, instance.service_instance_id)
        self._journal.client.record_event(
            "control.intent",
            {"action": "service_request", **entry.model_dump(exclude_unset=True)},
            context=context,
        )
        entry = _update_model(
            entry,
            service_instance_id=instance.service_instance_id,
            sent_at=datetime.now(UTC).isoformat(),
            sent_monotonic=time.monotonic(),
        )
        instance.pending_requests.pop(0)
        instance.active_request = entry
        self._save(state)
        self._journal.client.record_event(
            "service.send_started",
            entry.model_dump(exclude_unset=True),
            context=context,
        )
        self._sends[entry.request_id] = (
            service_id,
            asyncio.create_task(
                self._exchange(
                    service_id,
                    entry.request_id,
                    entry.command,
                    entry.args,
                    deadline=entry.deadline_monotonic,
                )
            ),
        )

    async def _handle_message(
        self, state: RunnerState, service_id: str, message: JsonObject
    ) -> None:
        """Validate an incoming service message and apply its observation.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            message: Raw service response to validate before applying any
                observation.
        """
        observation = ServiceObservation.model_validate(message)
        await self._handle_observation(state, service_id, observation)

    async def _handle_observation(
        self, state: RunnerState, service_id: str, observation: ServiceObservation
    ) -> None:
        """Journal a matching instance's reply, update service state, and notify waiters.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            observation: Validated participant or journal author observation.
        """
        instance = state.services[service_id]
        context = {
            "experiment_id": state.experiment_id,
            "run_id": state.run_id,
            "service_id": service_id,
            "service_instance_id": instance.service_instance_id,
            "participant_id": service_id,
            "participant_instance_id": instance.service_instance_id,
        }
        for key in ("experiment_id", "service_id", "service_instance_id"):
            if (observation.model_extra or {}).get(key, context[key]) != context[key]:
                self._journal.client.record_event(
                    "service.message_ignored",
                    {
                        "ignored": "different_instance",
                        "message": observation.model_dump(exclude_unset=True),
                    },
                    context=context,
                )
                return
        self._journal.client.record_event(
            "service.message",
            observation.model_dump(exclude_unset=True),
            context=context,
        )
        if observation.command == "heartbeat":
            if not self._handle_heartbeat(
                service_id, instance, observation, observation.request_id, context
            ):
                return
        elif not await self._handle_work_observation(
            state, service_id, instance, observation, context
        ):
            return
        try:
            self._state_store.save(state)
        except OSError as error:
            self._journal.client.record_error(error, context=context)
        self._changed.set()

    async def _handle_work_observation(
        self,
        state: RunnerState,
        service_id: str,
        instance: ServiceInstance,
        observation: ServiceObservation,
        context: JsonObject,
    ) -> bool:
        """Return False when retired work or restart must skip the final state save.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            observation: Validated participant or journal author observation.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            True when the caller should persist the updated service state; False for
            retired work or when a restart takes ownership.
        """
        request_id = observation.request_id
        active = instance.active_request
        if active is None or active.request_id != request_id:
            self._journal.client.record_event(
                "service.message_ignored",
                {
                    "ignored": "retired_or_unknown_request",
                    "message": observation.model_dump(exclude_unset=True),
                },
                context=context,
            )
            return False
        if (
            active.owner != "caller"
            and not active.timed_out
            and time.monotonic() - active.sent_monotonic
            >= instance.definition.command_timeout_seconds
        ):
            await self._handle_timeout(state, service_id, request_id)
            active = instance.active_request
            if active is None:
                return False
        if active.timed_out:
            self._journal.client.record_event(
                "service.message_ignored",
                {
                    "ignored": "command_timeout",
                    "message": observation.model_dump(exclude_unset=True),
                },
                context=context,
            )
            # A late result releases actual work, never changes its timed-out outcome.
            if (
                active.owner != "caller"
                and instance.definition.on_command_timeout == "restart"
            ):
                instance.failure = ServiceFailureDetails(
                    code="service_failure",
                    message="Timed-out command requires an explicit restart.",
                )
                self._restarts[service_id] = asyncio.create_task(
                    self.restart(state, service_id, automatic=True)
                )
                return False
            if instance.blocked_action == "pause":
                instance.blocked_action = None
        else:
            self._finish_request(
                state,
                instance,
                active,
                observation,
                "succeeded" if observation.result == "success" else "failed",
            )
        instance.active_request = None
        return True

    def _handle_heartbeat(
        self,
        service_id: str,
        instance: ServiceInstance,
        message: ServiceObservation,
        request_id: str,
        context: JsonObject,
    ) -> bool:
        """Return False for an old probe that must skip the caller's final save.

        Args:
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            message: Validated heartbeat observation returned by the participant.
            request_id: UUID correlating the admitted request and its eventual
                outcome.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            True after applying the current probe, including a failed/late
            heartbeat; False for a stale probe that must not trigger a state save.
        """
        probe = self._probes.get(service_id)
        if probe is None or probe["request_id"] != request_id:
            self._journal.client.record_event(
                "service.message_ignored",
                {
                    "ignored": "old_probe",
                    "message": message.model_dump(exclude_unset=True),
                },
                context=context,
            )
            return False
        if (
            not instance.ever_ready
            and instance.start_deadline is not None
            and time.monotonic() >= instance.start_deadline
        ):
            instance.failure = ServiceFailureDetails(
                code="startup_timeout",
                message="Service replied after its startup deadline.",
            )
            instance.ready = False
        elif (
            instance.ever_ready
            and time.monotonic() - probe["sent_monotonic"]
            >= instance.definition.heartbeat.grace_seconds
        ):
            instance.failure = ServiceFailureDetails(
                code="heartbeat_timeout",
                message="Service replied after heartbeat grace expired.",
            )
            instance.ready = False
        else:
            instance.last_status = RetainedServiceStatus.from_observation(
                message, datetime.now(UTC).isoformat(), time.monotonic()
            )
            instance.ready = message.result == "success"
            instance.ever_ready = instance.ever_ready or instance.ready
            instance.failure = (
                None
                if instance.ready
                else ServiceFailureDetails(
                    code="service_failure", message="Service requested a full restart."
                )
            )
            self._bad_replies[service_id] = 0
        self._probes.pop(service_id, None)
        self._next_probe[service_id] = (
            time.monotonic() + instance.definition.heartbeat.interval_seconds
        )
        return True

    async def _handle_timeout(
        self, state: RunnerState, service_id: str, request_id: str
    ) -> Literal["wait", "restart", "stop"]:
        """Accept a service-owned command timeout once and return wait/restart/stop policy.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            request_id: UUID correlating the admitted request and its eventual
                outcome.

        Returns:
            Wait for pause policy, otherwise restart or stop. Repeated handling does
            not replace the accepted timeout outcome.

        Raises:
            ValueError: The request ID does not match the active service work.
        """
        instance = state.services[service_id]
        active = instance.active_request
        if active is None or active.request_id != request_id:
            raise ValueError("Timeout does not match the active service request.")
        action = instance.definition.on_command_timeout
        if not active.timed_out:
            response = {"result": "fail", "data": {"reason": "command_timeout"}}
            self._journal.client.record_command_result(
                request_id,
                response,
                author="runner",
                outcome="timed_out",
                context={
                    "experiment_id": state.experiment_id,
                    "run_id": state.run_id,
                    "service_id": service_id,
                    "service_instance_id": instance.service_instance_id,
                    "participant_id": service_id,
                    "participant_instance_id": instance.service_instance_id,
                },
            )
            instance.active_request = _update_model(active, timed_out=True)
            if action != "restart":
                instance.blocked_action = "pause" if action == "pause" else "stop"
                self._pending_action = instance.blocked_action
                future = self._waiters.pop(request_id, None)
                if future is not None and not future.done():
                    future.set_result({"request_id": request_id, **response})
            try:
                self._state_store.save(state)
            except OSError as error:
                self._journal.client.record_error(
                    error,
                    context={
                        "experiment_id": state.experiment_id,
                        "service_id": service_id,
                    },
                )
            self._changed.set()
        return "wait" if action == "pause" else action

    async def prepare_rebuild(
        self,
        state: RunnerState,
        template: ExperimentTemplate,
        *,
        validate_only: bool = False,
    ) -> None:
        """Establish ownership and shutdown barriers for services changed by a template.

        Args:
            state: Current runner state and service instances.
            template: Validated candidate template.
            validate_only: Check ownership prerequisites without stopping participants.

        Raises:
            RuntimeError: Launcher identity is unavailable or affected processes cannot
                be confirmed stopped before their runtime files are rebuilt.
        """
        desired = {item.service_id: item for item in template.services}
        old_modules = {
            (item.module.name, item.module.version): item.module.hash
            for item in [*state.template.stages, *state.template.services]
            if not isinstance(item, ServiceCallDefinition)
        }
        new_modules = {
            (item.module.name, item.module.version): item.module.hash
            for item in [*template.stages, *template.services]
            if not isinstance(item, ServiceCallDefinition)
        }
        changed_code = {
            key for key, digest in old_modules.items() if new_modules.get(key) != digest
        }
        for service_id, instance in list(state.services.items()):
            module = instance.definition.module
            if (
                _definition_fingerprint(desired.get(service_id))
                == _definition_fingerprint(instance.definition)
                and (module.name, module.version) not in changed_code
            ):
                continue
            process = self._processes.get(service_id)
            process_file = (
                state.experiment_directory
                / "shared_artifacts/services"
                / service_id
                / instance.service_instance_id
                / "process.json"
            )
            record = read_json(process_file) if process_file.is_file() else {}
            launcher = record.get("launcher_process")
            # Legacy participant metadata cannot prove its parent's identity.
            # Reject before shutdown rather than enter a rollback with an
            # untracked process still able to write into the restored files.
            if launcher is None and process is None:
                raise RuntimeError(
                    f"Service {service_id} launcher identity is unavailable; "
                    "rebuilding is unsafe after recovery of legacy process metadata."
                )
            if launcher is not None and (
                record.get("experiment_id") != state.experiment_id
                or record.get("participant_id") != service_id
                or record.get("participant_instance_id") != instance.service_instance_id
            ):
                raise RuntimeError(
                    f"Service {service_id} launcher ownership cannot be verified."
                )
            if validate_only:
                continue
            results = await self.stop_all(
                state, service_ids={service_id}, preserve_pending=service_id in desired
            )
            if not results[service_id]["stopped"] or results[service_id]["error"]:
                raise RuntimeError(
                    f"Service {service_id} did not stop cleanly; rebuilding is unsafe: {results[service_id]}"
                )
            # A child participant can exit before its launcher finishes touching
            # runtime files. Keep those files in place until both have stopped.
            deadline = time.monotonic() + state.template.start_timeout
            if process is not None and process.poll() is None:
                try:
                    await asyncio.to_thread(process.wait, state.template.start_timeout)
                except subprocess.TimeoutExpired as error:
                    raise RuntimeError(
                        f"Service {service_id} launcher still owns runtime files; "
                        "rebuilding is unsafe."
                    ) from error
            if launcher is None:
                continue
            if not await self._wait_launcher_exit(launcher, deadline):
                raise RuntimeError(
                    f"Service {service_id} launcher {launcher['pid']} still owns "
                    "runtime files; rebuilding is unsafe."
                )

    async def _wait_launcher_exit(self, launcher: JsonObject, deadline: float) -> bool:
        """Return False only when the same launcher outlives the deadline.

        Args:
            launcher: Complete OS identity of the launcher whose exit is being
                confirmed.
            deadline: Absolute monotonic deadline in seconds for confirming launcher
                exit.

        Returns:
            True once the recorded launcher exited or no longer matches that OS
            identity; False if the same process remains alive beyond the deadline.
        """
        while True:
            try:
                if process_identity(launcher["pid"]) != launcher:
                    return True
                recovered_process = psutil.Process(launcher["pid"])
                if recovered_process.status() == psutil.STATUS_ZOMBIE:
                    return True
                try:
                    recovered_process.wait(timeout=0)
                    return True
                except psutil.TimeoutExpired:
                    pass
            except (FileNotFoundError, ProcessLookupError, psutil.NoSuchProcess):
                return True
            except OSError as error:
                if getattr(error, "winerror", None) not in (87, 1168):
                    raise
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(min(0.05, max(0, deadline - time.monotonic())))

    async def reconcile(
        self, state: RunnerState, template: ExperimentTemplate
    ) -> ServiceAction:
        """Match service instances to a candidate template and wait for required readiness.

        Args:
            state: Mutable runner state after affected old instances have stopped.
            template: Desired validated service definitions.

        Returns:
            Ready, pause, or stop from startup/readiness policy.

        Raises:
            RuntimeError: A removed or changed instance has not been stopped first.
        """
        desired = {item.service_id: item for item in template.services}
        for service_id, instance in list(state.services.items()):
            if service_id not in desired:
                if not instance.stopped:
                    raise RuntimeError(
                        "Removed services must stop before reconciliation."
                    )
                del state.services[service_id]
        for definition in template.services:
            instance = state.services.get(definition.service_id)
            if instance is not None and instance.manually_stopped:
                continue
            if instance is not None and instance.blocked_action is not None:
                return instance.blocked_action
            if instance is not None and not instance.stopped:
                if _definition_fingerprint(
                    instance.definition
                ) != _definition_fingerprint(definition):
                    raise RuntimeError(
                        "Changed services must stop before reconciliation."
                    )
                continue
            if instance is not None and instance.definition.module != definition.module:
                instance.restart_count = 0
            action = await self._start_with_recovery(state, definition)
            if action != "ready":
                return action
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self.monitor(state))
        return await self.wait_ready(state)

    async def recover(self, state: RunnerState) -> ServiceAction:
        """Reconcile saved service ownership and work before restoring live supervision.

        Args:
            state: Persisted runner state describing every declared service.

        Returns:
            Readiness action; a recovered snapshot barrier keeps the DAG paused.

        Raises:
            RuntimeError: Ownership is incomplete or previous channels remain open.
            ValueError: Saved snapshot barriers conflict.
        """
        if set(state.services) != {item.service_id for item in state.template.services}:
            raise RuntimeError(
                "Saved service ownership is incomplete; automatic launch is unsafe."
            )
        if self._closed:
            if self._connections or self._sends or self._waiters:
                raise RuntimeError(
                    "Finish closing the previous service channels before recovery."
                )
            self._closed = False
            self._monitor_task = None
            self._pending_action = None
            self._probes.clear()
            self._next_probe.clear()
            self._bad_replies.clear()
        markers = {
            item.prepared_freeze_id or item.freeze_id
            for item in state.services.values()
        } - {None}
        if len(markers) > 1:
            raise ValueError("Saved services refer to conflicting snapshot barriers.")
        if markers:
            self._snapshot_id = markers.pop()
            self._frozen_instances = {
                key: item.service_instance_id
                for key, item in state.services.items()
                if item.prepared_freeze_id or item.freeze_id
            }
        action = await self._recover_pending_requests(state)
        if action is not None:
            return action
        self._starting.update(state.services)
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self.monitor(state))
        for service_id, instance in state.services.items():
            try:
                return_action, action = await self._recover_instance(
                    state, service_id, instance
                )
                if return_action:
                    return action
            finally:
                self._starting.discard(service_id)
        action = await self.wait_ready(state)
        return (
            "pause" if action == "ready" and self._snapshot_id is not None else action
        )

    async def _recover_pending_requests(
        self, state: RunnerState
    ) -> Literal["stop"] | None:
        # A failed optional state write can leave an already sent request queued.
        # Consult the mandatory send record before the monitor can dispatch it.
        """Check journal send evidence before dispatch and stop on unresolved already-sent work.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.

        Returns:
            Stop if saved pending work was already sent and has no reconciled
            accepted outcome; otherwise None.
        """
        pending_ids = {
            entry.request_id
            for instance in state.services.values()
            for entry in instance.pending_requests
        }
        sent_ids = set()
        checkpoint = boundary = None
        while pending_ids:
            page = await asyncio.to_thread(
                self._journal.client.read_events, checkpoint, limit=1000
            )
            if boundary is None:
                boundary = page["boundary"]["cursor"]
            for entry in page["events"]:
                if entry["cursor"] > boundary:
                    break
                event = entry["event"]
                if (
                    event["event_type"] == "service.send_started"
                    and event["context"].get("experiment_id") == state.experiment_id
                    and event["data"].get("request_id") in pending_ids
                ):
                    sent_ids.add(event["data"]["request_id"])
            checkpoint = page["checkpoint"]
            if checkpoint["cursor"] >= boundary or not page["has_more"]:
                break
        for instance in state.services.values():
            for entry in list(instance.pending_requests):
                if entry.request_id not in sent_ids:
                    continue
                recorded = read_result(
                    self._journal.client,
                    entry.request_id,
                    expected={
                        "experiment_id": state.experiment_id,
                        "participant_id": instance.service_id,
                        "participant_instance_id": (entry.model_extra or {}).get(
                            "expected_instance", instance.service_instance_id
                        ),
                    },
                )
                if (
                    recorded is not None
                    and recorded["author"] == "runner"
                    and recorded["outcome"] in ("succeeded", "failed")
                ):
                    instance.pending_requests.remove(entry)
                    continue
                instance.failure = ServiceFailureDetails(
                    code="service_failure",
                    message="Saved queue contains an already sent request with an unresolved outcome.",
                )
                instance.blocked_action = self._pending_action = "stop"
                return "stop"
        return None

    async def _recover_instance(
        self, state: RunnerState, service_id: str, instance: ServiceInstance
    ) -> tuple[bool, ServiceAction | None]:
        """Recover manual-stop, exited, or live instance state without duplicating unknown ownership.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.

        Returns:
            A flag indicating whether recovery must return immediately and the
            associated readiness action, or False and None to continue.
        """
        if instance.manually_stopped:
            # Finish an interrupted manual stop before allowing explicit
            # start; recovery must never relaunch this service.
            if not instance.stopped:
                results = await self.stop_all(state, service_ids={service_id})
                if not results[service_id]["stopped"] or results[service_id]["error"]:
                    return True, "stop"
            instance.blocked_action = None
            self._state_store.save(state)
            return False, None
        if instance.stopped:
            instance.blocked_action = self._pending_action = "pause"
            # Still reconnect the remaining live services so a partial
            # shutdown cannot disable their supervision during recovery.
            return False, None
        instance.ready = False
        if instance.process_identity is None or instance.endpoint_path is None:
            instance.failure = ServiceFailureDetails(
                code="service_failure",
                message="Cannot identify the previous service process.",
            )
            instance.blocked_action = "stop"
            self._pending_action = "stop"
            return True, "stop"
        try:
            announced = read_json(instance.endpoint_path)
        except FileNotFoundError:
            announced = {}
        if (
            announced
            and announced.get("participant_instance_id") != instance.service_instance_id
        ):
            # A stale state file must not authorize a second copy while a
            # newer, unaccounted-for instance owns the endpoint.
            peer = announced.get("process")
            if isinstance(peer, dict):
                try:
                    peer_alive = process_identity(peer["pid"]) == peer
                except OSError:
                    peer_alive = False
                if peer_alive and instance.ever_ready:
                    instance.process_identity = None
                    instance.failure = ServiceFailureDetails(
                        code="ownership_unknown",
                        message="Endpoint belongs to an unaccounted-for service instance.",
                    )
                    instance.blocked_action = self._pending_action = "stop"
                    return True, "stop"
        try:
            actual = process_identity(instance.process_identity.pid)
        except OSError as error:
            if not isinstance(
                error, (FileNotFoundError, ProcessLookupError)
            ) and getattr(error, "winerror", None) not in (87, 1168):
                raise
            actual = None
        if actual != _process_identity_document(instance.process_identity):
            instance.failure = ServiceFailureDetails(
                code="service_failure",
                message="Previous service process no longer exists.",
            )
            action = await self.restart(state, service_id, automatic=True)
            if action != "ready":
                return True, action
            return False, None
        return await self._recover_connected_instance(state, service_id, instance)

    async def _recover_connected_instance(
        self, state: RunnerState, service_id: str, instance: ServiceInstance
    ) -> tuple[bool, ServiceAction | None]:
        """Reconnect within the retained startup/grace deadline and query current work.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.

        Returns:
            An early-return flag and readiness action, or False and None after
            command reconciliation permits continuing.
        """
        timeout = instance.definition.heartbeat.grace_seconds
        if not instance.ever_ready:
            if instance.start_deadline is None:
                raise ValueError(
                    "An unfinished service startup requires its original deadline."
                )
            timeout = max(0, instance.start_deadline - time.monotonic())
            if timeout == 0:
                instance.failure = ServiceFailureDetails(
                    code="startup_timeout",
                    message="Service startup expired while runner was unavailable.",
                )
                action = await self.restart(state, service_id, automatic=True)
                if action != "ready":
                    return True, action
                return False, None
        context = {
            "experiment_id": state.experiment_id,
            "service_id": service_id,
            "service_instance_id": instance.service_instance_id,
            "participant_id": service_id,
            "participant_instance_id": instance.service_instance_id,
        }
        connection = ParticipantConnection(instance.endpoint_path, context)
        self._connections[service_id] = connection
        reconnect_deadline = time.monotonic() + timeout
        await asyncio.wait_for(connection.connect(timeout_seconds=timeout), timeout)
        request_id = str(uuid4())
        if request_id in state.used_request_ids:
            raise RuntimeError("Request ID collision.")
        state.used_request_ids.add(request_id)
        self._journal.client.record_event(
            "control.intent",
            {"action": "command_state", "request_id": request_id},
            context=context,
        )
        reply = CommandStateResponse.model_validate(
            await connection.query_command_state(
                request_id,
                timeout_seconds=max(0.001, reconnect_deadline - time.monotonic()),
            )
        )
        return await self._reconcile_commands(
            state, service_id, instance, reply.data, context
        )

    async def _reconcile_commands(
        self,
        state: RunnerState,
        service_id: str,
        instance: ServiceInstance,
        observed: CommandState,
        context: JsonObject,
    ) -> tuple[bool, ServiceAction | None]:
        """Compare participant work with the saved active request and stop on unmatched work.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            observed: Validated current/pending command state returned by the
                participant.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            True and stop for work that has no matching saved owner; otherwise False
            and None after journal polling.
        """
        self._journal.client.record_event(
            "service.commands_reconciled",
            observed.model_dump(exclude_unset=True),
            context=context,
        )
        active = instance.active_request
        participant_work = [*observed.pending]
        if observed.current is not None:
            participant_work.append(observed.current)
        if any(
            active is None or item.request_id != active.request_id
            for item in participant_work
        ):
            instance.failure = ServiceFailureDetails(
                code="service_failure",
                message="Participant reports work not matched to the saved sent request.",
            )
            # Unknown work needs an owner decision, not an automatic restart
            # that would destroy the very evidence recovery must reconcile.
            instance.blocked_action = "stop"
            self._pending_action = "stop"
            return True, "stop"
        await self._poll_result(state, service_id)
        self._probes.pop(service_id, None)
        self._next_probe[service_id] = 0
        return False, None

    async def save_states(
        self, state: RunnerState, snapshot_id: str
    ) -> dict[str, Path]:
        """Drain working queues, freeze service writes, and export snapshot state.

        Args:
            state: Current runner state with ready services.
            snapshot_id: UUID identifying this snapshot barrier.

        Returns:
            Service IDs mapped to existing absolute export paths. Successful export
            leaves services frozen until unfreeze is called.

        Raises:
            RuntimeError: Work cannot drain, a barrier is already active, or service
                readiness/instance identity changes during export.
        """
        UUID(snapshot_id)
        if any(instance.manually_stopped for instance in state.services.values()):
            raise RuntimeError(
                "Start manually stopped services before exporting snapshot state."
            )
        if self._snapshot_id is not None:
            raise RuntimeError("A service snapshot barrier is already active.")
        self._snapshot_id = snapshot_id
        result = {}
        try:
            while any(
                instance.pending_requests or instance.active_request
                for instance in state.services.values()
            ):
                if any(
                    (
                        instance.blocked_action is not None
                        or instance.active_request is not None
                        and instance.active_request.timed_out
                    )
                    for instance in state.services.values()
                ):
                    raise RuntimeError(
                        "Service work cannot be drained for this snapshot."
                    )
                self._changed.clear()
                await self._changed.wait()
            for service_id, instance in state.services.items():
                resolved = await self._save_service_state(
                    state, service_id, instance, snapshot_id
                )
                if resolved is not None:
                    result[service_id] = resolved
            if any(
                state.services[key].service_instance_id != value
                for key, value in self._frozen_instances.items()
            ):
                raise RuntimeError("Service restart invalidated the snapshot barrier.")
            return result
        except BaseException as error:
            try:
                await self.unfreeze(state, snapshot_id)
            except Exception as unfreeze_error:  # noqa: BLE001 - Unknown unfreeze effects must stop restoration.
                error.add_note(
                    f"Snapshot write resumption is unconfirmed: {unfreeze_error}"
                )
                raise RuntimeError(
                    "Service unfreeze failed; the experiment must stop."
                ) from error
            raise

    async def _save_service_state(
        self,
        state: RunnerState,
        service_id: str,
        instance: ServiceInstance,
        snapshot_id: str,
    ) -> Path | None:
        """Freeze one instance and return its confined export path, or None when stateless.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            snapshot_id: UUID identifying the snapshot or active write-freeze
                barrier.

        Returns:
            Absolute existing export path inside the allocated output directory, or
            None for a permitted stateless service.

        Raises:
            RuntimeError: The instance is unready, changes during export, or fails
                to confirm freeze/save.
            ValueError: A required state path is missing or resolves outside the
                allocated export directory.
        """
        if not instance.ready:
            raise RuntimeError("Snapshot requires all services to be ready.")
        self._frozen_instances[service_id] = instance.service_instance_id
        instance.prepared_freeze_id = snapshot_id
        reply = await self.request(
            state, service_id, "freeze_writes", {"snapshot_id": snapshot_id}
        )
        if reply["result"] != "success" or state.services[service_id] is not instance:
            raise RuntimeError(
                f"Service {service_id} did not confirm the snapshot freeze."
            )
        instance.freeze_id = snapshot_id
        output = (
            state.experiment_directory
            / "shared_data"
            / "service_state"
            / service_id
            / snapshot_id
        )
        if not output.resolve().is_relative_to(state.experiment_directory.resolve()):
            raise ValueError("Service export directory escapes the experiment.")
        output.mkdir(parents=True, exist_ok=False)
        reply = await self.request(
            state,
            service_id,
            "save_state",
            {"snapshot_id": snapshot_id, "output_directory": str(output)},
        )
        if reply["result"] != "success" or state.services[service_id] is not instance:
            raise RuntimeError(f"Service {service_id} state export failed.")
        data = ServiceStateExport.model_validate(reply["data"])
        if data.state_path is None:
            if instance.definition.state_required:
                raise ValueError("Required service export is missing state_path.")
            return None
        relative = Path(data.state_path)
        resolved = (state.experiment_directory / relative).resolve()
        if not resolved.is_relative_to(output.resolve()) or not resolved.exists():
            raise ValueError(
                "Service state_path must exist inside its allocated export directory."
            )
        return resolved

    async def load_states(
        self,
        state: RunnerState,
        service_states: dict[str, Path],
        *,
        service_ids: set[str] | None = None,
    ) -> None:
        """Load selected service exports and require fresh readiness from the same instances.

        Args:
            state: Runner state containing the newly started services.
            service_states: Service IDs mapped to validated restoration paths.
            service_ids: Selected service subset, or None for every service.

        Raises:
            ValueError: Exports target unknown/unselected services or paths are invalid.
            RuntimeError: State loading fails or a loaded instance restarts before readiness.
        """
        if service_states.keys() - state.services.keys():
            raise ValueError("State was supplied for unknown services.")
        selected = set(state.services) if service_ids is None else service_ids
        if selected - state.services.keys() or service_states.keys() - selected:
            raise ValueError("State transfer must target selected existing services.")
        paths = _load_state_paths(state, service_states, selected)
        loaded_instances = {}
        for service_id, path in paths.items():
            instance_id = state.services[service_id].service_instance_id
            reply = await self.request(
                state, service_id, "load_state", {"state_path": path}
            )
            if (
                reply["result"] != "success"
                or state.services[service_id].service_instance_id != instance_id
            ):
                raise RuntimeError(f"Service {service_id} could not restore its state.")
            state.services[service_id].ready = False
            loaded_instances[service_id] = instance_id
            self._probes.pop(service_id, None)
            self._next_probe[service_id] = 0
        if await self.wait_ready(state) != "ready":
            raise RuntimeError("Restored services did not confirm fresh readiness.")
        if any(
            state.services[service_id].service_instance_id != instance_id
            for service_id, instance_id in loaded_instances.items()
        ):
            raise RuntimeError(
                "A loaded service restarted before confirming readiness."
            )

    async def unfreeze(self, state: RunnerState, snapshot_id: str) -> None:
        """Require matching frozen instances to resume writes, then clear the snapshot barrier.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            snapshot_id: UUID identifying the snapshot or active write-freeze
                barrier.

        Raises:
            ValueError: The snapshot ID does not match the active barrier.
            RuntimeError: An instance was replaced or cannot confirm resumption of
                writes.
        """
        if self._snapshot_id != snapshot_id:
            raise ValueError("No matching service snapshot barrier.")
        for service_id, instance_id in self._frozen_instances.items():
            instance = state.services[service_id]
            if instance.service_instance_id != instance_id:
                raise RuntimeError(
                    "A replacement service cannot confirm the old instance's unfreeze."
                )
            if (
                not instance.ready
                or instance.active_request is not None
                and instance.active_request.timed_out
            ):
                raise RuntimeError("Service write resumption cannot be confirmed.")
            reply = await self.request(
                state, service_id, "unfreeze_writes", {"snapshot_id": snapshot_id}
            )
            if (
                reply["result"] != "success"
                or state.services[service_id] is not instance
            ):
                raise RuntimeError(f"Service {service_id} did not confirm unfreeze.")
            instance.freeze_id = instance.prepared_freeze_id = None
        self._snapshot_id = None
        self._frozen_instances.clear()
        try:
            self._state_store.save(state)
        except OSError as error:
            self._journal.client.record_error(
                error, context={"experiment_id": state.experiment_id}
            )

    async def stop_all(
        self,
        state: RunnerState,
        *,
        service_ids: set[str] | None = None,
        preserve_pending: bool = False,
    ) -> JsonObject:
        """Request shutdown and confirm termination in reverse service order.

        Args:
            state: Runner state containing owned service instances.
            service_ids: Optional subset; None stops all current services.
            preserve_pending: Retain unsent requests for a subsequent restart.

        Returns:
            Per-service stopped flags and errors. A shutdown acknowledgement alone
            does not count as confirmed process termination.

        Raises:
            ValueError: The selection contains an unknown service.
        """
        selected = set(state.services) if service_ids is None else service_ids
        if selected - state.services.keys():
            raise ValueError("Cannot stop an unknown service.")
        results = {}
        for service_id in reversed(list(state.services)):
            if service_id not in selected:
                continue
            await self._stop_service(state, service_id, preserve_pending, results)
        return results

    async def _stop_service(
        self,
        state: RunnerState,
        service_id: str,
        preserve_pending: bool,
        results: JsonObject,
    ) -> None:
        """Detach supervision, request shutdown, confirm exit, and retire affected requests.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            preserve_pending: Retain unsent requests for restart instead of
                cancelling the whole queue.
            results: Mutable per-service shutdown result mapping.
        """
        instance = state.services[service_id]
        instance.ready = False
        instance.stopping = True
        context = _context(state, service_id, instance.service_instance_id)
        await self._detach_service_tasks(service_id)
        deadline = time.monotonic() + state.template.start_timeout
        owned_process = self._processes.get(service_id)
        if (
            owned_process is not None
            and instance.process_identity is not None
            and owned_process.pid == instance.process_identity.pid
            and owned_process.poll() is not None
        ):
            instance.stopped = True
        request_id = str(uuid4())
        if request_id in state.used_request_ids:
            raise RuntimeError("Request ID collision.")
        state.used_request_ids.add(request_id)
        error_message = self._record_service_stop_intent(
            state, service_id, request_id, context
        )
        shutdown_response, shutdown_error = await self._request_service_shutdown(
            state, service_id, instance, request_id, deadline
        )
        if shutdown_error is not None:
            error_message = shutdown_error
        process = self._processes.get(service_id)
        error_message = await self._confirm_service_exit(
            instance, process, deadline, error_message
        )
        if not instance.stopped:
            error_message = error_message or "Service termination is unconfirmed."
            instance.failure = (
                None
                if error_message is None
                else ServiceFailureDetails(
                    code="service_failure", message=str(error_message)
                )
            )
            instance.blocked_action = "stop"
            self._pending_action = "stop"
        error_message = self._cancel_service_stop_requests(
            state, instance, preserve_pending, error_message
        )
        instance.stopping = False
        if self._notify_resources is not None:
            self._notify_resources()
        if process is not None:
            process.poll()
        results[service_id] = {"stopped": instance.stopped, "error": error_message}
        self._record_service_stop_outcome(
            state, service_id, request_id, shutdown_response, context, results
        )

    async def _detach_service_tasks(self, service_id: str) -> None:
        """Cancel one service's send/reconnect/restart tasks and close its channel.

        Args:
            service_id: Stable service definition UUID.
        """
        tasks = [
            self._connecting.pop(service_id, None),
        ]
        restart = self._restarts.get(service_id)
        if restart is not asyncio.current_task():
            self._restarts.pop(service_id, None)
            tasks.append(restart)
        for request_id, (owner, sending) in list(self._sends.items()):
            if owner == service_id:
                del self._sends[request_id]
                tasks.append(sending)
        tasks = [task for task in tasks if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        connection = self._connections.pop(service_id, None)
        if connection is not None:
            await connection.close()
        self._probes.pop(service_id, None)

    def _record_service_stop_intent(
        self, state: RunnerState, service_id: str, request_id: str, context: JsonObject
    ) -> str | None:
        """Journal shutdown intent or write emergency metadata, returning any journal error.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            request_id: UUID correlating the admitted request and its eventual
                outcome.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            Journal failure message if emergency stop metadata was needed, otherwise
            None.
        """
        error_message = None
        try:
            self._journal.client.record_event(
                "control.intent",
                {"action": "stop_service", "request_id": request_id},
                context=context,
            )
        except LoggingError as error:
            error_message = str(error)
            try:
                write_json(
                    state.experiment_directory
                    / "runner"
                    / f"service-stop-{service_id}.emergency.json",
                    {**context, "error": error_message},
                )
            except OSError as secondary:
                error.add_note(f"Emergency service-stop recording failed: {secondary}")
        return error_message

    async def _request_service_shutdown(
        self,
        state: RunnerState,
        service_id: str,
        instance: ServiceInstance,
        request_id: str,
        deadline: float,
    ) -> tuple[ParticipantResult | None, str | None]:
        """Connect to the matching endpoint and return its shutdown response and optional error.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            request_id: UUID correlating the admitted request and its eventual
                outcome.
            deadline: Absolute monotonic deadline in seconds, or None when
                unbounded.

        Returns:
            Optional validated participant shutdown response and optional diagnostic
            message. A success reply does not itself confirm process exit.
        """
        connection = None
        shutdown_response = None
        error_message = None
        try:
            if not instance.stopped and instance.endpoint_path is not None:
                connection = ParticipantConnection(
                    instance.endpoint_path,
                    {
                        "experiment_id": state.experiment_id,
                        "service_id": service_id,
                        "service_instance_id": instance.service_instance_id,
                        "participant_id": service_id,
                        "participant_instance_id": instance.service_instance_id,
                    },
                )
                async with asyncio.timeout(max(0.001, deadline - time.monotonic())):
                    # Stop can interrupt startup after spawn but before the
                    # new endpoint replaces the previous instance's file.
                    while True:
                        try:
                            endpoint = read_json(instance.endpoint_path)
                        except (FileNotFoundError, PermissionError):
                            endpoint = {}
                        if (
                            endpoint.get("participant_instance_id")
                            == instance.service_instance_id
                        ):
                            break
                        await asyncio.sleep(0.05)
                    await connection.connect(
                        timeout_seconds=max(0.001, deadline - time.monotonic())
                    )
                    reply = await connection.request(
                        request_id,
                        "shutdown",
                        {},
                        timeout_seconds=max(0.001, deadline - time.monotonic()),
                    )
                    if (
                        reply.get("message_type") != "response"
                        or reply.get("result") not in ("success", "fail")
                        or "data" not in reply
                    ):
                        raise ValueError("Invalid shutdown result.")
                    shutdown_response = ParticipantResult.model_validate(
                        {
                            "result": reply["result"],
                            "data": reply["data"],
                        }
                    )
                    if shutdown_response.result == "fail":
                        error_message = "Service shutdown reported failure."
        except Exception as error:  # noqa: BLE001 - One failed stop must not leave other services untouched.
            error_message = str(error)
        finally:
            if connection is not None:
                await connection.close()
        return shutdown_response, error_message

    async def _confirm_service_exit(
        self,
        instance: ServiceInstance,
        process: subprocess.Popen | None,
        deadline: float,
        error_message: str | None,
    ) -> str | None:
        """Observe termination until deadline and kill only a matching owned process handle.

        Args:
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            process: Owned process handle used to observe exit without trusting a
                bare PID.
            deadline: Absolute monotonic deadline in seconds, or None when
                unbounded.
            error_message: Previously recorded shutdown diagnostic, or None before
                any error.

        Returns:
            Most recent diagnostic message, or None. The instance's stopped flag
            records actual confirmation independently of this message.
        """
        while not instance.stopped:
            try:
                if (
                    process is not None
                    and instance.process_identity is not None
                    and process.pid == instance.process_identity.pid
                    and process.poll() is not None
                ):
                    instance.stopped = True
                elif instance.process_identity is None:
                    instance.stopped = (
                        process is not None and process.poll() is not None
                    )
                else:
                    observed = process_identity(instance.process_identity.pid)
                    instance.stopped = observed != _process_identity_document(
                        instance.process_identity
                    )
                    if not instance.stopped:
                        participant = psutil.Process(observed["pid"])
                        instance.stopped = participant.status() == psutil.STATUS_ZOMBIE
                        if not instance.stopped:
                            try:
                                participant.wait(timeout=0)
                                instance.stopped = True
                            except psutil.TimeoutExpired:
                                pass
            except (FileNotFoundError, ProcessLookupError, psutil.NoSuchProcess):
                instance.stopped = True
            except OSError as error:
                if getattr(error, "winerror", None) in (87, 1168):
                    instance.stopped = True
                else:
                    error_message = str(error)
            except psutil.Error as error:
                error_message = str(error)
            if instance.stopped or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.05)
        if (
            not instance.stopped
            and process is not None
            and instance.process_identity is not None
            and process.pid == instance.process_identity.pid
        ):
            # The owned handle cannot target a reused PID. Do not grant another
            # command timeout or claim that an asynchronous kill has completed.
            if process.poll() is None:
                try:
                    process.kill()
                except OSError as error:
                    error_message = str(error)
            instance.stopped = process.poll() is not None
        return error_message

    def _cancel_service_stop_requests(
        self,
        state: RunnerState,
        instance: ServiceInstance,
        preserve_pending: bool,
        error_message: str | None,
    ) -> str | None:
        """Retire active and optionally pending requests, returning any journal failure message.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            preserve_pending: Retain unsent requests for restart instead of
                cancelling the whole queue.
            error_message: Previously recorded shutdown diagnostic, or None before
                any error.

        Returns:
            Previously retained or latest journal failure message, or None.
        """
        cancelled = [] if preserve_pending else instance.pending_requests[:]
        if not preserve_pending:
            instance.pending_requests.clear()
        if instance.active_request is not None:
            cancelled.insert(0, instance.active_request)
            instance.active_request = None
        for entry in cancelled:
            try:
                self._finish_request(
                    state,
                    instance,
                    entry,
                    {"result": "fail", "data": {"reason": "service_stopped"}},
                    "cancelled" if entry.sent_monotonic is None else "failed",
                )
            except LoggingError as error:
                error_message = str(error)
        return error_message

    def _record_service_stop_outcome(
        self,
        state: RunnerState,
        service_id: str,
        request_id: str,
        shutdown_response: ParticipantResult | None,
        context: JsonObject,
        results: JsonObject,
    ) -> None:
        """Record the accepted stop outcome and state, attaching journal failures to the result.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            request_id: UUID correlating the admitted request and its eventual
                outcome.
            shutdown_response: Participant shutdown result, or None when no response
                was obtained.
            context: Journal/participant coordinates associated with this operation.
            results: Mutable per-service shutdown result mapping.
        """
        try:
            self._journal.client.record_command_result(
                request_id,
                shutdown_response.model_dump(exclude_unset=True)
                if shutdown_response is not None
                else {
                    "result": "success" if results[service_id]["stopped"] else "fail",
                    "data": results[service_id],
                },
                author="runner",
                outcome="succeeded"
                if results[service_id]["stopped"]
                and (shutdown_response is None or shutdown_response.result != "fail")
                else "failed",
                context=context,
            )
        except LoggingError as error:
            results[service_id]["error"] = str(error)
        try:
            self._state_store.save(state)
        except OSError as error:
            try:
                self._journal.client.record_error(error, context=context)
            except LoggingError as logging_error:
                results[service_id]["error"] = str(logging_error)
        self._changed.set()

    async def reset(self, state: RunnerState) -> None:
        """Release a stopped generation before binding restored experiment state.

        Requires confirmed stops and empty queues, closes communication, and waits
        for launcher cleanup using handles or verified saved identities. Only then
        are process tracking and snapshot barriers cleared for a restored
        generation.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.

        Raises:
            RuntimeError: Services/queues are not stopped and empty, launcher
                ownership is inconsistent, or a launcher still holds runtime files.
        """
        if any(
            not item.stopped or item.active_request or item.pending_requests
            for item in state.services.values()
        ):
            raise RuntimeError(
                "Service reset requires confirmed stops and empty queues."
            )
        await self.close()
        deadline = time.monotonic() + state.template.start_timeout
        for process in self._launch_processes:
            if process.poll() is None:
                try:
                    await asyncio.to_thread(
                        process.wait, max(0.001, deadline - time.monotonic())
                    )
                except subprocess.TimeoutExpired as error:
                    raise RuntimeError(
                        "A service command still owns runtime files."
                    ) from error
        # A recovered manager has no Popen handles. The participant can stop
        # before its launcher finishes cleanup, so retain this separate barrier.
        for service_id, instance in state.services.items():
            process_file = (
                state.experiment_directory
                / "shared_artifacts/services"
                / service_id
                / instance.service_instance_id
                / "process.json"
            )
            if not process_file.is_file():
                continue
            record = read_json(process_file)
            launcher = record.get("launcher_process")
            if launcher is None:
                continue
            if (
                record.get("experiment_id") != state.experiment_id
                or record.get("participant_id") != service_id
                or record.get("participant_instance_id") != instance.service_instance_id
            ):
                raise RuntimeError(
                    f"Service {service_id} launcher ownership cannot be verified."
                )
            if not await self._wait_launcher_exit(launcher, deadline):
                raise RuntimeError(
                    f"Service {service_id} launcher {launcher['pid']} still owns "
                    "runtime files; restoration is unsafe."
                )
        self._processes.clear()
        self._launch_processes.clear()
        self._probes.clear()
        self._next_probe.clear()
        self._bad_replies.clear()
        self._starting.clear()
        self._frozen_instances.clear()
        self._snapshot_id = None
        for instance in state.services.values():
            instance.freeze_id = instance.prepared_freeze_id = None
        self._pending_action = None
        self._monitor_task = None
        self._closed = False

    async def close(self) -> None:
        """Detach supervision and connections, cancelling waiters without stopping service processes.

        Cancels monitoring, reconnects, sends, and waiter futures and closes all
        channels. Live launch handles remain tracked for later shutdown/reset; this
        operation does not terminate participant processes.
        """
        self._closed = True
        tasks = [
            self._monitor_task,
            *self._restarts.values(),
            *self._connecting.values(),
            *(task for _, task in self._sends.values()),
        ]
        tasks = [
            task
            for task in tasks
            if task is not None and task is not asyncio.current_task()
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for connection in self._connections.values():
            await connection.close()
        self._connections.clear()
        self._restarts.clear()
        self._connecting.clear()
        self._sends.clear()
        for future in self._waiters.values():
            future.cancel()
        self._waiters.clear()
        self._launch_processes = [
            process for process in self._launch_processes if process.poll() is None
        ]
        self._changed.set()

    def _save(self, state: RunnerState) -> None:
        """Persist service state and journal rebuild ownership before optional state-file publication.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
        """
        if state.pending_rebuild is not None:
            # Participant ownership must survive loss of the optional state.json copy.
            self._journal.client.record_event(
                "rebuild.checkpoint",
                state_to_document(state),
                context={"experiment_id": state.experiment_id, "run_id": state.run_id},
            )
        try:
            self._state_store.save(state)
        except OSError as error:
            self._journal.client.record_error(
                error, context={"experiment_id": state.experiment_id}
            )

    async def _exchange(
        self,
        service_id: str,
        request_id: str,
        command: str,
        args: JsonObject,
        *,
        timeout: float | None = None,
        deadline: float | None = None,
    ) -> JsonObject:
        """Send a service request and attach its command name to the correlated reply.

        Args:
            service_id: Connected service UUID.
            request_id: Fresh request UUID.
            command: Participant command name.
            args: JSON arguments.
            timeout: Optional local response timeout in seconds.
            deadline: Optional absolute monotonic deadline sent to the participant.

        Returns:
            Response envelope extended with the command name.
        """
        reply = await self._connections[service_id].request(
            request_id,
            command,
            args,
            timeout_seconds=timeout,
            deadline_monotonic=deadline,
        )
        return {**reply, "command": command}

    def _finish_request(
        self,
        state: RunnerState,
        instance: ServiceInstance,
        entry: WorkingServiceRequest,
        response: ServiceObservation | JsonObject,
        outcome: str,
    ) -> None:
        """Journal the owned outcome or caller retirement and resolve its waiting future.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            entry: Validated working queue entry being retired.
            response: Participant/controller result envelope being processed.
            outcome: Accepted stage/command outcome that determines the next policy
                action.
        """
        if isinstance(response, ServiceObservation):
            response = response.model_dump(
                exclude_unset=True,
                exclude={"protocol_version", "message_type", "request_id", "command"},
            )
        context = _context(state, instance.service_id, instance.service_instance_id)
        if entry.owner != "caller" and not entry.timed_out:
            self._journal.client.record_command_result(
                entry.request_id,
                response,
                author="runner",
                outcome=outcome,
                context=context,
            )
        elif entry.owner == "caller":
            self._journal.client.record_event(
                "service.request_retired",
                {"request_id": entry.request_id, "response": response},
                context=context,
            )
        future = self._waiters.pop(entry.request_id, None)
        if future is not None and not future.done():
            future.set_result({"request_id": entry.request_id, **response})
        self._changed.set()

    def enqueue(
        self,
        state: RunnerState,
        service_id: str,
        request_id: str,
        command: str,
        args: JsonObject,
        *,
        owner: str = "caller",
        deadline: float | None = None,
    ) -> asyncio.Future:
        """Persist fresh working-request admission and return a result future.

        Args:
            state: Runner state receiving request ownership and queue changes.
            service_id: Active target service UUID.
            request_id: UUID never allocated by this runner.
            command: Working command; control commands use a separate channel.
            args: JSON arguments copied into the queue.
            owner: Caller for DAG-owned results or service for manager-owned results.
            deadline: Optional absolute monotonic deadline including queue wait.

        Returns:
            Future resolved when the request is retired with a result.

        Raises:
            RuntimeError: The service is stopped or snapshot admission forbids work.
            ValueError: Request ID, ownership, or command violates admission rules.
        """
        instance = state.services[service_id]
        if (
            self._closed
            or instance.manually_stopped
            or instance.stopped
            or instance.stopping
        ):
            raise RuntimeError("Working requests require an active service.")
        self._validate_enqueue(state, command, args, request_id, owner)
        state.used_request_ids.add(request_id)
        values = {
            "request_id": request_id,
            "command": command,
            "args": copy_json_object(args, "args"),
            "owner": owner,
            "deadline_monotonic": deadline,
            "queued_monotonic": time.monotonic(),
            "sent_monotonic": None,
            "sent_at": None,
            "service_instance_id": None,
            "timed_out": False,
        }
        if owner == "caller" or command in (
            "freeze_writes",
            "save_state",
            "unfreeze_writes",
            "load_state",
        ):
            values["expected_instance"] = instance.service_instance_id
        entry = WorkingServiceRequest.model_validate(values)
        self._journal.client.record_event(
            "service.request_queued",
            entry.model_dump(exclude_unset=True),
            context=_context(state, service_id, instance.service_instance_id),
        )
        instance.pending_requests.append(entry)
        future = asyncio.get_running_loop().create_future()
        self._waiters[request_id] = future
        self._save(state)
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self.monitor(state))
        return future

    def _validate_enqueue(
        self,
        state: RunnerState,
        command: str,
        args: JsonObject,
        request_id: str,
        owner: str,
    ) -> None:
        """Reject control commands, reused IDs, invalid owners, and work outside a snapshot barrier.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            command: Validated command name or envelope selecting the operation.
            args: JSON command arguments; mutable inputs are detached at the
                validation boundary.
            request_id: UUID correlating the admitted request and its eventual
                outcome.
            owner: Caller for DAG-owned result acceptance, or service for manager-
                owned requests.
        """
        require_text(command, "service command")
        if command in ("heartbeat", "shutdown", "command_state", "interrupt"):
            raise ValueError("Control commands do not belong in the working queue.")
        if self._snapshot_id is not None and (
            command not in ("freeze_writes", "save_state", "unfreeze_writes")
            or args.get("snapshot_id") != self._snapshot_id
        ):
            raise RuntimeError("Ordinary work is frozen for a snapshot.")
        UUID(request_id)
        if owner not in ("caller", "service"):
            raise ValueError("Request owner must be caller or service.")
        if request_id in state.used_request_ids:
            raise ValueError("Request ID was already allocated.")
        if request_id in self._waiters or any(
            entry.request_id == request_id
            for item in state.services.values()
            for entry in [
                *item.pending_requests,
                *([] if item.active_request is None else [item.active_request]),
            ]
        ):
            raise ValueError("A queued or sent request cannot be submitted again.")

    async def cancel_request(
        self,
        state: RunnerState,
        service_id: str,
        request_id: str,
        *,
        interrupt: bool = False,
    ) -> bool:
        """Cancel queued work or mark active work timed out, optionally requesting interruption.

        Args:
            state: Runner state holding the persistent queue.
            service_id: Target service UUID.
            request_id: Queued or active request to cancel.
            interrupt: Whether to request actual cancellation of already-sent work.

        Returns:
            True when no matching work remains or interruption is confirmed; False
            when active work may still run.
        """
        instance = state.services[service_id]
        for entry in list(instance.pending_requests):
            if entry.request_id == request_id:
                instance.pending_requests.remove(entry)
                self._finish_request(
                    state,
                    instance,
                    entry,
                    {"result": "fail", "data": {"reason": "cancelled_before_send"}},
                    "cancelled",
                )
                self._save(state)
                return True
        active = instance.active_request
        if active is None or active.request_id != request_id:
            return True
        instance.active_request = _update_model(active, timed_out=True)
        future = self._waiters.pop(request_id, None)
        if future is not None and not future.done():
            future.cancel()
        self._save(state)
        if instance.stopped:
            return True
        if not interrupt:
            return False
        return await self._interrupt_active_request(
            state, service_id, instance, request_id
        )

    async def _interrupt_active_request(
        self,
        state: RunnerState,
        service_id: str,
        instance: ServiceInstance,
        request_id: str,
    ) -> bool:
        """Journal/send a targeted interrupt and clear active work only after success.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
            instance: Current service instance whose ownership/queue/lifecycle is
                being handled.
            request_id: UUID correlating the admitted request and its eventual
                outcome.

        Returns:
            True only after successful participant acknowledgement and clearing the
            active request; False on refusal or connection/timeout failure.
        """
        control_id = str(uuid4())
        state.used_request_ids.add(control_id)
        self._journal.client.record_event(
            "control.intent",
            {
                "action": "interrupt_request",
                "request_id": control_id,
                "target_request_id": request_id,
            },
            context=_context(state, service_id, instance.service_instance_id),
        )
        try:
            reply = await self._exchange(
                service_id,
                control_id,
                "interrupt",
                {"request_id": request_id},
                timeout=state.template.start_timeout,
            )
            if reply["result"] == "success":
                instance.active_request = None
                self._save(state)
                return True
        except (OSError, TimeoutError):
            pass
        return False

    async def _poll_result(self, state: RunnerState, service_id: str) -> None:
        """Read journal evidence for active service work and apply its matching result observation.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            service_id: Stable service definition UUID.
        """
        instance = state.services[service_id]
        active = instance.active_request
        if active is None:
            return
        record = read_result(
            self._journal.client,
            active.request_id,
            expected={
                "experiment_id": state.experiment_id,
                "participant_id": service_id,
                "participant_instance_id": instance.service_instance_id,
            },
        )
        if record is None:
            return
        if (
            record["author"] == "runner"
            and not active.timed_out
            and active.owner != "caller"
        ):
            self._finish_request(
                state, instance, active, record["response"], record["outcome"]
            )
            instance.active_request = None
            self._save(state)
            return
        participant = next(
            (
                item
                for item in record["observations"]
                if item["author"] == "participant"
            ),
            None,
        )
        if participant is not None:
            await self._handle_message(
                state,
                service_id,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "message_type": "response",
                    "command": active.command,
                    "request_id": active.request_id,
                    **participant["event"]["data"]["response"],
                },
            )
