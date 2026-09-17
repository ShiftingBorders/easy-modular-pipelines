"""Runner-side service supervision and persistent working-request queues.

External service internals
belong to their Python proxies. Global pause/stop decisions return to the runner.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

import psutil

from core.logger_utils.events import LoggingError, copy_json_object, require_text
from core.runner_utils.connection import ParticipantConnection
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.launch import ModuleLauncher
from core.runner_utils.protocol import PROTOCOL_VERSION, error_details
from core.runner_utils.results import read_result
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from core.runner_utils.state import (
    JsonObject,
    RunnerState,
    RunnerStateStore,
    ServiceInstance,
)

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
        if (
            self._closed
            or state.services
            or self._monitor_task is not None
            and not self._monitor_task.done()
        ):
            raise RuntimeError(
                "start_all requires an open manager and no existing services."
            )
        definitions = state.template["services"]
        if type(definitions) is not list:
            raise TypeError("services must be an array.")
        identifiers = [item["service_id"] for item in definitions]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("Service definition IDs must be unique.")
        if not definitions:
            return "ready"
        self._monitor_task = asyncio.create_task(self.monitor(state))
        try:
            for definition in definitions:
                try:
                    await self._start(state, definition)
                except (OSError, ConnectionError) as error:
                    instance = state.services.get(definition["service_id"])
                    if instance is None:
                        raise
                    instance.failure = error_details(
                        "service_failure", f"{type(error).__name__}: {error}"
                    )
                    action = await self.restart(
                        state, instance.service_id, automatic=True
                    )
                    if action != "ready":
                        return action
                action = await self.wait_ready(state)
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

    async def _start(
        self, state: RunnerState, definition: JsonObject
    ) -> ServiceInstance:
        service_id = require_text(definition["service_id"], "service_id")
        old = state.services.get(service_id)
        if old is not None and not old.stopped:
            raise RuntimeError(
                "The previous service must be confirmed stopped before launch."
            )
        instance_id = str(uuid4())
        context = self._context(state, service_id, instance_id)
        directory = (
            state.experiment_directory
            / "shared_artifacts/services"
            / service_id
            / instance_id
        )
        launch = self._launcher.prepare(state, definition, context, directory, None)
        instance = ServiceInstance(service_id, instance_id, definition)
        instance.implementation = launch["module"]["implementation"]
        instance.artifacts_directory = directory
        instance.endpoint_path = Path(launch["endpoint_path"])
        if old is not None:
            instance.restart_count = old.restart_count
            for entry in old.pending_requests:
                if entry.get("expected_instance") is not None:
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
        state.services[service_id] = instance
        self._starting.add(service_id)
        instance.start_deadline = time.monotonic() + state.template["start_timeout"]
        spawn = None
        try:
            self._journal.client.record_event(
                "service.parameters",
                {
                    "definition": definition,
                    "effective_settings": launch["effective_settings"],
                    "template_revision_id": state.template_revision_id,
                },
                context=context,
            )
            self._journal.client.record_event(
                "control.intent",
                {"action": "start_service", "argv": launch["argv"]},
                context=context,
            )
            with (
                (directory / "stdout.log").open("ab") as stdout,
                (directory / "stderr.log").open("ab") as stderr,
            ):
                environment = dict(os.environ)
                library = str(Path(__file__).resolve().parents[2])
                environment["PYTHONPATH"] = os.pathsep.join(
                    filter(None, (library, environment.get("PYTHONPATH")))
                )
                spawn = asyncio.create_task(
                    asyncio.to_thread(
                        subprocess.Popen,
                        launch["argv"],
                        cwd=launch["code_directory"],
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
                        process_identity(process.pid)
                        if process.poll() is None
                        else None
                    )
                    raise
            self._processes[service_id] = process
            self._launch_processes.append(process)
            instance.started_at = datetime.now(UTC).isoformat()
            instance.process_identity = (
                process_identity(process.pid) if process.poll() is None else None
            )
            write_json(
                directory / "process.json",
                {
                    **context,
                    "process": instance.process_identity,
                    "started_at": instance.started_at,
                },
            )
            self._save(state)
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
                if declared != instance.process_identity:
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
                instance.process_identity = declared
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
                        instance.failure["message"]
                        if instance.failure
                        else "Service did not confirm full readiness."
                    )
                break
            if not instance.ready:
                raise TimeoutError(
                    f"Service {service_id} readiness exceeded start_timeout."
                )
            write_json(
                directory / "process.json",
                {
                    **context,
                    "process": instance.process_identity,
                    "started_at": instance.started_at,
                },
            )
            self._journal.client.record_event(
                "service.started",
                {
                    "process": instance.process_identity,
                    "implementation": instance.implementation,
                },
                context=context,
            )
            self._save(state)
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

    async def wait_ready(self, state: RunnerState) -> ServiceAction:
        while not self._closed:
            if self._pending_action is not None:
                action, self._pending_action = self._pending_action, None
                return action
            for instance in state.services.values():
                if instance.blocked_action is not None:
                    return instance.blocked_action
            if all(
                instance.ready and not instance.stopping and not instance.stopped
                for instance in state.services.values()
            ):
                return "ready"
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

    async def monitor(self, state: RunnerState) -> Literal["pause", "stop"]:
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
                self._sends.pop(request_id, None)
                instance = state.services[service_id]
                try:
                    await self._handle_message(state, service_id, task.result())
                except (OSError, EOFError, ValueError, TypeError, KeyError) as error:
                    if (
                        service_id in self._connecting
                        or instance.stopping
                        or instance.stopped
                    ):
                        continue
                    instance.ready = False
                    self._probes.pop(service_id, None)
                    self._bad_replies[service_id] = (
                        self._bad_replies.get(service_id, 0) + 1
                    )
                    self._journal.client.record_error(
                        error,
                        context=self._context(
                            state, service_id, instance.service_instance_id
                        ),
                    )
                    if self._bad_replies[service_id] >= 2:
                        instance.failure = error_details(
                            "connection_failed",
                            f"Participant communication failed: {error}",
                        )
                    else:
                        self._connecting[service_id] = asyncio.create_task(
                            self._connections[service_id].connect(
                                timeout_seconds=instance.definition["heartbeat"][
                                    "grace_seconds"
                                ]
                            )
                        )
                        self._next_probe[service_id] = 0
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
                    instance.stopped
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
                        instance.failure = error_details(
                            "connection_failed", f"Reconnect failed: {error}"
                        )
                await self._poll_result(state, service_id)
                if (
                    not instance.ever_ready
                    and instance.start_deadline is not None
                    and time.monotonic() >= instance.start_deadline
                ):
                    instance.failure = error_details(
                        "startup_timeout",
                        "The original service startup deadline expired.",
                    )
                process = self._processes.get(service_id)
                if (
                    process is not None
                    and instance.process_identity is not None
                    and process.pid == instance.process_identity["pid"]
                    and process.poll() is not None
                ):
                    instance.failure = error_details(
                        "process_exited", "Service process exited unexpectedly."
                    )
                probe = self._probes.get(service_id)
                if (
                    probe is not None
                    and instance.ever_ready
                    and time.monotonic() - probe["sent_monotonic"]
                    >= instance.definition["heartbeat"]["grace_seconds"]
                ):
                    instance.failure = error_details(
                        "heartbeat_timeout", "Service heartbeat grace period expired."
                    )
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
                    and active.get("owner") != "caller"
                    and not active["timed_out"]
                    and time.monotonic() - active["sent_monotonic"]
                    >= instance.definition["command_timeout_seconds"]
                ):
                    action = await self._handle_timeout(
                        state, service_id, active["request_id"]
                    )
                    if action in ("wait", "stop"):
                        self._pending_action = None
                        return "pause" if action == "wait" else "stop"
                    self._restarts[service_id] = asyncio.create_task(
                        self.restart(state, service_id, automatic=True)
                    )
                    continue
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
                        context=self._context(
                            state, service_id, instance.service_instance_id
                        ),
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
                                    else instance.definition["heartbeat"][
                                        "grace_seconds"
                                    ]
                                ),
                            )
                        ),
                    )
                if instance.ready and instance.blocked_action is None:
                    await self._send_next(state, service_id)
            await asyncio.sleep(0.05)
        raise asyncio.CancelledError

    async def restart(
        self, state: RunnerState, service_id: str, *, automatic: bool
    ) -> ServiceAction:
        instance = state.services[service_id]
        retry = instance.active_request
        retry = (
            retry
            if retry is not None
            and retry["timed_out"]
            and retry.get("owner") != "caller"
            and instance.definition["on_command_timeout"] == "restart"
            else None
        )
        waiter = None if retry is None else self._waiters.pop(retry["request_id"], None)
        while not self._closed:
            policy = instance.definition["errors"]
            exhausted = (
                automatic
                and instance.restart_count >= policy["retries"]
                and policy["on_exhausted"] != "skip"
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
                instance.blocked_action = policy["on_exhausted"]
                if waiter is not None and not waiter.done():
                    waiter.set_result(
                        {
                            "request_id": retry["request_id"],
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
            if automatic:
                instance.restart_count += 1
                try:
                    await asyncio.sleep(policy["retry_delay_seconds"])
                except asyncio.CancelledError:
                    if waiter is not None and not waiter.done():
                        waiter.cancel()
                    raise
            try:
                instance = await self._start(state, instance.definition)
            except asyncio.CancelledError:
                if waiter is not None and not waiter.done():
                    waiter.cancel()
                raise
            except (OSError, ConnectionError) as error:
                instance = state.services[service_id]
                instance.failure = error_details(
                    "service_failure", f"{type(error).__name__}: {error}"
                )
                if not automatic:
                    if waiter is not None and not waiter.done():
                        waiter.set_exception(error)
                    raise
                continue
            if retry is not None:
                replacement = {
                    **retry,
                    "request_id": str(uuid4()),
                    "sent_monotonic": None,
                    "sent_at": None,
                    "service_instance_id": None,
                    "timed_out": False,
                    "retry_of": retry["request_id"],
                }
                if replacement["request_id"] in state.used_request_ids:
                    raise RuntimeError("Request ID collision.")
                state.used_request_ids.add(replacement["request_id"])
                instance.pending_requests.insert(0, replacement)
                if waiter is not None:
                    self._waiters[replacement["request_id"]] = waiter
                self._journal.client.record_event(
                    "service.request_queued",
                    replacement,
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
            self._changed.set()
            return "ready"
        raise RuntimeError("Service manager is closed.")

    async def request(
        self, state: RunnerState, service_id: str, command: str, args: JsonObject
    ) -> JsonObject:
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
                entry.get("deadline_monotonic") is None
                or time.monotonic() < entry["deadline_monotonic"]
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
        context = self._context(state, service_id, instance.service_instance_id)
        self._journal.client.record_event(
            "control.intent", {"action": "service_request", **entry}, context=context
        )
        instance.pending_requests.pop(0)
        instance.active_request = entry
        entry["service_instance_id"] = instance.service_instance_id
        entry["sent_at"] = datetime.now(UTC).isoformat()
        entry["sent_monotonic"] = time.monotonic()
        self._save(state)
        self._journal.client.record_event(
            "service.send_started", entry, context=context
        )
        self._sends[entry["request_id"]] = (
            service_id,
            asyncio.create_task(
                self._exchange(
                    service_id,
                    entry["request_id"],
                    entry["command"],
                    entry["args"],
                    deadline=entry.get("deadline_monotonic"),
                )
            ),
        )

    async def _handle_message(
        self, state: RunnerState, service_id: str, message: JsonObject
    ) -> None:
        instance = state.services[service_id]
        message = copy_json_object(message, "service message")
        context = {
            "experiment_id": state.experiment_id,
            "run_id": state.run_id,
            "service_id": service_id,
            "service_instance_id": instance.service_instance_id,
            "participant_id": service_id,
            "participant_instance_id": instance.service_instance_id,
        }
        if (
            type(message.get("protocol_version")) is not int
            or message["protocol_version"] != PROTOCOL_VERSION
        ):
            raise ValueError(f"Service protocol_version must be {PROTOCOL_VERSION}.")
        request_id = require_text(message.get("request_id"), "request_id")
        UUID(request_id)
        for key in ("experiment_id", "service_id", "service_instance_id"):
            if message.get(key, context[key]) != context[key]:
                self._journal.client.record_event(
                    "service.message_ignored",
                    {"ignored": "different_instance", "message": message},
                    context=context,
                )
                return
        if message.get("result") not in ("success", "fail") or "data" not in message:
            raise ValueError("Service replies require result=success/fail and data.")
        kind = "status" if message.get("command") == "heartbeat" else "command_result"
        if kind not in ("status", "command_result", "command_state"):
            raise ValueError("Unknown service message_type.")
        self._journal.client.record_event("service.message", message, context=context)
        if kind == "status":
            probe = self._probes.get(service_id)
            if probe is None or probe["request_id"] != request_id:
                self._journal.client.record_event(
                    "service.message_ignored",
                    {"ignored": "old_probe", "message": message},
                    context=context,
                )
                return
            if (
                not instance.ever_ready
                and instance.start_deadline is not None
                and time.monotonic() >= instance.start_deadline
            ):
                instance.failure = error_details(
                    "startup_timeout", "Service replied after its startup deadline."
                )
                instance.ready = False
            elif (
                instance.ever_ready
                and time.monotonic() - probe["sent_monotonic"]
                >= instance.definition["heartbeat"]["grace_seconds"]
            ):
                instance.failure = error_details(
                    "heartbeat_timeout",
                    "Service replied after heartbeat grace expired.",
                )
                instance.ready = False
            else:
                instance.last_status = {
                    **message,
                    "observed_at": datetime.now(UTC).isoformat(),
                    "observed_monotonic": time.monotonic(),
                }
                instance.ready = message["result"] == "success"
                instance.ever_ready = instance.ever_ready or instance.ready
                instance.failure = error_details(
                    "service_failure",
                    None if instance.ready else "Service requested a full restart.",
                )
                self._bad_replies[service_id] = 0
            self._probes.pop(service_id, None)
            self._next_probe[service_id] = (
                time.monotonic() + instance.definition["heartbeat"]["interval_seconds"]
            )
        elif kind == "command_result":
            active = instance.active_request
            if active is None or active["request_id"] != request_id:
                self._journal.client.record_event(
                    "service.message_ignored",
                    {"ignored": "retired_or_unknown_request", "message": message},
                    context=context,
                )
                return
            response = {
                key: value
                for key, value in message.items()
                if key
                not in ("protocol_version", "message_type", "request_id", "command")
            }
            if (
                active.get("owner") != "caller"
                and not active["timed_out"]
                and time.monotonic() - active["sent_monotonic"]
                >= instance.definition["command_timeout_seconds"]
            ):
                await self._handle_timeout(state, service_id, request_id)
            if active["timed_out"]:
                self._journal.client.record_event(
                    "service.message_ignored",
                    {"ignored": "command_timeout", "message": message},
                    context=context,
                )
                # A late result releases actual work, never changes its timed-out outcome.
                if (
                    active.get("owner") != "caller"
                    and instance.definition["on_command_timeout"] == "restart"
                ):
                    instance.failure = error_details(
                        "service_failure",
                        "Timed-out command requires an explicit restart.",
                    )
                    self._restarts[service_id] = asyncio.create_task(
                        self.restart(state, service_id, automatic=True)
                    )
                    return
                if instance.blocked_action == "pause":
                    instance.blocked_action = None
            else:
                self._finish_request(
                    state,
                    instance,
                    active,
                    response,
                    "succeeded" if response["result"] == "success" else "failed",
                )
            instance.active_request = None
        try:
            self._state_store.save(state)
        except OSError as error:
            self._journal.client.record_error(error, context=context)
        self._changed.set()

    async def _handle_timeout(
        self, state: RunnerState, service_id: str, request_id: str
    ) -> Literal["wait", "restart", "stop"]:
        instance = state.services[service_id]
        active = instance.active_request
        if active is None or active["request_id"] != request_id:
            raise ValueError("Timeout does not match the active service request.")
        action = instance.definition["on_command_timeout"]
        if not active["timed_out"]:
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
            active["timed_out"] = True
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

    async def prepare_rebuild(self, state: RunnerState, template: JsonObject) -> None:
        desired = {item["service_id"]: item for item in template["services"]}
        old_modules = {
            (item["module"]["name"], item["module"]["version"]): item["module"]["hash"]
            for item in [*state.template["stages"], *state.template["services"]]
            if "module" in item
        }
        new_modules = {
            (item["module"]["name"], item["module"]["version"]): item["module"]["hash"]
            for item in [*template["stages"], *template["services"]]
            if "module" in item
        }
        changed_code = {
            key for key, digest in old_modules.items() if new_modules.get(key) != digest
        }
        for service_id, instance in list(state.services.items()):
            module = instance.definition["module"]
            if (
                desired.get(service_id) == instance.definition
                and (module["name"], module["version"]) not in changed_code
            ):
                continue
            results = await self.stop_all(
                state, service_ids={service_id}, preserve_pending=service_id in desired
            )
            if not results[service_id]["stopped"]:
                raise RuntimeError(
                    f"Service {service_id} has not stopped; rebuilding is unsafe."
                )

    async def reconcile(
        self, state: RunnerState, template: JsonObject
    ) -> ServiceAction:
        desired = {item["service_id"]: item for item in template["services"]}
        for service_id, instance in list(state.services.items()):
            if service_id not in desired:
                if not instance.stopped:
                    raise RuntimeError(
                        "Removed services must stop before reconciliation."
                    )
                del state.services[service_id]
        for definition in template["services"]:
            instance = state.services.get(definition["service_id"])
            if instance is not None and instance.blocked_action is not None:
                return instance.blocked_action
            if instance is not None and not instance.stopped:
                if instance.definition != definition:
                    raise RuntimeError(
                        "Changed services must stop before reconciliation."
                    )
                continue
            if (
                instance is not None
                and instance.definition["module"] != definition["module"]
            ):
                instance.restart_count = 0
            try:
                await self._start(state, definition)
            except (OSError, ConnectionError) as error:
                instance = state.services.get(definition["service_id"])
                if instance is None:
                    raise
                instance.failure = error_details(
                    "service_failure", f"{type(error).__name__}: {error}"
                )
                action = await self.restart(state, instance.service_id, automatic=True)
                if action != "ready":
                    return action
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self.monitor(state))
        return await self.wait_ready(state)

    async def recover(self, state: RunnerState) -> ServiceAction:
        if set(state.services) != {
            item["service_id"] for item in state.template["services"]
        }:
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
        # A failed optional state write can leave an already sent request queued.
        # Consult the mandatory send record before the monitor can dispatch it.
        pending_ids = {
            entry["request_id"]
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
                if entry["request_id"] not in sent_ids:
                    continue
                recorded = read_result(
                    self._journal.client,
                    entry["request_id"],
                    expected={
                        "experiment_id": state.experiment_id,
                        "participant_id": instance.service_id,
                        "participant_instance_id": entry.get(
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
                instance.failure = error_details(
                    "service_failure",
                    "Saved queue contains an already sent request with an unresolved outcome.",
                )
                instance.blocked_action = self._pending_action = "stop"
                return "stop"
        self._starting.update(state.services)
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self.monitor(state))
        for service_id, instance in state.services.items():
            try:
                if instance.stopped:
                    instance.blocked_action = self._pending_action = "pause"
                    # Still reconnect the remaining live services so a partial
                    # shutdown cannot disable their supervision during recovery.
                    continue
                instance.ready = False
                if instance.process_identity is None or instance.endpoint_path is None:
                    instance.failure = error_details(
                        "service_failure",
                        "Cannot identify the previous service process.",
                    )
                    instance.blocked_action = "stop"
                    self._pending_action = "stop"
                    return "stop"
                try:
                    announced = read_json(instance.endpoint_path)
                except FileNotFoundError:
                    announced = {}
                if (
                    announced
                    and announced.get("participant_instance_id")
                    != instance.service_instance_id
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
                            instance.failure = error_details(
                                "ownership_unknown",
                                "Endpoint belongs to an unaccounted-for service instance.",
                            )
                            instance.blocked_action = self._pending_action = "stop"
                            return "stop"
                try:
                    actual = process_identity(instance.process_identity["pid"])
                except OSError as error:
                    if not isinstance(
                        error, (FileNotFoundError, ProcessLookupError)
                    ) and getattr(error, "winerror", None) not in (87, 1168):
                        raise
                    actual = None
                if actual != instance.process_identity:
                    instance.failure = error_details(
                        "service_failure", "Previous service process no longer exists."
                    )
                    action = await self.restart(state, service_id, automatic=True)
                    if action != "ready":
                        return action
                    continue
                timeout = instance.definition["heartbeat"]["grace_seconds"]
                if not instance.ever_ready:
                    if instance.start_deadline is None:
                        raise ValueError(
                            "An unfinished service startup requires its original deadline."
                        )
                    timeout = max(0, instance.start_deadline - time.monotonic())
                    if timeout == 0:
                        instance.failure = error_details(
                            "startup_timeout",
                            "Service startup expired while runner was unavailable.",
                        )
                        action = await self.restart(state, service_id, automatic=True)
                        if action != "ready":
                            return action
                        continue
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
                await asyncio.wait_for(
                    connection.connect(timeout_seconds=timeout), timeout
                )
                request_id = str(uuid4())
                if request_id in state.used_request_ids:
                    raise RuntimeError("Request ID collision.")
                state.used_request_ids.add(request_id)
                self._journal.client.record_event(
                    "control.intent",
                    {"action": "command_state", "request_id": request_id},
                    context=context,
                )
                reply = await connection.query_command_state(
                    request_id,
                    timeout_seconds=max(0.001, reconnect_deadline - time.monotonic()),
                )
                observed = copy_json_object(reply["data"], "service command state")
                if (
                    type(reply.get("protocol_version")) is not int
                    or reply["protocol_version"] != PROTOCOL_VERSION
                    or reply.get("message_type") != "response"
                ):
                    raise ValueError("Invalid service command_state envelope.")
                if reply.get("result") != "success" or not {
                    "current",
                    "pending",
                }.issubset(observed):
                    raise ValueError(
                        "command_state requires a successful current/pending response."
                    )
                if (
                    type(observed["pending"]) is not list
                    or observed["current"] is not None
                    and type(observed["current"]) is not dict
                ):
                    raise ValueError("Invalid current/pending service commands.")
                self._journal.client.record_event(
                    "service.commands_reconciled", observed, context=context
                )
                active = instance.active_request
                participant_work = [*observed["pending"]]
                if observed["current"] is not None:
                    participant_work.append(observed["current"])
                if any(
                    type(item) is not dict
                    or active is None
                    or item.get("request_id") != active["request_id"]
                    for item in participant_work
                ):
                    instance.failure = error_details(
                        "service_failure",
                        "Participant reports work not matched to the saved sent request.",
                    )
                    # Unknown work needs an owner decision, not an automatic restart
                    # that would destroy the very evidence recovery must reconcile.
                    instance.blocked_action = "stop"
                    self._pending_action = "stop"
                    return "stop"
                await self._poll_result(state, service_id)
                self._probes.pop(service_id, None)
                self._next_probe[service_id] = 0
            finally:
                self._starting.discard(service_id)
        action = await self.wait_ready(state)
        return (
            "pause" if action == "ready" and self._snapshot_id is not None else action
        )

    async def save_states(
        self, state: RunnerState, snapshot_id: str
    ) -> dict[str, Path]:
        UUID(snapshot_id)
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
                        and instance.active_request["timed_out"]
                    )
                    for instance in state.services.values()
                ):
                    raise RuntimeError(
                        "Service work cannot be drained for this snapshot."
                    )
                self._changed.clear()
                await self._changed.wait()
            for service_id, instance in state.services.items():
                if not instance.ready:
                    raise RuntimeError("Snapshot requires all services to be ready.")
                self._frozen_instances[service_id] = instance.service_instance_id
                instance.prepared_freeze_id = snapshot_id
                reply = await self.request(
                    state, service_id, "freeze_writes", {"snapshot_id": snapshot_id}
                )
                if (
                    reply["result"] != "success"
                    or state.services[service_id] is not instance
                ):
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
                if not output.resolve().is_relative_to(
                    state.experiment_directory.resolve()
                ):
                    raise ValueError("Service export directory escapes the experiment.")
                output.mkdir(parents=True, exist_ok=False)
                reply = await self.request(
                    state,
                    service_id,
                    "save_state",
                    {"snapshot_id": snapshot_id, "output_directory": str(output)},
                )
                if (
                    reply["result"] != "success"
                    or state.services[service_id] is not instance
                ):
                    raise RuntimeError(f"Service {service_id} state export failed.")
                data = copy_json_object(reply["data"], "service state export")
                state_path = data.get("state_path")
                if state_path is None and not instance.definition["state_required"]:
                    continue
                relative = Path(require_text(state_path, "state_path"))
                resolved = (state.experiment_directory / relative).resolve()
                if (
                    relative.anchor
                    or not resolved.is_relative_to(output.resolve())
                    or not resolved.exists()
                ):
                    raise ValueError(
                        "Service state_path must exist inside its allocated export directory."
                    )
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

    async def load_states(
        self, state: RunnerState, service_states: dict[str, Path]
    ) -> None:
        if service_states.keys() - state.services.keys():
            raise ValueError("State was supplied for unknown services.")
        paths = {}
        root = state.experiment_directory.resolve()
        for service_id, instance in state.services.items():
            supplied = service_states.get(service_id)
            if supplied is None:
                if instance.definition["state_required"]:
                    raise ValueError(f"Required service state is missing: {service_id}")
                continue
            path = (root / supplied).resolve()
            if not path.is_relative_to(root) or not path.exists():
                raise ValueError(
                    "Restored service state must exist inside the experiment."
                )
            paths[service_id] = path.relative_to(root).as_posix()
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
            self._probes.pop(service_id, None)
            self._next_probe[service_id] = 0
        if await self.wait_ready(state) != "ready":
            raise RuntimeError("Restored services did not confirm fresh readiness.")

    async def unfreeze(self, state: RunnerState, snapshot_id: str) -> None:
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
                and instance.active_request["timed_out"]
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
        selected = set(state.services) if service_ids is None else service_ids
        if selected - state.services.keys():
            raise ValueError("Cannot stop an unknown service.")
        results = {}
        for service_id in reversed(list(state.services)):
            if service_id not in selected:
                continue
            instance = state.services[service_id]
            instance.ready = False
            instance.stopping = True
            context = {
                "experiment_id": state.experiment_id,
                "run_id": state.run_id,
                "service_id": service_id,
                "service_instance_id": instance.service_instance_id,
                "participant_id": service_id,
                "participant_instance_id": instance.service_instance_id,
            }
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
            error_message = None
            shutdown_response = None
            deadline = time.monotonic() + state.template["start_timeout"]
            owned_process = self._processes.get(service_id)
            if (
                owned_process is not None
                and instance.process_identity is not None
                and owned_process.pid == instance.process_identity["pid"]
                and owned_process.poll() is not None
            ):
                instance.stopped = True
            request_id = str(uuid4())
            if request_id in state.used_request_ids:
                raise RuntimeError("Request ID collision.")
            state.used_request_ids.add(request_id)
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
                    error.add_note(
                        f"Emergency service-stop recording failed: {secondary}"
                    )
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
                        shutdown_response = {
                            "result": reply["result"],
                            "data": reply["data"],
                        }
                        if reply["result"] == "fail":
                            error_message = "Service shutdown reported failure."
            except Exception as error:  # noqa: BLE001 - One failed stop must not leave other services untouched.
                error_message = str(error)
            finally:
                if connection is not None:
                    await connection.close()
            process = self._processes.get(service_id)
            while not instance.stopped:
                try:
                    if (
                        process is not None
                        and instance.process_identity is not None
                        and process.pid == instance.process_identity["pid"]
                        and process.poll() is not None
                    ):
                        instance.stopped = True
                    elif instance.process_identity is None:
                        instance.stopped = (
                            process is not None and process.poll() is not None
                        )
                    else:
                        observed = process_identity(instance.process_identity["pid"])
                        instance.stopped = observed != instance.process_identity
                        if not instance.stopped:
                            participant = psutil.Process(observed["pid"])
                            instance.stopped = (
                                participant.status() == psutil.STATUS_ZOMBIE
                            )
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
                and process.pid == instance.process_identity["pid"]
            ):
                # The owned handle cannot target a reused PID. Do not grant another
                # command timeout or claim that an asynchronous kill has completed.
                if process.poll() is None:
                    try:
                        process.kill()
                    except OSError as error:
                        error_message = str(error)
                instance.stopped = process.poll() is not None
            if not instance.stopped:
                error_message = error_message or "Service termination is unconfirmed."
                instance.failure = error_details("service_failure", error_message)
                instance.blocked_action = "stop"
                self._pending_action = "stop"
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
                        "cancelled" if entry["sent_monotonic"] is None else "failed",
                    )
                except LoggingError as error:
                    error_message = str(error)
            instance.stopping = False
            if self._notify_resources is not None:
                self._notify_resources()
            if process is not None:
                process.poll()
            results[service_id] = {"stopped": instance.stopped, "error": error_message}
            try:
                self._journal.client.record_command_result(
                    request_id,
                    shutdown_response
                    or {
                        "result": "success" if instance.stopped else "fail",
                        "data": results[service_id],
                    },
                    author="runner",
                    outcome="succeeded"
                    if instance.stopped
                    and (shutdown_response or {}).get("result") != "fail"
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
        return results

    async def reset(self, state: RunnerState) -> None:
        """Release a stopped generation before binding restored experiment state."""
        if any(
            not item.stopped or item.active_request or item.pending_requests
            for item in state.services.values()
        ):
            raise RuntimeError(
                "Service reset requires confirmed stops and empty queues."
            )
        await self.close()
        deadline = time.monotonic() + state.template["start_timeout"]
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

    def _context(
        self, state: RunnerState, service_id: str, instance_id: str
    ) -> JsonObject:
        return {
            "experiment_id": state.experiment_id,
            "run_id": state.run_id,
            "service_id": service_id,
            "service_instance_id": instance_id,
            "participant_id": service_id,
            "participant_instance_id": instance_id,
        }

    def _save(self, state: RunnerState) -> None:
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
        entry: JsonObject,
        response: JsonObject,
        outcome: str,
    ) -> None:
        context = self._context(
            state, instance.service_id, instance.service_instance_id
        )
        if entry.get("owner") != "caller" and not entry["timed_out"]:
            self._journal.client.record_command_result(
                entry["request_id"],
                response,
                author="runner",
                outcome=outcome,
                context=context,
            )
        elif entry.get("owner") == "caller":
            self._journal.client.record_event(
                "service.request_retired",
                {"request_id": entry["request_id"], "response": response},
                context=context,
            )
        future = self._waiters.pop(entry["request_id"], None)
        if future is not None and not future.done():
            future.set_result({"request_id": entry["request_id"], **response})
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
        instance = state.services[service_id]
        if self._closed or instance.stopped or instance.stopping:
            raise RuntimeError("Working requests require an active service.")
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
            entry["request_id"] == request_id
            for item in state.services.values()
            for entry in [
                *item.pending_requests,
                *([] if item.active_request is None else [item.active_request]),
            ]
        ):
            raise ValueError("A queued or sent request cannot be submitted again.")
        state.used_request_ids.add(request_id)
        entry = {
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
            entry["expected_instance"] = instance.service_instance_id
        self._journal.client.record_event(
            "service.request_queued",
            entry,
            context=self._context(state, service_id, instance.service_instance_id),
        )
        instance.pending_requests.append(entry)
        future = asyncio.get_running_loop().create_future()
        self._waiters[request_id] = future
        self._save(state)
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self.monitor(state))
        return future

    async def cancel_request(
        self,
        state: RunnerState,
        service_id: str,
        request_id: str,
        *,
        interrupt: bool = False,
    ) -> bool:
        instance = state.services[service_id]
        for entry in list(instance.pending_requests):
            if entry["request_id"] == request_id:
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
        if active is None or active["request_id"] != request_id:
            return True
        active["timed_out"] = True
        future = self._waiters.pop(request_id, None)
        if future is not None and not future.done():
            future.cancel()
        self._save(state)
        if instance.stopped:
            return True
        if not interrupt:
            return False
        control_id = str(uuid4())
        state.used_request_ids.add(control_id)
        self._journal.client.record_event(
            "control.intent",
            {
                "action": "interrupt_request",
                "request_id": control_id,
                "target_request_id": request_id,
            },
            context=self._context(state, service_id, instance.service_instance_id),
        )
        try:
            reply = await self._exchange(
                service_id,
                control_id,
                "interrupt",
                {"request_id": request_id},
                timeout=state.template["start_timeout"],
            )
            if reply["result"] == "success":
                instance.active_request = None
                self._save(state)
                return True
        except (OSError, TimeoutError):
            pass
        return False

    async def _poll_result(self, state: RunnerState, service_id: str) -> None:
        instance = state.services[service_id]
        active = instance.active_request
        if active is None:
            return
        record = read_result(
            self._journal.client,
            active["request_id"],
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
            and not active["timed_out"]
            and active.get("owner") != "caller"
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
                    "command": active["command"],
                    "request_id": active["request_id"],
                    **participant["event"]["data"]["response"],
                },
            )
