"""Runner-owned DAG attempts through the common participant call protocol."""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

import psutil

from core.logger_utils.events import LoggingError
from core.runner_utils.connection import ParticipantConnection
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.launch import ModuleLauncher
from core.runner_utils.results import read_result
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from core.runner_utils.state import (
    JsonObject,
    JsonValue,
    RunnerState,
    RunnerStateStore,
    StageAttempt,
    StageOutcome,
)


class StageRunner:
    def __init__(
        self,
        launcher: ModuleLauncher,
        journal: RunnerJournal,
        state_store: RunnerStateStore,
        *,
        services=None,
        notify_resources: Callable[[], None] | None = None,
    ) -> None:
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
        self._executor_identities = {}
        self._notify_resources = notify_resources

    def bind_services(self, services) -> None:
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
        definition = state.template["stages"][state.stage_position - 1]
        stage_id = definition["stage_id"]
        input_data = (
            self.select_input(state) if recovered is None else recovered.input_data
        )
        if manual:
            state.stage_result_ids.pop(stage_id, None)
            state.stage_result_origins.pop(stage_id, None)
            state.last_result = state.last_result_id = None
        policy = definition["errors"]
        while True:
            attempt = (
                recovered
                if recovered is not None and recovered.outcome != "unknown_stopped"
                else await self._start_attempt(state, input_data)
            )
            recovered = None
            response = await self._collect_result(state, attempt)
            self._journal.client.record_event(
                "stage.finished",
                {
                    "result": response,
                    "outcome": attempt.outcome,
                    "result_request_id": attempt.result_request_id,
                },
                context=self._context(state, attempt),
            )
            self._save_state(state)
            if attempt.outcome == "unknown":
                return StageOutcome(attempt, response, "stop")
            if response is not None and response["result"] == "success":
                return StageOutcome(attempt, response, "advance")
            if state.pause_requested:
                return StageOutcome(attempt, response, "pause")
            used = state.stage_retry_counts.get(stage_id, 0)
            if used >= policy["retries"]:
                return StageOutcome(
                    attempt,
                    response,
                    "advance"
                    if policy["on_exhausted"] == "skip"
                    else policy["on_exhausted"],
                )
            await asyncio.sleep(policy["retry_delay_seconds"])
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

    def select_input(self, state: RunnerState) -> JsonValue:
        if state.stage_position == 1:
            return None
        previous = state.template["stages"][state.stage_position - 2]["stage_id"]
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

    def _context(self, state: RunnerState, attempt: StageAttempt) -> JsonObject:
        definition = next(
            item
            for item in state.template["stages"]
            if item["stage_id"] == attempt.stage_id
        )
        module = self._launcher._assembler.module_reference(state.template, definition)
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
            else attempt.participant["participant_instance_id"],
            "cycle_number": attempt.cycle_number,
            "attempt_number": attempt.attempt_number,
            "module_name": module["name"],
            "module_version": module["version"],
            "module_hash": module["hash"],
            "template_revision_id": state.template_revision_id,
            **(attempt.participant or {}),
        }

    async def _start_attempt(
        self, state: RunnerState, input_data: JsonValue
    ) -> StageAttempt:
        definition = state.template["stages"][state.stage_position - 1]
        module = self._launcher._assembler.module_reference(state.template, definition)
        stage_id = definition["stage_id"]
        number = state.stage_attempt_numbers.get(stage_id, 0) + 1
        attempt_id = str(uuid4())
        directory = (
            state.experiment_directory
            / "shared_artifacts"
            / f"epoch_{state.cycle_number}"
            / module["name"]
            / stage_id
            / f"attempt_{number}"
        )
        attempt = StageAttempt(
            attempt_id,
            stage_id,
            str(uuid4()),
            state.cycle_number,
            number,
            directory,
            input_data,
            {},
            definition["timeout_seconds"],
        )
        attempt.service_id = definition.get("service_id")
        instance = (
            None if attempt.service_id is None else state.services[attempt.service_id]
        )
        attempt.participant = {
            "experiment_id": state.experiment_id,
            "participant_id": stage_id if instance is None else instance.service_id,
            "participant_instance_id": attempt_id
            if instance is None
            else instance.service_instance_id,
        }
        attempt.queued_at = datetime.now(UTC).isoformat()
        attempt.queued_monotonic = time.monotonic()
        context = {
            **self._context(state, attempt),
            "template_revision_id": state.template_revision_id,
            "module_name": module["name"],
            "module_version": module["version"],
            "module_hash": module["hash"],
        }
        launch = self._launcher.prepare(
            state, definition, context, directory, input_data
        )
        attempt.endpoint_path = Path(launch["endpoint_path"])
        attempt.effective_settings = launch["effective_settings"]
        launch["runtime_context"].update(
            {
                "queued_at": attempt.queued_at,
                "queued_monotonic": attempt.queued_monotonic,
                "service_id": attempt.service_id,
            }
        )
        write_json(directory / "context.json", launch["runtime_context"])
        state.active_attempt = attempt
        state.stage_attempt_numbers[stage_id] = number
        if attempt.request_id in state.used_request_ids:
            raise RuntimeError("Attempt request ID collision.")
        if instance is None:
            state.used_request_ids.add(attempt.request_id)
        self._unstarted_attempt_id = attempt_id
        self._process_attempt_id = attempt_id
        self._process = None
        self._call_future = None
        if self._connection is not None:
            await self._connection.close()
        self._connection = None
        self._journal.client.record_attempt_parameters(
            state.template,
            attempt.effective_settings,
            template_yaml=state.template_yaml,
            context=context,
        )
        self._journal.client.record_event(
            "control.intent",
            {
                "action": "start_stage",
                "argv": None if instance is not None else launch["argv"],
                "service_id": attempt.service_id,
                "queued_monotonic": attempt.queued_monotonic,
                "queued_at": attempt.queued_at,
            },
            context=context,
        )
        self._save_state(state)
        self._prune_artifacts(state, attempt)
        if self._notify_resources is not None:
            self._notify_resources()
        deadline = self._deadline(attempt)
        if deadline is not None and time.monotonic() >= deadline:
            return attempt
        if instance is not None:
            if self._services is None:
                raise RuntimeError("Service attempts require a bound ServiceManager.")
            if state.services[instance.service_id] is not instance:
                return attempt
            self._call_future = self._services.enqueue(
                state,
                instance.service_id,
                attempt.request_id,
                "execute",
                launch["call"],
                deadline=deadline,
            )
            self._unstarted_attempt_id = None
            return attempt
        launch_path = directory / "launch.json"
        write_json(launch_path, launch)
        library_root = Path(__file__).resolve().parents[2]
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
                    "core.runner_utils.executor",
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
        startup_deadline = time.monotonic() + launch["control_timeout_seconds"]
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
                            launch["call"],
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
        return (
            None
            if attempt.timeout_seconds is None
            else attempt.queued_monotonic + attempt.timeout_seconds
        )

    async def _connect(self, attempt: StageAttempt, timeout: float) -> None:
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
    ) -> JsonObject:
        state.used_request_ids.add(attempt.request_id)
        self._journal.client.record_command_result(
            attempt.request_id,
            response,
            author="runner",
            outcome=outcome,
            context=self._context(state, attempt),
        )
        attempt.result_request_id = attempt.request_id
        attempt.outcome = outcome
        execution = response.get("execution", {})
        if execution:
            attempt.executor_status = execution
            attempt.process_identity = execution.get("process")
            attempt.started_at = execution.get("started_at")
        attempt.executor_status = {
            **(attempt.executor_status or {}),
            "finished": True,
            "current": None,
            "pending": [],
        }
        self._save_state(state)
        if self._notify_resources is not None:
            self._notify_resources()
        return response

    async def _collect_result(
        self, state: RunnerState, attempt: StageAttempt
    ) -> JsonObject:
        deadline = self._deadline(attempt)
        try:
            while True:
                record = read_result(
                    self._journal.client,
                    attempt.request_id,
                    expected={
                        **attempt.participant,
                        "stage_id": attempt.stage_id,
                        "attempt_id": attempt.attempt_id,
                    },
                )
                if record is not None and record["author"] == "runner":
                    attempt.result_request_id = attempt.request_id
                    attempt.outcome = record["outcome"]
                    execution = record["response"].get("execution", {})
                    attempt.executor_status = {
                        **execution,
                        "finished": True,
                        "current": None,
                        "pending": [],
                    }
                    if execution:
                        attempt.process_identity = execution.get("process")
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
                    return record["response"]
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
                        != attempt.participant["participant_instance_id"]
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
                    attempt.executor_status = {
                        "participant": attempt.participant,
                        "request_id": attempt.request_id,
                        "process": instance.process_identity,
                        "finished": False,
                        "current": instance.active_request,
                    }
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
                    if self._call_future is not None and self._call_future.done():
                        if not self._call_future.cancelled():
                            self._call_future.exception()
                        self._call_future = None
                    timeout = state.template["unknown_state"]["timeout_seconds"]
                    if deadline is not None:
                        timeout = min(timeout, max(0.001, deadline - time.monotonic()))
                    try:
                        if self._connection is None:
                            await self._connect(attempt, timeout)
                        request_id = str(uuid4())
                        state.used_request_ids.add(request_id)
                        reply = await self._connection.query_command_state(
                            request_id, timeout_seconds=timeout
                        )
                        first_observation = (
                            attempt.process_identity is None
                            and reply["data"].get("process") is not None
                        )
                        attempt.executor_status = reply["data"]
                        attempt.process_identity = reply["data"].get("process")
                        attempt.started_at = reply["data"].get("started_at")
                        self._save_state(state)
                        if first_observation and self._notify_resources is not None:
                            self._notify_resources()
                    except (OSError, EOFError):
                        if (
                            self._journal.client.read_command_result(attempt.request_id)
                            is not None
                        ):
                            continue
                        if deadline is not None and time.monotonic() >= deadline:
                            continue
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
                                continue
                            raise
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

    async def interrupt(self, state: RunnerState, reason: str) -> bool:
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
                instance is None
                or instance.stopped
                or instance.service_instance_id
                != attempt.participant["participant_instance_id"]
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
            attempt.process_identity = saved.get("stage")
            if saved.get("executor") is not None:
                self._executor_identities[attempt.request_id] = saved["executor"]
        deadline = time.monotonic() + state.template["unknown_state"]["timeout_seconds"]
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
                    timeout_seconds=state.template["start_timeout"]
                    + state.template["runner_timeout_margin_seconds"],
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
                    if (
                        process_identity(attempt.process_identity["pid"])
                        != attempt.process_identity
                    ):
                        return True
                    psutil.Process(attempt.process_identity["pid"]).wait(timeout=0)
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
        attempt = state.active_attempt
        if attempt is None:
            return None
        definition = state.template["stages"][state.stage_position - 1]
        if (
            definition["stage_id"] != attempt.stage_id
            or attempt.cycle_number != state.cycle_number
        ):
            raise ValueError("Saved attempt does not match the DAG cursor.")
        saved = read_json(attempt.artifacts_directory / "context.json")
        if (
            saved["context"]["attempt_id"] != attempt.attempt_id
            or json.dumps(saved["input_data"], sort_keys=True)
            != json.dumps(attempt.input_data, sort_keys=True)
            or json.dumps(saved["settings"], sort_keys=True)
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
                        attempt, state.template["unknown_state"]["timeout_seconds"]
                    )
                except (OSError, EOFError):
                    # The executor may finish between the journal read and connect.
                    # A matching committed result no longer needs a live endpoint.
                    if read_result(
                        self._journal.client,
                        attempt.request_id,
                        expected=attempt.participant,
                    ) is None:
                        raise
            self._journal.client.record_event(
                "stage.reconnected", {}, context=self._context(state, attempt)
            )
            return await self.execute(
                state, wait_services=wait_services, recovered=attempt
            )
        except (OSError, EOFError, ValueError, RuntimeError) as error:
            state.unknown_state_recovery_count += 1
            policy = state.template["unknown_state"]
            action = (
                policy["on_recovery_limit"]
                if state.unknown_state_recovery_count >= policy["recovery_limit"]
                else policy["on_timeout"]
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
        import shutil

        parent = attempt.artifacts_directory.parent
        keep = state.template["keep_attempts"]
        for path in parent.iterdir():
            if (
                path == attempt.artifacts_directory
                or not path.is_dir()
                or not path.name.startswith("attempt_")
            ):
                continue
            suffix = path.name.removeprefix("attempt_")
            if suffix.isdigit() and int(suffix) <= attempt.attempt_number - keep:
                if not path.resolve().is_relative_to(
                    state.experiment_directory.resolve()
                ):
                    raise ValueError("Attempt directory escapes the experiment.")
                shutil.rmtree(path)

    def termination_confirmed(self, state: RunnerState) -> bool:
        attempt = state.active_attempt
        if attempt is None or self._unstarted_attempt_id == attempt.attempt_id:
            return True
        if attempt.service_id is not None:
            instance = state.services.get(attempt.service_id)
            return instance is not None and (
                instance.stopped
                or instance.service_instance_id
                != attempt.participant["participant_instance_id"]
            )
        identity = attempt.process_identity
        if identity is None:
            return False
        try:
            if process_identity(identity["pid"]) != identity:
                return True
            psutil.Process(identity["pid"]).wait(timeout=0)
            return True
        except psutil.TimeoutExpired:
            return False
        except (FileNotFoundError, ProcessLookupError, psutil.NoSuchProcess):
            return True
        except OSError as error:
            if getattr(error, "winerror", None) in (87, 1168):
                return True
            raise

    def _save_state(self, state: RunnerState) -> None:
        try:
            self._state_store.save(state)
        except OSError as error:
            self._journal.client.record_error(
                error, context={"experiment_id": state.experiment_id}
            )

    async def close(self, state: RunnerState | None = None) -> None:
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
                    timeout=30 if state is None else state.template["start_timeout"],
                )
        self._executor_processes = remaining
        if state is not None:
            for request_id in state.stage_result_ids.values():
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
                        self._executor_identities[request_id] = process_record[
                            "executor"
                        ]
            for request_id, identity in list(self._executor_identities.items()):
                try:
                    if process_identity(identity["pid"]) == identity:
                        await asyncio.to_thread(
                            psutil.Process(identity["pid"]).wait,
                            state.template["start_timeout"],
                        )
                except (FileNotFoundError, ProcessLookupError, psutil.NoSuchProcess):
                    pass
                except OSError as error:
                    if getattr(error, "winerror", None) not in (87, 1168):
                        raise
                self._executor_identities.pop(request_id, None)
