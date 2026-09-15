"""Runner-side execution of sequential stage attempts through an independent executor."""

from __future__ import annotations

import asyncio
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

from core.logger_utils.events import LoggingError
from core.runner_utils.connection import ParticipantConnection
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.launch import ModuleLauncher
from core.runner_utils.runtimeio import read_json, write_json
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
        notify_resources: Callable[[], None] | None = None,
    ) -> None:
        self._launcher = launcher
        self._journal = journal
        self._state_store = state_store
        self._connection = None
        self._process = None
        self._process_attempt_id = None
        self._executor_processes = []
        self._notify_resources = notify_resources

    async def execute(
        self, state: RunnerState, *, manual: bool = False
    ) -> StageOutcome:
        definition = state.template["stages"][state.stage_position - 1]
        stage_id = definition["stage_id"]
        input_data = self.select_input(state)
        if manual:
            state.stage_result_paths.pop(stage_id, None)
            state.last_result = None
            state.last_result_path = None
        policy = definition["errors"]
        while True:
            attempt = await self._start_attempt(state, input_data)
            response = await self._collect_result(state, attempt)
            success = response is not None and response["result"] == "success"
            self._journal.client.record_event(
                "stage.finished",
                {
                    "result": response,
                    "outcome": attempt.outcome,
                    "result_path": str(
                        attempt.result_path.relative_to(state.experiment_directory)
                    ),
                },
                context={
                    "experiment_id": state.experiment_id,
                    "run_id": state.run_id,
                    "stage_id": stage_id,
                    "attempt_id": attempt.attempt_id,
                    "cycle_number": state.cycle_number,
                    "attempt_number": attempt.attempt_number,
                },
            )
            self._save_state(state)
            if success:
                return StageOutcome(attempt, response, "advance")
            if state.pause_requested:
                return StageOutcome(attempt, response, "pause")
            used = state.stage_retry_counts.get(stage_id, 0)
            if used >= policy["retries"]:
                action = (
                    "advance"
                    if policy["on_exhausted"] == "skip"
                    else policy["on_exhausted"]
                )
                return StageOutcome(attempt, response, action)
            await asyncio.sleep(policy["retry_delay_seconds"])
            if state.pause_requested:
                return StageOutcome(attempt, response, "pause")
            state.stage_retry_counts[stage_id] = used + 1
            self._save_state(state)

    def select_input(self, state: RunnerState) -> JsonValue:
        if state.stage_position == 1:
            return None
        previous = state.template["stages"][state.stage_position - 2]["stage_id"]
        path = state.stage_result_paths.get(previous)
        if path is None or not path.is_file():
            return None
        result = read_json(path)
        if (
            result.get("stage_id") != previous
            or result.get("experiment_id") != state.experiment_id
        ):
            raise ValueError("Saved predecessor result has a different identity.")
        response = result.get("response")
        if (
            result.get("exit_code") == 0
            and not result.get("interruption_reason")
            and isinstance(response, dict)
            and response.get("result") == "success"
        ):
            return response["data"]
        return None

    async def _start_attempt(
        self, state: RunnerState, input_data: JsonValue
    ) -> StageAttempt:
        self._process_attempt_id = None
        definition = state.template["stages"][state.stage_position - 1]
        module = definition["module"]
        stage_id = definition["stage_id"]
        number = state.stage_attempt_numbers.get(stage_id, 0) + 1
        attempt_id = str(uuid4())
        execution_id = str(uuid4())
        directory = (
            state.experiment_directory
            / "shared_artifacts"
            / f"epoch_{state.cycle_number}"
            / module["name"]
            / stage_id
            / f"attempt_{number}"
        )
        context = {
            "experiment_id": state.experiment_id,
            "run_id": state.run_id,
            "template_revision_id": state.template_revision_id,
            "stage_id": stage_id,
            "stage_execution_id": execution_id,
            "attempt_id": attempt_id,
            "cycle_number": state.cycle_number,
            "attempt_number": number,
            "module_name": module["name"],
            "module_version": module["version"],
            "module_hash": module["hash"],
        }
        launch = self._launcher.prepare(
            state, definition, context, directory, input_data
        )
        attempt = StageAttempt(
            attempt_id,
            stage_id,
            execution_id,
            state.cycle_number,
            number,
            directory,
            input_data,
            launch["effective_settings"],
            definition["timeout_seconds"],
        )
        state.active_attempt = attempt
        if self._notify_resources is not None:
            self._notify_resources()
        state.stage_attempt_numbers[stage_id] = number
        self._journal.client.record_attempt_parameters(
            state.template,
            attempt.effective_settings,
            template_yaml=state.template_yaml,
            context=context,
        )
        self._journal.client.record_event(
            "control.intent",
            {"action": "start_stage", "argv": launch["argv"]},
            context=context,
        )
        self._save_state(state)
        self._prune_artifacts(state, attempt)
        launch_path = directory / "launch.json"
        write_json(launch_path, launch)
        library_root = Path(__file__).resolve().parents[2]
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
            self._process_attempt_id = attempt_id
            self._executor_processes.append(
                (
                    self._process,
                    directory / "execution_result.json",
                    launch["control_timeout_seconds"] + launch["stop_timeout_seconds"],
                )
            )
            raise
        self._process_attempt_id = attempt_id
        self._executor_processes.append(
            (
                self._process,
                directory / "execution_result.json",
                launch["control_timeout_seconds"] + launch["stop_timeout_seconds"],
            )
        )
        self._connection = None
        deadline = time.monotonic() + launch["control_timeout_seconds"]
        lock = state.experiment_directory / "executor.lock.json"
        while time.monotonic() < deadline:
            if (directory / "execution_result.json").is_file():
                return attempt
            if lock.is_file():
                try:
                    metadata = read_json(lock)
                    if metadata.get("attempt_id") == attempt_id:
                        self._connection = ParticipantConnection(
                            lock,
                            {
                                "experiment_id": state.experiment_id,
                                "stage_id": stage_id,
                                "attempt_id": attempt_id,
                            },
                        )
                        await self._connection.connect(
                            timeout_seconds=max(0.001, deadline - time.monotonic())
                        )
                        return attempt
                except (OSError, EOFError):
                    # Completion publishes the result before deleting the endpoint.
                    # A short stage can finish between the existence check and read.
                    if not (directory / "execution_result.json").is_file():
                        raise
                    if self._connection is not None:
                        await self._connection.close()
                        self._connection = None
                    return attempt
            if self._process.poll() is not None:
                if (directory / "execution_result.json").is_file():
                    return attempt
                raise RuntimeError(
                    f"Executor exited before publishing a result (exit {self._process.returncode})."
                )
            await asyncio.sleep(0.02)
        raise TimeoutError(
            "Executor did not publish its endpoint before the startup deadline."
        )

    async def _collect_result(
        self, state: RunnerState, attempt: StageAttempt
    ) -> JsonObject | None:
        path = attempt.artifacts_directory / "execution_result.json"
        while not path.is_file():
            if self._connection is None:
                raise RuntimeError("Executor connection is unavailable.")
            request_id = str(uuid4())
            state.used_request_ids.add(request_id)
            try:
                reply = await self._connection.query_command_state(
                    request_id,
                    timeout_seconds=state.template["unknown_state"]["timeout_seconds"],
                )
            except (OSError, EOFError):
                if path.is_file():
                    break
                raise
            status = reply["data"]
            first_observation = (
                attempt.started_at is None and status["started_at"] is not None
            )
            attempt.executor_status = status
            attempt.process_identity = status["process"]
            attempt.started_at = status["started_at"]
            if first_observation:
                self._save_state(state)
                if self._notify_resources is not None:
                    self._notify_resources()
            started = status.get("started_monotonic")
            if attempt.timeout_seconds is not None and started is not None:
                deadline = (
                    started
                    + attempt.timeout_seconds
                    + state.template["runner_timeout_margin_seconds"]
                )
                if time.monotonic() >= deadline:
                    if not await self.interrupt(state, "timeout"):
                        raise RuntimeError(
                            "Could not confirm timed-out stage termination."
                        )
                    attempt.outcome = "timed_out"
            await asyncio.sleep(0.1)
        result = read_json(path)
        for key, value in (
            ("attempt_id", attempt.attempt_id),
            ("stage_id", attempt.stage_id),
            ("experiment_id", state.experiment_id),
        ):
            if result.get(key) != value:
                raise ValueError(f"Result identity mismatch: {key}")
        attempt.result_path = path
        attempt.started_at = result["started_at"]
        attempt.executor_status = {
            **(attempt.executor_status or {}),
            "finished": True,
            "current": None,
            "pending": [],
            "exit_code": result["exit_code"],
        }
        if self._notify_resources is not None:
            self._notify_resources()
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        response = result["response"]
        if attempt.outcome == "timed_out" or result["interruption_reason"] is not None:
            attempt.outcome = (
                "timed_out"
                if attempt.outcome == "timed_out"
                or result["interruption_reason"] == "timeout"
                else "interrupted"
            )
            return {"result": "fail", "data": {"reason": attempt.outcome}}
        if result["error"] is not None or result["exit_code"] != 0:
            attempt.outcome = "failed"
            return {
                "result": "fail",
                "data": {"error": result["error"], "exit_code": result["exit_code"]},
            }
        attempt.outcome = "succeeded" if response["result"] == "success" else "failed"
        return response

    async def interrupt(self, state: RunnerState, reason: str) -> bool:
        attempt = state.active_attempt
        if attempt is None:
            return True
        if self._process_attempt_id != attempt.attempt_id:
            return True
        # Cancelling the DAG may close a pending query while retaining its client.
        # Reconnect before sending the independent interruption command.
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        path = attempt.artifacts_directory / "execution_result.json"
        deadline = time.monotonic() + state.template["unknown_state"]["timeout_seconds"]
        lock = state.experiment_directory / "executor.lock.json"
        while time.monotonic() < deadline:
            if path.is_file():
                result = read_json(path)
                confirmed = (
                    result.get("attempt_id") == attempt.attempt_id
                    and result.get("exit_code") is not None
                )
                if confirmed:
                    attempt.result_path = path
                    attempt.outcome = (
                        "timed_out" if reason == "timeout" else "interrupted"
                    )
                    attempt.executor_status = {
                        **(attempt.executor_status or {}),
                        "finished": True,
                        "current": None,
                        "pending": [],
                        "exit_code": result["exit_code"],
                    }
                    if self._notify_resources is not None:
                        self._notify_resources()
                return confirmed
            try:
                if self._connection is None and lock.is_file():
                    self._connection = ParticipantConnection(
                        lock,
                        {
                            "experiment_id": state.experiment_id,
                            "stage_id": attempt.stage_id,
                            "attempt_id": attempt.attempt_id,
                        },
                    )
                    await self._connection.connect(
                        timeout_seconds=max(0.001, deadline - time.monotonic())
                    )
                if self._connection is not None:
                    request_id = str(uuid4())
                    state.used_request_ids.add(request_id)
                    intent = {
                        "action": "interrupt_stage",
                        "reason": reason,
                        "request_id": request_id,
                    }
                    try:
                        self._journal.client.record_event(
                            "control.intent",
                            intent,
                            context={
                                "experiment_id": state.experiment_id,
                                "attempt_id": attempt.attempt_id,
                            },
                        )
                    except LoggingError as error:
                        # Emergency stopping must still reach the independently owned executor.
                        try:
                            write_json(
                                attempt.artifacts_directory / "stop.emergency.json",
                                {**intent, "error": str(error)},
                            )
                        except OSError as diagnostic_error:
                            error.add_note(
                                f"Emergency stop diagnostic failed: {diagnostic_error}"
                            )
                    await self._connection.request(
                        request_id,
                        "interrupt",
                        {},
                        timeout_seconds=state.template["start_timeout"],
                    )
                    await self._connection.close()
                    self._connection = None
            except (
                OSError,
                ConnectionError,
                asyncio.IncompleteReadError,
                TimeoutError,
            ):
                if self._connection is not None:
                    await self._connection.close()
                self._connection = None
            await asyncio.sleep(0.05)
        return False

    async def recover(self, state: RunnerState) -> StageOutcome | None:
        raise NotImplementedError(
            "Crash recovery is outside the initial stage runtime."
        )

    def _prune_artifacts(self, state: RunnerState, attempt: StageAttempt) -> None:
        import shutil

        parent = attempt.artifacts_directory.parent
        folders = sorted(
            (item for item in parent.glob("attempt_*") if item.name[8:].isdigit()),
            key=lambda item: int(item.name[8:]),
        )
        for folder in folders[: -state.template["keep_attempts"]]:
            if (
                folder.is_symlink()
                or folder.is_junction()
                or not folder.resolve().is_relative_to(parent.resolve())
            ):
                raise ValueError("Unsafe attempt directory.")
            shutil.rmtree(folder)

    def _save_state(self, state: RunnerState) -> None:
        try:
            self._state_store.save(state)
        except OSError as error:
            self._journal.client.record_error(
                error, context={"experiment_id": state.experiment_id}
            )

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        remaining = []
        for process, result_path, timeout in self._executor_processes:
            if process.poll() is None and not result_path.is_file():
                # Detaching from an active attempt preserves its independent lifetime.
                remaining.append((process, result_path, timeout))
            else:
                await asyncio.to_thread(process.wait, timeout=timeout)
        self._executor_processes = remaining
