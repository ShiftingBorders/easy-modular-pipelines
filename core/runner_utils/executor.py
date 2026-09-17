"""Independent stage executor: one call, cooperative cancellation and journal result."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

from core.logger import OperationLogger
from core.logger_utils.events import JsonObject, copy_json_object, require_number
from core.runner_utils.participant_server import ParticipantServer
from core.runner_utils.protocol import participant_identity
from core.runner_utils.runtimeio import (
    capture_stream,
    process_identity,
    read_json,
    write_json,
)


class StageExecutor:
    def __init__(self, launch_path: Path) -> None:
        self._launch_path = Path(launch_path)
        if not self._launch_path.is_absolute():
            raise ValueError("launch_path must be absolute.")
        self._process = None
        self._process_identity = None
        self._reason = None
        self._started_at = None
        self._started_monotonic = None
        self._started = asyncio.Event()
        self._stop_requested = asyncio.Event()
        self._stop_lock = asyncio.Lock()
        self._progress = None
        self._module_state = {}
        self._finished = False

    async def run(self) -> None:
        self._launch = read_json(self._launch_path)
        self._context = self._launch["context"]
        self._identity = participant_identity(self._context)
        self._directory = self._launch_path.parent
        self._logger = OperationLogger(Path(self._launch["executor_logging_config"]))
        self._logger.open()
        self._server = ParticipantServer(
            Path(self._launch["endpoint_path"]),
            self._context,
            self._logger,
            self._handle_request,
            module_handler=self._handle_module,
            describe=self._describe,
            control_timeout_seconds=self._launch["control_timeout_seconds"],
        )
        completed = started = stopped = None
        try:
            await self._server.start()
            write_json(
                self._directory / "process.json",
                {
                    **self._context,
                    "executor": process_identity(os.getpid()),
                    "stage": None,
                    "started_at": None,
                },
            )
            completed = asyncio.create_task(
                self._server.wait_completed(self._context["request_id"])
            )
            started = asyncio.create_task(self._started.wait())
            stopped = asyncio.create_task(self._stop_requested.wait())
            done, _ = await asyncio.wait(
                (completed, started, stopped),
                timeout=self._launch["control_timeout_seconds"],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if (
                not done
                or stopped in done
                and not self._server.has_call(self._context["request_id"])
            ):
                self._logger.record_command_result(
                    self._context["request_id"],
                    {
                        "result": "fail",
                        "data": {"reason": self._reason or "execute_not_received"},
                    },
                    author="participant",
                    outcome="failed",
                    context=self._context,
                )
                return
            await completed
        finally:
            for task in (completed, started, stopped):
                if task is not None:
                    task.cancel()
            await asyncio.gather(
                *(task for task in (completed, started, stopped) if task is not None),
                return_exceptions=True,
            )
            try:
                await self._interrupt("executor_closed")
            finally:
                await self._server.close()
                self._logger.close()

    def _describe(self) -> JsonObject:
        return {
            "process": self._process_identity,
            "started_at": self._started_at,
            "started_monotonic": self._started_monotonic,
            "finished": self._finished,
            "exit_code": None if self._process is None else self._process.returncode,
            "progress": self._progress,
            "module_state": self._module_state,
        }

    async def _handle_module(self, request: JsonObject) -> JsonObject:
        command, data = request["command"], request["args"]
        if command == "module_status":
            return {
                "result": "success",
                "data": {"cancel_requested": self._reason is not None},
            }
        if command == "report_progress":
            if require_number(data["value"], "progress") > 1:
                raise ValueError("Progress exceeds 1.")
            self._progress = copy_json_object(data, "progress")
        elif command == "report_state":
            self._module_state = copy_json_object(data, "module state")
        else:
            return {"result": "fail", "data": {"code": "unsupported_module_command"}}
        return {"result": "success", "data": {}}

    async def _handle_request(self, request: JsonObject) -> JsonObject:
        command = request["command"]
        if command == "heartbeat":
            return {"result": "success", "data": self._describe()}
        if command in ("interrupt", "shutdown"):
            target = request["args"].get("request_id")
            if target is not None and target != self._context["request_id"]:
                return {"result": "fail", "data": {"code": "different_call"}}
            await self._interrupt(request["args"].get("reason", "stopped"))
            self._stop_requested.set()
            return {
                "result": "success",
                "data": {
                    "stopped": self._process is None
                    or self._process.returncode is not None
                },
            }
        if command != "execute" or request["request_id"] != self._context["request_id"]:
            return {"result": "fail", "data": {"code": "unsupported_executor_call"}}
        if self._started.is_set():
            raise RuntimeError("This executor already started its assigned call.")
        if json.dumps(request["args"], sort_keys=True) != json.dumps(
            self._launch["call"], sort_keys=True
        ):
            raise ValueError("Execute arguments differ from the fixed attempt context.")
        self._started.set()
        if self._reason is not None:
            return {"result": "fail", "data": {"reason": self._reason}}
        return await self._execute(request)

    async def _execute(self, request: JsonObject) -> JsonObject:
        streams = None
        output = bytearray()
        error = None
        response = None
        try:
            environment = dict(os.environ)
            library_root = str(Path(__file__).resolve().parents[2])
            environment["PYTHONPATH"] = os.pathsep.join(
                filter(None, (library_root, environment.get("PYTHONPATH")))
            )
            spawn = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    *self._launch["argv"],
                    cwd=self._launch["code_directory"],
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=environment,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
            )
            try:
                self._process = await asyncio.shield(spawn)
            except asyncio.CancelledError:
                self._process = await spawn
                raise
            self._started_at = datetime.now(UTC).isoformat()
            self._started_monotonic = time.monotonic()
            try:
                self._process_identity = process_identity(self._process.pid)
            except OSError:
                if self._process.returncode is None:
                    raise
            write_json(
                self._directory / "process.json",
                {
                    **self._context,
                    "executor": process_identity(os.getpid()),
                    "stage": self._process_identity,
                    "started_at": self._started_at,
                    "started_monotonic": self._started_monotonic,
                },
            )
            self._logger.record_event(
                "stage.process_started", self._describe(), context=self._context
            )
            streams = asyncio.gather(
                self._process.wait(),
                capture_stream(
                    self._process.stdout, "stdout", self._logger, self._context, output
                ),
                capture_stream(
                    self._process.stderr, "stderr", self._logger, self._context
                ),
            )
            deadline = request.get("deadline_monotonic")
            remaining = (
                None if deadline is None else max(0, deadline - time.monotonic())
            )
            try:
                await asyncio.wait_for(asyncio.shield(streams), remaining)
            except TimeoutError:
                await self._interrupt("timeout")
                await streams
            response = copy_json_object(
                json.loads(output.decode("utf-8")), "stage stdout"
            )
            if response.keys() != {"result", "data"} or response["result"] not in (
                "success",
                "fail",
            ):
                raise ValueError("Stage stdout requires exactly result and data.")
        except asyncio.CancelledError:
            await self._interrupt("interrupted")
        except Exception as failure:  # noqa: BLE001 - Every execution failure becomes a durable failed call.
            error = {"code": "stage_execution_failed", "message": str(failure)}
            await self._interrupt("executor_failure")
        finally:
            if streams is not None:
                await asyncio.gather(streams, return_exceptions=True)
            self._finished = True
        execution = {
            **self._describe(),
            "finished_at": datetime.now(UTC).isoformat(),
            "interruption_reason": self._reason,
        }
        if (
            error is not None
            or self._reason is not None
            or self._process is None
            or self._process.returncode != 0
            or response is None
        ):
            return {
                "result": "fail",
                "data": {"error": error, "reason": self._reason},
                "execution": execution,
            }
        return {**response, "execution": execution}

    async def _interrupt(self, reason: str) -> None:
        async with self._stop_lock:
            if not self._finished:
                self._reason = self._reason or reason
            if self._process is None or self._process.returncode is not None:
                return
            self._reason = self._reason or reason
            timeout = self._launch["stop_timeout_seconds"]
            deadline = time.monotonic() + timeout
            margin = self._launch["runner_timeout_margin_seconds"]
            if self._server.has_module_client():
                try:
                    async with asyncio.timeout(min(timeout, margin)):
                        await self._server.notify_modules(
                            "cancel", {"reason": self._reason}
                        )
                        await self._process.wait()
                    return
                except TimeoutError:
                    pass
            self._process.terminate()
            try:
                await asyncio.wait_for(
                    self._process.wait(), max(0.001, deadline - time.monotonic())
                )
            except TimeoutError:
                self._process.kill()
                await asyncio.wait_for(self._process.wait(), max(0.001, margin))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launch", type=Path, required=True)
    asyncio.run(StageExecutor(parser.parse_args().launch).run())


if __name__ == "__main__":
    main()
