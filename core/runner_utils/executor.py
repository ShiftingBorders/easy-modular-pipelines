"""Independent stage process: TCP control, stream capture, and durable result."""

from __future__ import annotations

import argparse
import asyncio
import codecs
import hmac
import json
import os
import secrets
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from core.logger import OperationLogger
from core.logger_utils.events import LoggingError, copy_json_object
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from core.runner_utils.state import JsonObject


class StageExecutor:
    def __init__(self, launch_path: Path) -> None:
        self._launch_path = Path(launch_path)
        if not self._launch_path.is_absolute():
            raise ValueError("launch_path must be absolute.")
        self._process = None
        self._result = None
        self._reason = None
        self._started_at = None
        self._started_monotonic = None
        self._process_identity = None
        self._clients: set[asyncio.Task] = set()
        self._request_ids: set[str] = set()
        self._stop_lock = asyncio.Lock()

    async def run(self) -> None:
        self._launch = read_json(self._launch_path)
        self._context = self._launch["context"]
        self._identity = {
            key: self._context[key]
            for key in ("experiment_id", "stage_id", "attempt_id")
        }
        self._directory = self._launch_path.parent
        self._result_path = self._directory / "execution_result.json"
        self._lock_path = (
            Path(self._launch["experiment_directory"]) / "executor.lock.json"
        )
        self._token = secrets.token_urlsafe(48)
        token_path = self._directory / "executor.token"
        token_path.write_text(self._token, encoding="utf-8")
        token_path.chmod(0o600)
        self._logger = OperationLogger(Path(self._launch["executor_logging_config"]))
        self._logger.open()
        server = await asyncio.start_server(self._handle_client, "127.0.0.1", 0)
        endpoint = {
            "schema_version": 1,
            **self._identity,
            "executor": process_identity(os.getpid()),
            "endpoint": {
                "host": "127.0.0.1",
                "port": server.sockets[0].getsockname()[1],
                "token_file": str(token_path),
            },
        }
        streams = None
        try:
            write_json(self._lock_path, endpoint)
            environment = dict(os.environ)
            library_root = str(Path(__file__).resolve().parents[2])
            environment["PYTHONPATH"] = os.pathsep.join(
                filter(None, (library_root, environment.get("PYTHONPATH")))
            )
            self._process = await asyncio.create_subprocess_exec(
                *self._launch["argv"],
                cwd=self._launch["code_directory"],
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=environment,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            self._started_at = datetime.now(UTC).isoformat()
            self._started_monotonic = time.monotonic()
            try:
                self._process_identity = process_identity(self._process.pid)
            except OSError:
                # An already exited short stage still has an observed exit code.
                if self._process.returncode is None:
                    raise
            write_json(
                self._directory / "process.json",
                {
                    **endpoint,
                    "stage": self._process_identity,
                    "started_at": self._started_at,
                    "started_monotonic": self._started_monotonic,
                },
            )
            await asyncio.to_thread(
                self._logger.record_event,
                "stage.process_started",
                {
                    "identity": self._process_identity,
                    "started_at": self._started_at,
                },
                context=self._context,
            )
            output = bytearray()
            streams = asyncio.gather(
                self._process.wait(),
                self._read_stream(self._process.stdout, "stdout", output),
                self._read_stream(self._process.stderr, "stderr", None),
            )
            try:
                await asyncio.wait_for(
                    asyncio.shield(streams), self._launch["timeout_seconds"]
                )
            except TimeoutError:
                await self._interrupt("timeout")
                await streams
            response = None
            error = None
            try:
                response = copy_json_object(
                    json.loads(output.decode("utf-8")), "stage output"
                )
                if response.keys() != {"result", "data"} or response["result"] not in (
                    "success",
                    "fail",
                ):
                    raise ValueError(
                        "Stage stdout must contain exactly result and data."
                    )
            except (ValueError, TypeError, UnicodeError) as failure:
                error = {"code": "invalid_stage_result", "message": str(failure)}
            self._result = {
                "schema_version": 1,
                **self._identity,
                "started_at": self._started_at,
                "finished_at": datetime.now(UTC).isoformat(),
                "exit_code": self._process.returncode,
                "response": response,
                "interruption_reason": self._reason,
                "error": error,
            }
        except Exception as error:  # noqa: BLE001 - Publish a failed attempt after any executor failure.
            await self._interrupt("executor_failure")
            if streams is not None:
                await asyncio.gather(streams, return_exceptions=True)
            self._result = {
                "schema_version": 1,
                **self._identity,
                "started_at": self._started_at,
                "finished_at": datetime.now(UTC).isoformat(),
                "exit_code": None
                if self._process is None
                else self._process.returncode,
                "response": None,
                "interruption_reason": self._reason,
                "error": {"code": "executor_failure", "message": str(error)},
            }
        finally:
            try:
                if self._result is not None:
                    write_json(self._result_path, self._result)
                    if (
                        self._lock_path.exists()
                        and read_json(self._lock_path).get("attempt_id")
                        == self._identity["attempt_id"]
                    ):
                        self._lock_path.unlink()
            finally:
                server.close()
                await server.wait_closed()
                if self._clients:
                    done, pending = await asyncio.wait(
                        self._clients, timeout=self._launch["control_timeout_seconds"]
                    )
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*done, *pending, return_exceptions=True)
                self._logger.close()
                token_path.unlink(missing_ok=True)

    async def _read_stream(self, stream, name: str, output: bytearray | None) -> None:
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        while chunk := await stream.read(65536):
            if output is not None:
                output.extend(chunk)
            text = decoder.decode(chunk)
            if text:
                await asyncio.to_thread(
                    self._logger.record_event,
                    "command.output",
                    {"stream": name, "text": text},
                    context=self._context,
                )
        remaining = decoder.decode(b"", final=True)
        if remaining:
            await asyncio.to_thread(
                self._logger.record_event,
                "command.output",
                {"stream": name, "text": remaining},
                context=self._context,
            )

    async def _interrupt(self, reason: str) -> None:
        async with self._stop_lock:
            if self._process is not None and self._process.returncode is None:
                self._reason = reason
                self._process.terminate()
                try:
                    await asyncio.wait_for(
                        self._process.wait(), self._launch["stop_timeout_seconds"]
                    )
                except TimeoutError:
                    self._process.kill()
                    await asyncio.wait_for(
                        self._process.wait(), self._launch["stop_timeout_seconds"]
                    )

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        self._clients.add(task)
        try:
            async with asyncio.timeout(self._launch["control_timeout_seconds"]):
                hello = await self._read_frame(reader)
            if (
                hello.get("message_type") != "hello"
                or hello.get("identity") != self._identity
            ):
                return
            token = hello.get("token")
            if not isinstance(token, str) or not hmac.compare_digest(
                token, self._token
            ):
                return
            await self._send_frame(
                writer, {"result": "success", "data": self._identity}
            )
            while True:
                request = await self._read_frame(reader)
                request_id = request.get("request_id")
                UUID(request_id)
                if (
                    request.get("protocol_version") != 1
                    or request.get("message_type") != "request"
                ):
                    raise ValueError("Invalid request envelope.")
                if request_id in self._request_ids:
                    raise ValueError("Reused request_id.")
                self._request_ids.add(request_id)
                command = request.get("command")
                if command == "interrupt":
                    intent = {"action": "interrupt", "request_id": request_id}
                    try:
                        await asyncio.to_thread(
                            self._logger.record_event,
                            "control.intent",
                            intent,
                            context=self._context,
                        )
                    except LoggingError as error:
                        try:
                            write_json(
                                self._directory / "executor-stop.emergency.json",
                                {**intent, "error": str(error)},
                            )
                        except OSError as diagnostic_error:
                            error.add_note(
                                f"Emergency stop diagnostic failed: {diagnostic_error}"
                            )
                    await self._interrupt("stopped")
                elif command not in ("heartbeat", "command_state"):
                    await self._send_frame(
                        writer,
                        {
                            "request_id": request_id,
                            "result": "fail",
                            "data": {"error": "unknown_command"},
                        },
                    )
                    continue
                await self._send_frame(
                    writer,
                    {
                        "protocol_version": 1,
                        "message_type": "command_result",
                        "request_id": request_id,
                        "result": "success",
                        "data": {
                            "pending": [],
                            "current": None
                            if self._result is not None
                            else self._identity,
                            "process": self._process_identity,
                            "started_at": self._started_at,
                            "started_monotonic": self._started_monotonic,
                            "finished": self._result is not None,
                            "exit_code": None
                            if self._process is None
                            else self._process.returncode,
                        },
                    },
                )
        except (
            asyncio.IncompleteReadError,
            ConnectionError,
            OSError,
            ValueError,
            TypeError,
            TimeoutError,
        ):
            # A broken control connection never terminates the independently owned stage.
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass
            self._clients.discard(task)

    async def _read_frame(self, reader: asyncio.StreamReader) -> JsonObject:
        size = int.from_bytes(await reader.readexactly(8), "big")
        if not size:
            raise ValueError("Empty frame.")
        return copy_json_object(
            json.loads((await reader.readexactly(size)).decode("utf-8")), "request"
        )

    async def _send_frame(
        self, writer: asyncio.StreamWriter, response: JsonObject
    ) -> None:
        payload = json.dumps(response, ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
        writer.write(len(payload).to_bytes(8, "big") + payload)
        await writer.drain()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launch", type=Path, required=True)
    options = parser.parse_args()
    asyncio.run(StageExecutor(options.launch).run())


if __name__ == "__main__":
    main()
