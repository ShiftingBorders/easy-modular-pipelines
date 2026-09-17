"""Synchronous module-side client for the independent StageExecutor."""

from __future__ import annotations

import asyncio
import json
import sys
import threading
from pathlib import Path
from typing import Self
from uuid import uuid4

from core.logger import OperationLogger
from core.logger_utils.events import (
    JsonObject,
    JsonValue,
    copy_json_object,
    require_number,
    require_text,
)
from core.runner_utils.connection import ParticipantConnection
from core.runner_utils.protocol import PROTOCOL_VERSION
from core.runner_utils.runtimeio import read_json


class StageClient:
    def __init__(self, context_path: Path) -> None:
        self._context_path = Path(context_path)
        if not self._context_path.is_absolute():
            raise ValueError("context_path must be absolute.")
        self.input_data: JsonValue = None
        self.settings: JsonObject = {}
        self.context: JsonObject = {}
        self._thread: threading.Thread | None = None
        self._loop = None
        self._task = None
        self._connection = None
        self._logger = None
        self._ready = threading.Event()
        self._cancelled = threading.Event()
        self._error: Exception | None = None
        self._timeout = 30.0
        self._result_written = False
        self._result_lock = threading.Lock()
        self._closing = False

    def open(self) -> None:
        if self._thread is not None:
            raise RuntimeError("StageClient is already open.")
        context = read_json(self._context_path)
        if (
            type(context.get("protocol_version")) is not int
            or context["protocol_version"] != PROTOCOL_VERSION
        ):
            raise ValueError("Unsupported module context protocol.")
        self.context = context
        self.input_data = context["input_data"]
        self.settings = copy_json_object(context["settings"], "settings")
        self._timeout = float(
            require_number(context["control_timeout_seconds"], "control timeout")
        )
        if require_number(self._timeout, "control timeout") <= 0:
            raise ValueError("control timeout must be positive.")
        self._ready.clear()
        self._cancelled.clear()
        self._error = None
        self._closing = False
        self._thread = threading.Thread(
            target=self._run, name="stage-client", daemon=True
        )
        self._thread.start()
        if not self._ready.wait(self._timeout):
            self.close()
            raise TimeoutError(
                "StageClient did not connect before its control deadline."
            )
        if self._error is not None:
            error = self._error
            self.close()
            raise ConnectionError(f"StageClient failed to open: {error}") from error

    def _run(self) -> None:
        try:
            asyncio.run(self._serve())
        except Exception as error:  # noqa: BLE001 - Forward background failures to the module's caller.
            self._error = error
        finally:
            self._ready.set()

    async def _serve(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.current_task()
        self._logger = OperationLogger(Path(self.context["logging_config_path"]))
        self._connection = ParticipantConnection(
            Path(self.context["endpoint_path"]), self.context["context"], role="module"
        )
        try:
            self._logger.open()
            await self._connection.connect(timeout_seconds=self._timeout)
            reply = await self._connection.request(
                str(uuid4()), "module_status", {}, timeout_seconds=self._timeout
            )
            if reply["result"] != "success":
                raise ConnectionError(str(reply["data"]))
            if reply["data"].get("cancel_requested"):
                self._cancelled.set()
            self._ready.set()
            while True:
                message = await self._connection.receive_message()
                if message.get("command") != "cancel":
                    raise ValueError("Unknown executor notification.")
                self._cancelled.set()
        except asyncio.CancelledError:
            if not self._closing:
                raise
        finally:
            await self._connection.close()
            self._logger.close()

    def _check_open(self) -> None:
        if self._error is not None:
            raise ConnectionError(
                f"StageClient connection failed: {self._error}"
            ) from self._error
        if self._thread is None or not self._thread.is_alive() or self._closing:
            raise RuntimeError("StageClient is not open.")

    async def _send(self, command: str, data: JsonObject) -> None:
        if command == "report_progress":
            self._logger.record_progress(
                data["value"], total=1, stage=data["message"], unit="fraction"
            )
        else:
            self._logger.record_event(f"module.{command}", data)
        reply = await self._connection.request(
            str(uuid4()), command, data, timeout_seconds=self._timeout
        )
        if reply["result"] != "success":
            raise RuntimeError(f"Executor rejected {command}: {reply['data']}")

    def _report(self, command: str, data: JsonObject) -> None:
        self._check_open()
        future = asyncio.run_coroutine_threadsafe(self._send(command, data), self._loop)
        try:
            future.result(timeout=self._timeout)
        except BaseException:
            future.cancel()
            raise

    def report_progress(self, value: float, message: str | None = None) -> None:
        if require_number(value, "progress") > 1:
            raise ValueError("progress must be between 0 and 1.")
        if message is not None:
            require_text(message, "progress message")
        self._report("report_progress", {"value": value, "message": message})

    def report_state(self, data: JsonObject) -> None:
        self._report("report_state", copy_json_object(data, "module state"))

    def cancel_requested(self) -> bool:
        self._check_open()
        return self._cancelled.is_set()

    def _finish(self, result: str, data: JsonValue) -> None:
        self._check_open()
        response = copy_json_object({"result": result, "data": data}, "stage result")
        encoded = json.dumps(response, ensure_ascii=True, allow_nan=False)
        with self._result_lock:
            if self._result_written:
                raise RuntimeError("A stage may publish its stdout result only once.")
            self._result_written = True
            sys.stdout.write(encoded + "\n")
            sys.stdout.flush()

    def succeed(self, data: JsonValue) -> None:
        self._finish("success", data)

    def fail(self, data: JsonValue) -> None:
        self._finish("fail", data)

    def close(self) -> None:
        thread = self._thread
        if thread is None:
            return
        self._closing = True
        if (
            self._loop is not None
            and not self._loop.is_closed()
            and self._task is not None
        ):
            try:
                self._loop.call_soon_threadsafe(self._task.cancel)
            except RuntimeError:
                if not self._loop.is_closed():
                    raise
        thread.join(timeout=self._timeout)
        if thread.is_alive():
            raise TimeoutError("StageClient background connection has not closed.")
        self._thread = None

    def __enter__(self) -> Self:
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        try:
            self.close()
        except Exception as error:
            if exc_value is None:
                raise
            exc_value.add_note(f"StageClient cleanup failed: {error}")
