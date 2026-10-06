"""Synchronous module-side client for the independent StageExecutor."""

from __future__ import annotations

import asyncio
import json
import sys
import threading
from pathlib import Path
from typing import Self
from uuid import uuid4

from core.journal.logger import OperationLogger
from core.models.participant_launch import ModuleContext
from core.models.participant_protocol import ModuleProgress, StageResult
from core.participants.connection import ParticipantConnection
from core.primitives.json_files import read_json
from core.primitives.json_values import (
    JsonObject,
    JsonValue,
    copy_json_object,
)


class StageClient:
    """Synchronous stage API backed by an executor connection in a worker thread.

    Use as a context manager or explicitly call open and close. Results are
    written once to stdout; publishing a result does not exit the stage process.

    Attributes:
        input_data: Input payload loaded from the fixed attempt context.
        settings: Effective module settings loaded when opened.
        context: Runner-provided context document containing absolute runtime paths.
    """
    def __init__(self, context_path: Path) -> None:
        """Initialize an unopened client for an absolute runner-created context path.

        Args:
            context_path: Absolute path to the runner-created fixed module context
                JSON.
        """
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
        """Read the attempt context and connect to its executor in a background thread.

        Raises:
            RuntimeError: The client is already open.
            TimeoutError: Connection does not become ready before the control deadline.
            ConnectionError: The background connection fails during startup.
        """
        if self._thread is not None:
            raise RuntimeError("StageClient is already open.")
        document = read_json(self._context_path)
        context = ModuleContext.model_validate(document)
        self.context = document
        self._open(context)

    def _open(self, context: ModuleContext) -> None:
        """Bind validated context, start the connection thread, and wait for readiness.

        Args:
            context: Validated module context with fixed inputs, logger, endpoint,
                and control timeout.
        """
        self._runtime_context = context
        self.input_data = context.input_data
        self.settings = context.settings
        self._timeout = float(context.control_timeout_seconds)
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
        """Run the background event loop and expose any failure to the synchronous caller."""
        try:
            asyncio.run(self._serve())
        except Exception as error:  # noqa: BLE001 - Forward background failures to the module's caller.
            self._error = error
        finally:
            self._ready.set()

    async def _serve(self) -> None:
        """Open the logger/executor connection and listen for cancellation notifications.

        Owns the background event loop's logger and authenticated module channel.
        Readiness is signalled after module_status, and cancel notifications set the
        synchronous cancellation event. Both resources close when the loop ends.
        """
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.current_task()
        context = self._runtime_context
        self._logger = OperationLogger(context.logging_config_path)
        self._connection = ParticipantConnection(
            context.endpoint_path, context.context, role="module"
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
                message = await self._connection._receive_notification()
                if message.command != "cancel":
                    raise ValueError("Unknown executor notification.")
                self._cancelled.set()
        except asyncio.CancelledError:
            if not self._closing:
                raise
        finally:
            await self._connection.close()
            self._logger.close()

    def _check_open(self) -> None:
        """Raise the background connection error or reject a closed client."""
        if self._error is not None:
            raise ConnectionError(
                f"StageClient connection failed: {self._error}"
            ) from self._error
        if self._thread is None or not self._thread.is_alive() or self._closing:
            raise RuntimeError("StageClient is not open.")

    async def _send(self, command: str, data: ModuleProgress | JsonObject) -> None:
        """Journal a progress/state report and require executor acknowledgement.

        Args:
            command: Progress or state-report command understood by the executor.
            data: Validated progress or copied module-state payload sent to the
                executor.
        """
        if command == "report_progress":
            if isinstance(data, ModuleProgress):
                value, message = data.value, data.message
            else:
                value, message = data["value"], data["message"]
            self._logger.record_progress(value, total=1, stage=message, unit="fraction")
        else:
            self._logger.record_event(f"module.{command}", data)
        reply = await self._connection.request(
            str(uuid4()),
            command,
            data.model_dump() if isinstance(data, ModuleProgress) else data,
            timeout_seconds=self._timeout,
        )
        if reply["result"] != "success":
            raise RuntimeError(f"Executor rejected {command}: {reply['data']}")

    def _report(self, command: str, data: ModuleProgress | JsonObject) -> None:
        """Send a report on the background loop and wait within the control timeout.

        Args:
            command: Validated command name or envelope selecting the operation.
            data: Validated progress or copied module-state payload sent to the
                executor.
        """
        self._check_open()
        future = asyncio.run_coroutine_threadsafe(self._send(command, data), self._loop)
        try:
            future.result(timeout=self._timeout)
        except BaseException:
            future.cancel()
            raise

    def report_progress(self, value: float, message: str | None = None) -> None:
        """Journal and send a progress fraction to the executor.

        Args:
            value: Progress fraction from zero to one.
            message: Optional nonempty description of the current work.

        Raises:
            ValueError: Progress fields are invalid.
            RuntimeError: The client is closed or the executor rejects the report.
        """
        progress = ModuleProgress.model_validate({"value": value, "message": message})
        self._report("report_progress", progress)

    def report_state(self, data: JsonObject) -> None:
        """Send a detached JSON object describing module state to the journal and executor.

        Args:
            data: Validated progress or copied module-state payload sent to the
                executor.
        """
        self._report("report_state", copy_json_object(data, "module state"))

    def cancel_requested(self) -> bool:
        """Return whether cooperative cancellation was requested for this open client."""
        self._check_open()
        return self._cancelled.is_set()

    def _finish(self, result: str, data: JsonValue) -> None:
        """Validate and flush the sole stdout result, rejecting a second publication.

        Args:
            result: Success or fail discriminator for the single stdout result.
            data: Application JSON payload, including conditional decision data
                where applicable.
        """
        self._check_open()
        response = StageResult.model_validate({"result": result, "data": data})
        encoded = json.dumps(
            response.model_dump(exclude_unset=True), ensure_ascii=True, allow_nan=False
        )
        with self._result_lock:
            if self._result_written:
                raise RuntimeError("A stage may publish its stdout result only once.")
            self._result_written = True
            sys.stdout.write(encoded + "\n")
            sys.stdout.flush()

    def succeed(self, data: JsonValue) -> None:
        """Publish the stage's success result once without exiting the process.

        Args:
            data: JSON application result, or conditional decision for a conditional stage.

        Raises:
            RuntimeError: The client is closed or a result was already published.
        """
        self._finish("success", data)

    def fail(self, data: JsonValue) -> None:
        """Publish the stage's failure result once without exiting the process.

        Args:
            data: JSON failure details; these are not forwarded as the next stage's input.

        Raises:
            RuntimeError: The client is closed or a result was already published.
        """
        self._finish("fail", data)

    def close(self) -> None:
        """Cancel the background connection and wait for the thread to close.

        Raises:
            TimeoutError: The thread remains alive after the control timeout.
        """
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
        """Open the client and return it for use in a with block."""
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Close the client, attaching cleanup failure to an existing exception.

        Args:
            exc_type: Exception type from the with block, or None on normal
                completion.
            exc_value: Primary exception from the with block, or None on normal
                completion.
            traceback: Traceback supplied by the context-manager protocol.
        """
        try:
            self.close()
        except Exception as error:
            if exc_value is None:
                raise
            exc_value.add_note(f"StageClient cleanup failed: {error}")
