"""Explicit operation logging for core code and independently launched modules.

See docs/logging.md for the event contract, ownership rules, and crash guarantees.
Importing this module and constructing a client perform no filesystem I/O.
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
import threading
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from types import TracebackType
from typing import Self
from uuid import uuid4

from core.logger_utils.events import (
    RESERVED_EVENT_TYPES,
    SCHEMA_VERSION,
    JsonObject,
    LoggingStateError,
    copy_json_object,
    load_logging_config,
    require_number,
    require_text,
    validate_context,
)
from core.logger_utils.storage import SQLiteEventStore


class OperationLogger:
    """Create a client from an absolute settings path; open explicitly or in a with block."""

    def __init__(self, config_path: str | Path) -> None:
        if not isinstance(config_path, (str, Path)):
            raise TypeError("config_path must be a string or Path.")
        path = Path(config_path)
        if not path.is_absolute() or "\x00" in str(path):
            raise ValueError("config_path must be an absolute filesystem path.")
        self._config_path = path
        self._store: SQLiteEventStore | None = None
        self._context: JsonObject = {}
        self._producer_instance_id: str | None = None
        self._sequence_number = 0
        self._failed = False
        self._process_id = os.getpid()
        self._lock = threading.RLock()

    def _check_process(self) -> None:
        if os.getpid() != self._process_id:
            raise LoggingStateError("Create a new logger in this process.")

    def _require_open(self) -> None:
        if self._store is None or self._failed:
            raise LoggingStateError("Logger is closed or failed; close and reopen it.")

    def open(self) -> None:
        self._check_process()
        with self._lock:
            if self._store is not None:
                raise LoggingStateError(
                    "Logger is already open; close it before reopening."
                )
            db_path, timeout, max_bytes, context = load_logging_config(
                self._config_path
            )
            context.setdefault("source", "library")
            # A context exported by a parent process must not identify this writer as it.
            context["host_name"] = socket.gethostname()
            context["process_id"] = self._process_id
            context = validate_context(context)
            store = SQLiteEventStore(
                db_path,
                busy_timeout_seconds=timeout,
                max_event_bytes=max_bytes,
            )
            producer_instance_id = uuid4().hex
            store.open()
            self._store = store
            self._context = context
            self._producer_instance_id = producer_instance_id
            self._sequence_number = 0
            self._failed = False

    def close(self) -> None:
        """Close storage without inventing outcomes for unfinished operations."""
        self._check_process()
        with self._lock:
            if self._store is None:
                return
            try:
                self._store.close()
            except BaseException:
                self._failed = True
                raise
            self._store = None
            self._producer_instance_id = None

    def __enter__(self) -> Self:
        self.open()
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        error: BaseException | None,
        error_traceback: TracebackType | None,
    ) -> bool:
        try:
            self.close()
        except BaseException as failure:
            if error is None:
                raise
            self._report_secondary_failure(error, failure, "closing the journal")
        return False

    def get_context(self) -> JsonObject:
        """Return a detached context suitable for JSON settings or explicit propagation."""
        self._check_process()
        with self._lock:
            self._require_open()
            return dict(self._context)

    def _merge_context(self, context: JsonObject | None) -> JsonObject:
        merged = dict(self._context)
        if context is not None:
            merged.update(validate_context(context))
        merged["host_name"] = self._context["host_name"]
        merged["process_id"] = self._process_id
        return validate_context(merged)

    def _check_operation(self, operation: Operation, state: str = "started") -> None:
        self._require_open()
        if not isinstance(operation, Operation) or operation._logger is not self:
            raise LoggingStateError("Operation belongs to another logger.")
        if operation._producer_instance_id != self._producer_instance_id:
            raise LoggingStateError("Operation belongs to a previous logger session.")
        if operation._state != state:
            raise LoggingStateError(
                f"Operation must be {state}; it is {operation._state}."
            )

    def _append(
        self,
        event_type: str,
        data: JsonObject,
        context: JsonObject,
        operation_id: str | None,
    ) -> str:
        """Append under the caller's client lock; retain sequence order across threads."""
        self._require_open()
        event_id = uuid4().hex
        sequence = self._sequence_number + 1
        event: JsonObject = {
            "schema_version": SCHEMA_VERSION,
            "event_id": event_id,
            "producer_instance_id": self._producer_instance_id,
            "sequence_number": sequence,
            "occurred_at": datetime.now(UTC).isoformat(timespec="microseconds"),
            "event_type": event_type,
            "context": context,
            "operation_id": operation_id,
            "data": data,
        }
        try:
            self._store.append(event)
            self._sequence_number = sequence
        except (TypeError, ValueError):
            # Input rejection happens before INSERT; the journal remains usable.
            raise
        except BaseException as error:
            self._failed = True
            error.add_note(f"Unconfirmed event_id: {event_id}.")
            raise
        return event_id

    def _record(
        self,
        event_type: str,
        data: JsonObject,
        operation: Operation | None,
        context: JsonObject | None,
    ) -> str:
        self._check_process()
        with self._lock:
            self._require_open()
            if operation is not None:
                self._check_operation(operation)
                if context is not None:
                    raise ValueError(
                        "An operation already owns its context; omit context."
                    )
                event_context = dict(operation._context)
                operation_id = operation._operation_id
            else:
                event_context = self._merge_context(context)
                operation_id = None
            return self._append(event_type, data, event_context, operation_id)

    def operation(
        self,
        operation_type: str,
        operation_name: str,
        *,
        context: JsonObject | None = None,
        attributes: JsonObject | None = None,
    ) -> Operation:
        """Prepare an operation; its start is persisted only on entering its with block."""
        self._check_process()
        with self._lock:
            self._require_open()
            return Operation(
                self,
                operation_type,
                operation_name,
                self._merge_context(context),
                copy_json_object(
                    {} if attributes is None else attributes, "attributes"
                ),
            )

    def _start_operation(self, operation: Operation) -> None:
        self._check_process()
        with self._lock:
            self._check_operation(operation, "created")
            started_ns = time.monotonic_ns()
            try:
                self._append(
                    "operation.started",
                    {
                        "operation_type": operation._operation_type,
                        "operation_name": operation._operation_name,
                        "parent_operation_id": operation._context.get(
                            "parent_operation_id"
                        ),
                        "attributes": operation._attributes,
                    },
                    operation._context,
                    operation._operation_id,
                )
                operation._started_ns = started_ns
                operation._state = "started"
            except (TypeError, ValueError):
                raise
            except BaseException:
                # Interruption after commit but before updating the handle is ambiguous too.
                operation._state = "uncertain"
                self._failed = True
                raise

    def start_operation(
        self,
        operation_type: str,
        operation_name: str,
        *,
        context: JsonObject | None = None,
        attributes: JsonObject | None = None,
    ) -> Operation:
        """Persist a start immediately for code that will explicitly call finish_operation."""
        operation = self.operation(
            operation_type,
            operation_name,
            context=context,
            attributes=attributes,
        )
        self._start_operation(operation)
        return operation

    def finish_operation(
        self,
        operation: Operation,
        *,
        status: str = "succeeded",
        reason_code: str | None = None,
        attributes: JsonObject | None = None,
    ) -> str:
        """Persist a terminal event once; durations refer only to this local operation."""
        if status not in ("succeeded", "failed", "cancelled"):
            raise ValueError("status must be succeeded, failed, or cancelled.")
        if reason_code is not None:
            require_text(reason_code, "reason_code")
        attributes = copy_json_object(
            {} if attributes is None else attributes, "attributes"
        )
        self._check_process()
        with self._lock:
            self._check_operation(operation)
            if operation._context_managed:
                raise LoggingStateError(
                    "Let the with block finish this operation; use start_operation "
                    "for manual completion."
                )
            try:
                event_id = self._append(
                    "operation.finished",
                    {
                        "status": status,
                        "duration_ms": (time.monotonic_ns() - operation._started_ns)
                        / 1000000,
                        "reason_code": reason_code,
                        "attributes": attributes,
                    },
                    operation._context,
                    operation._operation_id,
                )
                operation._state = "finished"
                return event_id
            except (TypeError, ValueError):
                raise
            except BaseException:
                operation._state = "uncertain"
                self._failed = True
                raise

    def record_event(
        self,
        event_type: str,
        data: JsonObject | None = None,
        *,
        operation: Operation | None = None,
        context: JsonObject | None = None,
    ) -> str:
        """Persist an application-defined fact, such as a DAG edge or recovery observation."""
        require_text(event_type, "event_type")
        if event_type in RESERVED_EVENT_TYPES:
            raise ValueError("Use the dedicated method for this reserved event_type.")
        payload = copy_json_object({} if data is None else data, "data")
        return self._record(event_type, payload, operation, context)

    def record_error(
        self,
        error: BaseException,
        *,
        operation: Operation | None = None,
        context: JsonObject | None = None,
        error_code: str | None = None,
        error_id: str | None = None,
        caused_by_error_id: str | None = None,
        include_traceback: bool = False,
    ) -> str:
        """Return error_id for propagation; recording an error does not finish an operation."""
        if not isinstance(error, BaseException):
            raise TypeError("error must be an exception instance.")
        if type(include_traceback) is not bool:
            raise TypeError("include_traceback must be a boolean.")
        for name, value in (
            ("error_code", error_code),
            ("error_id", error_id),
            ("caused_by_error_id", caused_by_error_id),
        ):
            if value is not None:
                require_text(value, name)
        if error_id is None:
            error_id = uuid4().hex
        if caused_by_error_id == error_id:
            raise ValueError("An error cannot cause itself.")
        try:
            message = str(error)
        except Exception:  # noqa: BLE001 - Exception.__str__ is user code.
            message = "[exception message unavailable]"
        payload: JsonObject = {
            "error_id": error_id,
            "error_type": f"{type(error).__module__}.{type(error).__qualname__}",
            "message": message,
            "error_code": error_code,
            "caused_by_error_id": caused_by_error_id,
            "traceback": "".join(traceback.format_exception(error))
            if include_traceback
            else None,
        }
        self._record("error.recorded", payload, operation, context)
        return error_id

    def record_resources(
        self,
        resources: JsonObject,
        *,
        operation: Operation | None = None,
        context: JsonObject | None = None,
    ) -> str:
        """Record measurements with explicit units and aggregation semantics."""
        measurements = copy_json_object(resources, "resources")
        if not measurements:
            raise ValueError("resources must contain at least one measurement.")
        for name, measurement in measurements.items():
            require_text(name, "resource name")
            if not isinstance(measurement, dict):
                raise TypeError("Each resource must be a JSON measurement object.")
            if measurement.keys() - {
                "value",
                "unit",
                "kind",
                "scope",
                "estimated",
                "attributes",
            }:
                raise ValueError("Unknown resource measurement fields.")
            require_number(measurement.get("value"), f"{name}.value")
            require_text(measurement.get("unit"), f"{name}.unit")
            measurement.setdefault("kind", "delta")
            measurement.setdefault("scope", "operation")
            measurement.setdefault("estimated", False)
            if measurement["kind"] not in ("delta", "total", "gauge", "peak"):
                raise ValueError("Resource kind must be delta, total, gauge, or peak.")
            if measurement["scope"] not in ("operation", "process", "service"):
                raise ValueError(
                    "Resource scope must be operation, process, or service."
                )
            if measurement["scope"] == "operation" and operation is None:
                raise ValueError(
                    "Operation-scoped resources require an operation handle."
                )
            if type(measurement["estimated"]) is not bool:
                raise TypeError("Resource estimated must be a boolean.")
            if "attributes" in measurement:
                copy_json_object(measurement["attributes"], "resource attributes")
        return self._record(
            "resources.recorded", {"resources": measurements}, operation, context
        )

    def record_progress(
        self,
        completed: float,
        *,
        total: float | None = None,
        stage: str | None = None,
        unit: str = "item",
        operation: Operation | None = None,
        context: JsonObject | None = None,
    ) -> str:
        require_number(completed, "completed")
        require_text(unit, "unit")
        if stage is not None:
            require_text(stage, "stage")
        if total is not None:
            require_number(total, "total")
            if completed > total:
                raise ValueError("completed must not exceed total.")
        return self._record(
            "progress.recorded",
            {"completed": completed, "total": total, "stage": stage, "unit": unit},
            operation,
            context,
        )

    def record_artifact(
        self,
        path: str,
        purpose: str,
        *,
        artifact_id: str | None = None,
        size_bytes: int | None = None,
        content_hash: str | None = None,
        operation: Operation | None = None,
        context: JsonObject | None = None,
    ) -> str:
        """Return artifact_id; register metadata without accessing or accepting the file."""
        require_text(path, "artifact path")
        require_text(purpose, "purpose")
        windows_path = PureWindowsPath(path)
        relative_path = PurePosixPath(path.replace("\\", "/"))
        if (
            windows_path.drive
            or windows_path.root
            or relative_path.is_absolute()
            or ".." in relative_path.parts
            or not relative_path.parts
            or ":" in path
        ):
            raise ValueError(
                "Artifact path must stay relative to its attempt directory."
            )
        if size_bytes is not None and (type(size_bytes) is not int or size_bytes < 0):
            raise ValueError("size_bytes must be a nonnegative integer or None.")
        for name, value in (
            ("artifact_id", artifact_id),
            ("content_hash", content_hash),
        ):
            if value is not None:
                require_text(value, name)
        if artifact_id is None:
            artifact_id = uuid4().hex
        self._record(
            "artifact.recorded",
            {
                "artifact_id": artifact_id,
                "path": str(relative_path),
                "purpose": purpose,
                "size_bytes": size_bytes,
                "content_hash": content_hash,
            },
            operation,
            context,
        )
        return artifact_id

    def read_events(
        self, *, after_cursor: int = 0, limit: int = 100
    ) -> list[JsonObject]:
        """Read local committed events, including after a write failure, for reconciliation."""
        self._check_process()
        with self._lock:
            if self._store is None:
                raise LoggingStateError("Logger is closed.")
            return self._store.read_events(after_cursor=after_cursor, limit=limit)

    def _report_secondary_failure(
        self,
        original: BaseException,
        failure: BaseException,
        action: str,
    ) -> None:
        """Best-effort diagnostics that must never replace the application's exception."""
        message = f"Journal failure while {action}: {type(failure).__name__}."
        try:
            original.add_note(message)
        except BaseException:  # noqa: BLE001, S110 - Preserve the original exception.
            pass
        try:
            if sys.stderr is not None:
                sys.stderr.write(message + "\n")
                sys.stderr.flush()
        except BaseException:  # noqa: BLE001, S110 - This is the last-resort error sink.
            pass


class Operation:
    """One local operation. Obtain it through OperationLogger.operation/start_operation."""

    def __init__(
        self,
        logger: OperationLogger,
        operation_type: str,
        operation_name: str,
        context: JsonObject,
        attributes: JsonObject,
    ) -> None:
        self._logger = logger
        self._operation_type = require_text(operation_type, "operation_type")
        self._operation_name = require_text(operation_name, "operation_name")
        self._context = validate_context(context)
        self._attributes = copy_json_object(attributes, "attributes")
        self._operation_id = uuid4().hex
        self._producer_instance_id = logger._producer_instance_id
        self._state = "created"
        self._started_ns = 0
        self._context_managed = False

    def get_operation_id(self) -> str:
        return self._operation_id

    def get_child_context(self) -> JsonObject:
        """Pass this context explicitly to nested operations or another module's settings."""
        self._logger._check_process()
        with self._logger._lock:
            self._logger._check_operation(self)
            context = dict(self._context)
            context["parent_operation_id"] = self._operation_id
            return context

    def __enter__(self) -> Self:
        self._logger._check_process()
        with self._logger._lock:
            self._logger._start_operation(self)
            self._context_managed = True
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        error: BaseException | None,
        error_traceback: TracebackType | None,
    ) -> bool:
        self._context_managed = False
        if error is None:
            self._logger.finish_operation(self)
            return False
        telemetry_incomplete = False
        try:
            self._logger.record_error(error, operation=self, include_traceback=True)
        except BaseException as failure:  # noqa: BLE001 - Preserve the body exception.
            telemetry_incomplete = True
            self._logger._report_secondary_failure(
                error, failure, "recording the error"
            )
        status = "failed"
        if isinstance(error, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
            status = "cancelled"
        try:
            self._logger.finish_operation(
                self,
                status=status,
                reason_code="cancelled"
                if status == "cancelled"
                else "unhandled_exception",
                attributes={"error_recording_failed": True}
                if telemetry_incomplete
                else {},
            )
        except BaseException as failure:  # noqa: BLE001 - Preserve the body exception.
            self._logger._report_secondary_failure(
                error, failure, "recording the outcome"
            )
        return False
