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
from collections.abc import Callable
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
    LoggingStorageError,
    copy_json_object,
    load_logging_settings,
    require_number,
    require_text,
    validate_command_result,
    validate_context,
)
from core.logger_utils.storage import SQLiteEventStore


class OperationLogger:
    """Create a client from an absolute settings path; open explicitly or in a with block."""

    def __init__(self, config_path: str | Path, *, read_only: bool = False) -> None:
        if type(read_only) is not bool:
            raise TypeError("read_only must be a boolean.")
        self._read_only = read_only
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
        self._journal_path: Path | None = None
        self._journal_identity: tuple[int, int] | None = None
        self._journal_info: JsonObject | None = None

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
            settings, context = load_logging_settings(self._config_path)
            if self._read_only and settings["open_mode"] != "existing":
                raise LoggingStateError(
                    "A read-only client requires an existing journal."
                )
            db_path = Path(settings["db_path"])
            context.setdefault("source", "library")
            # A context exported by a parent process must not identify this writer as it.
            context["host_name"] = socket.gethostname()
            context["process_id"] = self._process_id
            context = validate_context(context)
            reopening = db_path == self._journal_path
            expected = settings["expected_journal"]
            if reopening and self._journal_info is not None:
                expected = {
                    name: self._journal_info[name]
                    for name in ("journal_id", "generation")
                }
            store = SQLiteEventStore(
                db_path,
                busy_timeout_seconds=settings["busy_timeout_seconds"],
                max_event_bytes=settings["max_event_bytes"],
                open_mode="existing" if reopening else settings["open_mode"],
                min_free_bytes=settings["min_free_bytes"],
                expected_journal=expected,
                diagnostic_context=context,
                read_only=self._read_only,
            )
            producer_instance_id = uuid4().hex
            expected_identity = (
                self._journal_identity if db_path == self._journal_path else None
            )
            try:
                if expected_identity is not None:
                    file_status = db_path.stat()
                    if (file_status.st_dev, file_status.st_ino) != expected_identity:
                        raise LoggingStorageError(
                            "The known journal file was replaced; use an explicitly "
                            "selected new journal rather than silently resuming."
                        )
                store.open()
                journal_info = store.get_journal_info()
                if (
                    db_path == self._journal_path
                    and self._journal_info is not None
                    and self._journal_info["journal_id"] is not None
                    and journal_info != self._journal_info
                ):
                    raise LoggingStorageError(
                        "The known journal identity or generation changed."
                    )
                file_status = db_path.stat()
                identity = (file_status.st_dev, file_status.st_ino)
                if expected_identity is not None and identity != expected_identity:
                    raise LoggingStorageError(
                        "The journal file changed while the logger was opening."
                    )
            except BaseException as error:
                self._failed = True
                try:
                    store._report_failure(error, "open logger")
                except BaseException:  # noqa: BLE001, S110 - Preserve original failure.
                    pass
                try:
                    store.close()
                except BaseException as failure:  # noqa: BLE001 - Preserve open failure.
                    self._report_secondary_failure(
                        error, failure, "closing a failed journal open"
                    )
                if isinstance(error, OSError):
                    raise LoggingStorageError(
                        f"Cannot reopen the known journal at {db_path}."
                    ) from error
                raise
            self._store = store
            self._journal_path = db_path
            self._journal_identity = identity
            self._journal_info = journal_info
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
        *,
        persist: Callable[[JsonObject], str | None] | None = None,
    ) -> str:
        """Append under the caller's client lock; retain sequence order across threads."""
        self._require_open()
        if self._read_only:
            raise LoggingStateError("Cannot record events through a read-only logger.")
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
            confirmed_id = (
                self._store.append(event) if persist is None else persist(event)
            )
            if confirmed_id is None:
                confirmed_id = event_id
            if confirmed_id == event_id:
                self._sequence_number = sequence
        except (TypeError, ValueError):
            # Input rejection happens before INSERT; the journal remains usable.
            raise
        except BaseException as error:
            self._failed = True
            try:
                self._store._report_failure(error, "record event", event)
            except BaseException:  # noqa: BLE001, S110 - Preserve original failure.
                pass
            try:
                error.add_note(f"Unconfirmed event_id: {event_id}.")
            except BaseException:  # noqa: BLE001, S110 - Preserve the write failure.
                pass
            raise
        return confirmed_id

    def _get_record_context(
        self, operation: Operation | None, context: JsonObject | None
    ) -> tuple[JsonObject, str | None]:
        """Resolve explicit context while the caller holds the logger lock."""
        self._require_open()
        if operation is not None:
            self._check_operation(operation)
            if context is not None:
                raise ValueError("An operation already owns its context; omit context.")
            return dict(operation._context), operation._operation_id
        return self._merge_context(context), None

    def _record(
        self,
        event_type: str,
        data: JsonObject,
        operation: Operation | None,
        context: JsonObject | None,
        *,
        required_context: tuple[str, ...] = (),
    ) -> str:
        self._check_process()
        with self._lock:
            self._require_open()
            event_context, operation_id = self._get_record_context(operation, context)
            for name in required_context:
                if event_context.get(name) is None:
                    raise ValueError(f"This record requires context.{name}.")
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
        payload = copy_json_object({} if data is None else data, "data")
        self._check_process()
        with self._lock:
            if event_type in RESERVED_EVENT_TYPES:
                raise ValueError(
                    "Use the dedicated method for this reserved event_type."
                )
            return self._record(event_type, payload, operation, context)

    def record_template_applied(
        self,
        template: JsonObject,
        *,
        template_yaml: str,
        template_revision_id: str,
        previous_template_revision_id: str | None = None,
        reason: str | None = None,
        operation: Operation | None = None,
        context: JsonObject | None = None,
    ) -> str:
        """Persist the full accepted template, even when no stage starts afterwards."""
        require_text(template_revision_id, "template_revision_id")
        require_text(template_yaml, "template_yaml")
        for name, value in (
            ("previous_template_revision_id", previous_template_revision_id),
            ("reason", reason),
        ):
            if value is not None:
                require_text(value, name)
        if previous_template_revision_id == template_revision_id:
            raise ValueError("A template revision cannot be its own predecessor.")
        return self._record(
            "template.applied",
            {
                "template_revision_id": template_revision_id,
                "previous_template_revision_id": previous_template_revision_id,
                "reason": reason,
                "template": copy_json_object(template, "template"),
                "template_yaml": template_yaml,
            },
            operation,
            context,
            required_context=("experiment_id",),
        )

    def record_attempt_parameters(
        self,
        template: JsonObject,
        effective_settings: JsonObject,
        *,
        template_yaml: str,
        operation: Operation | None = None,
        context: JsonObject | None = None,
    ) -> str:
        """Confirm complete parameters before the caller is allowed to start a stage."""
        require_text(template_yaml, "template_yaml")
        return self._record(
            "attempt.parameters",
            {
                "template": copy_json_object(template, "template"),
                "template_yaml": template_yaml,
                "effective_settings": copy_json_object(
                    effective_settings, "effective_settings"
                ),
            },
            operation,
            context,
            required_context=(
                "experiment_id",
                "run_id",
                "stage_id",
                "stage_execution_id",
                "attempt_id",
                "template_revision_id",
                "cycle_number",
                "attempt_number",
                "module_name",
                "module_version",
                "module_hash",
            ),
        )

    def record_command_result(
        self,
        request_id: str,
        response: JsonObject,
        *,
        author: str,
        outcome: str,
        operation: Operation | None = None,
        context: JsonObject | None = None,
    ) -> str:
        """Return the persisted observation ID; identical responses reuse an earlier ID."""
        payload = validate_command_result(
            {
                "request_id": request_id,
                "author": author,
                "outcome": outcome,
                "response": response,
                "ignored": None,
                "supersedes": [],
            }
        )
        self._check_process()
        with self._lock:
            self._require_open()
            event_context, operation_id = self._get_record_context(operation, context)
            for name in ("experiment_id", "participant_id"):
                require_text(event_context.get(name), f"context.{name}")
            if event_context.get("request_id") not in (None, request_id):
                raise ValueError("request_id disagrees with the supplied context.")
            event_context["request_id"] = request_id
            return self._append(
                "command.result",
                payload,
                event_context,
                operation_id,
                persist=self._store.append_command_result,
            )

    def read_command_result(self, request_id: str) -> JsonObject | None:
        """Read the authoritative result and all recorded participant observations."""
        self._check_process()
        with self._lock:
            if self._store is None:
                raise LoggingStateError("Logger is closed.")
            try:
                return self._store.read_command_result(request_id)
            except BaseException as error:
                if getattr(error, "journal_failed", False):
                    self._failed = True
                raise

    def get_journal_info(self) -> JsonObject:
        """Return the open journal's schema, logical identity and generation."""
        self._check_process()
        with self._lock:
            if self._store is None:
                raise LoggingStateError("Logger is closed.")
            try:
                return self._store.get_journal_info()
            except BaseException as error:
                if getattr(error, "journal_failed", False):
                    self._failed = True
                raise

    def export_snapshot(
        self, destination: str | Path, *, min_free_bytes: int
    ) -> JsonObject:
        """Export a journal boundary, not a full experiment or a destructive rollback."""
        self._check_process()
        with self._lock:
            if self._store is None:
                raise LoggingStateError("Logger is closed.")
            try:
                return self._store.export_snapshot(
                    destination, min_free_bytes=min_free_bytes
                )
            except BaseException as error:
                if getattr(error, "journal_failed", False):
                    self._failed = True
                raise

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
            if measurement.get("value") is not None:
                require_number(measurement.get("value"), f"{name}.value")
            else:
                measurement["value"] = None
            require_text(measurement.get("unit"), f"{name}.unit")
            measurement.setdefault("kind", "delta")
            measurement.setdefault("scope", "operation")
            measurement.setdefault("estimated", False)
            if measurement["kind"] not in ("delta", "total", "gauge", "peak"):
                raise ValueError("Resource kind must be delta, total, gauge, or peak.")
            scopes = ("operation", "process", "service", "host")
            if measurement["scope"] not in scopes:
                raise ValueError(f"Resource scope must be one of: {', '.join(scopes)}.")
            if measurement["scope"] == "operation" and operation is None:
                raise ValueError(
                    "Operation-scoped resources require an operation handle."
                )
            if type(measurement["estimated"]) is not bool:
                raise TypeError("Resource estimated must be a boolean.")
            if "attributes" in measurement:
                copy_json_object(measurement["attributes"], "resource attributes")
        return self._record(
            "resources.recorded",
            {"resources": measurements},
            operation,
            context,
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
        self,
        checkpoint: JsonObject | None = None,
        *,
        limit: int = 100,
        view: str = "raw",
    ) -> JsonObject:
        """Read local committed events, including after a write failure, for reconciliation."""
        self._check_process()
        with self._lock:
            if self._store is None:
                raise LoggingStateError("Logger is closed.")
            try:
                return self._store.read_events(checkpoint, limit=limit, view=view)
            except BaseException as error:
                if getattr(error, "journal_failed", False):
                    self._failed = True
                raise

    def read_event_batch(
        self,
        event_ids: list[str] | None = None,
        *,
        limit: int = 1000,
        before: int | None = None,
    ) -> JsonObject:
        """Read selected original events or a tail page using existing indexes."""
        self._check_process()
        with self._lock:
            if self._store is None:
                raise LoggingStateError("Logger is closed.")
            try:
                return self._store.read_event_batch(event_ids, limit=limit, before=before)
            except BaseException as error:
                if getattr(error, "journal_failed", False):
                    self._failed = True
                raise

    def read_changes(
        self, checkpoint: JsonObject | None = None, *, limit: int = 100
    ) -> JsonObject:
        """Read committed transitions, including confirmations without a new event."""
        self._check_process()
        with self._lock:
            if self._store is None:
                raise LoggingStateError("Logger is closed.")
            try:
                return self._store.read_changes(checkpoint, limit=limit)
            except BaseException as error:
                if getattr(error, "journal_failed", False):
                    self._failed = True
                raise

    def export_diagnostics(
        self, operation_ids: list[str], destination: str | Path
    ) -> JsonObject:
        """Preserve selected operations outside files that runner will restore."""
        self._check_process()
        with self._lock:
            if self._store is None:
                raise LoggingStateError("Logger is closed.")
            try:
                return self._store.export_diagnostics(operation_ids, destination)
            except BaseException as error:
                if getattr(error, "journal_failed", False):
                    self._failed = True
                raise

    def _report_secondary_failure(
        self,
        original: BaseException,
        failure: BaseException,
        action: str,
    ) -> None:
        """Best-effort diagnostics that must never replace the application's exception."""
        if self._store is not None and not isinstance(
            failure, (TypeError, ValueError, LoggingStateError)
        ):
            try:
                self._store._report_failure(failure, action)
            except BaseException:  # noqa: BLE001, S110 - Preserve the body exception.
                pass
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
