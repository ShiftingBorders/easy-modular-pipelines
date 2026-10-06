"""Explicit operation logging for core code and independently launched modules.

See docs/logging.md for the event contract, ownership rules, and crash guarantees.
Importing this module and constructing a client perform no filesystem I/O."""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Self
from uuid import uuid4

from core.journal.events import (
    RESERVED_EVENT_TYPES,
    SCHEMA_VERSION,
    LoggingStateError,
    LoggingStorageError,
    _validated_command_result,
)

if TYPE_CHECKING:
    from core.models.journal_records import JournalContext
from core.journal.measurements import _validate_measurement
from core.journal.settings import _load_logging_settings
from core.journal.storage import SQLiteEventStore
from core.journal.streams import _write_stderr_best_effort
from core.primitives.json_values import (
    JsonObject,
    copy_json_object,
    require_number,
    require_text,
)


class OperationLogger:
    """Create a client from an absolute settings path; open explicitly or in a with block."""

    def __init__(self, config_path: str | Path, *, read_only: bool = False) -> None:
        """Initialize a process-local logger without reading settings or opening storage.

        Args:
            config_path: Absolute logging configuration path.
            read_only: Whether to require an existing journal and prohibit event writes.

        Raises:
            TypeError: Path or read-only flag has an unsupported type.
            ValueError: The path is relative or contains a null character.
        """
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
        self._context: JournalContext
        self._producer_instance_id: str | None = None
        self._sequence_number = 0
        self._failed = False
        self._process_id = os.getpid()
        self._lock = threading.RLock()
        self._journal_path: Path | None = None
        self._journal_identity: tuple[int, int] | None = None
        self._journal_info: JsonObject | None = None

    def _check_process(self) -> None:
        """Reject use from a process other than the logger's creator."""
        if os.getpid() != self._process_id:
            raise LoggingStateError("Create a new logger in this process.")

    def _require_open(self) -> None:
        """Reject a closed logger or one whose write outcome became uncertain."""
        if self._store is None or self._failed:
            raise LoggingStateError("Logger is closed or failed; close and reopen it.")

    def open(self) -> None:
        """Load settings and open an identity-checked journal for this process.

        Relative database paths are resolved from the configuration file. Reopening
        a known path checks the retained file identity and journal generation.

        Raises:
            LoggingStateError: The logger is already open, belongs to another process,
                or read-only mode is paired with journal creation.
            LoggingError: Configuration, storage, or retained identity checks fail.
        """
        self._check_process()
        with self._lock:
            if self._store is not None:
                raise LoggingStateError(
                    "Logger is already open; close it before reopening."
                )
            settings, context = _load_logging_settings(self._config_path)
            if self._read_only and settings.open_mode != "existing":
                raise LoggingStateError(
                    "A read-only client requires an existing journal."
                )
            db_path = settings.db_path
            from core.models.journal_records import JournalContext

            values = dict(context.root)
            values.setdefault("source", "library")
            # A context exported by a parent process must not identify this writer as it.
            values["host_name"] = socket.gethostname()
            values["process_id"] = self._process_id
            context = JournalContext.model_validate(values)
            reopening = db_path == self._journal_path
            if reopening and self._journal_info is not None:
                from core.models.journal_settings import JournalConfiguration

                # Reopening intentionally replaces the file's original create identity.
                settings = JournalConfiguration.model_validate(
                    {
                        "db_path": settings.db_path,
                        "busy_timeout_seconds": settings.busy_timeout_seconds,
                        "max_event_bytes": settings.max_event_bytes,
                        "min_free_bytes": settings.min_free_bytes,
                        "open_mode": "existing",
                        "expected_journal": {
                            name: self._journal_info[name]
                            for name in ("journal_id", "generation")
                        },
                    }
                )
            store = SQLiteEventStore._from_settings(settings, context, self._read_only)
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
        """Close storage without inventing outcomes for unfinished operations.

        Runs under process and thread ownership checks. A failed store close keeps
        the logger failed and retains the connection reference; successful close
        detaches the store so later use requires an explicit reopen.
        """
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
        """Open the logger and return it for a with block."""
        self.open()
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        error: BaseException | None,
        error_traceback: TracebackType | None,
    ) -> bool:
        """Close the journal, preserving an existing exception, and return False.

        Args:
            exception_type: Exception type from the context-manager body, or None on
                normal exit.
            error: Primary failure retained while cleanup or error translation
                proceeds.
            error_traceback: Traceback supplied by the context-manager protocol.

        Returns:
            False to propagate any body exception; secondary close failures are
            reported without replacing it.
        """
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
            return self._context.model_dump()

    def _merge_context(self, context: JsonObject | None) -> JournalContext:
        """Merge validated record context while retaining this writer's host/process identity.

        Args:
            context: Journal/participant coordinates associated with this operation.

        Returns:
            Validated merged context retaining the actual writer's host/process
            identity despite caller overrides.
        """
        from core.models.journal_records import JournalContext

        merged = dict(self._context.root)
        if context is not None:
            merged.update(JournalContext.model_validate(context).root)
        merged["host_name"] = self._context.root["host_name"]
        merged["process_id"] = self._process_id
        return JournalContext.model_validate(merged)

    def _check_operation(self, operation: Operation, state: str = "started") -> None:
        """Require an open logger and an operation owned by this session in the expected state.

        Args:
            operation: Owning journal operation handle, or None for an unscoped
                record.
            state: Required operation-handle lifecycle state, started by default.
        """
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
        context: JournalContext,
        operation_id: str | None,
        *,
        persist: Callable[[JsonObject], str | None] | None = None,
    ) -> str:
        """Append under the caller's client lock; retain sequence order across threads.

        Increments the producer sequence and creates the complete event envelope. A
        failed or interrupted persistence operation marks the logger failed because
        commit status may be uncertain; callers must not blindly replay the write.

        Args:
            event_type: Journal event category selecting the payload contract.
            data: JSON payload recorded or sent by this operation.
            context: Journal/participant coordinates associated with this operation.
            operation_id: Journal operation identity associated with the event.
            persist: Optional persistence callback replacing the ordinary store
                append.

        Returns:
            Identifier of the committed event.
        """
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
            "context": context.model_dump(),
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
    ) -> tuple[JournalContext, str | None]:
        """Resolve explicit context while the caller holds the logger lock.

        Args:
            operation: Owning journal operation handle, or None for an unscoped
                record.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            Validated event context and optional operation ID after confirming
            operation/session ownership.
        """
        self._require_open()
        if operation is not None:
            self._check_operation(operation)
            if context is not None:
                raise ValueError("An operation already owns its context; omit context.")
            return operation._context, operation._operation_id
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
        """Validate record context and append an event under the logger lock.

        Args:
            event_type: Journal event type.
            data: JSON event payload.
            operation: Optional local operation supplying identity and context.
            context: Optional record-specific context overrides.
            required_context: Context field names that must have non-null values.

        Returns:
            Identifier of the appended event.
        """
        self._check_process()
        with self._lock:
            self._require_open()
            event_context, operation_id = self._get_record_context(operation, context)
            for name in required_context:
                if event_context.root.get(name) is None:
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
        """Prepare an operation; its start is persisted only on entering its with block.

        Args:
            operation_type: Nonempty operation category.
            operation_name: Nonempty operation display name.
            context: Journal/participant coordinates associated with this operation.
            attributes: Additional JSON metadata copied into the operation or event.

        Returns:
            Unstarted local operation handle; entering its context manager records
            the start.
        """
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
        """Record operation start and mark uncertain publication as a failed logger session.

        Args:
            operation: Owning journal operation handle, or None for an unscoped
                record.
        """
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
                        "parent_operation_id": operation._context.root.get(
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
        """Persist a start immediately for code that will explicitly call finish_operation.

        Args:
            operation_type: Nonempty operation category.
            operation_name: Nonempty operation display name.
            context: Journal/participant coordinates associated with this operation.
            attributes: Additional JSON metadata copied into the operation or event.

        Returns:
            Started operation handle bound to this logger session.
        """
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
        """Persist a terminal event once; durations refer only to this local operation.

        Args:
            operation: Owning journal operation handle, or None for an unscoped
                record.
            status: Terminal operation status selected by the caller.
            reason_code: Optional machine-readable explanation of the terminal
                status.
            attributes: Additional JSON metadata copied into the operation or event.

        Returns:
            Identifier of the committed terminal operation event.
        """
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
        """Persist an application-defined fact, such as a DAG edge or recovery observation.

        Args:
            event_type: Journal event category selecting the payload contract.
            data: JSON payload recorded or sent by this operation.
            operation: Owning journal operation handle, or None for an unscoped
                record.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            Identifier of the committed event.
        """
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
        """Persist the full accepted template, even when no stage starts afterwards.

        Args:
            template: Validated experiment template supplying definitions and
                policies.
            template_yaml: Applied or candidate template YAML retained for audit and
                publication.
            template_revision_id: UUID identifying the applied template revision.
            previous_template_revision_id: Prior template revision UUID, or None for
                initial application.
            reason: Nonempty reason recorded for the template, interruption, or
                observation.
            operation: Owning journal operation handle, or None for an unscoped
                record.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            Identifier of the applied-template event containing full YAML and
            validated template data.
        """
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
        """Confirm complete parameters before the caller is allowed to start a stage.

        Args:
            template: Validated experiment template supplying definitions and
                policies.
            effective_settings: Actual merged module settings used by the attempt.
            template_yaml: Applied or candidate template YAML retained for audit and
                publication.
            operation: Owning journal operation handle, or None for an unscoped
                record.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            Identifier of the event recording the attempt's applied template and
            effective settings.
        """
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
        """Return the persisted observation ID; identical responses reuse an earlier ID.

        Args:
            request_id: UUID correlating the admitted request and its eventual
                outcome.
            response: Participant/controller result envelope being processed.
            author: Runner or participant author; runner acceptance takes
                precedence.
            outcome: Accepted stage/command outcome that determines the next policy
                action.
            operation: Owning journal operation handle, or None for an unscoped
                record.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            The persisted observation ID; identical responses reuse an earlier ID.
        """
        payload = _validated_command_result(
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
                require_text(event_context.root.get(name), f"context.{name}")
            if event_context.root.get("request_id") not in (None, request_id):
                raise ValueError("request_id disagrees with the supplied context.")
            from core.models.updates import _update_model

            event_context = _update_model(event_context, request_id=request_id)
            return self._append(
                "command.result",
                payload.model_dump(),
                event_context,
                operation_id,
                persist=self._store.append_command_result,
            )

    def _read_store[Result](self, call: Callable[[SQLiteEventStore], Result]) -> Result:
        """Run a store read under process/lock checks and retain fatal journal failures.

        Args:
            call: Callback receiving the owned SQLite store and returning a read
                result.

        Returns:
            The callback's result; journal_failed exceptions also mark the logger
            failed before propagation.
        """
        self._check_process()
        with self._lock:
            if self._store is None:
                raise LoggingStateError("Logger is closed.")
            try:
                return call(self._store)
            except BaseException as error:
                if getattr(error, "journal_failed", False):
                    self._failed = True
                raise

    def read_command_result(self, request_id: str) -> JsonObject | None:
        """Read the authoritative result and all recorded participant observations.

        Args:
            request_id: UUID correlating the admitted request and its eventual
                outcome.

        Returns:
            Considered command result and author observations, or None when no
            result is indexed.
        """
        return self._read_store(lambda store: store.read_command_result(request_id))

    def get_journal_info(self) -> JsonObject:
        """Return the open journal's schema, logical identity and generation."""
        return self._read_store(lambda store: store.get_journal_info())

    def export_snapshot(
        self, destination: str | Path, *, min_free_bytes: int
    ) -> JsonObject:
        """Export a journal boundary, not a full experiment or a destructive rollback.

        Args:
            destination: Absolute output path for the requested file or directory.
            min_free_bytes: Additional free-space reserve in bytes for the export.

        Returns:
            Manifest describing the copied journal, identity, boundary, and content
            checksum.
        """
        return self._read_store(
            lambda store: store.export_snapshot(
                destination, min_free_bytes=min_free_bytes
            )
        )

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
        """Return error_id for propagation; recording an error does not finish an operation.

        Args:
            error: Primary failure retained while cleanup or error translation
                proceeds.
            operation: Owning journal operation handle, or None for an unscoped
                record.
            context: Journal/participant coordinates associated with this operation.
            error_code: Optional stable diagnostic code for the recorded failure.
            error_id: Optional caller-provided error identity, or None to allocate
                one.
            caused_by_error_id: Optional preceding error identity linking causal
                diagnostics.
            include_traceback: Whether to capture the exception traceback in the
                journal event.

        Returns:
            Error_id for propagation; recording an error does not finish an
            operation.
        """
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
        """Record measurements with explicit units and aggregation semantics.

        Args:
            resources: Validated static resource definitions whose optional hashes
                are checked.
            operation: Owning journal operation handle, or None for an unscoped
                record.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            Identifier of the committed resource-measurement event.
        """
        measurements = copy_json_object(resources, "resources")
        if not measurements:
            raise ValueError("resources must contain at least one measurement.")
        validated = {
            name: _validate_measurement(name, measurement, operation)
            for name, measurement in measurements.items()
        }
        # Update the original ordered objects only when constructing event data.
        for name, measurement in validated.items():
            excluded = (
                {"attributes"}
                if "attributes" not in measurement.model_fields_set
                else set()
            )
            original = measurements[name]
            if isinstance(original, dict):
                original.update(measurement.model_dump(exclude=excluded))
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
        """Record progress in explicitly named units.

        Args:
            completed: Nonnegative finite amount completed.
            total: Optional nonnegative total, at least completed.
            stage: Optional nonempty work description.
            unit: Nonempty unit name, such as item or fraction.
            operation: Optional owning operation.
            context: Optional validated context overrides.

        Returns:
            Identifier of the committed progress event.

        Raises:
            ValueError: Values are invalid or completed exceeds total.
            LoggingError: Logger state or storage prevents recording.
        """
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
        """Return artifact_id; register metadata without accessing or accepting the file.

        Args:
            path: Artifact path relative to the attempt directory, not the
                experiment root.
            purpose: Nonempty description of why the artifact was produced.
            artifact_id: Optional artifact identity; None lets the logger allocate
                one.
            size_bytes: Optional nonnegative artifact size in bytes.
            content_hash: Optional caller-supplied artifact content hash; no hashing
                is performed here.
            operation: Owning journal operation handle, or None for an unscoped
                record.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            Artifact_id; register metadata without accessing or accepting the file.
        """
        from core.models.artifacts import ArtifactRegistration

        parameters = ArtifactRegistration.model_validate({
            "artifact_id": artifact_id,
            "path": path,
            "purpose": purpose,
            "size_bytes": size_bytes,
            "content_hash": content_hash,
        })
        artifact = parameters.model_dump()
        if artifact_id is None:
            artifact_id = uuid4().hex
            artifact["artifact_id"] = artifact_id
        self._record(
            "artifact.recorded",
            artifact,
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
        """Read local committed events, including after a write failure, for reconciliation.

        Args:
            checkpoint: Identity-bound exclusive journal cursor, or None to start
                reading.
            limit: Maximum page item count, from 1 through 1000.
            view: Raw or effective journal view; effective suppresses superseded
                observations.

        Returns:
            Event page with continuation checkpoint, observed boundary, and
            has_more; an effective page may advance past filtered-out observations.
        """
        return self._read_store(
            lambda store: store.read_events(checkpoint, limit=limit, view=view)
        )

    def read_event_batch(
        self,
        event_ids: list[str] | None = None,
        *,
        limit: int = 1000,
        before: int | None = None,
    ) -> JsonObject:
        """Read selected original events or a tail page using existing indexes.

        Args:
            event_ids: Selected original event IDs, or None to read a descending
                tail page.
            limit: Maximum page item count, from 1 through 1000.
            before: Exclusive upper event cursor for descending tail reads, or None
                for the latest boundary.

        Returns:
            Original event entries and their observed boundary, selected by IDs or
            descending cursor tail. The byte limit may shorten the batch.
        """
        return self._read_store(
            lambda store: store.read_event_batch(event_ids, limit=limit, before=before)
        )

    def read_changes(
        self, checkpoint: JsonObject | None = None, *, limit: int = 100
    ) -> JsonObject:
        """Read committed transitions, including confirmations without a new event.

        Args:
            checkpoint: Identity-bound exclusive journal cursor, or None to start
                reading.
            limit: Maximum page item count, from 1 through 1000.

        Returns:
            Change-feed page including confirmations, checkpoint, source boundary,
            and has_more.
        """
        return self._read_store(
            lambda store: store.read_changes(checkpoint, limit=limit)
        )

    def export_diagnostics(
        self, operation_ids: list[str], destination: str | Path
    ) -> JsonObject:
        """Preserve selected operations outside files that runner will restore.

        Args:
            operation_ids: Root operation IDs whose diagnostic descendants should be
                exported.
            destination: Absolute output path for the requested file or directory.

        Returns:
            Manifest for the diagnostic records file, including source boundary,
            record counts, and checksum.
        """
        return self._read_store(
            lambda store: store.export_diagnostics(operation_ids, destination)
        )

    def _report_secondary_failure(
        self,
        original: BaseException,
        failure: BaseException,
        action: str,
    ) -> None:
        """Best-effort diagnostics that must never replace the application's exception.

        Args:
            original: Primary exception to annotate without replacing it.
            failure: Primary exception whose identity must survive secondary cleanup
                failures.
            action: Human-readable action associated with the failure or current
                policy decision.
        """
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
        _write_stderr_best_effort(message)


class Operation:
    """One local operation. Obtain it through OperationLogger.operation/start_operation."""

    def __init__(
        self,
        logger: OperationLogger,
        operation_type: str,
        operation_name: str,
        context: JournalContext | JsonObject,
        attributes: JsonObject,
    ) -> None:
        """Initialize a validated local operation handle without writing its start event.

        Args:
            logger: Owning logger session.
            operation_type: Nonempty operation category.
            operation_name: Nonempty operation display name.
            context: Validated journal context.
            attributes: JSON attributes copied into the handle.
        """
        self._logger = logger
        self._operation_type = require_text(operation_type, "operation_type")
        self._operation_name = require_text(operation_name, "operation_name")
        from core.models.journal_records import JournalContext

        self._context = JournalContext.model_validate(context)
        self._attributes = copy_json_object(attributes, "attributes")
        self._operation_id = uuid4().hex
        self._producer_instance_id = logger._producer_instance_id
        self._state = "created"
        self._started_ns = 0
        self._context_managed = False

    def get_operation_id(self) -> str:
        """Return the unique identifier assigned to this operation handle."""
        return self._operation_id

    def get_child_context(self) -> JsonObject:
        """Pass this context explicitly to nested operations or another module's settings."""
        self._logger._check_process()
        with self._logger._lock:
            self._logger._check_operation(self)
            context = self._context.model_dump()
            context["parent_operation_id"] = self._operation_id
            return context

    def __enter__(self) -> Self:
        """Record the operation start and return its context-managed handle."""
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
        """Record success, failure, or cancellation while preserving the body exception.

        Args:
            exception_type: Exception type from the context-manager body, or None on
                normal exit.
            error: Primary failure retained while cleanup or error translation
                proceeds.
            error_traceback: Traceback supplied by the context-manager protocol.

        Returns:
            False so exceptions from the with block propagate. Secondary logging
            failures are attached without replacing the body exception.
        """
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
