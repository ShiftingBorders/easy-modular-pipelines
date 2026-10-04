"""One durable journal format, command reconciliation and identified recovery."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from core.journal.diagnostics import (
    _add_diagnostic_observer_events,
    _close_preserving_failure,
    _content_digest,
    _decode_author_observation,
    _diagnostic_dependencies,
    _diagnostic_manifest,
    _diagnostic_operation_events,
    _merge_diagnostic_command,
    _prepare_diagnostic_events,
    _publish_manifest,
    _save_command_state,
    _validate_diagnostic_command,
    _validate_restore_input,
    _write_diagnostic_records,
)
from core.journal.events import (
    SCHEMA_VERSION,
    JournalGenerationChanged,
    LoggingConfigurationError,
    LoggingStateError,
    LoggingStorageError,
    _validated_checkpoint,
    _validated_command_result,
    encode_event,
    validate_context,
)
from core.journal.records import (
    _checkpoint_position,
    _command_result_document,
    _decode_row,
    _prepare_other_author_result,
    _read_boundary,
    _read_journal_info,
    _validate_result_precedence,
)
from core.journal.schema import (
    _APPLICATION_ID,
    _TABLES,
    _create_tables,
    _matches_table_definition,
)
from core.journal.streams import _write_stderr_best_effort
from core.models.journal_options import JournalOptions, validate_journal_options
from core.primitives.json_values import (
    JsonObject,
    JsonValue,
    copy_json_object,
    require_text,
)

_PAGE_BYTES = 16777216

if TYPE_CHECKING:
    from core.models.journal_diagnostics import (
        DiagnosticCommand,
        DiagnosticManifest,
        JournalSnapshotManifest,
    )
    from core.models.journal_records import (
        JournalChangeData,
        JournalChangeEntry,
        JournalContext,
        JournalEntry,
        JournalReadBoundary,
    )
    from core.models.journal_settings import JournalConfiguration


class SQLiteEventStore:
    """One explicitly opened connection per process, shared through a local DB path."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        busy_timeout_seconds: float,
        max_event_bytes: int | None,
        open_mode: str,
        min_free_bytes: int,
        expected_journal: JsonObject | None,
        diagnostic_context: JsonObject | None = None,
        read_only: bool = False,
    ) -> None:
        if type(read_only) is not bool:
            raise TypeError("read_only must be a boolean.")
        if read_only and open_mode != "existing":
            raise ValueError("Read-only access requires an existing journal.")
        settings = validate_journal_options(
            db_path=db_path, busy_timeout_seconds=busy_timeout_seconds,
            max_event_bytes=max_event_bytes, open_mode=open_mode,
            min_free_bytes=min_free_bytes, expected_journal=expected_journal,
        )
        context = validate_context(
            {} if diagnostic_context is None else diagnostic_context,
        )
        self._configure(settings, context, read_only)

    @classmethod
    def _from_settings(
        cls,
        settings: JournalConfiguration,
        context: JournalContext,
        read_only: bool,
    ) -> SQLiteEventStore:
        """Use settings already validated at the configuration-file boundary."""
        store = cls.__new__(cls)
        store._configure(settings, context, read_only)
        return store

    def _configure(
        self,
        settings: JournalConfiguration | JournalOptions,
        diagnostic_context: JournalContext | JsonObject,
        read_only: bool,
    ) -> None:
        self._read_only = read_only
        self.db_path = settings.db_path
        self._timeout = float(settings.busy_timeout_seconds)
        self._max_event_bytes = settings.max_event_bytes
        self._expected_journal = settings.expected_journal
        self._open_mode = settings.open_mode
        self._min_free_bytes = settings.min_free_bytes
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.RLock()
        self._process_id = os.getpid()
        self._failed = False
        self._file_identity: tuple[int, int] | None = None
        self._journal_id: str | None = None
        self._generation: str | None = None
        self._diagnostic_context = (
            dict(diagnostic_context)
            if isinstance(diagnostic_context, dict)
            else diagnostic_context
        )

    def _check_process(self) -> None:
        if os.getpid() != self._process_id:
            raise LoggingStateError("Create a new journal client in this process.")

    def _require_open(self, *, writing: bool = False) -> None:
        if writing and self._read_only:
            raise LoggingStateError("This journal client is read-only.")
        if self._connection is None:
            raise LoggingStateError("Journal is closed.")
        if writing and self._failed:
            raise LoggingStateError(
                "Journal failed; close and reopen it before writing."
            )

    def _check_schema(self, connection: sqlite3.Connection) -> bool:
        """Recognize exactly one format; never alter a file during validation."""
        objects = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name"
        ).fetchall()
        application_id = connection.execute("PRAGMA application_id").fetchone()[0]
        stored_version = connection.execute("PRAGMA user_version").fetchone()[0]
        if not objects and application_id == 0 and stored_version == 0:
            return True
        expected = _TABLES
        compatible = (
            application_id == _APPLICATION_ID and stored_version == SCHEMA_VERSION
        )
        compatible = compatible and len(objects) == len(expected)
        for kind, name, sql in objects:
            if not _matches_table_definition(kind, name, sql, expected):
                compatible = False
        if not compatible:
            raise LoggingConfigurationError(
                f"Incompatible journal schema at {self.db_path}; "
                "The file was not converted."
            )
        return False

    def _check_file(self) -> tuple[int, int]:
        status = self.db_path.stat()
        identity = (status.st_dev, status.st_ino)
        if self._file_identity is not None and identity != self._file_identity:
            raise LoggingStorageError("The journal file was replaced.")
        if status.st_size < 16:
            raise LoggingStorageError("The journal file has an invalid SQLite header.")
        # A raw open/read/close would release this process's SQLite locks on
        # POSIX. A fresh SQLite connection checks the disk header without
        # bypassing SQLite's file-descriptor and lock management.
        reader = sqlite3.connect(
            self.db_path.as_uri() + "?mode=ro",
            timeout=self._timeout,
            isolation_level=None,
            uri=True,
        )
        try:
            reader.execute("PRAGMA schema_version").fetchone()
        finally:
            reader.close()
        return identity

    def _check_health(self, *, writing: bool = False) -> None:
        self._check_file()
        if self._check_schema(self._connection):
            raise LoggingStorageError("The open journal lost its schema.")
        info = _read_journal_info(self._connection)
        if (info["journal_id"], info["generation"]) != (
            self._journal_id,
            self._generation,
        ):
            raise LoggingStorageError("Journal identity or generation changed.")
        if writing:
            self._check_free_space(self.db_path.parent, self._min_free_bytes)

    def _check_free_space(self, directory: Path, minimum: int) -> None:
        if minimum and shutil.disk_usage(directory).free < minimum:
            raise LoggingStorageError(
                f"Free space at {directory} is below the required {minimum} bytes."
            )

    def _report_failure(
        self, error: BaseException, action: str, event: JsonObject | None = None
    ) -> None:
        """One independent best-effort diagnostic; never reenter the primary journal."""
        if self._read_only:
            return
        try:
            if getattr(error, "_logging_diagnostic_attempted", False):
                return
            error._logging_diagnostic_attempted = True
        except BaseException:  # noqa: BLE001, S110 - Diagnostics cannot replace failure.
            pass
        diagnostic_path = self.db_path.parent
        try:
            diagnostic_path = self.db_path.with_name(
                f"{self.db_path.name}.emergency-{uuid4().hex}.jsonl"
            )
            try:
                message = str(error)
            except BaseException:  # noqa: BLE001 - Exception.__str__ is caller code.
                message = "[exception message unavailable]"
            cause = error.__cause__
            try:
                cause_message = str(cause) if cause is not None else None
            except BaseException:  # noqa: BLE001 - Keep diagnostics independent.
                cause_message = "[cause message unavailable]"
            record = {
                "occurred_at": datetime.now(UTC).isoformat(timespec="microseconds"),
                "diagnostic_type": "journal.failure",
                "action": action,
                "db_path": str(self.db_path),
                "host_name": socket.gethostname(),
                "process_id": os.getpid(),
                "error_type": f"{type(error).__module__}.{type(error).__qualname__}",
                "message": message,
                "cause_type": type(cause).__name__ if cause is not None else None,
                "cause_message": cause_message,
                "event_id": event.get("event_id") if event else None,
                "event_type": event.get("event_type") if event else None,
                "producer_instance_id": event.get("producer_instance_id")
                if event
                else None,
                "sequence_number": event.get("sequence_number") if event else None,
                "operation_id": event.get("operation_id") if event else None,
                "context": event.get("context", {})
                if event
                else (
                    self._diagnostic_context
                    if isinstance(self._diagnostic_context, dict)
                    else self._diagnostic_context.model_dump()
                ),
            }
            # ASCII escapes also preserve exception strings with malformed Unicode.
            with diagnostic_path.open("x", encoding="utf-8") as destination:
                destination.write(json.dumps(record, ensure_ascii=True) + "\n")
                destination.flush()
                os.fsync(destination.fileno())
            note = f"Emergency journal diagnostic: {diagnostic_path}."
        except BaseException as diagnostic_error:  # noqa: BLE001 - No recursive fallback.
            note = (
                f"Emergency journal diagnostic failed at {diagnostic_path}: "
                f"{type(diagnostic_error).__name__}."
            )
            _write_stderr_best_effort(note)
        try:
            if event is not None:
                error.event_id = event["event_id"]
                error.add_note(f"Unconfirmed event_id: {event['event_id']}.")
            error.add_note(note)
        except BaseException:  # noqa: BLE001, S110 - Retain original exception.
            pass

    def _storage_failure(
        self, error: BaseException, action: str, event: JsonObject | None = None
    ) -> BaseException:
        self._failed = True
        if isinstance(
            error,
            (OSError, sqlite3.Error, LoggingConfigurationError, ValueError, TypeError),
        ):
            failure = LoggingStorageError(f"Cannot {action} journal at {self.db_path}.")
            failure.__cause__ = error
        else:
            failure = error
        try:
            failure.journal_failed = True
        except BaseException:  # noqa: BLE001, S110 - Preserve nonstandard exceptions.
            pass
        try:
            self._report_failure(failure, action, event)
        except BaseException:  # noqa: BLE001, S110 - Preserve failure if diagnostics break.
            pass
        return failure

    def _rollback(self, connection: sqlite3.Connection, error: BaseException) -> None:
        try:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
        except BaseException as cleanup_error:  # noqa: BLE001 - Preserve commit outcome.
            try:
                error.add_note(
                    f"Journal rollback failed: {type(cleanup_error).__name__}."
                )
            except BaseException:  # noqa: BLE001, S110
                pass
            self._failed = True

    def open(self) -> None:
        self._check_process()
        with self._lock:
            if self._connection is not None:
                raise LoggingStateError("Journal is already open.")
            connection = None
            try:
                try:
                    status = self.db_path.stat()
                    previous_identity = (status.st_dev, status.st_ino)
                except FileNotFoundError:
                    if self._file_identity is not None or self._open_mode == "existing":
                        raise
                    previous_identity = None
                if (
                    self._file_identity is not None
                    and previous_identity != self._file_identity
                ):
                    raise LoggingStorageError("The known journal file was replaced.")
                if self._open_mode == "create":
                    self.db_path.parent.mkdir(parents=True, exist_ok=True)
                    if previous_identity is not None and self._file_identity is None:
                        raise LoggingConfigurationError(
                            "create requires a new journal path."
                        )
                if not self._read_only:
                    self._check_free_space(self.db_path.parent, self._min_free_bytes)
                if previous_identity is None:
                    # Reserve the name exclusively; a racing creator cannot be adopted.
                    with self.db_path.open("xb"):
                        pass
                    status = self.db_path.stat()
                    previous_identity = (status.st_dev, status.st_ino)
                connection = sqlite3.connect(
                    self.db_path.as_uri()
                    + ("?mode=ro" if self._read_only else "?mode=rw"),
                    timeout=self._timeout,
                    isolation_level=None,
                    check_same_thread=False,
                    uri=True,
                )
                empty = self._check_schema(connection)
                if empty and (
                    self._file_identity is not None or self._open_mode == "existing"
                ):
                    raise LoggingConfigurationError(
                        "An initialized journal is required."
                    )
                if not empty and self._expected_journal is not None:
                    existing = _read_journal_info(connection)
                    actual = {
                        name: existing[name] for name in ("journal_id", "generation")
                    }
                    if actual != self._expected_journal:
                        raise JournalGenerationChanged(self._expected_journal, actual)
                connection.execute("PRAGMA foreign_keys=ON")
                if self._read_only:
                    connection.execute("PRAGMA query_only=ON")
                else:
                    connection.execute("PRAGMA synchronous=EXTRA")
                    connection.execute("BEGIN IMMEDIATE")
                    if self._check_schema(connection):
                        if (
                            self._file_identity is not None
                            or self._open_mode == "existing"
                        ):
                            raise LoggingStorageError(
                                "The known journal lost its schema."
                            )
                        self._initialize_empty_journal(connection)
                    connection.execute("COMMIT")
                    if (
                        connection.execute("PRAGMA journal_mode=WAL")
                        .fetchone()[0]
                        .lower()
                        != "wal"
                    ):
                        raise LoggingConfigurationError(
                            "The journal requires SQLite WAL mode."
                        )
                    connection.execute("PRAGMA synchronous=FULL")
                    if connection.execute("PRAGMA synchronous").fetchone()[0] != 2:
                        raise LoggingConfigurationError(
                            "SQLite FULL synchronization is required."
                        )
                if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise LoggingStorageError("Journal integrity check failed.")
                info = _read_journal_info(connection)
                actual = {name: info[name] for name in ("journal_id", "generation")}
                if (
                    self._expected_journal is not None
                    and actual != self._expected_journal
                ):
                    raise JournalGenerationChanged(self._expected_journal, actual)
                if self._journal_id is not None and (
                    info["journal_id"],
                    info["generation"],
                ) != (self._journal_id, self._generation):
                    raise LoggingStorageError("The known journal identity changed.")
                identity = self._check_file()
                if previous_identity is not None and identity != previous_identity:
                    raise LoggingStorageError("The journal changed while opening.")
                self._connection = connection
                self._file_identity = identity
                self._journal_id = info["journal_id"]
                self._generation = info["generation"]
                self._failed = False
            except BaseException as error:
                if connection is not None:
                    try:
                        connection.close()
                    except BaseException:  # noqa: BLE001, S110 - Preserve open failure.
                        pass
                self._failed = True
                if isinstance(error, LoggingConfigurationError):
                    self._report_failure(error, "open")
                    raise
                raise self._storage_failure(error, "open")

    def _initialize_empty_journal(self, connection: sqlite3.Connection) -> None:
        _create_tables(connection)
        connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def _insert_event(
        self,
        event: JsonObject,
        encoded: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        writer = self._connection if connection is None else connection
        writer.execute(
            "INSERT INTO events "
            "(event_id, producer_instance_id, sequence_number, event_json) "
            "VALUES (?, ?, ?, ?)",
            (
                event["event_id"],
                event["producer_instance_id"],
                event["sequence_number"],
                encoded,
            ),
        )

    def append(self, event: JsonObject) -> None:
        self._check_process()
        if self._read_only:
            raise LoggingStateError("This journal client is read-only.")
        encoded = encode_event(event, self._max_event_bytes)
        snapshot = json.loads(encoded)
        if snapshot["event_type"] == "command.result":
            raise ValueError(
                "Use append_command_result for managed command observations."
            )
        with self._lock:
            self._require_open(writing=True)
            try:
                self._check_health(writing=True)
                self._connection.execute("BEGIN IMMEDIATE")
                self._check_health(writing=True)
                self._insert_event(snapshot, encoded)
                self._record_change(snapshot["event_id"])
                self._connection.execute("COMMIT")
                self._check_health()
            except BaseException as error:  # noqa: BLE001 - Include interrupted transactions.
                self._rollback(self._connection, error)
                raise self._storage_failure(error, "append event", snapshot)

    def _decode_row(self, row: tuple) -> JournalEntry:
        return _decode_row(row)

    def read_events(
        self,
        checkpoint: JsonObject | None = None,
        *,
        limit: int = 100,
        view: str = "raw",
    ) -> JsonObject:
        self._check_process()
        parsed = _validated_checkpoint(checkpoint, "cursor")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000.")
        if view not in ("raw", "effective"):
            raise ValueError("view must be raw or effective.")
        with self._lock:
            self._require_open()
            try:
                self._check_health()
                self._connection.execute("BEGIN")
                boundary = _read_boundary(self._connection)
                after = _checkpoint_position(parsed, boundary, "cursor")
                rows = self._connection.execute(
                    "SELECT cursor, event_id, producer_instance_id, sequence_number, "
                    "event_json FROM events WHERE cursor > ? ORDER BY cursor LIMIT 1000",
                    (after,),
                )
                result = []
                page_bytes = 0
                try:
                    result, after = self._read_event_page(
                        rows, view, limit, after, result, page_bytes
                    )
                finally:
                    rows.close()
                self._connection.execute("COMMIT")
                self._check_health()
                from core.models.journal_records import JournalEventPage

                return JournalEventPage(
                    events=result, boundary=boundary, after=after
                ).document()
            except LoggingStateError as error:
                self._rollback(self._connection, error)
                raise
            except BaseException as error:  # noqa: BLE001 - Fail closed on interrupted I/O.
                self._rollback(self._connection, error)
                raise self._storage_failure(error, "read events")

    def _read_event_page(
        self,
        rows: sqlite3.Cursor,
        view: str,
        limit: int,
        after: int,
        result: list[JournalEntry],
        page_bytes: int,
    ) -> tuple[list[JournalEntry], int]:
        for row in rows:
            entry = self._decode_row(row)
            if view == "effective":
                entry = self._effective_entry(entry, self._connection)
            if entry is None:
                after = row[0]
                continue
            size = len(row[4].encode("utf-8"))
            if result and page_bytes + size > _PAGE_BYTES:
                break
            result.append(entry)
            page_bytes += size
            after = row[0]
            if len(result) == limit:
                break
        return result, after

    def read_event_batch(
        self,
        event_ids: list[str] | None = None,
        *,
        limit: int = 1000,
        before: int | None = None,
    ) -> JsonObject:
        """Indexed raw lookup or descending cursor page; never modify the journal."""
        self._check_process()
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000.")
        if before is not None and (type(before) is not int or before < 1):
            raise ValueError("before must be a positive event cursor.")
        if event_ids is not None:
            if type(event_ids) is not list or len(event_ids) > limit:
                raise ValueError("event_ids must be a list no larger than limit.")
            for identifier in event_ids:
                require_text(identifier, "event_id")
            if len(set(event_ids)) != len(event_ids) or before is not None:
                raise ValueError("Use distinct event IDs or cursor pagination.")
        with self._lock:
            self._require_open()
            try:
                self._check_health()
                self._connection.execute("BEGIN")
                boundary = _read_boundary(self._connection)
                rows = self._select_event_batch_rows(event_ids, before, boundary, limit)
                entries, size = [], 0
                try:
                    for row in rows:
                        length = len(row[4].encode("utf-8"))
                        if size + length > _PAGE_BYTES and entries:
                            break
                        entries.append(self._decode_row(row))
                        size += length
                finally:
                    rows.close()
                self._connection.execute("COMMIT")
                self._check_health()
                from core.models.journal_records import JournalEventPage

                return JournalEventPage(events=entries, boundary=boundary).document()
            except LoggingStateError as error:
                self._rollback(self._connection, error)
                raise
            except BaseException as error:  # noqa: BLE001 - Roll back interrupted read transactions.
                self._rollback(self._connection, error)
                raise self._storage_failure(error, "read event batch")

    def _select_event_batch_rows(
        self,
        event_ids: list[str] | None,
        before: int | None,
        boundary: JournalReadBoundary,
        limit: int,
    ) -> sqlite3.Cursor:
        columns = "cursor, event_id, producer_instance_id, sequence_number, event_json"
        if event_ids is not None:
            placeholders = ",".join("?" for _ in event_ids)
            rows = self._connection.execute(
                f"SELECT {columns} FROM events WHERE event_id IN ({placeholders}) ORDER BY cursor",
                event_ids,
            )
        else:
            rows = self._connection.execute(
                f"SELECT {columns} FROM events WHERE cursor < ? ORDER BY cursor DESC LIMIT ?",
                (
                    before if before is not None else boundary.cursor + 1,
                    limit,
                ),
            )
        return rows

    def _event_entry(
        self, event_id: str, connection: sqlite3.Connection
    ) -> JournalEntry:
        row = connection.execute(
            "SELECT cursor, event_id, producer_instance_id, sequence_number, event_json "
            "FROM events WHERE event_id=?",
            (event_id,),
        ).fetchone()
        if row is None:
            raise LoggingStorageError("Journal reference points to a missing event.")
        return self._decode_row(row)

    def _effective_entry(
        self, entry: JournalEntry, connection: sqlite3.Connection
    ) -> JournalEntry | None:
        event = entry.event
        if event.event_type != "command.result":
            return entry._with_result(None, False)
        data = _validated_command_result(event.data)
        state = self._load_command_result(data.request_id, connection=connection)
        if state is None:
            raise LoggingStorageError("Command observation has no result index.")
        if event.event_id not in {
            state[author]["event_id"]
            for author in ("runner", "participant")
            if state[author] is not None
        }:
            raise LoggingStorageError(
                "Command observation is absent from its result index."
            )
        if event.event_id != state["effective_event_id"]:
            return None
        return entry._with_result(state["effective_author"], state["runner"] is None)

    def _record_change(
        self,
        event_id: str,
        *,
        request_id: str | None = None,
        result_changed: bool = True,
        observation: JsonObject | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        writer = self._connection if connection is None else connection
        change = {
            "kind": "event" if request_id is None else "command.result",
            "request_id": request_id,
            "effective_event_id": event_id,
            "effective_author": None,
            "provisional": False,
            "result_changed": result_changed,
            "related_event_ids": [event_id],
            "recorded_at": datetime.now(UTC).isoformat(timespec="microseconds"),
            "observation": observation,
        }
        if request_id is not None:
            state = self._load_command_result(request_id, connection=writer)
            change.update(
                effective_event_id=state["effective_event_id"],
                effective_author=state["effective_author"],
                provisional=state["runner"] is None,
                related_event_ids=list(
                    dict.fromkeys(
                        state[author]["event_id"]
                        for author in ("runner", "participant")
                        if state[author] is not None
                    )
                ),
            )
        writer.execute(
            "INSERT INTO journal_changes (event_id, change_json) VALUES (?, ?)",
            (event_id, json.dumps(change, ensure_ascii=True)),
        )

    def _decode_change(
        self, row: tuple, connection: sqlite3.Connection
    ) -> JournalChangeEntry:
        from core.models.journal_records import JournalChangeData, JournalChangeEntry

        cursor, event_id, encoded = row
        change = JournalChangeData.model_validate(json.loads(encoded))
        entry = self._event_entry(change.effective_event_id, connection)
        observed = (
            entry
            if event_id == change.effective_event_id
            else self._event_entry(event_id, connection)
        )
        related = change.related_event_ids
        if event_id not in related or change.effective_event_id not in related:
            raise LoggingStorageError("Change is missing its event references.")
        if change.kind == "event":
            if related != [event_id] or entry.event.event_type == "command.result":
                raise LoggingStorageError("Invalid ordinary event change.")
        else:
            self._validate_command_change(change, entry, connection, related)
        return JournalChangeEntry(
            change_cursor=cursor,
            event_id=event_id,
            change=change,
            entry=entry._with_result(change.effective_author, change.provisional),
            observed=observed,
            encoded_change=encoded,
        )

    def _validate_command_change(
        self,
        change: JournalChangeData,
        entry: JournalEntry,
        connection: sqlite3.Connection,
        related: list[JsonValue],
    ) -> None:
        for related_id in related:
            event = self._event_entry(related_id, connection).event
            if event.event_type != "command.result":
                raise LoggingStorageError("Change refers to an unrelated event.")
            if _validated_command_result(event.data).request_id != change.request_id:
                raise LoggingStorageError("Change refers to another request.")
            if any(
                event.context.root.get(key) != entry.event.context.root.get(key)
                for key in (
                    "experiment_id",
                    "participant_id",
                    "participant_instance_id",
                    "request_id",
                )
            ):
                raise LoggingStorageError("Change mixes request owners.")
        observation = change.observation
        if observation is not None:
            context = observation.context.root
            if any(
                context.get(key) != entry.event.context.root.get(key)
                for key in (
                    "experiment_id",
                    "participant_id",
                    "participant_instance_id",
                    "request_id",
                )
            ):
                raise LoggingStorageError("Change observer belongs to another request.")

    def read_changes(
        self, checkpoint: JsonObject | None = None, *, limit: int = 100
    ) -> JsonObject:
        self._check_process()
        parsed = _validated_checkpoint(checkpoint, "change_cursor")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000.")
        with self._lock:
            self._require_open()
            try:
                self._check_health()
                self._connection.execute("BEGIN")
                boundary = _read_boundary(self._connection)
                after = _checkpoint_position(parsed, boundary, "change_cursor")
                result, after = self._read_change_page(after, limit)
                self._connection.execute("COMMIT")
                self._check_health()
                from core.models.journal_records import JournalChangePage

                return JournalChangePage(
                    changes=result, boundary=boundary, after=after
                ).document()
            except LoggingStateError as error:
                self._rollback(self._connection, error)
                raise
            except BaseException as error:  # noqa: BLE001 - Include interrupted reads.
                self._rollback(self._connection, error)
                raise self._storage_failure(error, "read changes")

    def _read_change_page(
        self, after: int, limit: int
    ) -> tuple[list[JournalChangeEntry], int]:
        result = []
        size = 0
        for row in self._connection.execute(
            "SELECT change_cursor, event_id, change_json FROM journal_changes "
            "WHERE change_cursor > ? ORDER BY change_cursor LIMIT ?",
            (after, limit),
        ):
            item = self._decode_change(row, self._connection)
            item_size = len(
                json.dumps(item.document(), ensure_ascii=False).encode("utf-8")
            )
            if result and size + item_size > _PAGE_BYTES:
                break
            result.append(item)
            size += item_size
            after = row[0]
        return result, after

    def get_journal_info(self) -> JsonObject:
        self._check_process()
        with self._lock:
            self._require_open()
            try:
                self._check_health()
                return _read_journal_info(self._connection)
            except BaseException as error:  # noqa: BLE001 - Fail closed on interrupted I/O.
                raise self._storage_failure(error, "read journal identity")

    def _load_command_result(
        self, request_id: str, *, connection: sqlite3.Connection | None = None
    ) -> dict | None:
        reader = self._connection if connection is None else connection
        row = reader.execute(
            "SELECT identity_json, runner_event_id, participant_event_id, "
            "runner_observation_json, participant_observation_json, "
            "effective_event_id, effective_author FROM command_results WHERE request_id=?",
            (request_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            identity = copy_json_object(json.loads(row[0]), "request identity")
            if identity.keys() != {
                "experiment_id",
                "participant_id",
                "participant_instance_id",
            }:
                raise ValueError("Invalid request identity fields.")
            validate_context(identity)
            require_text(identity["experiment_id"], "experiment_id")
            require_text(identity["participant_id"], "participant_id")
            result = {
                "identity": identity,
                "runner": None,
                "participant": None,
                "effective_event_id": row[5],
                "effective_author": row[6],
            }
            for author, event_id, observation_json in (
                ("runner", row[1], row[3]),
                ("participant", row[2], row[4]),
            ):
                if event_id is None:
                    if observation_json is not None:
                        raise ValueError("Observation has no associated event.")
                    continue
                stored = reader.execute(
                    "SELECT cursor, event_id, producer_instance_id, sequence_number, "
                    "event_json FROM events WHERE event_id=?",
                    (event_id,),
                ).fetchone()
                if stored is None:
                    raise ValueError("Command result refers to a missing event.")
                entry = self._decode_row(stored)
                event = entry.event
                observation, command_data = _decode_author_observation(
                    event, observation_json, request_id, identity
                )
                result[author] = {
                    "event_id": event_id,
                    "entry": entry,
                    "observation": observation,
                    "result": command_data,
                }
            _validate_result_precedence(result, row)
            return result
        except (TypeError, ValueError, RecursionError) as error:
            raise LoggingStorageError("Corrupt command-result index.") from error

    def append_command_result(self, event: JsonObject) -> str:
        """Atomically reconcile a response; duplicate payloads reuse their event ID."""
        self._check_process()
        encoded = encode_event(event, self._max_event_bytes)
        snapshot = json.loads(encoded)
        if (
            snapshot["schema_version"] != SCHEMA_VERSION
            or snapshot["event_type"] != "command.result"
        ):
            raise ValueError("append_command_result requires a command.result event.")
        data = _validated_command_result(snapshot["data"])
        if data.ignored is not None or data.supersedes:
            raise ValueError("Ignored/superseded state is assigned by the journal.")
        identity = {
            name: snapshot["context"].get(name)
            for name in ("experiment_id", "participant_id", "participant_instance_id")
        }
        require_text(identity["experiment_id"], "experiment_id")
        require_text(identity["participant_id"], "participant_id")
        if snapshot["context"].get("request_id") != data.request_id:
            raise ValueError("Command request_id must match the event context.")
        response_key = json.dumps(
            [data.outcome, data.response], sort_keys=True, ensure_ascii=True
        )
        request_id = data.request_id
        author = data.author
        other_author = "participant" if author == "runner" else "runner"
        from core.models.journal_diagnostics import AuthorObservation

        observation = AuthorObservation.model_validate(
            {
                "producer_instance_id": snapshot["producer_instance_id"],
                "occurred_at": snapshot["occurred_at"],
                "context": snapshot["context"],
                "operation_id": snapshot["operation_id"],
            }
        )
        with self._lock:
            self._require_open(writing=True)
            writing_started = False
            try:
                self._check_health(writing=True)
                self._connection.execute("BEGIN IMMEDIATE")
                self._check_health(writing=True)
                state = self._load_command_result(request_id)
                if state is not None and state["identity"] != identity:
                    raise ValueError(
                        "request_id already belongs to another request context."
                    )
                if state is None:
                    state = {"identity": identity, "runner": None, "participant": None}
                before_effective = (
                    state.get("effective_author"),
                    state.get("effective_event_id"),
                )
                own = state[author]
                other = state[other_author]
                if own is not None:
                    own_data = own["result"]
                    own_key = json.dumps(
                        [own_data.outcome, own_data.response],
                        sort_keys=True,
                        ensure_ascii=True,
                    )
                    if response_key != own_key:
                        raise ValueError(
                            "This author already reported a different result."
                        )
                    event_id = own["event_id"]
                else:
                    reused, event_id = _prepare_other_author_result(
                        response_key, other, author, snapshot, data
                    )
                    if not reused:
                        encoded = encode_event(snapshot, self._max_event_bytes)
                        writing_started = True
                        self._insert_event(snapshot, encoded)
                        event_id = snapshot["event_id"]
                    state[author] = {
                        "event_id": event_id,
                        "observation": observation,
                    }
                    effective_author = (
                        "runner" if state["runner"] is not None else "participant"
                    )
                    writing_started = True
                    _save_command_state(self._connection, request_id, state)
                    self._record_change(
                        event_id,
                        request_id=request_id,
                        result_changed=before_effective
                        != (effective_author, state[effective_author]["event_id"]),
                        observation={"author": author, **observation.model_dump()},
                    )
                self._connection.execute("COMMIT")
                self._check_health()
                return event_id
            except (TypeError, ValueError) as error:
                self._rollback(self._connection, error)
                if writing_started or self._failed:
                    raise self._storage_failure(error, "reconcile command", snapshot)
                raise
            except BaseException as error:  # noqa: BLE001 - Include interrupted transactions.
                self._rollback(self._connection, error)
                raise self._storage_failure(error, "reconcile command", snapshot)

    def read_command_result(self, request_id: str) -> JsonObject | None:
        self._check_process()
        require_text(request_id, "request_id")
        with self._lock:
            self._require_open()
            try:
                self._check_health()
                # The index and all referenced events must come from the same read view.
                self._connection.execute("BEGIN")
                state = self._load_command_result(request_id)
                if state is None:
                    result = None
                else:
                    result = _command_result_document(request_id, state)
                self._connection.execute("COMMIT")
                self._check_health()
                return result
            except BaseException as error:  # noqa: BLE001 - Include interrupted transactions.
                self._rollback(self._connection, error)
                raise self._storage_failure(error, "read command result")

    def _validate_snapshot_source(self, reader: sqlite3.Connection) -> None:
        """Validate envelopes and result references inside the selected read view."""
        rows = reader.execute(
            "SELECT cursor, event_id, producer_instance_id, sequence_number, "
            "event_json FROM events ORDER BY cursor"
        )
        try:
            for row in rows:
                entry = self._decode_row(row)
                if entry.event.event_type == "command.result":
                    self._effective_entry(entry, reader)
        finally:
            rows.close()
        requests = reader.execute("SELECT request_id FROM command_results")
        try:
            for (request_id,) in requests:
                self._load_command_result(request_id, connection=reader)
        finally:
            requests.close()
        for row in reader.execute(
            "SELECT change_cursor, event_id, change_json FROM journal_changes ORDER BY change_cursor"
        ):
            self._decode_change(row, reader)
        if reader.execute("PRAGMA foreign_key_check").fetchall():
            raise LoggingStorageError("Journal contains broken references.")

    def export_snapshot(
        self, destination: str | Path, *, min_free_bytes: int
    ) -> JsonObject:
        """Export one confirmed read view into a new directory; never reset the source."""
        self._check_process()
        if not isinstance(destination, (str, Path)):
            raise TypeError("Snapshot destination must be a string or Path.")
        target = Path(destination)
        if not target.is_absolute() or "\x00" in str(target):
            raise ValueError("Snapshot destination must be an absolute path.")
        if type(min_free_bytes) is not int or min_free_bytes < 0:
            raise ValueError("Snapshot min_free_bytes must be a nonnegative integer.")
        with self._lock:
            self._require_open()
            try:
                self._check_health()
            except BaseException as error:  # noqa: BLE001 - Fail closed on interrupted I/O.
                raise self._storage_failure(error, "read snapshot source")
            reader = None
            backup = None
            failure = None
            manifest = None
            try:
                self._check_free_space(target.parent, min_free_bytes)
                # mkdir without exist_ok is the exclusive reservation for this export.
                target.mkdir()
                reader = sqlite3.connect(
                    self.db_path.as_uri() + "?mode=ro",
                    uri=True,
                    isolation_level=None,
                    timeout=self._timeout,
                )
                reader.execute("BEGIN")
                info = _read_journal_info(reader)
                if (info["journal_id"], info["generation"]) != (
                    self._journal_id,
                    self._generation,
                ):
                    raise LoggingStorageError("Snapshot source identity changed.")
                cursor, event_count = reader.execute(
                    "SELECT COALESCE(MAX(cursor), 0), COUNT(*) FROM events"
                ).fetchone()
                try:
                    self._validate_snapshot_source(reader)
                    digest = _content_digest(reader)
                    change_cursor = _read_boundary(reader).change_cursor
                except BaseException as error:  # noqa: BLE001 - Source failure is critical.
                    raise self._storage_failure(error, "validate snapshot source")
                temporary_database = target / "journal.sqlite.part"
                backup = sqlite3.connect(temporary_database, isolation_level=None)
                # This destination is private until publication. Avoid a mapped SHM
                # file whose pending deletion can outlive close on Windows.
                if (
                    backup.execute("PRAGMA locking_mode=EXCLUSIVE").fetchone()[0]
                    != "exclusive"
                ):
                    raise LoggingStorageError(
                        "Snapshot destination requires exclusive access."
                    )
                reader.backup(backup, pages=128, sleep=0.01)
                reader.execute("ROLLBACK")
                if (
                    backup.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
                    != "delete"
                ):
                    raise LoggingStorageError(
                        "Snapshot must be independent of WAL files."
                    )
                backup.execute("PRAGMA synchronous=FULL")
                if backup.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise LoggingStorageError(
                        "Snapshot database integrity check failed."
                    )
                copied_boundary = backup.execute(
                    "SELECT COALESCE(MAX(cursor), 0), COUNT(*) FROM events"
                ).fetchone()
                if copied_boundary != (cursor, event_count):
                    raise LoggingStorageError(
                        "Snapshot does not match the selected boundary."
                    )
                if _read_journal_info(backup) != info:
                    raise LoggingStorageError(
                        "Snapshot journal identity does not match."
                    )
                if _content_digest(backup) != digest:
                    raise LoggingStorageError(
                        "Snapshot content differs from the selected source."
                    )
                backup.close()
                backup = None
                reader.close()
                reader = None
                with temporary_database.open("r+b") as database_file:
                    os.fsync(database_file.fileno())
                temporary_database.rename(target / "journal.sqlite")
                manifest = {
                    "schema_version": SCHEMA_VERSION,
                    "snapshot_id": uuid4().hex,
                    "journal_id": info["journal_id"],
                    "generation": info["generation"],
                    "storage_schema_version": SCHEMA_VERSION,
                    "cursor": cursor,
                    "event_count": event_count,
                    "change_cursor": change_cursor,
                    "content_sha256": digest,
                    "created_at": datetime.now(UTC).isoformat(timespec="microseconds"),
                    "database": "journal.sqlite",
                }
                self._check_health()
                _publish_manifest(target, manifest)
            except BaseException as error:  # noqa: BLE001 - Preserve failure through cleanup.
                failure = error
            finally:
                for connection in (backup, reader):
                    if connection is not None:
                        try:
                            connection.close()
                        except BaseException as cleanup_error:  # noqa: BLE001
                            if failure is None:
                                failure = cleanup_error
                            else:
                                try:
                                    failure.add_note(
                                        f"Snapshot cleanup failed: {type(cleanup_error).__name__}."
                                    )
                                except BaseException:  # noqa: BLE001, S110
                                    pass
            if failure is not None:
                if isinstance(failure, FileExistsError):
                    raise ValueError(
                        "Snapshot destination already exists."
                    ) from failure
                try:
                    self._check_health()
                    if self._connection.execute("PRAGMA quick_check").fetchall() != [
                        ("ok",)
                    ]:
                        raise LoggingStorageError(
                            "Snapshot source failed its integrity check."
                        )
                except BaseException:  # noqa: BLE001 - Preserve the original export failure.
                    raise self._storage_failure(failure, "export snapshot source")
                if isinstance(failure, (OSError, sqlite3.Error)):
                    error = LoggingStorageError(f"Cannot export snapshot to {target}.")
                    error.__cause__ = failure
                else:
                    error = failure
                self._report_failure(error, "export snapshot")
                raise error
            return manifest

    def export_diagnostics(
        self, operation_ids: list[str], destination: str | Path
    ) -> JsonObject:
        """Export operations and referenced facts; caller keeps this outside rollback files."""
        self._check_process()
        if type(operation_ids) is not list or not operation_ids:
            raise ValueError("operation_ids must be a nonempty list.")
        roots = list(
            dict.fromkeys(require_text(item, "operation_id") for item in operation_ids)
        )
        target = Path(destination)
        if not target.is_absolute() or "\x00" in str(target):
            raise ValueError("Diagnostic destination must be an absolute path.")
        with self._lock:
            self._require_open()
            reader = None
            source_phase = True
            try:
                self._check_health()
                reader = sqlite3.connect(
                    self.db_path.as_uri() + "?mode=ro",
                    uri=True,
                    isolation_level=None,
                    timeout=self._timeout,
                )
                reader.execute("BEGIN")
                boundary = _read_boundary(reader)
                if (
                    boundary.generation != self._generation
                    or boundary.journal_id != self._journal_id
                ):
                    raise LoggingStorageError("Diagnostic source changed.")
                self._validate_snapshot_source(reader)
                read_event = lambda event_id: self._event_entry(event_id, reader)
                read_result = lambda request_id: self._load_command_result(
                    request_id, connection=reader
                )
                metadata, starts, selected_operations, selected = (
                    _diagnostic_operation_events(reader, roots, self._decode_row)
                )
                _add_diagnostic_observer_events(
                    reader, selected_operations, selected, read_result
                )
                requests = _diagnostic_dependencies(
                    metadata, starts, selected, read_event, read_result
                )
                source_phase = False
                target.mkdir()
                records_sha256 = _write_diagnostic_records(
                    target, metadata, selected, requests, read_event
                )
                reader.execute("ROLLBACK")
                source_phase = True
                self._check_health()
                manifest = _diagnostic_manifest(
                    roots, boundary, records_sha256, len(selected), len(requests)
                )
                source_phase = False
                (target / "records.jsonl.part").rename(target / "records.jsonl")
                _publish_manifest(target, manifest)
                return manifest
            except FileExistsError as error:
                raise ValueError("Diagnostic destination already exists.") from error
            except LoggingStateError:
                raise
            except (sqlite3.Error, LoggingStorageError, ValueError, TypeError) as error:
                raise self._storage_failure(error, "export diagnostic source")
            except OSError as error:
                if source_phase:
                    raise self._storage_failure(error, "export diagnostic source")
                failure = LoggingStorageError(f"Cannot export diagnostics to {target}.")
                self._report_failure(failure, "export diagnostics")
                raise failure from error
            finally:
                if reader is not None:
                    _close_preserving_failure(reader)

    def _read_diagnostics(
        self, directory: str | Path
    ) -> tuple[JsonObject, list[JsonObject], list[DiagnosticCommand]]:
        path = Path(directory)
        if not path.is_absolute():
            raise ValueError("Diagnostics must use an absolute directory path.")
        with (path / "manifest.json").open(encoding="utf-8") as source:
            manifest = copy_json_object(json.load(source), "diagnostics manifest")
        from core.models.journal_diagnostics import DiagnosticManifest

        validated = DiagnosticManifest.model_validate(manifest)
        events, commands = self._read_diagnostic_records(path, validated)
        return manifest, events, commands

    def _read_diagnostic_records(
        self, path: Path, manifest: DiagnosticManifest
    ) -> tuple[list[JsonObject], list[DiagnosticCommand]]:
        events, commands = [], []
        digest = hashlib.sha256()
        event_ids, request_ids = set(), set()
        with (path / "records.jsonl").open("rb") as source:
            for line in source:
                digest.update(line)
                # Validate the envelope separately: wrapping an accepted event must
                # not consume an extra level of its JSON depth allowance.
                record = json.loads(line)
                if type(record) is not dict:
                    raise ValueError("Diagnostic record must be an object.")
                if record.get("kind") == "event" and record.keys() == {"kind", "event"}:
                    event = json.loads(
                        encode_event(record["event"], self._max_event_bytes)
                    )
                    if event["event_id"] in event_ids:
                        raise ValueError(
                            "Diagnostic bundle contains duplicate event IDs."
                        )
                    event_ids.add(event["event_id"])
                    events.append(event)
                elif record.get("kind") == "command" and record.keys() == {
                    "kind",
                    "request_id",
                    "identity",
                    "runner",
                    "participant",
                }:
                    _validate_diagnostic_command(record, request_ids, commands)
                else:
                    raise ValueError("Unknown diagnostic record format.")
        if digest.hexdigest() != manifest.records_sha256:
            raise ValueError("Diagnostic records checksum does not match.")
        if (len(events), len(commands)) != (
            manifest.event_count,
            manifest.command_count,
        ):
            raise ValueError("Diagnostic record counts do not match.")
        for record in commands:
            for author in ("runner", "participant"):
                observation = (
                    record.runner if author == "runner" else record.participant
                )
                if observation is not None and observation.event_id not in event_ids:
                    raise ValueError("Diagnostic result refers outside its bundle.")
        return events, commands

    def complete_restore(
        self,
        snapshot_manifest: JsonObject,
        *,
        restoration_id: str,
        new_generation: str,
        diagnostics: str | Path | None = None,
    ) -> JsonObject:
        """Finalize an already restored journal; runner owns the file/process barrier."""
        if self._read_only:
            raise LoggingStateError(
                "A read-only journal cannot finalize a restoration."
            )
        self._check_process()
        manifest, identity, restoration_id, new_generation = _validate_restore_input(
            snapshot_manifest, restoration_id, new_generation
        )
        with self._lock:
            if self._connection is not None:
                raise LoggingStateError(
                    "Close the journal before completing restoration."
                )
            prepared_events, commands, parameters = self._prepare_restore_import(
                manifest, identity, new_generation, diagnostics
            )
            connection = None
            try:
                file_identity = self._check_file()
                self._check_free_space(self.db_path.parent, self._min_free_bytes)
                connection = sqlite3.connect(
                    self.db_path.as_uri() + "?mode=rw",
                    uri=True,
                    timeout=self._timeout,
                    isolation_level=None,
                )
                if self._check_schema(connection):
                    raise LoggingStorageError(
                        "Restoration requires an initialized journal."
                    )
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA synchronous=EXTRA")
                connection.execute("BEGIN IMMEDIATE")
                self._check_free_space(self.db_path.parent, self._min_free_bytes)
                result, changed = self._restore_journal_records(
                    connection, manifest, identity, restoration_id, new_generation,
                    parameters, prepared_events, commands,
                )
                if not changed:
                    connection.execute("ROLLBACK")
                    self._remember_restored_journal(result, file_identity)
                    return result
                self._validate_snapshot_source(connection)
                # Content was checked through this transaction. A second SQLite
                # reader here could wait on our own write lock after cache spill.
                status = self.db_path.stat()
                if (status.st_dev, status.st_ino) != file_identity:
                    raise LoggingStorageError("Restored file changed before commit.")
                connection.execute("COMMIT")
                self._remember_restored_journal(result, file_identity)
                return result
            except (ValueError, LoggingStateError) as error:
                if connection is not None:
                    self._rollback(connection, error)
                raise
            except BaseException as error:  # noqa: BLE001 - Preserve interrupted recovery.
                if connection is not None:
                    self._rollback(connection, error)
                raise self._storage_failure(error, "complete journal restoration")
            finally:
                if connection is not None:
                    _close_preserving_failure(connection)

    def _prepare_restore_import(
        self,
        manifest: JournalSnapshotManifest,
        identity: JsonObject,
        new_generation: str,
        diagnostics: str | Path | None,
    ) -> tuple[list[tuple[JsonObject, str]], list[DiagnosticCommand], str]:
        bundle, events, commands = None, [], []
        if diagnostics is not None:
            bundle, events, commands = self._read_diagnostics(diagnostics)
            if bundle["journal_id"] != identity["journal_id"]:
                raise ValueError("Diagnostic bundle belongs to another journal.")
        prepared_events = _prepare_diagnostic_events(events, self._max_event_bytes)
        parameters = json.dumps(
            {
                "snapshot": manifest.model_dump(),
                "new_generation": new_generation,
                "diagnostics": bundle,
            },
            sort_keys=True,
            ensure_ascii=True,
        )
        return prepared_events, commands, parameters

    def _restore_journal_records(
        self,
        connection: sqlite3.Connection,
        manifest: JournalSnapshotManifest,
        identity: JsonObject,
        restoration_id: str,
        new_generation: str,
        parameters: str,
        prepared_events: list[tuple[JsonObject, str]],
        commands: list[DiagnosticCommand],
    ) -> tuple[JsonObject, bool]:
        info = _read_journal_info(connection)
        actual = {name: info[name] for name in ("journal_id", "generation")}
        if self._expected_journal not in (identity, actual):
            raise LoggingStateError(
                "Restoration client must identify the restored journal."
            )
        previous = connection.execute(
            "SELECT parameters_json, result_json FROM journal_restorations WHERE restoration_id=?",
            (restoration_id,),
        ).fetchone()
        if previous is not None:
            result = self._recorded_restoration_result(
                previous, parameters, restoration_id, new_generation, actual
            )
            return result, False
        if actual != identity:
            raise JournalGenerationChanged(identity, actual)
        self._check_restored_snapshot(connection, manifest)
        inserted = self._import_diagnostic_events(connection, prepared_events)
        changed_requests = self._import_diagnostic_commands(connection, commands)
        self._record_restored_changes(connection, inserted, changed_requests)
        connection.execute(
            "UPDATE journal_info SET generation=? WHERE singleton=1",
            (new_generation,),
        )
        result = {
            **_read_boundary(connection).model_dump(),
            "restoration_id": restoration_id,
            "snapshot_id": manifest.snapshot_id,
            "imported_events": len(inserted),
        }
        connection.execute(
            "INSERT INTO journal_restorations VALUES (?, ?, ?)",
            (restoration_id, parameters, json.dumps(result, sort_keys=True)),
        )
        return result, True

    def _recorded_restoration_result(
        self, previous: tuple, parameters: str, restoration_id: str,
        new_generation: str, actual: JsonObject,
    ) -> JsonObject:
        if previous[0] != parameters:
            raise ValueError(
                "restoration_id already belongs to another restoration."
            )
        result = copy_json_object(
            json.loads(previous[1]), "restoration result"
        )
        if (
            result.keys()
            != {
                "schema_version",
                "journal_id",
                "generation",
                "cursor",
                "event_count",
                "change_cursor",
                "restoration_id",
                "snapshot_id",
                "imported_events",
            }
            or result["restoration_id"] != restoration_id
            or result["generation"] != new_generation
        ):
            raise LoggingStorageError(
                "Invalid recorded restoration result."
            )
        if actual != {
            name: result[name] for name in ("journal_id", "generation")
        }:
            raise LoggingStateError(
                "This restoration has already been superseded."
            )
        return result

    def _check_restored_snapshot(
        self, connection: sqlite3.Connection, manifest: JournalSnapshotManifest
    ) -> None:
        if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
            raise LoggingStorageError("Restored journal failed its integrity check.")
        self._validate_snapshot_source(connection)
        boundary = _read_boundary(connection)
        if any(
            getattr(boundary, key) != getattr(manifest, key)
            for key in ("cursor", "event_count", "change_cursor")
        ):
            raise ValueError("Restored journal does not match the snapshot boundary.")
        if _content_digest(connection) != manifest.content_sha256:
            raise ValueError("Restored journal does not match the snapshot contents.")

    def _import_diagnostic_events(
        self, connection: sqlite3.Connection, events: list[tuple[JsonObject, str]]
    ) -> list[JsonObject]:
        inserted = []
        for event, encoded in events:
            previous = connection.execute(
                "SELECT event_json FROM events WHERE event_id=?",
                (event["event_id"],),
            ).fetchone()
            if previous is not None:
                if json.dumps(
                    json.loads(previous[0]), sort_keys=True
                ) != json.dumps(event, sort_keys=True):
                    raise ValueError(
                        "Diagnostic event ID conflicts with restored history."
                    )
                continue
            if connection.execute(
                "SELECT 1 FROM events WHERE producer_instance_id=? AND sequence_number=?",
                (event["producer_instance_id"], event["sequence_number"]),
            ).fetchone():
                raise ValueError(
                    "Diagnostic producer sequence conflicts with restored history."
                )
            self._insert_event(event, encoded, connection=connection)
            inserted.append(event)
        return inserted

    def _import_diagnostic_commands(
        self, connection: sqlite3.Connection, commands: list[DiagnosticCommand]
    ) -> dict[str, tuple[str, bool]]:
        changed_requests = {}
        for record in commands:
            request_id = record.request_id
            state = self._load_command_result(request_id, connection=connection)
            state, before, changed = _merge_diagnostic_command(state, record)
            _save_command_state(connection, request_id, state)
            checked = self._load_command_result(request_id, connection=connection)
            if changed:
                changed_requests[request_id] = (
                    checked["effective_event_id"],
                    before
                    != (
                        checked["effective_author"],
                        checked["effective_event_id"],
                    ),
                )
        return changed_requests

    def _record_restored_changes(
        self, connection: sqlite3.Connection, inserted: list[JsonObject],
        changed_requests: dict[str, tuple[str, bool]],
    ) -> None:
        for event in inserted:
            if event["event_type"] != "command.result":
                self._record_change(event["event_id"], connection=connection)
            else:
                self._effective_entry(
                    self._event_entry(event["event_id"], connection), connection
                )
        for request_id, (event_id, changed) in changed_requests.items():
            self._record_change(
                event_id,
                request_id=request_id,
                result_changed=changed,
                connection=connection,
            )

    def _remember_restored_journal(self, result: JsonObject, file_identity: tuple[int, int]) -> None:
        self._journal_id, self._generation = (
            result["journal_id"],
            result["generation"],
        )
        self._expected_journal = {
            name: result[name] for name in ("journal_id", "generation")
        }
        self._file_identity = file_identity
        self._failed = False

    def close(self) -> None:
        self._check_process()
        with self._lock:
            if self._connection is None:
                return
            try:
                self._connection.close()
            except BaseException as error:  # noqa: BLE001 - Fail closed on interrupted I/O.
                raise self._storage_failure(error, "close")
            self._connection = None
