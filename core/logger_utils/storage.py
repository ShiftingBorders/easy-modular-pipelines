"""One durable journal format, command reconciliation and identified recovery."""

import hashlib
import json
import os
import shutil
import socket
import sqlite3
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from core.logger_utils.events import (
    SCHEMA_VERSION,
    JournalGenerationChanged,
    JsonObject,
    LoggingConfigurationError,
    LoggingStateError,
    LoggingStorageError,
    copy_json_object,
    encode_event,
    require_number,
    require_text,
    validate_checkpoint,
    validate_command_result,
    validate_context,
    validate_journal_identity,
)

_APPLICATION_ID = 0x454D504C
_CREATE_EVENTS = """CREATE TABLE events (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    producer_instance_id TEXT NOT NULL,
    sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
    event_json TEXT NOT NULL,
    UNIQUE (producer_instance_id, sequence_number)
)"""
_CREATE_JOURNAL_INFO = """CREATE TABLE journal_info (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    journal_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    created_at TEXT NOT NULL
)"""

_CREATE_CHANGES = """CREATE TABLE journal_changes (
    change_cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES events(event_id),
    change_json TEXT NOT NULL
)"""
_CREATE_RESTORATIONS = """CREATE TABLE journal_restorations (
    restoration_id TEXT PRIMARY KEY NOT NULL,
    parameters_json TEXT NOT NULL,
    result_json TEXT NOT NULL
)"""
_CREATE_COMMAND_RESULTS = """CREATE TABLE command_results (
    request_id TEXT PRIMARY KEY NOT NULL,
    identity_json TEXT NOT NULL,
    runner_event_id TEXT REFERENCES events(event_id),
    participant_event_id TEXT REFERENCES events(event_id),
    runner_observation_json TEXT,
    participant_observation_json TEXT,
    effective_event_id TEXT NOT NULL REFERENCES events(event_id),
    effective_author TEXT NOT NULL CHECK (effective_author IN ('runner', 'participant')),
    CHECK (runner_event_id IS NOT NULL OR participant_event_id IS NOT NULL)
)"""


_TABLES = {
    "events": _CREATE_EVENTS,
    "journal_info": _CREATE_JOURNAL_INFO,
    "command_results": _CREATE_COMMAND_RESULTS,
    "journal_changes": _CREATE_CHANGES,
    "journal_restorations": _CREATE_RESTORATIONS,
}
_PAGE_BYTES = 16777216


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
        self._read_only = read_only
        if not isinstance(db_path, (str, Path)):
            raise TypeError("db_path must be a string or Path.")
        path = Path(db_path)
        if not path.is_absolute() or "\x00" in str(path):
            raise ValueError("db_path must be an absolute filesystem path.")
        timeout = require_number(busy_timeout_seconds, "busy_timeout_seconds")
        if not 0 < timeout <= 60:
            raise ValueError(
                "busy_timeout_seconds must be greater than 0 and at most 60."
            )
        if max_event_bytes is not None and (
            type(max_event_bytes) is not int or max_event_bytes < 1
        ):
            raise ValueError("max_event_bytes must be a positive integer or None.")
        if open_mode not in ("create", "existing"):
            raise ValueError("open_mode must be create or existing.")
        if type(min_free_bytes) is not int or min_free_bytes < 0:
            raise ValueError("min_free_bytes must be a nonnegative integer.")
        if open_mode == "existing":
            expected_journal = validate_journal_identity(expected_journal)
        elif expected_journal is not None:
            raise ValueError("create requires expected_journal=None.")
        self.db_path = path
        self._timeout = float(timeout)
        self._max_event_bytes = max_event_bytes
        self._expected_journal = expected_journal
        self._open_mode = open_mode
        self._min_free_bytes = min_free_bytes
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.RLock()
        self._process_id = os.getpid()
        self._failed = False
        self._file_identity: tuple[int, int] | None = None
        self._journal_id: str | None = None
        self._generation: str | None = None
        self._diagnostic_context = validate_context(
            {} if diagnostic_context is None else diagnostic_context,
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
            if (
                kind != "table"
                or name not in expected
                or not isinstance(sql, str)
                or " ".join(sql.split()) != " ".join(expected[name].split())
            ):
                compatible = False
        if not compatible:
            raise LoggingConfigurationError(
                f"Incompatible journal schema at {self.db_path}; "
                "The file was not converted."
            )
        return False

    def _read_journal_info(self, connection: sqlite3.Connection) -> JsonObject:
        rows = connection.execute(
            "SELECT singleton, journal_id, generation, created_at FROM journal_info"
        ).fetchall()
        if len(rows) != 1 or rows[0][0] != 1:
            raise LoggingStorageError("Invalid journal identity record.")
        _, journal_id, generation, created_at = rows[0]
        try:
            identity = validate_journal_identity(
                {"journal_id": journal_id, "generation": generation}
            )
            timestamp = datetime.fromisoformat(created_at)
        except (TypeError, ValueError) as error:
            raise LoggingStorageError("Invalid journal identity metadata.") from error
        if timestamp.utcoffset() != UTC.utcoffset(None):
            raise LoggingStorageError("Journal creation time must be UTC.")
        return {
            "schema_version": SCHEMA_VERSION,
            **identity,
        }

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
        info = self._read_journal_info(self._connection)
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
                else self._diagnostic_context,
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
            try:
                if sys.stderr is not None:
                    sys.stderr.write(note + "\n")
                    sys.stderr.flush()
            except BaseException:  # noqa: BLE001, S110 - Last-resort sink.
                pass
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

    def _create_tables(self, connection: sqlite3.Connection) -> None:
        for statement in _TABLES.values():
            connection.execute(statement)
        connection.execute(
            "INSERT INTO journal_info VALUES (1, ?, ?, ?)",
            (
                uuid4().hex,
                uuid4().hex,
                datetime.now(UTC).isoformat(timespec="microseconds"),
            ),
        )

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
                    existing = self._read_journal_info(connection)
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
                        self._create_tables(connection)
                        connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
                        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
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
                info = self._read_journal_info(connection)
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

    def _decode_row(self, row: tuple) -> JsonObject:
        cursor, event_id, producer, sequence, encoded = row
        if not isinstance(encoded, str):
            raise TypeError("Stored event must be JSON text.")
        event = json.loads(encoded)
        encode_event(event, None)
        if (
            event["event_id"],
            event["producer_instance_id"],
            event["sequence_number"],
        ) != (event_id, producer, sequence):
            raise ValueError("Stored event disagrees with its indexed identity.")
        return {"cursor": cursor, "event": event}

    def read_events(
        self,
        checkpoint: JsonObject | None = None,
        *,
        limit: int = 100,
        view: str = "raw",
    ) -> JsonObject:
        self._check_process()
        checkpoint = validate_checkpoint(checkpoint, "cursor")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000.")
        if view not in ("raw", "effective"):
            raise ValueError("view must be raw or effective.")
        with self._lock:
            self._require_open()
            try:
                self._check_health()
                self._connection.execute("BEGIN")
                boundary = self._read_boundary(self._connection)
                after = self._checkpoint_position(checkpoint, boundary, "cursor")
                rows = self._connection.execute(
                    "SELECT cursor, event_id, producer_instance_id, sequence_number, "
                    "event_json FROM events WHERE cursor > ? ORDER BY cursor LIMIT 1000",
                    (after,),
                )
                result = []
                page_bytes = 0
                try:
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
                finally:
                    rows.close()
                self._connection.execute("COMMIT")
                self._check_health()
                return {
                    "events": result,
                    "checkpoint": self._checkpoint(boundary, "cursor", after),
                    "boundary": boundary,
                    "has_more": after < boundary["cursor"],
                }
            except LoggingStateError as error:
                self._rollback(self._connection, error)
                raise
            except BaseException as error:  # noqa: BLE001 - Fail closed on interrupted I/O.
                self._rollback(self._connection, error)
                raise self._storage_failure(error, "read events")

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
                boundary = self._read_boundary(self._connection)
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
                            before if before is not None else boundary["cursor"] + 1,
                            limit,
                        ),
                    )
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
                return {"events": entries, "boundary": boundary}
            except LoggingStateError as error:
                self._rollback(self._connection, error)
                raise
            except BaseException as error:  # noqa: BLE001 - Roll back interrupted read transactions.
                self._rollback(self._connection, error)
                raise self._storage_failure(error, "read event batch")

    def _read_boundary(self, connection: sqlite3.Connection) -> JsonObject:
        info = self._read_journal_info(connection)
        cursor = connection.execute(
            "SELECT COALESCE(MAX(cursor), 0) FROM events"
        ).fetchone()[0]
        count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        change = connection.execute(
            "SELECT COALESCE(MAX(change_cursor), 0) FROM journal_changes"
        ).fetchone()[0]
        return {**info, "cursor": cursor, "event_count": count, "change_cursor": change}

    def _checkpoint(self, info: JsonObject, key: str, position: int) -> JsonObject:
        return {
            "journal_id": info["journal_id"],
            "generation": info["generation"],
            key: position,
        }

    def _checkpoint_position(
        self, checkpoint: JsonObject | None, boundary: JsonObject, key: str
    ) -> int:
        if checkpoint is None:
            return 0
        expected = {name: checkpoint[name] for name in ("journal_id", "generation")}
        actual = {name: boundary[name] for name in expected}
        if expected != actual:
            raise JournalGenerationChanged(expected, actual)
        if checkpoint[key] > boundary[key]:
            raise LoggingStateError(
                "Checkpoint is beyond the committed journal boundary."
            )
        return checkpoint[key]

    def _event_entry(self, event_id: str, connection: sqlite3.Connection) -> JsonObject:
        row = connection.execute(
            "SELECT cursor, event_id, producer_instance_id, sequence_number, event_json "
            "FROM events WHERE event_id=?",
            (event_id,),
        ).fetchone()
        if row is None:
            raise LoggingStorageError("Journal reference points to a missing event.")
        return self._decode_row(row)

    def _effective_entry(
        self, entry: JsonObject, connection: sqlite3.Connection
    ) -> JsonObject | None:
        event = entry["event"]
        if event["event_type"] != "command.result":
            return {**entry, "effective_author": None, "provisional": False}
        data = validate_command_result(event["data"])
        state = self._load_command_result(data["request_id"], connection=connection)
        if state is None:
            raise LoggingStorageError("Command observation has no result index.")
        if event["event_id"] not in {
            state[author]["event_id"]
            for author in ("runner", "participant")
            if state[author] is not None
        }:
            raise LoggingStorageError(
                "Command observation is absent from its result index."
            )
        if event["event_id"] != state["effective_event_id"]:
            return None
        return {
            **entry,
            "effective_author": state["effective_author"],
            "provisional": state["runner"] is None,
        }

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

    def _decode_change(self, row: tuple, connection: sqlite3.Connection) -> JsonObject:
        cursor, event_id, encoded = row
        change = copy_json_object(json.loads(encoded), "journal change")
        if change.keys() != {
            "kind",
            "request_id",
            "effective_event_id",
            "effective_author",
            "provisional",
            "result_changed",
            "related_event_ids",
            "recorded_at",
            "observation",
        }:
            raise LoggingStorageError("Invalid journal change fields.")
        for field in ("provisional", "result_changed"):
            if type(change[field]) is not bool:
                raise LoggingStorageError("Invalid change result state.")
        timestamp = datetime.fromisoformat(change["recorded_at"])
        if timestamp.utcoffset() != UTC.utcoffset(None):
            raise LoggingStorageError("Change timestamp must be UTC.")
        entry = self._event_entry(change["effective_event_id"], connection)
        observed = (
            entry
            if event_id == change["effective_event_id"]
            else self._event_entry(event_id, connection)
        )
        related = change["related_event_ids"]
        if (
            type(related) is not list
            or not related
            or len(set(related)) != len(related)
        ):
            raise LoggingStorageError("Invalid change event references.")
        if event_id not in related or change["effective_event_id"] not in related:
            raise LoggingStorageError("Change is missing its event references.")
        if change["kind"] == "event":
            if (
                change["request_id"] is not None
                or change["effective_author"] is not None
                or related != [event_id]
                or change["provisional"]
                or not change["result_changed"]
                or change["observation"] is not None
                or entry["event"]["event_type"] == "command.result"
            ):
                raise LoggingStorageError("Invalid ordinary event change.")
        elif change["kind"] == "command.result":
            require_text(change["request_id"], "request_id")
            if change["effective_author"] not in ("runner", "participant"):
                raise LoggingStorageError("Invalid effective change author.")
            if change["provisional"] != (change["effective_author"] == "participant"):
                raise LoggingStorageError(
                    "Change confirmation state disagrees with author."
                )
            for related_id in related:
                event = self._event_entry(related_id, connection)["event"]
                if event["event_type"] != "command.result":
                    raise LoggingStorageError("Change refers to an unrelated event.")
                if (
                    validate_command_result(event["data"])["request_id"]
                    != change["request_id"]
                ):
                    raise LoggingStorageError("Change refers to another request.")
                if any(
                    event["context"].get(key) != entry["event"]["context"].get(key)
                    for key in (
                        "experiment_id",
                        "participant_id",
                        "participant_instance_id",
                        "request_id",
                    )
                ):
                    raise LoggingStorageError("Change mixes request owners.")
            observation = change["observation"]
            if observation is not None:
                if (
                    type(observation) is not dict
                    or observation.keys()
                    != {
                        "author",
                        "producer_instance_id",
                        "occurred_at",
                        "context",
                        "operation_id",
                    }
                    or observation["author"] not in ("runner", "participant")
                ):
                    raise LoggingStorageError("Invalid change observer.")
                if observation["operation_id"] is not None:
                    require_text(observation["operation_id"], "operation_id")
                require_text(
                    observation["producer_instance_id"], "producer_instance_id"
                )
                if datetime.fromisoformat(
                    observation["occurred_at"]
                ).utcoffset() != UTC.utcoffset(None):
                    raise LoggingStorageError("Change observation time must be UTC.")
                context = validate_context(observation["context"])
                if any(
                    context.get(key) != entry["event"]["context"].get(key)
                    for key in (
                        "experiment_id",
                        "participant_id",
                        "participant_instance_id",
                        "request_id",
                    )
                ):
                    raise LoggingStorageError(
                        "Change observer belongs to another request."
                    )
        else:
            raise LoggingStorageError("Unknown change kind.")
        return {
            "change_cursor": cursor,
            "event_id": event_id,
            **change,
            "entry": {
                **entry,
                "effective_author": change["effective_author"],
                "provisional": change["provisional"],
            },
            "observed_event": observed["event"],
        }

    def read_changes(
        self, checkpoint: JsonObject | None = None, *, limit: int = 100
    ) -> JsonObject:
        self._check_process()
        checkpoint = validate_checkpoint(checkpoint, "change_cursor")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000.")
        with self._lock:
            self._require_open()
            try:
                self._check_health()
                self._connection.execute("BEGIN")
                boundary = self._read_boundary(self._connection)
                after = self._checkpoint_position(checkpoint, boundary, "change_cursor")
                result = []
                size = 0
                for row in self._connection.execute(
                    "SELECT change_cursor, event_id, change_json FROM journal_changes "
                    "WHERE change_cursor > ? ORDER BY change_cursor LIMIT ?",
                    (after, limit),
                ):
                    item = self._decode_change(row, self._connection)
                    item_size = len(
                        json.dumps(item, ensure_ascii=False).encode("utf-8")
                    )
                    if result and size + item_size > _PAGE_BYTES:
                        break
                    result.append(item)
                    size += item_size
                    after = row[0]
                self._connection.execute("COMMIT")
                self._check_health()
                return {
                    "changes": result,
                    "checkpoint": self._checkpoint(boundary, "change_cursor", after),
                    "boundary": boundary,
                    "has_more": after < boundary["change_cursor"],
                }
            except LoggingStateError as error:
                self._rollback(self._connection, error)
                raise
            except BaseException as error:  # noqa: BLE001 - Include interrupted reads.
                self._rollback(self._connection, error)
                raise self._storage_failure(error, "read changes")

    def get_journal_info(self) -> JsonObject:
        self._check_process()
        with self._lock:
            self._require_open()
            try:
                self._check_health()
                return self._read_journal_info(self._connection)
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
                event = self._decode_row(stored)["event"]
                if (
                    event["schema_version"] != SCHEMA_VERSION
                    or event["event_type"] != "command.result"
                ):
                    raise ValueError("Command result refers to an unrelated event.")
                data = validate_command_result(event["data"])
                if (
                    data["request_id"] != request_id
                    or event["context"].get("request_id") != request_id
                ):
                    raise ValueError("Command result request identity does not match.")
                if {key: event["context"].get(key) for key in identity} != identity:
                    raise ValueError(
                        "Command result belongs to another request context."
                    )
                observation = copy_json_object(
                    json.loads(observation_json), "observation"
                )
                if observation.keys() != {
                    "producer_instance_id",
                    "occurred_at",
                    "context",
                    "operation_id",
                }:
                    raise ValueError("Invalid observation fields.")
                if observation["operation_id"] is not None:
                    require_text(observation["operation_id"], "operation_id")
                require_text(
                    observation["producer_instance_id"], "producer_instance_id"
                )
                timestamp = datetime.fromisoformat(observation["occurred_at"])
                if timestamp.utcoffset() != UTC.utcoffset(None):
                    raise ValueError("Observation time must be UTC.")
                context = validate_context(observation["context"])
                if context.get("request_id") != request_id:
                    raise ValueError("Observation request_id does not match.")
                if {key: context.get(key) for key in identity} != identity:
                    raise ValueError("Observation belongs to another request context.")
                result[author] = {
                    "event_id": event_id,
                    "event": event,
                    "observation": observation,
                }
            runner = result["runner"]
            participant = result["participant"]
            shared_event = (
                runner is not None
                and participant is not None
                and runner["event_id"] == participant["event_id"]
            )
            if not shared_event:
                for author in ("runner", "participant"):
                    entry = result[author]
                    if entry is not None and entry["event"]["data"]["author"] != author:
                        raise ValueError(
                            "Command event author does not match its index."
                        )
            effective_author = (
                "runner" if result["runner"] is not None else "participant"
            )
            effective = result[effective_author]
            if (
                effective is None
                or row[6] != effective_author
                or row[5] != effective["event_id"]
            ):
                raise ValueError("Invalid effective command result.")
            if runner is None and participant["event"]["data"]["ignored"] is not None:
                raise ValueError(
                    "An ignored participant response requires a runner result."
                )
            if shared_event:
                shared_data = effective["event"]["data"]
                if shared_data["ignored"] is not None or shared_data["supersedes"]:
                    raise ValueError("A shared result cannot be ignored or superseded.")
            else:
                if (
                    participant is not None
                    and participant["event"]["data"]["supersedes"]
                ):
                    raise ValueError("A participant response cannot supersede runner.")
                if runner is not None:
                    runner_data = runner["event"]["data"]
                    if runner_data["ignored"] is not None:
                        raise ValueError("A runner result cannot be ignored.")
                    supersedes = runner_data["supersedes"]
                    if supersedes and (
                        participant is None
                        or supersedes
                        != [
                            {
                                "event_id": participant["event_id"],
                                "ignored": self._ignored_reason(runner["event"]),
                            }
                        ]
                    ):
                        raise ValueError("Invalid superseded participant reference.")
                    if participant is not None:
                        participant_data = participant["event"]["data"]
                        reason = participant_data["ignored"]
                        if reason is not None and reason != self._ignored_reason(
                            runner["event"]
                        ):
                            raise ValueError("Invalid ignored participant reason.")
                        if json.dumps(
                            [runner_data["outcome"], runner_data["response"]],
                            sort_keys=True,
                        ) == json.dumps(
                            [participant_data["outcome"], participant_data["response"]],
                            sort_keys=True,
                        ):
                            raise ValueError(
                                "Matching results must share one event ID."
                            )
            return result
        except (TypeError, ValueError, RecursionError) as error:
            raise LoggingStorageError("Corrupt command-result index.") from error

    def _ignored_reason(self, runner_event: JsonObject) -> str:
        return {
            "timed_out": "request_timed_out",
            "cancelled": "request_cancelled",
            "invalidated": "request_invalidated",
        }.get(runner_event["data"]["outcome"], "runner_result_precedence")

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
        data = validate_command_result(snapshot["data"])
        if data["ignored"] is not None or data["supersedes"]:
            raise ValueError("Ignored/superseded state is assigned by the journal.")
        identity = {
            name: snapshot["context"].get(name)
            for name in ("experiment_id", "participant_id", "participant_instance_id")
        }
        require_text(identity["experiment_id"], "experiment_id")
        require_text(identity["participant_id"], "participant_id")
        if snapshot["context"].get("request_id") != data["request_id"]:
            raise ValueError("Command request_id must match the event context.")
        response_key = json.dumps(
            [data["outcome"], data["response"]], sort_keys=True, ensure_ascii=True
        )
        request_id = data["request_id"]
        author = data["author"]
        other_author = "participant" if author == "runner" else "runner"
        observation = {
            "producer_instance_id": snapshot["producer_instance_id"],
            "occurred_at": snapshot["occurred_at"],
            "context": snapshot["context"],
            "operation_id": snapshot["operation_id"],
        }
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
                    own_data = own["event"]["data"]
                    own_key = json.dumps(
                        [own_data["outcome"], own_data["response"]],
                        sort_keys=True,
                        ensure_ascii=True,
                    )
                    if response_key != own_key:
                        raise ValueError(
                            "This author already reported a different result."
                        )
                    event_id = own["event_id"]
                else:
                    other_key = None
                    if other is not None:
                        other_data = other["event"]["data"]
                        other_key = json.dumps(
                            [other_data["outcome"], other_data["response"]],
                            sort_keys=True,
                            ensure_ascii=True,
                        )
                    if response_key == other_key:
                        event_id = other["event_id"]
                    else:
                        if author == "participant" and other is not None:
                            snapshot["data"]["ignored"] = self._ignored_reason(
                                other["event"]
                            )
                        elif author == "runner" and other is not None:
                            snapshot["data"]["supersedes"] = [
                                {
                                    "event_id": other["event_id"],
                                    "ignored": self._ignored_reason(snapshot),
                                }
                            ]
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
                    self._save_command_state(self._connection, request_id, state)
                    self._record_change(
                        event_id,
                        request_id=request_id,
                        result_changed=before_effective
                        != (effective_author, state[effective_author]["event_id"]),
                        observation={"author": author, **observation},
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
                    effective = state[state["effective_author"]]
                    data = effective["event"]["data"]
                    observations = []
                    for author in ("runner", "participant"):
                        entry = state[author]
                        if entry is None:
                            continue
                        ignored = None
                        if (
                            author == "participant"
                            and state["runner"] is not None
                            and entry["event_id"] != state["runner"]["event_id"]
                        ):
                            ignored = self._ignored_reason(state["runner"]["event"])
                        observations.append(
                            {
                                "author": author,
                                "event_id": entry["event_id"],
                                "event": entry["event"],
                                "observation": entry["observation"],
                                "ignored": ignored,
                            }
                        )
                    result = {
                        "request_id": request_id,
                        "event_id": effective["event_id"],
                        "author": state["effective_author"],
                        "outcome": data["outcome"],
                        "response": data["response"],
                        "event": effective["event"],
                        "observations": observations,
                        "provisional": state["runner"] is None,
                    }
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
                if entry["event"]["event_type"] == "command.result":
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

    def _content_digest(self, connection: sqlite3.Connection) -> str:
        """Hash logical rows, independent of WAL layout and checkpoint timing."""
        digest = hashlib.sha256()
        for table, order in (
            ("journal_info", "singleton"),
            ("events", "cursor"),
            ("command_results", "request_id"),
            ("journal_changes", "change_cursor"),
            ("journal_restorations", "restoration_id"),
            ("sqlite_sequence", "name"),
        ):
            digest.update(table.encode("ascii") + b"\n")
            for row in connection.execute(f"SELECT * FROM {table} ORDER BY {order}"):
                digest.update(
                    json.dumps(row, ensure_ascii=True, separators=(",", ":")).encode(
                        "ascii"
                    )
                )
                digest.update(b"\n")
        return digest.hexdigest()

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
                info = self._read_journal_info(reader)
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
                    digest = self._content_digest(reader)
                    change_cursor = self._read_boundary(reader)["change_cursor"]
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
                if self._read_journal_info(backup) != info:
                    raise LoggingStorageError(
                        "Snapshot journal identity does not match."
                    )
                if self._content_digest(backup) != digest:
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
                temporary_manifest = target / "manifest.json.part"
                with temporary_manifest.open("x", encoding="utf-8") as manifest_file:
                    json.dump(manifest, manifest_file, ensure_ascii=True, indent=2)
                    manifest_file.write("\n")
                    manifest_file.flush()
                    os.fsync(manifest_file.fileno())
                temporary_manifest.rename(target / "manifest.json")
                if os.name != "nt":
                    descriptor = os.open(target, os.O_RDONLY)
                    try:
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
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
                boundary = self._read_boundary(reader)
                if (
                    boundary["generation"] != self._generation
                    or boundary["journal_id"] != self._journal_id
                ):
                    raise LoggingStorageError("Diagnostic source changed.")
                self._validate_snapshot_source(reader)
                metadata = {}
                starts = {}
                for row in reader.execute("SELECT * FROM events ORDER BY cursor"):
                    event = self._decode_row(row)["event"]
                    operation_id = event["operation_id"]
                    parent = event["context"].get("parent_operation_id")
                    metadata[event["event_id"]] = (operation_id, parent)
                    if event["event_type"] == "operation.started":
                        starts[operation_id] = event["event_id"]
                known = {op for op, _ in metadata.values() if op is not None}
                if not set(roots) <= known:
                    raise LoggingStateError(
                        "Some selected operations are absent from this journal."
                    )
                selected_operations = set(roots)
                while True:
                    children = {
                        op
                        for op, parent in metadata.values()
                        if op is not None and parent in selected_operations
                    }
                    if children <= selected_operations:
                        break
                    selected_operations.update(children)
                selected = {
                    event_id
                    for event_id, (op, parent) in metadata.items()
                    if op in selected_operations or parent in selected_operations
                }
                # A matching reply can exist only in observer metadata. Its raw
                # event may belong to the other participant, outside this tree.
                for (request_id,) in reader.execute(
                    "SELECT request_id FROM command_results"
                ):
                    state = self._load_command_result(request_id, connection=reader)
                    for author in ("runner", "participant"):
                        entry = state[author]
                        if entry is None:
                            continue
                        observation = entry["observation"]
                        if (
                            observation["operation_id"] in selected_operations
                            or observation["context"].get("parent_operation_id")
                            in selected_operations
                        ):
                            selected.add(entry["event_id"])
                pending = list(selected)
                requests = {}
                while pending:
                    event_id = pending.pop()
                    event = self._event_entry(event_id, reader)["event"]
                    dependencies = []
                    if event["operation_id"] in starts:
                        dependencies.append(starts[event["operation_id"]])
                    parent = event["context"].get("parent_operation_id")
                    if parent in starts:
                        dependencies.append(starts[parent])
                    if event["event_type"] in (
                        "control.intent",
                        "control.observed",
                        "control.reconciled",
                    ):
                        for field in ("intent_event_id", "parameters_event_id"):
                            reference = event["data"].get(field)
                            if reference is not None:
                                dependencies.append(require_text(reference, field))
                    if event["event_type"] == "command.result":
                        request_id = event["data"]["request_id"]
                        state = self._load_command_result(request_id, connection=reader)
                        requests[request_id] = state
                        dependencies.extend(
                            state[author]["event_id"]
                            for author in ("runner", "participant")
                            if state[author] is not None
                        )
                    for reference in dependencies:
                        if reference not in metadata:
                            raise LoggingStorageError(
                                "Diagnostic dependency is missing."
                            )
                        if reference not in selected:
                            selected.add(reference)
                            pending.append(reference)
                source_phase = False
                target.mkdir()
                digest = hashlib.sha256()
                with (target / "records.jsonl.part").open("xb") as output:
                    for event_id in metadata:
                        if event_id not in selected:
                            continue
                        record = {
                            "kind": "event",
                            "event": self._event_entry(event_id, reader)["event"],
                        }
                        line = (json.dumps(record, ensure_ascii=True) + "\n").encode(
                            "ascii"
                        )
                        output.write(line)
                        digest.update(line)
                    for request_id, state in requests.items():
                        record = {
                            "kind": "command",
                            "request_id": request_id,
                            "identity": state["identity"],
                            "runner": None,
                            "participant": None,
                        }
                        for author in ("runner", "participant"):
                            if state[author] is not None:
                                record[author] = {
                                    field: state[author][field]
                                    for field in ("event_id", "observation")
                                }
                        line = (json.dumps(record, ensure_ascii=True) + "\n").encode(
                            "ascii"
                        )
                        output.write(line)
                        digest.update(line)
                    output.flush()
                    os.fsync(output.fileno())
                reader.execute("ROLLBACK")
                source_phase = True
                self._check_health()
                manifest = {
                    "schema_version": SCHEMA_VERSION,
                    "kind": "journal.diagnostics",
                    "diagnostics_id": uuid4().hex,
                    "journal_id": boundary["journal_id"],
                    "generation": boundary["generation"],
                    "operation_ids": roots,
                    "cursor": boundary["cursor"],
                    "change_cursor": boundary["change_cursor"],
                    "created_at": datetime.now(UTC).isoformat(timespec="microseconds"),
                    "records": "records.jsonl",
                    "records_sha256": digest.hexdigest(),
                    "event_count": len(selected),
                    "command_count": len(requests),
                }
                source_phase = False
                (target / "records.jsonl.part").rename(target / "records.jsonl")
                self._publish_manifest(target, manifest)
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
                    self._close_preserving_failure(reader)

    def _close_preserving_failure(
        self, connection: sqlite3.Connection | sqlite3.Cursor
    ) -> None:
        primary = sys.exc_info()[1]
        try:
            connection.close()
        except BaseException as error:
            if primary is None:
                raise
            try:
                primary.add_note(
                    f"Journal connection cleanup failed: {type(error).__name__}."
                )
            except BaseException:  # noqa: BLE001, S110
                pass

    def _publish_manifest(self, target: Path, manifest: JsonObject) -> None:
        temporary = target / "manifest.json.part"
        with temporary.open("x", encoding="utf-8") as output:
            json.dump(manifest, output, ensure_ascii=True, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        temporary.rename(target / "manifest.json")
        if os.name != "nt":
            descriptor = os.open(target, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _read_diagnostics(self, directory: str | Path) -> tuple[JsonObject, list, list]:
        path = Path(directory)
        if not path.is_absolute():
            raise ValueError("Diagnostics must use an absolute directory path.")
        with (path / "manifest.json").open(encoding="utf-8") as source:
            manifest = copy_json_object(json.load(source), "diagnostics manifest")
        if manifest.keys() != {
            "schema_version",
            "kind",
            "diagnostics_id",
            "journal_id",
            "generation",
            "operation_ids",
            "cursor",
            "change_cursor",
            "created_at",
            "records",
            "records_sha256",
            "event_count",
            "command_count",
        }:
            raise ValueError("Invalid diagnostics manifest fields.")
        if (
            type(manifest["schema_version"]) is not int
            or manifest["schema_version"] != SCHEMA_VERSION
            or manifest["kind"] != "journal.diagnostics"
            or manifest["records"] != "records.jsonl"
        ):
            raise ValueError("Unsupported diagnostics format.")
        validate_journal_identity(
            {field: manifest[field] for field in ("journal_id", "generation")}
        )
        UUID(require_text(manifest["diagnostics_id"], "diagnostics_id"))
        for field in ("cursor", "change_cursor", "event_count", "command_count"):
            if type(manifest[field]) is not int or manifest[field] < 0:
                raise ValueError(f"Invalid diagnostics {field}.")
        if type(manifest["operation_ids"]) is not list or not manifest["operation_ids"]:
            raise ValueError("Diagnostics must identify selected operations.")
        for operation_id in manifest["operation_ids"]:
            require_text(operation_id, "operation_id")
        timestamp = datetime.fromisoformat(manifest["created_at"])
        if timestamp.utcoffset() != UTC.utcoffset(None):
            raise ValueError("Diagnostics creation time must be UTC.")
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
                    request_id = require_text(record["request_id"], "request_id")
                    if request_id in request_ids:
                        raise ValueError("Duplicate diagnostic request ID.")
                    request_ids.add(request_id)
                    identity = validate_context(record["identity"])
                    if identity.keys() != {
                        "experiment_id",
                        "participant_id",
                        "participant_instance_id",
                    }:
                        raise ValueError("Invalid diagnostic request identity.")
                    require_text(identity["experiment_id"], "experiment_id")
                    require_text(identity["participant_id"], "participant_id")
                    if record["runner"] is None and record["participant"] is None:
                        raise ValueError("Diagnostic result requires an observation.")
                    for author in ("runner", "participant"):
                        entry = record[author]
                        if entry is None:
                            continue
                        if type(entry) is not dict or entry.keys() != {
                            "event_id",
                            "observation",
                        }:
                            raise ValueError("Invalid diagnostic observation fields.")
                        require_text(entry["event_id"], "event_id")
                        observation = copy_json_object(
                            entry["observation"], "observation"
                        )
                        if observation.keys() != {
                            "producer_instance_id",
                            "occurred_at",
                            "context",
                            "operation_id",
                        }:
                            raise ValueError("Invalid diagnostic observer.")
                        if observation["operation_id"] is not None:
                            require_text(observation["operation_id"], "operation_id")
                        require_text(
                            observation["producer_instance_id"], "producer_instance_id"
                        )
                        timestamp = datetime.fromisoformat(observation["occurred_at"])
                        if timestamp.utcoffset() != UTC.utcoffset(None):
                            raise ValueError("Observation timestamp must be UTC.")
                        validate_context(observation["context"])
                    commands.append(record)
                else:
                    raise ValueError("Unknown diagnostic record format.")
        if digest.hexdigest() != manifest["records_sha256"]:
            raise ValueError("Diagnostic records checksum does not match.")
        if (len(events), len(commands)) != (
            manifest["event_count"],
            manifest["command_count"],
        ):
            raise ValueError("Diagnostic record counts do not match.")
        for record in commands:
            for author in ("runner", "participant"):
                if (
                    record[author] is not None
                    and record[author]["event_id"] not in event_ids
                ):
                    raise ValueError("Diagnostic result refers outside its bundle.")
        return manifest, events, commands

    def _save_command_state(
        self, connection: sqlite3.Connection, request_id: str, state: dict
    ) -> None:
        runner, participant = state["runner"], state["participant"]
        author = "runner" if runner is not None else "participant"
        connection.execute(
            "INSERT INTO command_results VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(request_id) DO UPDATE SET "
            "identity_json=excluded.identity_json, runner_event_id=excluded.runner_event_id, "
            "participant_event_id=excluded.participant_event_id, "
            "runner_observation_json=excluded.runner_observation_json, "
            "participant_observation_json=excluded.participant_observation_json, "
            "effective_event_id=excluded.effective_event_id, effective_author=excluded.effective_author",
            (
                request_id,
                json.dumps(state["identity"], sort_keys=True),
                runner["event_id"] if runner else None,
                participant["event_id"] if participant else None,
                json.dumps(runner["observation"]) if runner else None,
                json.dumps(participant["observation"]) if participant else None,
                state[author]["event_id"],
                author,
            ),
        )

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
        manifest = copy_json_object(snapshot_manifest, "snapshot manifest")
        if manifest.keys() != {
            "schema_version",
            "snapshot_id",
            "journal_id",
            "generation",
            "storage_schema_version",
            "cursor",
            "event_count",
            "change_cursor",
            "content_sha256",
            "created_at",
            "database",
        }:
            raise ValueError("Snapshot manifest fields do not match the format.")
        if (
            type(manifest["schema_version"]) is not int
            or manifest["schema_version"] != SCHEMA_VERSION
            or type(manifest["storage_schema_version"]) is not int
            or manifest["storage_schema_version"] != SCHEMA_VERSION
            or manifest["database"] != "journal.sqlite"
        ):
            raise ValueError("Unsupported snapshot manifest.")
        identity = validate_journal_identity(
            {name: manifest[name] for name in ("journal_id", "generation")}
        )
        UUID(require_text(manifest["snapshot_id"], "snapshot_id"))
        restoration_id = UUID(require_text(restoration_id, "restoration_id")).hex
        new_generation = UUID(require_text(new_generation, "new_generation")).hex
        if new_generation == identity["generation"]:
            raise ValueError("Restoration requires a fresh generation.")
        for field in ("cursor", "event_count", "change_cursor"):
            if (
                type(manifest[field]) is not int
                or not 0 <= manifest[field] <= 9223372036854775807
            ):
                raise ValueError(f"Invalid snapshot {field}.")
        timestamp = datetime.fromisoformat(manifest["created_at"])
        if timestamp.utcoffset() != UTC.utcoffset(None):
            raise ValueError("Snapshot creation time must be UTC.")
        digest_text = require_text(manifest["content_sha256"], "content_sha256")
        if len(digest_text) != 64 or any(
            char not in "0123456789abcdef" for char in digest_text
        ):
            raise ValueError("Invalid snapshot content checksum.")
        with self._lock:
            if self._connection is not None:
                raise LoggingStateError(
                    "Close the journal before completing restoration."
                )
            bundle, events, commands = None, [], []
            if diagnostics is not None:
                bundle, events, commands = self._read_diagnostics(diagnostics)
                if bundle["journal_id"] != identity["journal_id"]:
                    raise ValueError("Diagnostic bundle belongs to another journal.")
            parameters = json.dumps(
                {
                    "snapshot": manifest,
                    "new_generation": new_generation,
                    "diagnostics": bundle,
                },
                sort_keys=True,
                ensure_ascii=True,
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
                info = self._read_journal_info(connection)
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
                    connection.execute("ROLLBACK")
                    self._journal_id, self._generation = (
                        result["journal_id"],
                        result["generation"],
                    )
                    self._expected_journal = actual
                    self._file_identity = file_identity
                    self._failed = False
                    return result
                if actual != identity:
                    raise JournalGenerationChanged(identity, actual)
                if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise LoggingStorageError(
                        "Restored journal failed its integrity check."
                    )
                self._validate_snapshot_source(connection)
                boundary = self._read_boundary(connection)
                if any(
                    boundary[key] != manifest[key]
                    for key in ("cursor", "event_count", "change_cursor")
                ):
                    raise ValueError(
                        "Restored journal does not match the snapshot boundary."
                    )
                if self._content_digest(connection) != manifest["content_sha256"]:
                    raise ValueError(
                        "Restored journal does not match the snapshot contents."
                    )
                inserted = []
                for event in events:
                    encoded = encode_event(event, self._max_event_bytes)
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
                changed_requests = {}
                for record in commands:
                    request_id = record["request_id"]
                    state = self._load_command_result(request_id, connection=connection)
                    before = (
                        None
                        if state is None
                        else (state["effective_author"], state["effective_event_id"])
                    )
                    if state is None:
                        state = {
                            "identity": record["identity"],
                            "runner": None,
                            "participant": None,
                        }
                    if state["identity"] != record["identity"]:
                        raise ValueError(
                            "Diagnostic request belongs to another context."
                        )
                    changed = False
                    for author in ("runner", "participant"):
                        incoming, current = record[author], state[author]
                        if incoming is None:
                            continue
                        if current is not None:
                            old = {
                                key: current[key] for key in ("event_id", "observation")
                            }
                            if json.dumps(old, sort_keys=True) != json.dumps(
                                incoming, sort_keys=True
                            ):
                                raise ValueError(
                                    "Diagnostic observation conflicts with restored history."
                                )
                        else:
                            state[author] = incoming
                            changed = True
                    self._save_command_state(connection, request_id, state)
                    checked = self._load_command_result(
                        request_id, connection=connection
                    )
                    if changed:
                        changed_requests[request_id] = (
                            checked["effective_event_id"],
                            before
                            != (
                                checked["effective_author"],
                                checked["effective_event_id"],
                            ),
                        )
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
                connection.execute(
                    "UPDATE journal_info SET generation=? WHERE singleton=1",
                    (new_generation,),
                )
                result = {
                    **self._read_boundary(connection),
                    "restoration_id": restoration_id,
                    "snapshot_id": manifest["snapshot_id"],
                    "imported_events": len(inserted),
                }
                connection.execute(
                    "INSERT INTO journal_restorations VALUES (?, ?, ?)",
                    (restoration_id, parameters, json.dumps(result, sort_keys=True)),
                )
                self._validate_snapshot_source(connection)
                # Content was checked through this transaction. A second SQLite
                # reader here could wait on our own write lock after cache spill.
                status = self.db_path.stat()
                if (status.st_dev, status.st_ino) != file_identity:
                    raise LoggingStorageError("Restored file changed before commit.")
                connection.execute("COMMIT")
                self._journal_id, self._generation = (
                    result["journal_id"],
                    result["generation"],
                )
                self._expected_journal = {
                    name: result[name] for name in ("journal_id", "generation")
                }
                self._file_identity = file_identity
                self._failed = False
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
                    self._close_preserving_failure(connection)

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
