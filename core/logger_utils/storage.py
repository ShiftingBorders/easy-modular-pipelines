"""Append-only local SQLite storage, independent of DAG and event payload kinds."""

import json
import os
import sqlite3
import threading
from pathlib import Path

from core.logger_utils.events import (
    SCHEMA_VERSION,
    JsonObject,
    LoggingConfigurationError,
    LoggingStateError,
    LoggingStorageError,
    encode_event,
    require_number,
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


class SQLiteEventStore:
    """Own a local connection; call open/close explicitly. Never share across processes."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        busy_timeout_seconds: float = 5,
        max_event_bytes: int = 1048576,
    ) -> None:
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
        if type(max_event_bytes) is not int or not 1024 <= max_event_bytes <= 16777216:
            raise ValueError(
                "max_event_bytes must be an integer from 1024 to 16777216."
            )
        self.db_path = path
        self._timeout = float(timeout)
        self._max_event_bytes = max_event_bytes
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.RLock()
        self._process_id = os.getpid()
        self._failed = False

    def _check_process(self) -> None:
        # Check before taking a lock that a forked child could inherit as locked.
        if os.getpid() != self._process_id:
            raise LoggingStateError("Create a new journal client in this process.")

    def _check_schema(self, connection: sqlite3.Connection) -> bool:
        """Return True for an empty unclaimed database; reject other schemas."""
        objects = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name"
        ).fetchall()
        application_id = connection.execute("PRAGMA application_id").fetchone()[0]
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if not objects and application_id == 0 and version == 0:
            return True
        if (
            application_id != _APPLICATION_ID
            or version != SCHEMA_VERSION
            or len(objects) != 1
            or objects[0][:2] != ("table", "events")
            or " ".join(objects[0][2].split()) != " ".join(_CREATE_EVENTS.split())
        ):
            raise LoggingConfigurationError(
                f"Incompatible journal schema at {self.db_path}; no migration was applied."
            )
        return False

    def open(self) -> None:
        self._check_process()
        with self._lock:
            if self._connection is not None:
                raise LoggingStateError("Journal is already open.")
            connection = None
            try:
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
                connection = sqlite3.connect(
                    self.db_path,
                    timeout=self._timeout,
                    isolation_level=None,
                    check_same_thread=False,
                )
                self._check_schema(connection)
                # EXTRA protects the initial schema commit in rollback-journal mode.
                connection.execute("PRAGMA synchronous=EXTRA")
                connection.execute("BEGIN IMMEDIATE")
                if self._check_schema(connection):
                    connection.execute(_CREATE_EVENTS)
                    connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
                    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                connection.execute("COMMIT")
                mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                if mode.lower() != "wal":
                    raise LoggingConfigurationError(
                        "The journal requires SQLite WAL mode."
                    )
                connection.execute("PRAGMA synchronous=FULL")
                if connection.execute("PRAGMA synchronous").fetchone()[0] != 2:
                    raise LoggingConfigurationError(
                        "SQLite FULL synchronization is required."
                    )
                self._connection = connection
                self._failed = False
            except BaseException as error:
                if connection is not None:
                    try:
                        connection.close()
                    except sqlite3.Error:
                        pass
                if isinstance(error, (OSError, sqlite3.Error)):
                    raise LoggingStorageError(
                        f"Cannot open journal at {self.db_path}."
                    ) from error
                raise

    def append(self, event: JsonObject) -> None:
        """Return only after commit; reject writes after an uncertain storage failure."""
        self._check_process()
        encoded = encode_event(event, self._max_event_bytes)
        # Read indexed values from the validated snapshot, not the caller's dictionary.
        snapshot = json.loads(encoded)
        with self._lock:
            if self._connection is None or self._failed:
                raise LoggingStateError(
                    "Journal is closed or failed; close and reopen it."
                )
            try:
                # In autocommit mode this single statement is one complete transaction.
                self._connection.execute(
                    "INSERT INTO events "
                    "(event_id, producer_instance_id, sequence_number, event_json) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        snapshot["event_id"],
                        snapshot["producer_instance_id"],
                        snapshot["sequence_number"],
                        encoded,
                    ),
                )
            except BaseException as error:
                self._failed = True
                if isinstance(error, sqlite3.Error):
                    raise LoggingStorageError(
                        "Event commit failed; its outcome may be uncertain. "
                        "Close and reopen the journal before writing again."
                    ) from error
                raise

    def read_events(
        self, *, after_cursor: int = 0, limit: int = 100
    ) -> list[JsonObject]:
        """Read committed envelopes in local cursor order without marking them consumed."""
        self._check_process()
        if (
            type(after_cursor) is not int
            or not 0 <= after_cursor <= 9223372036854775807
        ):
            raise ValueError("after_cursor must be a nonnegative SQLite integer.")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000.")
        with self._lock:
            if self._connection is None:
                raise LoggingStateError("Journal is closed.")
            try:
                rows = self._connection.execute(
                    "SELECT cursor, event_id, producer_instance_id, sequence_number, "
                    "event_json FROM events WHERE cursor > ? ORDER BY cursor LIMIT ?",
                    (after_cursor, limit),
                )
                result = []
                page_bytes = 0
                try:
                    for cursor, event_id, producer, sequence, encoded in rows:
                        event_bytes = len(encoded.encode("utf-8"))
                        if event_bytes > 16777216:
                            raise ValueError(
                                "Stored event exceeds the format size limit."
                            )
                        if result and page_bytes + event_bytes > 16777216:
                            break
                        event = json.loads(encoded)
                        # Older records may exceed the current client's configured limit.
                        encode_event(event, 16777216)
                        if (
                            event["event_id"],
                            event["producer_instance_id"],
                            event["sequence_number"],
                        ) != (event_id, producer, sequence):
                            raise ValueError(
                                "Stored event disagrees with its indexed identity."
                            )
                        result.append({"cursor": cursor, "event": event})
                        page_bytes += event_bytes
                finally:
                    rows.close()
                return result
            except (sqlite3.Error, TypeError, ValueError, RecursionError) as error:
                raise LoggingStorageError(
                    "Cannot read valid events from the journal."
                ) from error

    def close(self) -> None:
        self._check_process()
        with self._lock:
            if self._connection is None:
                return
            try:
                self._connection.close()
            except sqlite3.Error as error:
                self._failed = True
                raise LoggingStorageError(
                    "Cannot close the journal connection."
                ) from error
            self._connection = None
