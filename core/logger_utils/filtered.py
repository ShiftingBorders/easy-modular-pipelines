"""Explicitly scheduled, rebuildable projection of one local operation journal."""

import json
import os
import sqlite3
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from core.logger import OperationLogger
from core.logger_utils.events import (
    JournalGenerationChanged,
    JsonObject,
    LoggingStateError,
    LoggingStorageError,
    copy_json_object,
    encode_event,
    load_logging_settings,
    validate_checkpoint,
)

_APPLICATION_ID = 0x454D5046
_CREATE_INFO = """CREATE TABLE filtered_info (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    metadata_json TEXT NOT NULL
)"""
_CREATE_EVENTS = """CREATE TABLE filtered_events (
    cursor INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    event_json TEXT NOT NULL,
    effective_author TEXT,
    provisional INTEGER NOT NULL CHECK (provisional IN (0, 1))
)"""
_PAGE_BYTES = 16777216


class FilteredJournal:
    """One caller-owned publisher; no implicit threads, timers or filesystem I/O."""

    def __init__(self, config_path: str | Path, view_path: str | Path) -> None:
        self._config_path = Path(config_path)
        self.view_path = Path(view_path)
        for path in (self._config_path, self.view_path):
            if not path.is_absolute() or "\x00" in str(path):
                raise ValueError("Filtered journal paths must be absolute.")
        self._process_id = os.getpid()
        self._lock = threading.RLock()
        self._logger: OperationLogger | None = None
        self._connection: sqlite3.Connection | None = None
        self._file_identity: tuple[int, int] | None = None
        self._interval: float | None = None
        self._timeout: float | None = None
        self._last_error: JsonObject | None = None

    def _check_process(self) -> None:
        if os.getpid() != self._process_id:
            raise LoggingStateError("Create a filtered journal in this process.")

    def _require_open(self) -> None:
        self._check_process()
        if self._logger is None:
            raise LoggingStateError("Filtered journal is closed.")

    def open(self) -> None:
        self._check_process()
        with self._lock:
            if self._logger is not None:
                raise LoggingStateError("Filtered journal is already open.")
            settings, _ = load_logging_settings(self._config_path)
            if settings["open_mode"] != "existing":
                raise ValueError(
                    "FilteredJournal connects to an existing primary journal."
                )
            source_path = Path(settings["db_path"])
            if self.view_path.resolve() in (
                source_path.resolve(),
                self._config_path.resolve(),
            ):
                raise ValueError("The derived database must have its own path.")
            if self.view_path.exists() and (
                os.path.samefile(self.view_path, source_path)
                or os.path.samefile(self.view_path, self._config_path)
            ):
                raise ValueError(
                    "The derived database cannot alias primary data or settings."
                )
            logger = OperationLogger(self._config_path)
            logger.open()
            self._logger = logger
            self._interval = float(settings["filtered_refresh_interval_seconds"])
            self._timeout = settings["busy_timeout_seconds"]
            try:
                try:
                    self._open_view()
                except (OSError, sqlite3.Error, LoggingStorageError) as error:
                    self._report_refresh_failure(error)
            except BaseException:
                primary = sys.exc_info()[1]
                try:
                    self.close()
                except BaseException as cleanup_error:  # noqa: BLE001 - Preserve open failure.
                    try:
                        primary.add_note(
                            f"Filtered open cleanup failed: {type(cleanup_error).__name__}."
                        )
                    except BaseException:  # noqa: BLE001, S110
                        pass
                raise

    def _open_view(self) -> None:
        if self._connection is not None:
            return
        self.view_path.parent.mkdir(parents=True, exist_ok=True)
        created = False
        try:
            with self.view_path.open("xb"):
                pass
            created = True
        except FileExistsError:
            pass
        connection = sqlite3.connect(
            self.view_path.as_uri() + "?mode=rw",
            uri=True,
            timeout=self._timeout,
            isolation_level=None,
            check_same_thread=False,
        )
        try:
            if created:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(_CREATE_INFO)
                connection.execute(_CREATE_EVENTS)
                connection.execute(f"PRAGMA application_id={_APPLICATION_ID}")
                connection.execute("PRAGMA user_version=1")
                connection.execute("COMMIT")
            self._validate_view_schema(connection)
            if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise LoggingStorageError("Derived database integrity check failed.")
            if (
                connection.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower()
                != "wal"
            ):
                raise LoggingStorageError("Derived database requires WAL mode.")
            connection.execute("PRAGMA synchronous=FULL")
            status = self.view_path.stat()
            self._file_identity = (status.st_dev, status.st_ino)
            self._connection = connection
        except BaseException:
            self._logger._store._close_preserving_failure(connection)
            raise

    def _validate_view_schema(self, connection: sqlite3.Connection) -> None:
        actual = connection.execute(
            "SELECT type, name, sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'"
        ).fetchall()
        expected = {"filtered_info": _CREATE_INFO, "filtered_events": _CREATE_EVENTS}
        if (
            len(actual) != len(expected)
            or connection.execute("PRAGMA application_id").fetchone()[0]
            != _APPLICATION_ID
            or connection.execute("PRAGMA user_version").fetchone()[0] != 1
        ):
            raise LoggingStorageError(
                "Unrecognized derived database; no file was replaced."
            )
        for kind, name, sql in actual:
            if (
                kind != "table"
                or name not in expected
                or not isinstance(sql, str)
                or " ".join(sql.split()) != " ".join(expected[name].split())
            ):
                raise LoggingStorageError("Unrecognized derived database schema.")

    def _check_view(self) -> None:
        if self._connection is None:
            raise LoggingStorageError("No derived database is available.")
        status = self.view_path.stat()
        if (status.st_dev, status.st_ino) != self._file_identity:
            raise LoggingStorageError("The derived database file was replaced.")
        self._validate_view_schema(self._connection)

    def _metadata(self) -> JsonObject | None:
        row = self._connection.execute(
            "SELECT metadata_json FROM filtered_info WHERE singleton=1"
        ).fetchone()
        if row is None:
            return None
        data = copy_json_object(json.loads(row[0]), "filtered metadata")
        if data.keys() != {
            "schema_version",
            "journal_id",
            "generation",
            "cursor",
            "event_count",
            "change_cursor",
            "publication_id",
            "published_at",
        }:
            raise ValueError("Invalid derived publication metadata.")
        for key in ("journal_id", "generation", "publication_id"):
            UUID(data[key])
        for key in ("cursor", "event_count", "change_cursor"):
            if type(data[key]) is not int or data[key] < 0:
                raise ValueError("Invalid derived publication boundary.")
        if type(data["schema_version"]) is not int or data["schema_version"] != 1:
            raise ValueError("Invalid derived publication format.")
        if datetime.fromisoformat(data["published_at"]).utcoffset() != UTC.utcoffset(
            None
        ):
            raise ValueError("Derived publication timestamp must be UTC.")
        return data

    def _write_entry(self, entry: JsonObject) -> None:
        event = entry["event"]
        self._connection.execute(
            "INSERT INTO filtered_events VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(event_id) DO UPDATE SET cursor=excluded.cursor, "
            "event_json=excluded.event_json, effective_author=excluded.effective_author, "
            "provisional=excluded.provisional",
            (
                entry["cursor"],
                event["event_id"],
                encode_event(event, None),
                entry["effective_author"],
                int(entry["provisional"]),
            ),
        )

    def _report_refresh_failure(self, error: Exception) -> None:
        try:
            message = str(error)
        except BaseException:  # noqa: BLE001 - Exception formatting cannot hide the failure.
            message = "[exception message unavailable]"
        record = {
            "error_type": f"{type(error).__module__}.{type(error).__qualname__}",
            "message": message,
            "view_path": str(self.view_path),
        }
        if record != self._last_error:
            # Failure here is a primary-journal failure and must reach the caller.
            self._logger.record_event("logger.filtered_refresh_failed", record)
        self._last_error = record

    def refresh(self) -> JsonObject | None:
        """Publish one consistent source view; derived failures preserve primary reads."""
        self._check_process()
        with self._lock:
            self._require_open()
            self._logger._require_open()
            store = self._logger._store
            phase = "view"
            source = None
            rows = None
            try:
                self._open_view()
                self._check_view()
                try:
                    previous = self._metadata()
                except (ValueError, TypeError, KeyError) as error:
                    self._report_refresh_failure(error)
                    previous = None
                phase = "source"
                store._check_health()
                source = sqlite3.connect(
                    store.db_path.as_uri() + "?mode=ro",
                    uri=True,
                    timeout=self._timeout,
                    isolation_level=None,
                )
                source.execute("BEGIN")
                boundary = store._read_boundary(source)
                expected = {
                    "journal_id": store._journal_id,
                    "generation": store._generation,
                }
                actual = {key: boundary[key] for key in expected}
                if expected != actual:
                    raise JournalGenerationChanged(expected, actual)
                rebuild = (
                    self._last_error is not None
                    or previous is None
                    or any(
                        previous[key] != boundary[key]
                        for key in ("journal_id", "generation")
                    )
                    or previous["change_cursor"] > boundary["change_cursor"]
                )
                phase = "view"
                self._connection.execute("BEGIN IMMEDIATE")
                if rebuild:
                    self._connection.execute("DELETE FROM filtered_events")
                    phase = "source"
                    rows = source.execute("SELECT * FROM events ORDER BY cursor")
                    while True:
                        phase = "source"
                        row = rows.fetchone()
                        if row is None:
                            break
                        entry = store._effective_entry(store._decode_row(row), source)
                        if entry is not None:
                            phase = "view"
                            self._write_entry(entry)
                    rows.close()
                    rows = None
                else:
                    phase = "source"
                    rows = source.execute(
                        "SELECT change_cursor, event_id, change_json FROM journal_changes "
                        "WHERE change_cursor > ? ORDER BY change_cursor",
                        (previous["change_cursor"],),
                    )
                    while True:
                        phase = "source"
                        row = rows.fetchone()
                        if row is None:
                            break
                        change = store._decode_change(row, source)
                        phase = "view"
                        for event_id in change["related_event_ids"]:
                            if event_id != change["effective_event_id"]:
                                self._connection.execute(
                                    "DELETE FROM filtered_events WHERE event_id=?",
                                    (event_id,),
                                )
                        self._write_entry(change["entry"])
                    rows.close()
                    rows = None
                phase = "source"
                source.execute("ROLLBACK")
                source.close()
                source = None
                store._check_health()
                changed = (
                    rebuild or previous["change_cursor"] != boundary["change_cursor"]
                )
                publication = {
                    **boundary,
                    "publication_id": uuid4().hex
                    if changed
                    else previous["publication_id"],
                    "published_at": datetime.now(UTC).isoformat(
                        timespec="microseconds"
                    ),
                }
                phase = "view"
                self._connection.execute(
                    "INSERT INTO filtered_info VALUES (1, ?) ON CONFLICT(singleton) "
                    "DO UPDATE SET metadata_json=excluded.metadata_json",
                    (json.dumps(publication),),
                )
                self._check_view()
                self._connection.execute("COMMIT")
                self._check_view()
            except BaseException as error:
                if self._connection is not None and self._connection.in_transaction:
                    try:
                        self._connection.execute("ROLLBACK")
                    except sqlite3.Error:
                        self._connection.close()
                        self._connection = None
                if getattr(error, "journal_failed", False):
                    raise
                if phase == "source":
                    raise store._storage_failure(error, "read filtered source")
                if not isinstance(error, Exception):
                    raise
                self._report_refresh_failure(error)
                return None
            finally:
                try:
                    if rows is not None:
                        store._close_preserving_failure(rows)
                finally:
                    if source is not None:
                        store._close_preserving_failure(source)
            if self._last_error is not None:
                self._logger.record_event(
                    "logger.filtered_refresh_recovered",
                    {"view_path": str(self.view_path)},
                )
                self._last_error = None
            return publication

    def _fallback(self, checkpoint: JsonObject | None, limit: int) -> JsonObject:
        base = (
            None
            if checkpoint is None
            else {
                key: value
                for key, value in checkpoint.items()
                if key != "publication_id"
            }
        )
        page = self._logger.read_events(base, limit=limit, view="effective")
        boundary = page["boundary"]
        publication = f"source:{boundary['generation']}:{boundary['change_cursor']}"
        if checkpoint is not None and checkpoint["publication_id"] != publication:
            raise LoggingStateError(
                "Publication changed; restart reading from the first page."
            )
        page["checkpoint"]["publication_id"] = publication
        return {
            **page,
            "source": "journal",
            "published_at": None,
            "publication_id": publication,
            "refresh_error": None
            if self._last_error is None
            else dict(self._last_error),
        }

    def read_events(
        self, checkpoint: JsonObject | None = None, *, limit: int = 100
    ) -> JsonObject:
        self._check_process()
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000.")
        if checkpoint is not None:
            checkpoint = copy_json_object(checkpoint, "publication checkpoint")
            if checkpoint.keys() != {
                "journal_id",
                "generation",
                "cursor",
                "publication_id",
            }:
                raise ValueError("Publication checkpoint fields do not match.")
            if (
                type(checkpoint["publication_id"]) is not str
                or not checkpoint["publication_id"]
            ):
                raise ValueError("publication_id must be a nonempty string.")
            base = validate_checkpoint(
                {
                    key: value
                    for key, value in checkpoint.items()
                    if key != "publication_id"
                },
                "cursor",
            )
            checkpoint.update(base)
        with self._lock:
            self._require_open()
            current = self._logger.get_journal_info()
            if checkpoint is not None:
                expected = {
                    key: checkpoint[key] for key in ("journal_id", "generation")
                }
                actual = {key: current[key] for key in expected}
                if expected != actual:
                    raise JournalGenerationChanged(expected, actual)
            if self._connection is None:
                return self._fallback(checkpoint, limit)
            try:
                self._check_view()
                self._connection.execute("BEGIN")
                metadata = self._metadata()
                if (
                    metadata is None
                    or self._last_error is not None
                    or any(
                        metadata[key] != current[key]
                        for key in ("journal_id", "generation")
                    )
                ):
                    self._connection.execute("ROLLBACK")
                    raise LookupError("No current publication is available.")
                if (
                    checkpoint is not None
                    and checkpoint["publication_id"] != metadata["publication_id"]
                ):
                    raise LoggingStateError(
                        "Publication changed; restart reading from the first page."
                    )
                after = 0 if checkpoint is None else checkpoint["cursor"]
                if after > metadata["cursor"]:
                    raise LoggingStateError("Checkpoint is beyond this publication.")
                entries, size = [], 0
                more = False
                for (
                    cursor,
                    event_id,
                    encoded,
                    author,
                    provisional,
                ) in self._connection.execute(
                    "SELECT * FROM filtered_events WHERE cursor > ? ORDER BY cursor LIMIT ?",
                    (after, limit + 1),
                ):
                    item_size = len(encoded.encode("utf-8"))
                    if len(entries) == limit or (
                        entries and size + item_size > _PAGE_BYTES
                    ):
                        more = True
                        break
                    event = json.loads(encode_event(json.loads(encoded), None))
                    if event["event_id"] != event_id or provisional not in (0, 1):
                        raise ValueError("Invalid derived event identity.")
                    if author not in (None, "runner", "service") or bool(
                        provisional
                    ) != (author == "service"):
                        raise ValueError("Invalid derived result state.")
                    entries.append(
                        {
                            "cursor": cursor,
                            "event": event,
                            "effective_author": author,
                            "provisional": bool(provisional),
                        }
                    )
                    size += item_size
                    after = cursor
                if not more:
                    after = metadata["cursor"]
                self._connection.execute("COMMIT")
                self._check_view()
            except LookupError:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                return self._fallback(checkpoint, limit)
            except LoggingStateError:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise
            except (
                OSError,
                sqlite3.Error,
                ValueError,
                TypeError,
                LoggingStorageError,
            ) as error:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                self._report_refresh_failure(error)
                return self._fallback(checkpoint, limit)
            self._logger.get_journal_info()
            return {
                "events": entries,
                "checkpoint": {
                    key: metadata[key]
                    for key in ("journal_id", "generation", "publication_id")
                }
                | {"cursor": after},
                "boundary": metadata,
                "has_more": more,
                "source": "filtered",
                "published_at": metadata["published_at"],
                "publication_id": metadata["publication_id"],
                "refresh_error": None,
            }

    def run(self, stop_event: threading.Event) -> None:
        """Block in a caller-owned thread; primary failures propagate to that caller."""
        self._check_process()
        if not isinstance(stop_event, threading.Event):
            raise TypeError("stop_event must be a threading.Event.")
        if self._logger is not None:
            raise LoggingStateError(
                "run owns open/close; use refresh for an already open publisher."
            )
        try:
            self.open()
            while not stop_event.is_set():
                deadline = time.monotonic() + self._interval
                self.refresh()
                while not stop_event.is_set():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    stop_event.wait(min(remaining, 60))
        finally:
            primary = sys.exc_info()[1]
            try:
                self.close()
            except BaseException as error:
                if primary is None:
                    raise
                try:
                    primary.add_note(
                        f"Filtered journal cleanup failed: {type(error).__name__}."
                    )
                except BaseException:  # noqa: BLE001, S110
                    pass

    def close(self) -> None:
        self._check_process()
        with self._lock:
            failure = None
            try:
                if self._connection is not None:
                    self._connection.close()
                    self._connection = None
            except BaseException as error:  # noqa: BLE001 - Still close the primary client.
                failure = error
            try:
                if self._logger is not None:
                    self._logger.close()
                    self._logger = None
            except BaseException as error:  # noqa: BLE001 - Preserve the first close failure.
                if failure is None:
                    failure = error
                else:
                    try:
                        failure.add_note(
                            f"Primary client close failed: {type(error).__name__}."
                        )
                    except BaseException:  # noqa: BLE001, S110
                        pass
            if failure is not None:
                raise failure
