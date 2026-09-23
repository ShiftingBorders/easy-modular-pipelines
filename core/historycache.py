"""Rebuildable journal projections. The original read-only journal is authoritative.

Only this separate database is writable. Source identities and existing source
indexes are used for hydration; no source schema or payload is changed.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import closing
from fractions import Fraction
from pathlib import Path
from typing import BinaryIO

from core.logger import OperationLogger
from core.logger_utils.events import LoggingStateError


class _ExactMean:
    """Match statistics.mean without retaining historical samples in memory."""

    def __init__(self) -> None:
        self.total = Fraction(0)
        self.count = 0

    def step(self, value: float | None) -> None:
        if value is not None:
            self.total += Fraction(value)
            self.count += 1

    def finalize(self) -> float | None:
        return float(self.total / self.count) if self.count else None


class HistoryCacheLimit(ValueError):
    """A bounded working set cannot be loaded without dropping information."""


class HistoryCacheBusy(LoggingStateError):
    """Another process currently owns this experiment's cache writer."""


class HistoryCacheChanged(LoggingStateError):
    """A reader must restart against the newly published cache version."""


def acquire_cache_writer(path: Path) -> BinaryIO:
    """Acquire a derived-cache writer lock; closing the stream releases it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.with_suffix(".lock").open("a+b")
    try:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
        stream.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return stream
    except OSError as error:
        stream.close()
        raise HistoryCacheBusy("Another process is caching this experiment.") from error


class JournalHistoryCache:
    def __init__(
        self,
        path: Path,
        config_path: Path,
        identity: dict,
        file_key: tuple,
        experiment_id: str,
        window_events: int,
        max_bytes: int,
        max_events: int = 100000,
    ) -> None:
        self.path = path
        self.config_path = config_path
        self.identity = identity
        self.file_key = file_key
        self.experiment_id = experiment_id
        self.window_events = window_events
        self.max_bytes = max_bytes
        self.max_events = max_events
        self.window: OrderedDict[str, dict] = OrderedDict()
        self._window_bytes = 0
        self._lock = threading.RLock()
        self._opened = False
        self._reading: sqlite3.Connection | None = None

    def open(self) -> None:
        """Create only the disposable cache, never the source journal."""
        with self._lock, OperationLogger(self.config_path, read_only=True) as source:
            if self.path.exists():
                status = self.path.stat()
                if (status.st_dev, status.st_ino) == self.file_key:
                    raise ValueError(
                        "The disposable cache must not alias the source journal."
                    )
            info = source.get_journal_info()
            if any(info[key] != self.identity[key] for key in self.identity):
                raise LoggingStateError("Journal changed before cache initialization.")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            expected = {
                "version": 5,
                "identity": self.identity,
                "file_key": list(self.file_key),
                "experiment_id": self.experiment_id,
            }
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute(
                    "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                row = db.execute(
                    "SELECT value FROM metadata WHERE key='source'"
                ).fetchone()
                if row and json.loads(row[0]) == expected:
                    self._opened = True
                    return
                if row and json.loads(row[0]) != expected:
                    for table in (
                        "facts",
                        "records",
                        "runs",
                        "scopes",
                        "dirty",
                        "metadata",
                    ):
                        db.execute(f"DROP TABLE IF EXISTS {table}")
                    db.execute(
                        "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                    )
                db.executescript("""
                    BEGIN IMMEDIATE;
                    CREATE TABLE IF NOT EXISTS facts (
                        cursor INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
                        run_id TEXT, revision TEXT, cycle INTEGER, attempt_id TEXT,
                        operation_id TEXT, kind TEXT NOT NULL, occurred_at TEXT NOT NULL,
                        scope TEXT NOT NULL, effective INTEGER NOT NULL,
                        metadata TEXT NOT NULL, compact TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS facts_scope ON facts(scope, cursor);
                    CREATE INDEX IF NOT EXISTS facts_run ON facts(run_id, cursor);
                    CREATE INDEX IF NOT EXISTS facts_kind ON facts(kind, run_id, cursor);
                    CREATE INDEX IF NOT EXISTS facts_attempt ON facts(attempt_id, cursor);
                    CREATE INDEX IF NOT EXISTS facts_operation ON facts(operation_id, kind, cursor);
                    CREATE INDEX IF NOT EXISTS facts_parent ON facts(json_extract(compact,'$.data.parent_operation_id'), operation_id) WHERE kind='operation.started';
                    CREATE INDEX IF NOT EXISTS facts_time ON facts(kind, occurred_at);
                    CREATE INDEX IF NOT EXISTS facts_effective ON facts(effective, cursor);
                    CREATE INDEX IF NOT EXISTS facts_run_effective ON facts(run_id, effective, cursor);
                    CREATE TABLE IF NOT EXISTS dirty (scope TEXT PRIMARY KEY);
                    CREATE TABLE IF NOT EXISTS runs (
                        run_id TEXT PRIMARY KEY, first_cursor INTEGER NOT NULL,
                        started_at TEXT NOT NULL, revision TEXT
                    );
                    CREATE INDEX IF NOT EXISTS runs_page ON runs(first_cursor, run_id);
                    CREATE TABLE IF NOT EXISTS scopes (
                        scope TEXT PRIMARY KEY, run_id TEXT, revision TEXT, cycle INTEGER,
                        summary TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS scopes_run ON scopes(run_id, revision, cycle);
                    CREATE TABLE IF NOT EXISTS records (
                        kind TEXT NOT NULL, record_key TEXT NOT NULL, scope TEXT NOT NULL,
                        run_id TEXT, revision TEXT, cycle INTEGER, position INTEGER NOT NULL,
                        payload TEXT NOT NULL, PRIMARY KEY(kind, record_key, scope)
                    );
                    CREATE INDEX IF NOT EXISTS records_page ON records(kind, position, record_key, scope);
                    CREATE INDEX IF NOT EXISTS records_run ON records(kind, run_id, position, record_key, scope);
                    CREATE INDEX IF NOT EXISTS records_scope ON records(scope);
                    CREATE INDEX IF NOT EXISTS records_module ON records(kind, json_extract(payload,'$.module_name'), json_extract(payload,'$.module_version'), json_extract(payload,'$.module_hash'));
                    CREATE INDEX IF NOT EXISTS records_duration ON records(kind, json_extract(payload,'$.module_name'), json_extract(payload,'$.module_version'), json_extract(payload,'$.module_hash'), json_extract(payload,'$.duration_seconds'));
                    COMMIT;
                """)
                db.execute(
                    "INSERT OR REPLACE INTO metadata VALUES ('source', ?)",
                    (json.dumps(expected),),
                )
                empty = json.dumps({**self.identity, "cursor": 0, "change_cursor": 0})
                for key in ("cached_through", "ingested_through"):
                    db.execute(
                        "INSERT OR IGNORE INTO metadata VALUES (?, ?)", (key, empty)
                    )
            self._opened = True

    def _remember(self, entry: dict) -> None:
        event = entry["event"]
        identifier = event["event_id"]
        size = len(json.dumps(entry, ensure_ascii=False).encode("utf-8"))
        old = self.window.pop(identifier, None)
        if old is not None:
            self._window_bytes -= old["size"]
        self.window[identifier] = {"entry": entry, "size": size}
        self.window = OrderedDict(
            sorted(self.window.items(), key=lambda pair: pair[1]["entry"]["cursor"])
        )
        self._window_bytes += size
        while len(self.window) > self.window_events:
            _, removed = self.window.popitem(last=False)
            self._window_bytes -= removed["size"]
        if self._window_bytes > self.max_bytes:
            self.window.clear()
            self._window_bytes = 0
            raise HistoryCacheLimit(
                "The configured active event window exceeds history_max_bytes; increase the byte budget or reduce history_window_events."
            )

    def refresh(
        self,
        state: dict,
        compact: Callable,
        project: Callable,
        *,
        target: dict | None = None,
        window: bool = True,
        reader_context: dict | None = None,
    ) -> dict:
        """Advance ingestion, projections and the active window explicitly."""
        with self._lock:
            writer = acquire_cache_writer(self.path)
            try:
                return self._refresh_owned(
                    state, compact, project, target, window, reader_context
                )
            finally:
                writer.close()

    def _refresh_owned(
        self,
        state: dict,
        compact: Callable,
        project: Callable,
        target: dict | None,
        window: bool,
        reader_context: dict | None = None,
    ) -> dict:
        if not self._opened:
            self.open()
        with (
            OperationLogger(self.config_path, read_only=True) as source,
            closing(sqlite3.connect(self.path)) as db,
        ):
            target = target or source.read_event_batch([])["boundary"]
            if any(target[key] != self.identity[key] for key in self.identity):
                raise LoggingStateError(
                    "The precache target belongs to replaced history."
                )
            deadline = time.monotonic() + 1
            page, changed = self._read_changes(db, source, compact, deadline, target)
            projected = self._project_pending(db, state, project, deadline)
            self._publish_template(db, source)
            if reader_context is not None:
                encoded = json.dumps(
                    {**reader_context, "state": state},
                    ensure_ascii=False,
                    allow_nan=False,
                )
                revision = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
                with db:
                    previous = db.execute(
                        "SELECT value FROM metadata WHERE key='reader_revision'"
                    ).fetchone()
                    if previous is None or previous[0] != revision:
                        db.execute(
                            "INSERT OR REPLACE INTO metadata VALUES ('reader_context', ?)",
                            (encoded,),
                        )
                        db.execute(
                            "INSERT OR REPLACE INTO metadata VALUES ('reader_revision', ?)",
                            (revision,),
                        )
            if window:
                self._refresh_window(source, changed)
            result = self._publication(db, page, bool(changed) or projected)
            return {**result, "target_boundary": target}

    def _publish_template(
        self, db: sqlite3.Connection, source: OperationLogger
    ) -> None:
        """Keep the current settings document ready; older revisions stay lazy."""
        latest = db.execute(
            "SELECT event_id FROM facts WHERE kind='template.applied' AND effective=1 ORDER BY cursor DESC LIMIT 1"
        ).fetchone()
        if latest is None:
            return
        previous = db.execute(
            "SELECT value FROM metadata WHERE key='template_event_id'"
        ).fetchone()
        if previous and previous[0] == latest[0]:
            return
        entries = source.read_event_batch([latest[0]])["events"]
        if not entries:
            raise LoggingStateError(
                "The recorded template is missing from its source journal."
            )
        document = {"event_id": latest[0], "data": entries[0]["event"]["data"]}
        with db:
            db.execute(
                "INSERT OR REPLACE INTO metadata VALUES ('template_document', ?)",
                (json.dumps(document, ensure_ascii=False),),
            )
            db.execute(
                "INSERT OR REPLACE INTO metadata VALUES ('template_event_id', ?)",
                (latest[0],),
            )

    def _read_changes(
        self,
        db: sqlite3.Connection,
        source: OperationLogger,
        compact: Callable,
        deadline: float,
        target: dict,
    ) -> tuple[dict, list[dict]]:
        row = db.execute("SELECT value FROM metadata WHERE key='checkpoint'").fetchone()
        checkpoint = json.loads(row[0]) if row else None
        changed = []
        while True:
            if checkpoint and checkpoint["change_cursor"] >= target["change_cursor"]:
                cursor, count = db.execute(
                    "SELECT COALESCE(MAX(cursor),0), COUNT(*) FROM facts"
                ).fetchone()
                if cursor < target["cursor"] or count < target["event_count"]:
                    raise LoggingStateError(
                        "Cached checkpoint skips source events; rebuild the disposable cache."
                    )
                return {
                    "checkpoint": checkpoint,
                    "boundary": target,
                    "has_more": False,
                }, changed
            page = source.read_changes(checkpoint, limit=1000)
            with db:
                for change in page["changes"]:
                    changed.extend(self._apply_change(db, source, change, compact))
                if page["changes"]:
                    db.execute(
                        "INSERT INTO metadata VALUES ('version','1') ON CONFLICT(key) DO UPDATE SET value=CAST(value AS INTEGER)+1"
                    )
                    db.execute("INSERT OR REPLACE INTO metadata VALUES ('ready','0')")
                checkpoint = page["checkpoint"]
                db.execute(
                    "INSERT OR REPLACE INTO metadata VALUES ('checkpoint', ?)",
                    (json.dumps(checkpoint),),
                )
                db.execute(
                    "INSERT OR REPLACE INTO metadata VALUES ('boundary', ?)",
                    (json.dumps(page["boundary"]),),
                )
                event_cursor = db.execute(
                    "SELECT COALESCE(MAX(cursor),0) FROM facts"
                ).fetchone()[0]
                ingested = {
                    **self.identity,
                    "cursor": event_cursor,
                    "change_cursor": checkpoint["change_cursor"],
                }
                db.execute(
                    "INSERT OR REPLACE INTO metadata VALUES ('ingested_through', ?)",
                    (json.dumps(ingested),),
                )
            if checkpoint["change_cursor"] >= target["change_cursor"]:
                page = {**page, "boundary": target, "has_more": False}
            changed = sorted(
                {item["event"]["event_id"]: item for item in changed}.values(),
                key=lambda item: item["cursor"],
            )[-self.window_events :]
            if not page["has_more"] or time.monotonic() >= deadline:
                return page, changed

    def _apply_change(
        self,
        db: sqlite3.Connection,
        source: OperationLogger,
        change: dict,
        compact: Callable,
    ) -> list[dict]:
        entry = change["entry"]
        related = change["related_event_ids"]
        entries = [entry]
        if related != [entry["event"]["event_id"]]:
            entries = []
            missing = list(related)
            while missing:
                page = source.read_event_batch(missing)["events"]
                if not page:
                    raise LoggingStateError("A change refers to an unavailable event.")
                entries.extend(page)
                found = {item["event"]["event_id"] for item in page}
                missing = [key for key in missing if key not in found]
        confirmation = "recorded"
        if change["effective_author"]:
            confirmation = "confirmed"
        if change["provisional"]:
            confirmation = "provisional"
        for item in entries:
            event = item["event"]
            metadata = {
                "effective": event["event_id"] == change["effective_event_id"],
                "effective_author": change["effective_author"],
                "provisional": change["provisional"],
                "ignored": event["data"].get("ignored"),
                "confirmation": confirmation,
            }
            superseded = {
                item["event_id"]: item["ignored"]
                for item in entry["event"]["data"].get("supersedes", [])
            }
            metadata["ignored"] = superseded.get(event["event_id"], metadata["ignored"])
            self._store_event(db, item, metadata, compact)
        return entries

    def _scope_context(self, db: sqlite3.Connection, context: dict) -> dict:
        coordinates = ("run_id", "template_revision_id", "cycle_number")
        if not context.get("attempt_id") or all(key in context for key in coordinates):
            return context
        parent = db.execute(
            "SELECT compact FROM facts WHERE attempt_id=? AND kind='attempt.parameters' ORDER BY cursor DESC LIMIT 1",
            (context["attempt_id"],),
        ).fetchone()
        if parent is None:
            return context
        return {**json.loads(parent[0])["context"], **context}

    def _store_event(
        self, db: sqlite3.Connection, item: dict, metadata: dict, compact: Callable
    ) -> None:
        event = item["event"]
        context = event["context"]
        if context.get("experiment_id") not in (None, self.experiment_id):
            raise ValueError("The journal contains another experiment's events.")
        previous = db.execute(
            "SELECT scope FROM facts WHERE event_id=?", (event["event_id"],)
        ).fetchone()
        scope_context = self._scope_context(db, context)
        if event["event_type"] == "template.applied":
            scope_context = {
                **scope_context,
                "template_revision_id": event["data"]["template_revision_id"],
            }
        scope = json.dumps(
            [
                scope_context.get("run_id"),
                scope_context.get("template_revision_id"),
                scope_context.get("cycle_number"),
            ]
        )
        if event["event_type"] in {
            "control.intent",
            "control.result",
            "control.reconciled",
            "command.result",
        }:
            request_id = (
                context.get("request_id")
                or event["data"].get("request_id")
                or event["data"].get("intent_event_id")
                or event["event_id"]
            )
            # Runner and service observations can have different cycle contexts.
            scope = json.dumps({"request_id": request_id})
        reduced = compact(event)
        reduced.update(cursor=item["cursor"], **metadata)
        db.execute(
            "INSERT OR REPLACE INTO facts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                item["cursor"],
                event["event_id"],
                scope_context.get("run_id"),
                scope_context.get("template_revision_id"),
                scope_context.get("cycle_number"),
                context.get("attempt_id"),
                event.get("operation_id"),
                event["event_type"],
                event["occurred_at"],
                scope,
                int(metadata["effective"] and not metadata["ignored"]),
                json.dumps(metadata),
                json.dumps(reduced, ensure_ascii=False),
            ),
        )
        self._update_run(db, item)
        if not reduced["data"] and event["event_type"] != "call.started":
            return
        db.execute("INSERT OR IGNORE INTO dirty VALUES (?)", (scope,))
        if previous:
            db.execute("INSERT OR IGNORE INTO dirty VALUES (?)", previous)
        if event["event_type"] == "operation.started":
            # A newly recorded ancestor can change overlap in other cycles.
            # Keep recursive IDs outside the indexed lookup; SQLite may otherwise
            # scan all operation starts at every level of the hierarchy.
            db.execute(
                """
                WITH RECURSIVE descendants(operation_id) AS (
                    VALUES (?)
                    UNION
                    SELECT child.operation_id FROM descendants AS parent
                    CROSS JOIN facts AS child INDEXED BY facts_parent
                      ON json_extract(child.compact,'$.data.parent_operation_id')=parent.operation_id
                    WHERE child.kind='operation.started'
                )
                INSERT OR IGNORE INTO dirty
                SELECT facts.scope FROM descendants
                CROSS JOIN facts INDEXED BY facts_operation USING (operation_id)
                """,
                (event["operation_id"],),
            )
        if event["event_type"] == "template.applied":
            db.execute(
                "INSERT OR IGNORE INTO dirty SELECT DISTINCT scope FROM facts WHERE run_id IS ? AND revision IS ?",
                (context.get("run_id"), scope_context.get("template_revision_id")),
            )
        if event["event_type"] == "runner.checkpoint":
            db.execute(
                "INSERT OR IGNORE INTO dirty SELECT scope FROM scopes WHERE run_id IS ? AND json_extract(summary,'$.first_started')<? AND json_extract(summary,'$.last_finished')>?",
                (context.get("run_id"), event["occurred_at"], event["occurred_at"]),
            )

    def _update_run(self, db: sqlite3.Connection, item: dict) -> None:
        event = item["event"]
        run_id = event["context"].get("run_id")
        if not run_id:
            return
        revision = event["context"].get("template_revision_id")
        applied = event["event_type"] == "template.applied"
        if applied:
            revision = event["data"]["template_revision_id"]
        db.execute(
            """
            INSERT INTO runs VALUES (?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                started_at=CASE WHEN excluded.first_cursor<first_cursor THEN excluded.started_at ELSE started_at END,
                first_cursor=MIN(first_cursor, excluded.first_cursor),
                revision=CASE WHEN ? THEN excluded.revision ELSE COALESCE(revision,excluded.revision) END
        """,
            (run_id, item["cursor"], event["occurred_at"], revision, applied),
        )

    def _project_pending(
        self, db: sqlite3.Connection, state: dict, project: Callable, deadline: float
    ) -> bool:
        projected = False
        while True:
            dirty = db.execute("SELECT scope FROM dirty LIMIT 1").fetchone()
            if dirty is None:
                return projected
            with db:
                self._project_scope(db, dirty[0], state, project)
                db.execute("DELETE FROM dirty WHERE scope=?", dirty)
                db.execute(
                    "INSERT INTO metadata VALUES ('version','1') ON CONFLICT(key) DO UPDATE SET value=CAST(value AS INTEGER)+1"
                )
            projected = True
            if time.monotonic() >= deadline:
                return projected

    def _refresh_window(self, source: OperationLogger, changed: list[dict]) -> None:
        if self.window:
            for item in changed:
                self._remember(item)
            return
        before, remaining = None, self.window_events
        while remaining:
            tail = source.read_event_batch(limit=min(remaining, 1000), before=before)
            if not tail["events"]:
                return
            for item in tail["events"]:
                self._remember(item)
            remaining -= len(tail["events"])
            before = min(item["cursor"] for item in tail["events"])

    def _publication(self, db: sqlite3.Connection, page: dict, changed: bool) -> dict:
        complete = (
            not page["has_more"]
            and db.execute("SELECT 1 FROM dirty LIMIT 1").fetchone() is None
        )
        latest = db.execute(
            "SELECT occurred_at FROM facts ORDER BY cursor DESC LIMIT 1"
        ).fetchone()
        version = db.execute(
            "SELECT value FROM metadata WHERE key='version'"
        ).fetchone()
        version = (int(version[0]) if version else 0) + int(changed)
        with db:
            db.execute(
                "INSERT OR REPLACE INTO metadata VALUES ('publication_boundary', ?)",
                (json.dumps(page["boundary"]),),
            )
            db.execute(
                "INSERT OR REPLACE INTO metadata VALUES ('version', ?)", (str(version),)
            )
            db.execute(
                "INSERT OR REPLACE INTO metadata VALUES ('ready', ?)",
                (str(int(complete)),),
            )
            if db.execute("SELECT 1 FROM dirty LIMIT 1").fetchone() is None:
                checkpoint = db.execute(
                    "SELECT value FROM metadata WHERE key='checkpoint'"
                ).fetchone()
                cursor = db.execute(
                    "SELECT COALESCE(MAX(cursor),0) FROM facts"
                ).fetchone()[0]
                cached = {
                    **self.identity,
                    "cursor": cursor,
                    "change_cursor": json.loads(checkpoint[0])["change_cursor"]
                    if checkpoint
                    else 0,
                }
                db.execute(
                    "INSERT OR REPLACE INTO metadata VALUES ('cached_through', ?)",
                    (json.dumps(cached),),
                )
        cached_row = db.execute(
            "SELECT value FROM metadata WHERE key='cached_through'"
        ).fetchone()
        return {
            "complete": complete,
            "version": version,
            "observed_at": latest[0] if latest else None,
            "boundary": page["boundary"],
            "window_count": len(self.window),
            "cached_through": json.loads(cached_row[0]),
        }

    def observe(self, target: dict | None = None) -> dict:
        """Read worker-owned projections and reconstruct the latest source window."""
        with self._lock, OperationLogger(self.config_path, read_only=True) as source:
            boundary = source.read_event_batch([])["boundary"]
            if target and any(
                target[key] != self.identity[key] for key in self.identity
            ):
                raise LoggingStateError(
                    "The requested read boundary belongs to replaced history."
                )
            latest = next(reversed(self.window.values()), None)
            last_cursor = latest["entry"]["cursor"] if latest else None
            if (
                last_cursor is not None
                and 0 < boundary["cursor"] - last_cursor <= self.window_events
            ):
                checkpoint = {**self.identity, "cursor": last_cursor}
                while checkpoint["cursor"] < boundary["cursor"]:
                    page = source.read_events(
                        checkpoint, limit=min(self.window_events, 1000)
                    )
                    if not page["events"]:
                        raise LoggingStateError(
                            "The source window could not reach its captured boundary."
                        )
                    for entry in page["events"]:
                        if entry["cursor"] > boundary["cursor"]:
                            break
                        self._remember(entry)
                    checkpoint = page["checkpoint"]
            elif last_cursor != boundary["cursor"]:
                self.window.clear()
                self._window_bytes = 0
                before, remaining = boundary["cursor"] + 1, self.window_events
                while remaining:
                    page = source.read_event_batch(
                        before=before, limit=min(remaining, 1000)
                    )
                    if not page["events"]:
                        break
                    for entry in page["events"]:
                        self._remember(entry)
                    before = min(entry["cursor"] for entry in page["events"])
                    remaining -= len(page["events"])
            publication = self._observed_publication(boundary, target)
            cached = publication["cached_through"]
            if (
                cached["cursor"] > boundary["cursor"]
                or cached["change_cursor"] > boundary["change_cursor"]
            ):
                # A worker may have ingested appends made while the RAM tail was read.
                current = source.read_event_batch([])["boundary"]
                if (
                    cached["cursor"] > current["cursor"]
                    or cached["change_cursor"] > current["change_cursor"]
                ):
                    raise LoggingStateError(
                        "The cached boundary exceeds the source journal."
                    )
            first = next(iter(self.window.values()), None)
            start = first["entry"]["cursor"] if first else None
            end = publication["cached_through"]["cursor"]
            gap = None
            predecessor = None
            if start is not None and end < start:
                checkpoint = {**self.identity, "cursor": end}
                missing = source.read_events(checkpoint, limit=1)["events"]
                if missing and missing[0]["cursor"] < start:
                    gap = {"after": end, "before": start}
                    preceding = source.read_event_batch(before=start, limit=1)["events"]
                    predecessor = preceding[0]["cursor"] if preceding else 0
            return {
                **publication,
                "boundary": boundary,
                "window_count": len(self.window),
                "window_start_cursor": start,
                "window_predecessor_cursor": predecessor,
                "gap": gap,
                "complete": publication["complete"] and gap is None,
                "target_boundary": target or boundary,
            }

    def _observed_publication(self, boundary: dict, target: dict | None = None) -> dict:
        empty = {
            "complete": False,
            "version": 0,
            "observed_at": None,
            "cache_available": False,
            "cached_through": {**self.identity, "cursor": 0, "change_cursor": 0},
        }
        if not self.path.exists():
            return empty
        try:
            with closing(
                sqlite3.connect(f"{self.path.as_uri()}?mode=ro", uri=True)
            ) as db:
                db.execute("BEGIN")
                metadata = dict(db.execute("SELECT key, value FROM metadata"))
                recorded = json.loads(metadata.get("source", "{}"))
                if recorded.get("identity") != self.identity or recorded.get(
                    "file_key"
                ) != list(self.file_key):
                    return empty
                if "checkpoint" not in metadata:
                    return empty
                cached = json.loads(
                    metadata.get("cached_through", json.dumps(empty["cached_through"]))
                )
                latest = db.execute(
                    "SELECT occurred_at FROM facts ORDER BY cursor DESC LIMIT 1"
                ).fetchone()
                requested = target or boundary
                complete = (
                    metadata.get("ready") == "1"
                    and cached["cursor"] >= requested["cursor"]
                    and cached["change_cursor"] >= requested["change_cursor"]
                )
                return {
                    "complete": complete,
                    "version": int(metadata.get("version", 0)),
                    "observed_at": latest[0] if latest else None,
                    "cached_through": cached,
                    "cache_available": True,
                }
        except sqlite3.OperationalError as error:
            if "no such table" not in str(error):
                raise
            return empty

    def read_view(self, version: int, reader: Callable, *args):
        """One SQLite read snapshot while an independent worker may publish."""
        with (
            self._lock,
            closing(sqlite3.connect(f"{self.path.as_uri()}?mode=ro", uri=True)) as db,
        ):
            db.execute("PRAGMA query_only=ON")
            db.create_aggregate("exact_mean", 1, _ExactMean)
            db.execute("BEGIN")
            metadata = dict(
                db.execute(
                    "SELECT key, value FROM metadata WHERE key IN ('version','source')"
                )
            )
            if (
                int(metadata.get("version", -1)) != version
                or json.loads(metadata["source"])["identity"] != self.identity
            ):
                raise HistoryCacheChanged(
                    "The cache publication changed; refresh the selection."
                )
            self._reading = db
            try:
                return reader(*args)
            finally:
                self._reading = None

    def _operation_ancestors(
        self,
        db: sqlite3.Connection,
        scope: str,
        run_id: str | None,
        size: int,
        count: int,
    ) -> dict:
        """Load only indexed ancestry needed for this scope's measurements."""
        parents = {}
        rows = db.execute(
            """
            WITH RECURSIVE ancestors(operation_id) AS (
                SELECT operation_id FROM facts
                WHERE scope=? AND effective=1 AND operation_id IS NOT NULL
                UNION
                SELECT json_extract(parent.compact,'$.data.parent_operation_id')
                FROM ancestors
                CROSS JOIN facts AS parent INDEXED BY facts_operation USING (operation_id)
                WHERE parent.kind='operation.started' AND parent.effective=1
                  AND (? IS NULL OR parent.run_id=?)
                  AND json_extract(parent.compact,'$.data.parent_operation_id') IS NOT NULL
            )
            SELECT facts.operation_id,
                   json_extract(facts.compact,'$.data.parent_operation_id')
            FROM ancestors
            CROSS JOIN facts INDEXED BY facts_operation USING (operation_id)
            WHERE facts.kind='operation.started' AND facts.effective=1
              AND (? IS NULL OR facts.run_id=?)
              AND facts.scope!=?
            ORDER BY facts.cursor
            """,
            (scope, run_id, run_id, run_id, run_id, scope),
        )
        for operation_id, parent_id in rows:
            count += 1
            size += len(json.dumps([operation_id, parent_id]).encode("utf-8"))
            if count > self.max_events or size > self.max_bytes:
                raise HistoryCacheLimit(
                    "Operation ancestry exceeds the projection working budget."
                )
            parents[operation_id] = {"parent_operation_id": parent_id}
        return parents

    def _project_scope(
        self, db: sqlite3.Connection, scope: str, state: dict, project: Callable
    ) -> None:
        scope_key = json.loads(scope)
        command_scope = isinstance(scope_key, dict)
        run_id, revision, cycle = (None, None, None) if command_scope else scope_key
        size, count = db.execute(
            "SELECT COALESCE(SUM(length(CAST(compact AS BLOB))),0), COUNT(*) FROM facts WHERE scope=? AND effective=1",
            (scope,),
        ).fetchone()
        if size > self.max_bytes or count > self.max_events:
            raise HistoryCacheLimit(
                "An execution scope exceeds history_max_bytes/history_max_events; increase the projection working budget."
            )
        entries = []
        for encoded, event_run, event_revision, event_cycle in db.execute(
            "SELECT compact, run_id, revision, cycle FROM facts WHERE scope=? AND effective=1 AND (json_extract(compact,'$.data')!='{}' OR kind='call.started') ORDER BY cursor",
            (scope,),
        ):
            event = json.loads(encoded)
            coordinates = {
                "run_id": event_run,
                "template_revision_id": event_revision,
                "cycle_number": event_cycle,
            }
            # Scope selection and projection filtering must use the same context.
            # Enrich only this working copy; raw event pages retain source context.
            event["context"] = {
                **{
                    key: value
                    for key, value in coordinates.items()
                    if value is not None
                },
                **event["context"],
            }
            entries.append(event)
        template = None
        if not command_scope:
            template = db.execute(
                "SELECT compact FROM facts WHERE kind='template.applied' AND run_id IS ? AND revision IS ? AND effective=1 ORDER BY cursor DESC LIMIT 1",
                (run_id, revision),
            ).fetchone()
        if template:
            event = json.loads(template[0])
            if all(item["event_id"] != event["event_id"] for item in entries):
                entries.insert(0, event)
        observations = [
            item for item in entries if item["event_type"] != "template.applied"
        ]
        if cycle is not None and observations:
            first = min(item["occurred_at"] for item in observations)
            last = max(item["occurred_at"] for item in entries)
            for row in db.execute(
                "SELECT compact FROM facts WHERE kind='runner.checkpoint' AND occurred_at>=? AND occurred_at<=? AND run_id IS ? AND scope!=? AND effective=1",
                (first, last, run_id, scope),
            ):
                entries.append(json.loads(row[0]))
        entries.sort(key=lambda item: item["cursor"])
        result = project(
            {
                "experiment_id": self.experiment_id,
                "entries": entries,
                "operation_ancestors": self._operation_ancestors(
                    db, scope, run_id, size, count
                )
                if cycle is not None
                else {},
                "state": state,
                "complete": True,
            },
            run_id,
            cycle,
        )
        db.execute("DELETE FROM records WHERE scope=?", (scope,))
        for kind, items in result["records"].items():
            for item in items:
                key = item.pop("_record_key")
                position = item.pop("_position")
                db.execute(
                    "INSERT OR REPLACE INTO records VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        kind,
                        key,
                        scope,
                        item.get("run_id") if command_scope else run_id,
                        revision,
                        cycle,
                        position,
                        json.dumps(item, ensure_ascii=False),
                    ),
                )
        if command_scope:
            return
        db.execute(
            "INSERT OR REPLACE INTO scopes VALUES (?, ?, ?, ?, ?)",
            (
                scope,
                run_id,
                revision,
                cycle,
                json.dumps(result["summary"], ensure_ascii=False),
            ),
        )

    def query(self, sql: str, parameters: tuple = ()) -> list[tuple]:
        """Query the separate projection database, never the source tables."""
        with self._lock:
            if self._reading is not None:
                return self._reading.execute(sql, parameters).fetchall()
        with (
            self._lock,
            closing(sqlite3.connect(f"{self.path.as_uri()}?mode=ro", uri=True)) as db,
        ):
            db.execute("PRAGMA query_only=ON")
            db.create_aggregate("exact_mean", 1, _ExactMean)
            return db.execute(sql, parameters).fetchall()

    def iter_query(self, sql: str, parameters: tuple = ()) -> Iterator[tuple]:
        """Stream scalar projection statistics with bounded working memory."""
        with (
            self._lock,
            closing(sqlite3.connect(f"{self.path.as_uri()}?mode=ro", uri=True)) as db,
        ):
            db.execute("PRAGMA query_only=ON")
            yield from db.execute(sql, parameters)

    def events(self, identifiers: list[str]) -> list[dict]:
        """Hydrate a bounded result without changing active-window membership."""
        result, size = [], 0
        with closing(self.iter_events(identifiers)) as events:
            for event in events:
                size += len(json.dumps(event, ensure_ascii=False).encode("utf-8"))
                if size > self.max_bytes:
                    raise HistoryCacheLimit(
                        "Requested source details exceed history_max_bytes; request fewer events."
                    )
                result.append(event)
        return result

    def iter_events(self, identifiers: list[str]) -> Iterator[dict]:
        """One read-only source connection, indexed lookups and bounded payloads."""
        with self._lock, OperationLogger(self.config_path, read_only=True) as source:
            info = source.get_journal_info()
            if any(info[key] != self.identity[key] for key in self.identity):
                raise HistoryCacheChanged(
                    "The journal changed before loading source details."
                )
            for start in range(0, len(identifiers), 20):
                batch = identifiers[start : start + 20]
                placeholders = ",".join("?" for _ in batch)
                metadata = dict(
                    self.query(
                        f"SELECT event_id, metadata FROM facts WHERE event_id IN ({placeholders})",
                        tuple(batch),
                    )
                )
                entries = {
                    identifier: self.window[identifier]["entry"]
                    for identifier in batch
                    if identifier in self.window
                }
                missing = list(
                    dict.fromkeys(
                        identifier for identifier in batch if identifier not in entries
                    )
                )
                while missing:
                    page = source.read_event_batch(missing)["events"]
                    if not page:
                        raise LoggingStateError(
                            "A referenced journal event is unavailable."
                        )
                    entries.update((entry["event"]["event_id"], entry) for entry in page)
                    # The source byte budget can shorten an indexed batch.
                    missing = [
                        identifier
                        for identifier in missing
                        if identifier not in entries
                    ]
                for identifier in batch:
                    entry = entries[identifier]
                    yield {
                        **entry["event"],
                        "cursor": entry["cursor"],
                        **json.loads(metadata.get(identifier, "{}")),
                    }
            info = source.get_journal_info()
            if any(info[key] != self.identity[key] for key in self.identity):
                raise HistoryCacheChanged(
                    "The journal changed while loading source details."
                )

    def close(self) -> None:
        with self._lock:
            self.window.clear()
            self._window_bytes = 0
            self._opened = False
