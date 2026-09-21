"""Rebuildable journal projections. The original read-only journal is authoritative.

Only this separate database is writable. Source identities and existing source
indexes are used for hydration; no source schema or payload is changed.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import closing
from fractions import Fraction
from pathlib import Path

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
                "version": 2,
                "identity": self.identity,
                "file_key": list(self.file_key),
                "experiment_id": self.experiment_id,
            }
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute(
                    "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                row = db.execute(
                    "SELECT value FROM metadata WHERE key='source'"
                ).fetchone()
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
                    CREATE INDEX IF NOT EXISTS facts_time ON facts(kind, occurred_at);
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
                """)
                db.execute(
                    "INSERT OR REPLACE INTO metadata VALUES ('source', ?)",
                    (json.dumps(expected),),
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

    def refresh(self, state: dict, compact: Callable, project: Callable) -> dict:
        """Advance ingestion, projections and the active window explicitly."""
        with self._lock:
            if not self._opened:
                self.open()
            with (
                OperationLogger(self.config_path, read_only=True) as source,
                closing(sqlite3.connect(self.path)) as db,
            ):
                deadline = time.monotonic() + 1
                page, changed = self._read_changes(db, source, compact, deadline)
                projected = self._project_pending(db, state, project, deadline)
                self._refresh_window(source, changed)
                return self._publication(db, page, bool(changed) or projected)

    def _read_changes(
        self,
        db: sqlite3.Connection,
        source: OperationLogger,
        compact: Callable,
        deadline: float,
    ) -> tuple[dict, list[dict]]:
        row = db.execute("SELECT value FROM metadata WHERE key='checkpoint'").fetchone()
        checkpoint = json.loads(row[0]) if row else None
        changed = []
        while True:
            page = source.read_changes(checkpoint, limit=100)
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
        if not context.get("attempt_id") or context.get("cycle_number") is not None:
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
        scope = json.dumps(
            [
                scope_context.get("run_id"),
                scope_context.get("template_revision_id"),
                scope_context.get("cycle_number"),
            ]
        )
        reduced = compact(event)
        reduced.update(cursor=item["cursor"], **metadata)
        db.execute(
            "INSERT OR REPLACE INTO facts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                item["cursor"],
                event["event_id"],
                context.get("run_id"),
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
                "INSERT OR REPLACE INTO metadata VALUES ('version', ?)", (str(version),)
            )
            db.execute(
                "INSERT OR REPLACE INTO metadata VALUES ('ready', ?)",
                (str(int(complete)),),
            )
        return {
            "complete": complete,
            "version": version,
            "observed_at": latest[0] if latest else None,
            "boundary": page["boundary"],
            "window_count": len(self.window),
        }

    def _project_scope(
        self, db: sqlite3.Connection, scope: str, state: dict, project: Callable
    ) -> None:
        run_id, revision, cycle = json.loads(scope)
        size, count = db.execute(
            "SELECT COALESCE(SUM(length(CAST(compact AS BLOB))),0), COUNT(*) FROM facts WHERE scope=? AND effective=1",
            (scope,),
        ).fetchone()
        if size > self.max_bytes or count > self.max_events:
            raise HistoryCacheLimit(
                "An execution scope exceeds history_max_bytes/history_max_events; increase the projection working budget."
            )
        entries = [
            json.loads(row[0])
            for row in db.execute(
                "SELECT compact FROM facts WHERE scope=? AND effective=1 AND (json_extract(compact,'$.data')!='{}' OR kind='call.started') ORDER BY cursor",
                (scope,),
            )
        ]
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
                        run_id,
                        revision,
                        cycle,
                        position,
                        json.dumps(item, ensure_ascii=False),
                    ),
                )
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
            for start in range(0, len(identifiers), 20):
                batch = identifiers[start : start + 20]
                placeholders = ",".join("?" for _ in batch)
                metadata = dict(
                    self.query(
                        f"SELECT event_id, metadata FROM facts WHERE event_id IN ({placeholders})",
                        tuple(batch),
                    )
                )
                for identifier in batch:
                    cached = self.window.get(identifier)
                    entries = (
                        [cached["entry"]]
                        if cached
                        else source.read_event_batch([identifier])["events"]
                    )
                    if not entries:
                        raise LoggingStateError(
                            "A referenced journal event is unavailable."
                        )
                    entry = entries[0]
                    yield {
                        **entry["event"],
                        "cursor": entry["cursor"],
                        **json.loads(metadata.get(identifier, "{}")),
                    }
            source.get_journal_info()

    def close(self) -> None:
        with self._lock:
            self.window.clear()
            self._window_bytes = 0
            self._opened = False
