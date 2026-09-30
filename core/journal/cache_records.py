"""Projection input records and cache metadata publication."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator

from core.journal.events import LoggingStateError
from core.journal.logger import OperationLogger


def _publish_reader_context(
    db: sqlite3.Connection, reader_context: dict, state: dict
) -> None:
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


def _publish_template(db: sqlite3.Connection, source: OperationLogger) -> None:
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


def _event_scope(event: dict, scope_context: dict, context: dict) -> str:
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
    return scope


def _update_run(db: sqlite3.Connection, item: dict) -> None:
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


def _add_template_input(
    db: sqlite3.Connection,
    entries: list[dict],
    command_scope: bool,
    run_id: str | None,
    revision: str | None,
) -> None:
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


def _add_checkpoint_inputs(
    db: sqlite3.Connection,
    entries: list[dict],
    cycle: int | None,
    run_id: str | None,
    scope: str,
) -> None:
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


def _missing_event_pages(
    source: OperationLogger, missing: list[str], missing_message: str
) -> Iterator[list[dict]]:
    while missing:
        page = source.read_event_batch(missing)["events"]
        if not page:
            raise LoggingStateError(missing_message)
        yield page
        found = {item["event"]["event_id"] for item in page}
        missing = [key for key in missing if key not in found]


def _scope_context(db: sqlite3.Connection, context: dict) -> dict:
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
