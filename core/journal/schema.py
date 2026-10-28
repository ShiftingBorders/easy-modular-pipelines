"""Durable journal table definitions and creation."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from uuid import uuid4

_APPLICATION_ID = 0x454D504C
STORAGE_SCHEMA_VERSION = 3

_CREATE_EVENTS_V2 = """CREATE TABLE events (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    producer_instance_id TEXT NOT NULL,
    sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
    event_json TEXT NOT NULL,
    UNIQUE (producer_instance_id, sequence_number)
)"""

_CREATE_EVENTS = """CREATE TABLE events (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    producer_instance_id TEXT NOT NULL,
    sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
    event_json TEXT NOT NULL,
    payload_encoding TEXT NOT NULL DEFAULT 'json'
        CHECK (payload_encoding IN ('json', 'checkpoint_ids')),
    UNIQUE (producer_instance_id, sequence_number)
)"""

_CREATE_CHECKPOINT_IDS = """CREATE TABLE checkpoint_request_ids (
    event_id TEXT PRIMARY KEY NOT NULL REFERENCES events(event_id),
    event_cursor INTEGER NOT NULL UNIQUE REFERENCES events(cursor),
    experiment_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    generation TEXT NOT NULL,
    base_event_id TEXT REFERENCES checkpoint_request_ids(event_id),
    depth INTEGER NOT NULL CHECK (depth >= 0 AND depth < 32),
    ids_json TEXT NOT NULL,
    item_count INTEGER NOT NULL CHECK (item_count >= 0),
    event_sha256 TEXT NOT NULL
)"""

_INDEXES = {
    "checkpoint_ids_scope": """CREATE INDEX checkpoint_ids_scope
        ON checkpoint_request_ids
        (experiment_id, run_id, event_type, generation, event_cursor)""",
}

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

_TABLES_V2 = {
    "events": _CREATE_EVENTS_V2,
    "journal_info": _CREATE_JOURNAL_INFO,
    "command_results": _CREATE_COMMAND_RESULTS,
    "journal_changes": _CREATE_CHANGES,
    "journal_restorations": _CREATE_RESTORATIONS,
}

_TABLES = {
    **_TABLES_V2,
    "events": _CREATE_EVENTS,
    "checkpoint_request_ids": _CREATE_CHECKPOINT_IDS,
}
_SCHEMAS = {2: _TABLES_V2, 3: {**_TABLES, **_INDEXES}}


def _matches_table_definition(
    kind: str, name: str, sql: object, expected: dict[str, str]
) -> bool:
    return not (
        kind != ("index" if name in _INDEXES else "table")
        or name not in expected
        or not isinstance(sql, str)
        or " ".join(sql.split()) != " ".join(expected[name].split())
    )


def _create_tables(connection: sqlite3.Connection) -> None:
    for statement in _TABLES.values():
        connection.execute(statement)
    for statement in _INDEXES.values():
        connection.execute(statement)
    connection.execute(
        "INSERT INTO journal_info VALUES (1, ?, ?, ?)",
        (
            uuid4().hex,
            uuid4().hex,
            datetime.now(UTC).isoformat(timespec="microseconds"),
        ),
    )
