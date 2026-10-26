"""Derived journal table definitions and schema validation."""

from __future__ import annotations

import sqlite3

from core.journal.events import LoggingStorageError
from core.journal.schema import _matches_table_definition

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


def _validate_view_schema(connection: sqlite3.Connection) -> None:
    actual = connection.execute(
        "SELECT type, name, sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'"
    ).fetchall()
    expected = {"filtered_info": _CREATE_INFO, "filtered_events": _CREATE_EVENTS}
    if (
        len(actual) != len(expected)
        or connection.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID
        or connection.execute("PRAGMA user_version").fetchone()[0] != 1
    ):
        raise LoggingStorageError(
            "Unrecognized derived database; no file was replaced."
        )
    for kind, name, sql in actual:
        if not _matches_table_definition(kind, name, sql, expected):
            raise LoggingStorageError("Unrecognized derived database schema.")
