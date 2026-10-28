"""Lossless bounded request-ID history inside caller-owned SQL transactions."""

from __future__ import annotations

import hashlib
import json
import sqlite3

from pydantic import ValidationError

from core.journal.events import LoggingStorageError
from core.models.journal_checkpoint_ids import CheckpointIdHistory, CheckpointIdsInput
from core.primitives.json_values import JsonObject

_HISTORY_COLUMNS = (
    "event_id, experiment_id, run_id, event_type, generation, base_event_id, "
    "depth, ids_json, item_count, event_sha256"
)


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _history_from_row(row: tuple) -> CheckpointIdHistory:
    names = (
        "event_id",
        "experiment_id",
        "run_id",
        "event_type",
        "generation",
        "base_event_id",
        "depth",
        "ids",
        "item_count",
        "event_sha256",
    )
    document = dict(zip(names, row, strict=True))
    document["ids"] = json.loads(document["ids"])
    return CheckpointIdHistory.model_validate(document)


def _read_history(
    connection: sqlite3.Connection, event_id: str
) -> tuple[CheckpointIdHistory, tuple]:
    """Read metadata and its actual event together from the caller's SQL view."""
    row = connection.execute(
        f"SELECT {_HISTORY_COLUMNS}, event_cursor FROM checkpoint_request_ids "
        "WHERE event_id=?",
        (event_id,),
    ).fetchone()
    if row is None:
        raise ValueError("Compact checkpoint is missing its request-ID history.")
    history = _history_from_row(row[:10])
    event_row = connection.execute(
        "SELECT cursor, event_id, producer_instance_id, sequence_number, "
        "event_json, payload_encoding FROM events WHERE event_id=?",
        (event_id,),
    ).fetchone()
    if event_row is None or event_row[0] != row[10] or event_row[5] != "checkpoint_ids":
        raise ValueError("Checkpoint history does not match its stored event.")
    if not isinstance(event_row[4], str):
        raise TypeError("Stored checkpoint must be JSON text.")
    history.validate_envelope(json.loads(event_row[4]))
    return history, event_row


def _reconstruct_ids(
    connection: sqlite3.Connection, history: CheckpointIdHistory, cursor: int
) -> list[str]:
    """Follow at most 31 earlier links, then apply validated additions in order."""
    chain = [history]
    while history.base_event_id is not None:
        parent, row = _read_history(connection, history.base_event_id)
        if (
            parent.scope() != history.scope()
            or parent.depth != history.depth - 1
            or row[0] >= cursor
        ):
            raise ValueError("Checkpoint parent has an invalid scope, depth, or order.")
        chain.append(parent)
        history, cursor = parent, row[0]
    identifiers: set[str] = set()
    for item in reversed(chain):
        if identifiers.intersection(item.ids):
            raise ValueError("Checkpoint additions repeat an existing request ID.")
        identifiers.update(item.ids)
        if len(identifiers) != item.item_count:
            raise ValueError("Reconstructed checkpoint request-ID count differs.")
    return sorted(identifiers)


def _reconstruct_event(connection: sqlite3.Connection | None, row: tuple) -> tuple:
    """Return the original five-column event row, validating compact dependencies."""
    if len(row) == 5 or row[5] == "json":
        return row[:5]
    if row[5] != "checkpoint_ids":
        raise LoggingStorageError("Unknown stored event encoding.")
    if connection is None:
        raise LoggingStorageError("Compact checkpoint requires its database read view.")
    try:
        history, stored_row = _read_history(connection, row[1])
        if stored_row != row:
            raise ValueError("Checkpoint changed within the selected read view.")
        identifiers = _reconstruct_ids(connection, history, row[0])
        event = json.loads(row[4])
        event["data"]["used_request_ids"] = identifiers
        encoded = _json_text(event)
        if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != history.event_sha256:
            raise ValueError("Reconstructed checkpoint content checksum differs.")
        return (*row[:4], encoded)
    except (ValueError, TypeError, KeyError) as error:
        raise LoggingStorageError("Invalid checkpoint request-ID history.") from error


def _select_request_id_delta(
    connection: sqlite3.Connection,
    payload: CheckpointIdsInput,
    event_type: str,
    generation: str,
) -> tuple[list[str], str | None, int]:
    """Choose a smaller delta only within a committed scope and bounded chain."""
    anchor = payload.used_request_ids, None, 0
    row = connection.execute(
        "SELECT event_id FROM checkpoint_request_ids "
        "WHERE experiment_id=? AND run_id=? AND event_type=? AND generation=? "
        "ORDER BY event_cursor DESC LIMIT 1",
        (payload.experiment_id, payload.run_id, event_type, generation),
    ).fetchone()
    if row is None:
        return anchor
    base, base_row = _read_history(connection, row[0])
    if base.depth == 31:
        return anchor
    original = _reconstruct_event(connection, base_row)
    previous = set(json.loads(original[4])["data"]["used_request_ids"])
    current = set(payload.used_request_ids)
    if not previous <= current:
        return anchor
    delta = sorted(current - previous), base.event_id, base.depth + 1
    if len(_json_text(delta).encode("utf-8")) >= len(
        _json_text(anchor).encode("utf-8")
    ):
        return anchor
    return delta


def _prepare_checkpoint(
    connection: sqlite3.Connection, event: JsonObject, encoded: str
) -> tuple[str, CheckpointIdHistory | None]:
    """Pack eligible canonical checkpoints, retaining other application JSON."""
    if event["event_type"] not in ("runner.checkpoint", "rebuild.checkpoint"):
        return encoded, None
    try:
        payload = CheckpointIdsInput.model_validate(event["data"])
    except ValidationError:
        # Ordinary record_event historically accepts arbitrary application JSON.
        return encoded, None
    if _json_text(event) != encoded:
        return encoded, None
    generation = connection.execute(
        "SELECT generation FROM journal_info WHERE singleton=1"
    ).fetchone()[0]
    ids, base_event_id, depth = _select_request_id_delta(
        connection, payload, event["event_type"], generation
    )
    history = CheckpointIdHistory(
        event_id=event["event_id"],
        experiment_id=payload.experiment_id,
        run_id=payload.run_id,
        event_type=event["event_type"],
        generation=generation,
        base_event_id=base_event_id,
        depth=depth,
        ids=ids,
        item_count=len(payload.used_request_ids),
        event_sha256=hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
    )
    stored = {**event, "data": {**event["data"], "used_request_ids": []}}
    return _json_text(stored), history


def _validate_history_links(connection: sqlite3.Connection) -> None:
    """Reject surplus history or cursor mismatches during full snapshot validation."""
    invalid = connection.execute(
        "SELECT 1 FROM checkpoint_request_ids h JOIN events e ON e.event_id=h.event_id "
        "WHERE e.payload_encoding!='checkpoint_ids' OR e.cursor!=h.event_cursor LIMIT 1"
    ).fetchone()
    if invalid is not None:
        raise LoggingStorageError(
            "Request-ID history refers to a different stored event."
        )


def _insert_stored_event(
    connection: sqlite3.Connection,
    event: JsonObject,
    encoded: str,
    storage_version: int,
) -> None:
    """Insert an event and its optional history; caller commits both with its change."""
    stored, history = (encoded, None)
    if storage_version == 3:
        stored, history = _prepare_checkpoint(connection, event, encoded)
    columns = "event_id, producer_instance_id, sequence_number"
    values = [
        event["event_id"],
        event["producer_instance_id"],
        event["sequence_number"],
    ]
    if storage_version == 3:
        columns += ", payload_encoding"
        values.append("json" if history is None else "checkpoint_ids")
    columns += ", event_json"
    values.append(stored)
    inserted = connection.execute(
        f"INSERT INTO events ({columns}) VALUES ({','.join('?' for _ in values)})",
        values,
    )
    if history is not None:
        connection.execute(
            f"INSERT INTO checkpoint_request_ids ({_HISTORY_COLUMNS}, event_cursor) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                history.event_id,
                history.experiment_id,
                history.run_id,
                history.event_type,
                history.generation,
                history.base_event_id,
                history.depth,
                _json_text(history.ids),
                history.item_count,
                history.event_sha256,
                inserted.lastrowid,
            ),
        )
