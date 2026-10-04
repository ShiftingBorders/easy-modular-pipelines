"""Journal record decoding and identified checkpoints."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from core.journal.events import (
    SCHEMA_VERSION,
    JournalGenerationChanged,
    LoggingStateError,
    LoggingStorageError,
    validate_journal_identity,
)
from core.primitives.json_values import JsonObject

if TYPE_CHECKING:
    from core.models.journal_records import (
        CommandObservation,
        JournalCheckpoint,
        JournalEntry,
        JournalReadBoundary,
    )


def _read_journal_info(connection: sqlite3.Connection) -> JsonObject:
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


def _decode_row(row: tuple) -> JournalEntry:
    from core.models.journal_records import JournalEntry, JournalEvent

    cursor, event_id, producer, sequence, encoded = row
    if not isinstance(encoded, str):
        raise TypeError("Stored event must be JSON text.")
    event = JournalEvent.model_validate(json.loads(encoded))
    if (
        event.event_id,
        event.producer_instance_id,
        event.sequence_number,
    ) != (event_id, producer, sequence):
        raise ValueError("Stored event disagrees with its indexed identity.")
    return JournalEntry(cursor=cursor, event=event, encoded_event=encoded)


def _checkpoint(info: JournalReadBoundary, key: str, position: int) -> JsonObject:
    return {"journal_id": info.journal_id, "generation": info.generation, key: position}


def _checkpoint_position(
    checkpoint: JournalCheckpoint | None, boundary: JournalReadBoundary, key: str
) -> int:
    if checkpoint is None:
        return 0
    expected = {
        "journal_id": checkpoint.journal_id,
        "generation": checkpoint.generation,
    }
    actual = {"journal_id": boundary.journal_id, "generation": boundary.generation}
    if expected != actual:
        raise JournalGenerationChanged(expected, actual)
    if checkpoint.position > getattr(boundary, key):
        raise LoggingStateError("Checkpoint is beyond the committed journal boundary.")
    return checkpoint.position


def _ignored_reason(result: CommandObservation) -> str:
    return {
        "timed_out": "request_timed_out",
        "cancelled": "request_cancelled",
        "invalidated": "request_invalidated",
    }.get(result.outcome, "runner_result_precedence")


def _read_boundary(connection: sqlite3.Connection) -> JournalReadBoundary:
    from core.models.journal_records import JournalReadBoundary

    info = _read_journal_info(connection)
    cursor = connection.execute(
        "SELECT COALESCE(MAX(cursor), 0) FROM events"
    ).fetchone()[0]
    count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    change = connection.execute(
        "SELECT COALESCE(MAX(change_cursor), 0) FROM journal_changes"
    ).fetchone()[0]
    return JournalReadBoundary(
        **info, cursor=cursor, event_count=count, change_cursor=change
    )


def _validate_result_precedence(result: dict, row: tuple) -> None:
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
            if entry is not None and entry["result"].author != author:
                raise ValueError("Command event author does not match its index.")
    effective_author = "runner" if result["runner"] is not None else "participant"
    effective = result[effective_author]
    if (
        effective is None
        or row[6] != effective_author
        or row[5] != effective["event_id"]
    ):
        raise ValueError("Invalid effective command result.")
    if runner is None and participant["result"].ignored is not None:
        raise ValueError("An ignored participant response requires a runner result.")
    if shared_event:
        shared_data = effective["result"]
        if shared_data.ignored is not None or shared_data.supersedes:
            raise ValueError("A shared result cannot be ignored or superseded.")
    else:
        if participant is not None and participant["result"].supersedes:
            raise ValueError("A participant response cannot supersede runner.")
        if runner is not None:
            runner_data = runner["result"]
            if runner_data.ignored is not None:
                raise ValueError("A runner result cannot be ignored.")
            supersedes = runner_data.supersedes
            if supersedes and (
                participant is None
                or len(supersedes) != 1
                or supersedes[0].event_id != participant["event_id"]
                or supersedes[0].ignored != _ignored_reason(runner_data)
            ):
                raise ValueError("Invalid superseded participant reference.")
            if participant is not None:
                participant_data = participant["result"]
                reason = participant_data.ignored
                if reason is not None and reason != _ignored_reason(runner_data):
                    raise ValueError("Invalid ignored participant reason.")
                if json.dumps(
                    [runner_data.outcome, runner_data.response],
                    sort_keys=True,
                ) == json.dumps(
                    [participant_data.outcome, participant_data.response],
                    sort_keys=True,
                ):
                    raise ValueError("Matching results must share one event ID.")


def _prepare_other_author_result(
    response_key: str,
    other: dict | None,
    author: str,
    snapshot: JsonObject,
    result: CommandObservation,
) -> tuple[bool, str | None]:
    other_key = None
    if other is not None:
        other_data = other["result"]
        other_key = json.dumps(
            [other_data.outcome, other_data.response],
            sort_keys=True,
            ensure_ascii=True,
        )
    if response_key == other_key:
        return True, other["event_id"]
    else:
        if author == "participant" and other is not None:
            snapshot["data"]["ignored"] = _ignored_reason(other["result"])
        elif author == "runner" and other is not None:
            snapshot["data"]["supersedes"] = [
                {
                    "event_id": other["event_id"],
                    "ignored": _ignored_reason(result),
                }
            ]
    return False, None


def _command_result_document(request_id: str, state: dict) -> JsonObject:
    effective = state[state["effective_author"]]
    data = effective["result"]
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
            ignored = _ignored_reason(state["runner"]["result"])
        observations.append(
            {
                "author": author,
                "event_id": entry["event_id"],
                "event": entry["entry"].document()["event"],
                "observation": entry["observation"].model_dump(),
                "ignored": ignored,
            }
        )
    result = {
        "request_id": request_id,
        "event_id": effective["event_id"],
        "author": state["effective_author"],
        "outcome": data.outcome,
        "response": data.response,
        "event": effective["entry"].document()["event"],
        "observations": observations,
        "provisional": state["runner"] is None,
    }
    return result
