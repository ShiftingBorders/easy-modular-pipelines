"""Journal diagnostic exports and restoration record operations."""

import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import BinaryIO

from core.journal.events import (
    SCHEMA_VERSION,
    validate_command_result,
    validate_journal_identity,
)
from core.primitives.json_values import JsonObject


def _decode_author_observation(
    event: JsonObject,
    observation_json: str,
    request_id: str,
    identity: JsonObject,
) -> JsonObject:
    from core.models.journal_diagnostics import AuthorObservation

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
        raise ValueError("Command result belongs to another request context.")
    observation = AuthorObservation.model_validate(json.loads(observation_json))
    context = observation.context.root
    if context.get("request_id") != request_id:
        raise ValueError("Observation request_id does not match.")
    if {key: context.get(key) for key in identity} != identity:
        raise ValueError("Observation belongs to another request context.")
    return observation.model_dump()


def _content_digest(connection: sqlite3.Connection) -> str:
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


def _validate_diagnostic_command(
    record: JsonObject, request_ids: set[str], commands: list[JsonObject]
) -> None:
    from core.models.journal_diagnostics import DiagnosticCommand

    command = DiagnosticCommand.model_validate(record)
    request_id = command.request_id
    if request_id in request_ids:
        raise ValueError("Duplicate diagnostic request ID.")
    request_ids.add(request_id)
    commands.append(command.model_dump(exclude_unset=True))


def _validate_restore_input(
    snapshot_manifest: JsonObject, restoration_id: str, new_generation: str
) -> tuple[JsonObject, JsonObject, str, str]:
    from core.models.journal_diagnostics import JournalRestorationInput

    restoration = JournalRestorationInput.model_validate(
        {
            "snapshot": snapshot_manifest,
            "restoration_id": restoration_id,
            "new_generation": new_generation,
        }
    )
    manifest = restoration.snapshot.model_dump()
    identity = validate_journal_identity(
        {name: manifest[name] for name in ("journal_id", "generation")}
    )
    return manifest, identity, restoration.restoration_id, restoration.new_generation


def _write_diagnostic_record(output: BinaryIO, digest, record: JsonObject) -> None:
    line = (json.dumps(record, ensure_ascii=True) + "\n").encode("ascii")
    output.write(line)
    digest.update(line)


def _close_preserving_failure(connection: sqlite3.Connection | sqlite3.Cursor) -> None:
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


def _publish_manifest(target: Path, manifest: JsonObject) -> None:
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


def _save_command_state(
    connection: sqlite3.Connection, request_id: str, state: dict
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
