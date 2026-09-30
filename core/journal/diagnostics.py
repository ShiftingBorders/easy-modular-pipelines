"""Journal diagnostic exports and restoration record operations."""

import hashlib
import json
import os
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO
from uuid import UUID

from core.journal.events import (
    SCHEMA_VERSION,
    validate_command_result,
    validate_context,
    validate_journal_identity,
)
from core.primitives.json_values import JsonObject, copy_json_object, require_text


def _decode_author_observation(
    event: JsonObject,
    observation_json: str,
    request_id: str,
    identity: JsonObject,
) -> JsonObject:
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
    observation = copy_json_object(json.loads(observation_json), "observation")
    if observation.keys() != {
        "producer_instance_id",
        "occurred_at",
        "context",
        "operation_id",
    }:
        raise ValueError("Invalid observation fields.")
    if observation["operation_id"] is not None:
        require_text(observation["operation_id"], "operation_id")
    require_text(observation["producer_instance_id"], "producer_instance_id")
    timestamp = datetime.fromisoformat(observation["occurred_at"])
    if timestamp.utcoffset() != UTC.utcoffset(None):
        raise ValueError("Observation time must be UTC.")
    context = validate_context(observation["context"])
    if context.get("request_id") != request_id:
        raise ValueError("Observation request_id does not match.")
    if {key: context.get(key) for key in identity} != identity:
        raise ValueError("Observation belongs to another request context.")
    return observation


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
    request_id = require_text(record["request_id"], "request_id")
    if request_id in request_ids:
        raise ValueError("Duplicate diagnostic request ID.")
    request_ids.add(request_id)
    identity = validate_context(record["identity"])
    if identity.keys() != {
        "experiment_id",
        "participant_id",
        "participant_instance_id",
    }:
        raise ValueError("Invalid diagnostic request identity.")
    require_text(identity["experiment_id"], "experiment_id")
    require_text(identity["participant_id"], "participant_id")
    if record["runner"] is None and record["participant"] is None:
        raise ValueError("Diagnostic result requires an observation.")
    for author in ("runner", "participant"):
        entry = record[author]
        if entry is None:
            continue
        if type(entry) is not dict or entry.keys() != {
            "event_id",
            "observation",
        }:
            raise ValueError("Invalid diagnostic observation fields.")
        require_text(entry["event_id"], "event_id")
        observation = copy_json_object(entry["observation"], "observation")
        if observation.keys() != {
            "producer_instance_id",
            "occurred_at",
            "context",
            "operation_id",
        }:
            raise ValueError("Invalid diagnostic observer.")
        if observation["operation_id"] is not None:
            require_text(observation["operation_id"], "operation_id")
        require_text(observation["producer_instance_id"], "producer_instance_id")
        timestamp = datetime.fromisoformat(observation["occurred_at"])
        if timestamp.utcoffset() != UTC.utcoffset(None):
            raise ValueError("Observation timestamp must be UTC.")
        validate_context(observation["context"])
    commands.append(record)


def _validate_restore_input(
    snapshot_manifest: JsonObject, restoration_id: str, new_generation: str
) -> tuple[JsonObject, JsonObject, str, str]:
    manifest = copy_json_object(snapshot_manifest, "snapshot manifest")
    if manifest.keys() != {
        "schema_version",
        "snapshot_id",
        "journal_id",
        "generation",
        "storage_schema_version",
        "cursor",
        "event_count",
        "change_cursor",
        "content_sha256",
        "created_at",
        "database",
    }:
        raise ValueError("Snapshot manifest fields do not match the format.")
    if (
        type(manifest["schema_version"]) is not int
        or manifest["schema_version"] != SCHEMA_VERSION
        or type(manifest["storage_schema_version"]) is not int
        or manifest["storage_schema_version"] != SCHEMA_VERSION
        or manifest["database"] != "journal.sqlite"
    ):
        raise ValueError("Unsupported snapshot manifest.")
    identity = validate_journal_identity(
        {name: manifest[name] for name in ("journal_id", "generation")}
    )
    UUID(require_text(manifest["snapshot_id"], "snapshot_id"))
    restoration_id = UUID(require_text(restoration_id, "restoration_id")).hex
    new_generation = UUID(require_text(new_generation, "new_generation")).hex
    if new_generation == identity["generation"]:
        raise ValueError("Restoration requires a fresh generation.")
    for field in ("cursor", "event_count", "change_cursor"):
        if (
            type(manifest[field]) is not int
            or not 0 <= manifest[field] <= 9223372036854775807
        ):
            raise ValueError(f"Invalid snapshot {field}.")
    timestamp = datetime.fromisoformat(manifest["created_at"])
    if timestamp.utcoffset() != UTC.utcoffset(None):
        raise ValueError("Snapshot creation time must be UTC.")
    digest_text = require_text(manifest["content_sha256"], "content_sha256")
    if len(digest_text) != 64 or any(
        char not in "0123456789abcdef" for char in digest_text
    ):
        raise ValueError("Invalid snapshot content checksum.")
    return manifest, identity, restoration_id, new_generation


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
