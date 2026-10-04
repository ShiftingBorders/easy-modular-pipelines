"""Journal diagnostic exports and restoration record operations."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO
from uuid import uuid4

from core.journal.events import (
    SCHEMA_VERSION,
    LoggingStateError,
    LoggingStorageError,
    _validated_command_result,
    encode_event,
    validate_journal_identity,
)
from core.primitives.json_values import JsonObject, require_text

if TYPE_CHECKING:
    from core.models.journal_diagnostics import (
        AuthorObservation,
        DiagnosticCommand,
        JournalSnapshotManifest,
    )
    from core.models.journal_records import CommandObservation


def _decode_author_observation(
    event: JsonObject,
    observation_json: str,
    request_id: str,
    identity: JsonObject,
) -> tuple[AuthorObservation, CommandObservation]:
    from core.models.journal_diagnostics import AuthorObservation

    if (
        event["schema_version"] != SCHEMA_VERSION
        or event["event_type"] != "command.result"
    ):
        raise ValueError("Command result refers to an unrelated event.")
    data = _validated_command_result(event["data"])
    if (
        data.request_id != request_id
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
    return observation, data


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
    record: JsonObject, request_ids: set[str], commands: list[DiagnosticCommand]
) -> None:
    from core.models.journal_diagnostics import DiagnosticCommand

    command = DiagnosticCommand.model_validate(record)
    request_id = command.request_id
    if request_id in request_ids:
        raise ValueError("Duplicate diagnostic request ID.")
    request_ids.add(request_id)
    commands.append(command)


def _validate_restore_input(
    snapshot_manifest: JsonObject, restoration_id: str, new_generation: str
) -> tuple[JournalSnapshotManifest, JsonObject, str, str]:
    from core.models.journal_diagnostics import JournalRestorationInput

    restoration = JournalRestorationInput.model_validate(
        {
            "snapshot": snapshot_manifest,
            "restoration_id": restoration_id,
            "new_generation": new_generation,
        }
    )
    manifest = restoration.snapshot
    identity = validate_journal_identity(
        {"journal_id": manifest.journal_id, "generation": manifest.generation}
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
            json.dumps(runner["observation"].model_dump()) if runner else None,
            json.dumps(participant["observation"].model_dump())
            if participant
            else None,
            state[author]["event_id"],
            author,
        ),
    )


def _diagnostic_operation_events(
    reader: sqlite3.Connection, roots: list[str], decode_row: Callable[[tuple], JsonObject]
) -> tuple[dict[str, tuple[str | None, str | None]], dict[str | None, str], set[str], set[str]]:
    metadata = {}
    starts = {}
    for row in reader.execute("SELECT * FROM events ORDER BY cursor"):
        event = decode_row(row)["event"]
        operation_id = event["operation_id"]
        parent = event["context"].get("parent_operation_id")
        metadata[event["event_id"]] = (operation_id, parent)
        if event["event_type"] == "operation.started":
            starts[operation_id] = event["event_id"]
    known = {op for op, _ in metadata.values() if op is not None}
    if not set(roots) <= known:
        raise LoggingStateError(
            "Some selected operations are absent from this journal."
        )
    selected_operations = set(roots)
    while True:
        children = {
            op
            for op, parent in metadata.values()
            if op is not None and parent in selected_operations
        }
        if children <= selected_operations:
            break
        selected_operations.update(children)
    selected = {
        event_id
        for event_id, (op, parent) in metadata.items()
        if op in selected_operations or parent in selected_operations
    }
    return metadata, starts, selected_operations, selected


def _add_diagnostic_observer_events(
    reader: sqlite3.Connection,
    selected_operations: set[str],
    selected: set[str],
    read_result: Callable[[str], dict],
) -> None:
    # A matching reply can exist only in observer metadata. Its raw
    # event may belong to the other participant, outside this tree.
    for (request_id,) in reader.execute("SELECT request_id FROM command_results"):
        state = read_result(request_id)
        for author in ("runner", "participant"):
            entry = state[author]
            if entry is None:
                continue
            observation = entry["observation"]
            if (
                observation.operation_id in selected_operations
                or observation.context.root.get("parent_operation_id")
                in selected_operations
            ):
                selected.add(entry["event_id"])


def _diagnostic_dependencies(
    metadata: dict[str, tuple[str | None, str | None]], starts: dict[str | None, str],
    selected: set[str], read_event: Callable[[str], JsonObject],
    read_result: Callable[[str], dict],
) -> dict[str, dict]:
    pending = list(selected)
    requests = {}
    while pending:
        event_id = pending.pop()
        event = read_event(event_id)["event"]
        dependencies = []
        if event["operation_id"] in starts:
            dependencies.append(starts[event["operation_id"]])
        parent = event["context"].get("parent_operation_id")
        if parent in starts:
            dependencies.append(starts[parent])
        if event["event_type"] in (
            "control.intent",
            "control.observed",
            "control.reconciled",
        ):
            for field in ("intent_event_id", "parameters_event_id"):
                reference = event["data"].get(field)
                if reference is not None:
                    dependencies.append(require_text(reference, field))
        if event["event_type"] == "command.result":
            request_id = event["data"]["request_id"]
            state = read_result(request_id)
            requests[request_id] = state
            dependencies.extend(
                state[author]["event_id"]
                for author in ("runner", "participant")
                if state[author] is not None
            )
        for reference in dependencies:
            if reference not in metadata:
                raise LoggingStorageError(
                    "Diagnostic dependency is missing."
                )
            if reference not in selected:
                selected.add(reference)
                pending.append(reference)
    return requests


def _write_diagnostic_records(
    target: Path,
    metadata: dict[str, tuple[str | None, str | None]],
    selected: set[str],
    requests: dict[str, dict],
    read_event: Callable[[str], JsonObject],
) -> str:
    digest = hashlib.sha256()
    with (target / "records.jsonl.part").open("xb") as output:
        for event_id in metadata:
            if event_id not in selected:
                continue
            record = {
                "kind": "event",
                "event": read_event(event_id)["event"],
            }
            _write_diagnostic_record(output, digest, record)
        for request_id, state in requests.items():
            record = {
                "kind": "command",
                "request_id": request_id,
                "identity": state["identity"],
                "runner": None,
                "participant": None,
            }
            for author in ("runner", "participant"):
                if state[author] is not None:
                    record[author] = {
                        "event_id": state[author]["event_id"],
                        "observation": state[author]["observation"].model_dump(),
                    }
            _write_diagnostic_record(output, digest, record)
        output.flush()
        os.fsync(output.fileno())
    return digest.hexdigest()


def _prepare_diagnostic_events(
    events: list[JsonObject], max_event_bytes: int | None
) -> list[tuple[JsonObject, str]]:
    return [(event, encode_event(event, max_event_bytes)) for event in events]


def _merge_diagnostic_command(
    state: dict | None, record: DiagnosticCommand
) -> tuple[dict, tuple[str, str] | None, bool]:
    before = (
        None
        if state is None
        else (state["effective_author"], state["effective_event_id"])
    )
    if state is None:
        state = {
            "identity": record.identity.model_dump(),
            "runner": None,
            "participant": None,
        }
    if state["identity"] != record.identity.model_dump():
        raise ValueError("Diagnostic request belongs to another context.")
    changed = False
    for author in ("runner", "participant"):
        incoming = record.runner if author == "runner" else record.participant
        current = state[author]
        if incoming is None:
            continue
        if current is not None:
            old = {
                "event_id": current["event_id"],
                "observation": current["observation"].model_dump(),
            }
            if json.dumps(old, sort_keys=True) != json.dumps(
                incoming.model_dump(), sort_keys=True
            ):
                raise ValueError(
                    "Diagnostic observation conflicts with restored history."
                )
        else:
            state[author] = {
                "event_id": incoming.event_id,
                "observation": incoming.observation,
            }
            changed = True
    return state, before, changed


def _diagnostic_manifest(
    roots: list[str], boundary: JsonObject, records_sha256: str,
    event_count: int, command_count: int,
) -> JsonObject:
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": "journal.diagnostics",
        "diagnostics_id": uuid4().hex,
        "journal_id": boundary["journal_id"],
        "generation": boundary["generation"],
        "operation_ids": roots,
        "cursor": boundary["cursor"],
        "change_cursor": boundary["change_cursor"],
        "created_at": datetime.now(UTC).isoformat(timespec="microseconds"),
        "records": "records.jsonl",
        "records_sha256": records_sha256,
        "event_count": event_count,
        "command_count": command_count,
    }
    return manifest
