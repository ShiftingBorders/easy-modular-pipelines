"""Snapshot manifest, export and result validation."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from uuid import UUID

from core.experiments.results import read_result
from core.experiments.state import RunnerState
from core.journal.storage import SQLiteEventStore
from core.primitives.json_values import JsonObject, copy_json_object, require_text


def _validate_snapshot_manifest(document: JsonObject) -> None:
    required = {
        "schema_version",
        "snapshot_id",
        "experiment_id",
        "experiment_folder",
        "created_at",
        "sequence",
        "kind",
        "label",
        "state",
        "services",
        "journal",
        "directories",
        "files",
    }
    if (
        document.keys() != required
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 2
    ):
        raise ValueError("Unsupported experiment snapshot manifest.")
    UUID(require_text(document["snapshot_id"], "snapshot_id"))
    require_text(document["experiment_id"], "experiment_id")
    folder = require_text(document["experiment_folder"], "experiment folder")
    if (
        folder in (".", "..")
        or Path(folder).name != folder
        or any(character in folder for character in '/\\:*?"<>|')
    ):
        raise ValueError("Invalid experiment folder in snapshot.")
    if (
        type(document["sequence"]) is not int
        or not 0 < document["sequence"] <= 9223372036854775807
        or document["kind"] not in ("regular", "final")
    ):
        raise ValueError("Invalid snapshot sequence or kind.")
    if datetime.fromisoformat(document["created_at"]).utcoffset() != UTC.utcoffset(
        None
    ):
        raise ValueError("Snapshot time must be UTC.")
    if document["label"] is not None:
        require_text(document["label"], "snapshot label")


def _validate_snapshot_exports(
    directory: Path, document: JsonObject, state: RunnerState
) -> None:
    if set(state.services) != {
        item["service_id"] for item in state.template["services"]
    }:
        raise ValueError("Snapshot does not describe every service.")
    exports = copy_json_object(document["services"], "snapshot service exports")
    if exports.keys() - state.services.keys():
        raise ValueError("Snapshot exports an unknown service.")
    for service_id, instance in state.services.items():
        definition = next(
            item
            for item in state.template["services"]
            if item["service_id"] == service_id
        )
        if instance.definition != definition:
            raise ValueError(
                "Snapshot service settings differ from the applied template."
            )
        path = exports.get(service_id)
        if (
            instance.active_request
            or instance.pending_requests
            or instance.freeze_id
            or instance.prepared_freeze_id
        ):
            raise ValueError("Snapshot contains unresolved service work.")
        if path is None:
            if instance.definition["state_required"]:
                raise ValueError("Required service export is missing.")
            continue
        name = require_text(path, "service state path")
        member = directory / "files" / name
        allocated = (
            directory
            / "files/shared_data/service_state"
            / service_id
            / document["snapshot_id"]
        )
        if (
            PureWindowsPath(name).anchor
            or ".." in PurePosixPath(name).parts
            or not member.resolve().is_relative_to(allocated.resolve())
            or not member.exists()
        ):
            raise ValueError("Service export escapes its allocated snapshot directory.")


def _validate_journal_results(state: RunnerState, store: SQLiteEventStore) -> None:
    for transfer in (state.pending_input, state.last_dag_decision):
        if transfer is None:
            continue
        record = read_result(
            store,
            transfer["request_id"],
            expected={
                "experiment_id": transfer["experiment_id"],
                "stage_id": transfer["source_stage_id"],
            },
            accepted=True,
        )
        if (
            record is None
            or record["outcome"] != "succeeded"
            or record["response"]["result"] != "success"
        ):
            raise ValueError("Snapshot transition has no accepted result.")
        decision = record["response"].get("execution", {}).get("dag_decision")
        expected = transfer.get(
            "decision",
            {"command": "move", "stage_id": transfer.get("stage_id")},
        )
        if decision != expected:
            raise ValueError("Snapshot transition differs from its journal decision.")
        if (
            transfer["request_id"] == state.last_result_id
            and record["response"]["data"] != state.last_result
        ):
            raise ValueError(
                "Snapshot transferred data differs from its journal result."
            )
    for stage_id, request_id in state.stage_result_ids.items():
        record = read_result(
            store,
            request_id,
            expected={
                "experiment_id": state.stage_result_origins.get(
                    stage_id, state.experiment_id
                ),
                "stage_id": stage_id,
            },
            accepted=True,
        )
        if (
            record is None
            or record["outcome"] != "succeeded"
            or record["response"]["result"] != "success"
        ):
            raise ValueError(
                "Snapshot result is missing or unsuccessful in its journal."
            )
        if (
            request_id == state.last_result_id
            and record["response"]["data"] != state.last_result
        ):
            raise ValueError("Snapshot retained data differs from its journal result.")
