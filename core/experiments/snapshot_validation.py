"""Snapshot manifest, export and result validation."""

from __future__ import annotations

from pathlib import Path, PurePosixPath, PureWindowsPath

from core.experiments.results import read_result
from core.experiments.state import RunnerState
from core.journal.storage import SQLiteEventStore
from core.models.snapshot_documents import SnapshotManifest
from core.primitives.json_values import JsonObject, copy_json_object, require_text


def _validate_snapshot_manifest(document: JsonObject) -> None:
    SnapshotManifest.model_validate(document)


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
