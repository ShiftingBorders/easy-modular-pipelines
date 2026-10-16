"""Snapshot manifest, export and result validation."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath, PureWindowsPath
from uuid import UUID

from core.experiments.results import read_result
from core.experiments.state import RunnerState
from core.journal.storage import SQLiteEventStore
from core.models.journal_diagnostics import JournalSnapshotManifest
from core.models.runner_state import LastDecision, SavedRunnerState
from core.models.snapshot_documents import (
    SnapshotInventory,
    SnapshotManifest,
    SnapshotPayload,
    SnapshotRetentionHeader,
)
from core.primitives.json_files import read_json
from core.primitives.json_values import JsonObject, copy_json_object, require_text


def _retention_candidates(
    parent: Path, experiment_id: str, experiment_folder: str
) -> tuple[list[tuple[int, Path]], list[Path]]:
    """Enumerate owned UUID folders; metadata only orders candidates to retain."""
    candidates: list[tuple[int, Path]] = []
    owned: list[Path] = []
    for path in parent.iterdir():
        if path.is_symlink() or path.is_junction() or not path.is_dir():
            continue
        try:
            if str(UUID(path.name)) != path.name:
                continue
        except ValueError:
            continue
        owned.append(path)
        manifest = path / "manifest.json"
        if manifest.is_symlink() or manifest.is_junction():
            continue
        try:
            header = SnapshotRetentionHeader.model_validate(read_json(manifest))
            if (
                header.snapshot_id == path.name
                and header.experiment_id == experiment_id
                and header.experiment_folder == experiment_folder
            ):
                candidates.append((header.sequence, path))
        except (OSError, ValueError, TypeError):
            continue
    return sorted(candidates, reverse=True), owned


def _validate_snapshot_manifest(document: JsonObject) -> SnapshotPayload:
    metadata = SnapshotManifest.model_validate(document)
    inventory = SnapshotInventory.model_validate(
        {"directories": metadata.directories, "files": metadata.files}
    )
    state = SavedRunnerState.model_validate(metadata.state)
    journal = JournalSnapshotManifest.model_validate(metadata.journal)
    return SnapshotPayload.model_validate(
        {
            "schema_version": metadata.schema_version,
            "snapshot_id": metadata.snapshot_id,
            "experiment_id": metadata.experiment_id,
            "experiment_folder": metadata.experiment_folder,
            "created_at": metadata.created_at,
            "sequence": metadata.sequence,
            "kind": metadata.kind,
            "label": metadata.label,
            "state": state,
            "services": copy_json_object(metadata.services, "snapshot service exports"),
            "journal": journal,
            "inventory": inventory,
            "encoded_document": json.dumps(
                document, ensure_ascii=False, allow_nan=False
            ),
        }
    )


def _validate_snapshot_exports(
    directory: Path, document: SnapshotPayload, state: RunnerState
) -> None:
    exports = document.services
    for service_id in state.services:
        path = exports.get(service_id)
        if path is None:
            continue
        name = require_text(path, "service state path")
        member = directory / "files" / name
        allocated = (
            directory
            / "files/shared_data/service_state"
            / service_id
            / document.snapshot_id
        )
        if (
            PureWindowsPath(name).anchor
            or ".." in PurePosixPath(name).parts
            or not member.resolve().is_relative_to(allocated.resolve())
            or not member.exists()
        ):
            raise ValueError("Service export escapes its allocated snapshot directory.")


def _validate_journal_results(state: RunnerState, store: SQLiteEventStore) -> None:
    for transfer in (
        state.pending_input, state.last_dag_decision, state.pending_dag_decision
    ):
        if transfer is None:
            continue
        record = read_result(
            store,
            transfer.request_id,
            expected={
                "experiment_id": transfer.experiment_id,
                "stage_id": transfer.source_stage_id,
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
        expected = (
            transfer.decision.model_dump(exclude_unset=True)
            if isinstance(transfer, LastDecision)
            else {"command": "move", "stage_id": transfer.stage_id}
        )
        if decision != expected:
            raise ValueError("Snapshot transition differs from its journal decision.")
        if (
            transfer.request_id == state.last_result_id
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
