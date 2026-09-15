"""Consistent experiment snapshots and restoration, coordinated inside the runner.

This component owns snapshot transactions. Ordinary exchange archive import
and export belong to core.experimentarchiver and are not implemented here.
"""

from __future__ import annotations

from pathlib import Path

from core.runner_utils.journal import RunnerJournal
from core.runner_utils.services import ServiceManager
from core.runner_utils.stages import StageRunner
from core.runner_utils.state import JsonObject, RunnerState, RunnerStateStore


class ExperimentSnapshots:
    """Coordinate participant barriers, snapshot files, and journal restoration."""

    _project_root: Path
    _stages: StageRunner
    _services: ServiceManager
    _journal: RunnerJournal
    _state_store: RunnerStateStore

    def __init__(
        self,
        project_root: Path,
        stages: StageRunner,
        services: ServiceManager,
        journal: RunnerJournal,
        state_store: RunnerStateStore,
    ) -> None:
        # Retain collaborators; transaction markers live outside experiment folders.
        pass

    async def create(self, state: RunnerState, label: str | None = None) -> JsonObject:
        # Require a stage-free barrier and check storage.min_snapshot_free_bytes.
        # Freeze/export services, export a consistent journal, then copy stable files.
        # Invalidate on service restart or lost consistency; always confirm unfreeze.
        # Exclude snapshot archives themselves; publish only complete valid snapshots
        # and enforce snapshots.keep. Return ID/path; propagate failures to the runner.
        # The runner chooses whether the reason was regular, explicit, or pre-rebuild.
        pass

    async def finalize(self, state: RunnerState) -> JsonObject:
        # After stage interruption, save service state into storage that survives stop.
        # Stop services even if saving fails; journal stop outcomes before final export.
        # Publish final snapshot data separately from diagnostic-only/incomplete exports.
        # Report unconfirmed stops and invalid state; neither permits continuation.
        pass

    async def restore(
        self,
        state: RunnerState,
        snapshot_id: str,
        *,
        source_directory: Path | None = None,
        preserve_rebuild_diagnostics: bool = False,
    ) -> RunnerState:
        # Validate the complete snapshot and confirm journal intent before mutations.
        # Persist controller/restore_transaction.json outside the replaced directory.
        # Stop stage/services and old writers, restore files, then complete journal
        # restoration through RunnerJournal. Reopen clients with the new generation.
        # Load required service state and await readiness; return the saved cycle/pointer.
        # Optional source_directory clones into this new instance without changing its
        # ID/folder or the source; resolve internal paths against the destination root.
        # Preserve failed-rebuild diagnostics outside replaced files and restore once.
        # Any partial failure propagates for a full error stop; never auto-resume here.
        pass

    def latest_valid(self, experiment_directory: Path) -> JsonObject:
        # Select a fully validated ready snapshot with runner/files/journal/service state.
        # Reject absent/incomplete snapshots; exact experiment manifest schema is TBD.
        pass
