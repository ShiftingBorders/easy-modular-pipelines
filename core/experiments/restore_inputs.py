"""Computed restoration paths and state-document preparation without I/O."""

from dataclasses import dataclass
from pathlib import Path

from core.experiments.state import RunnerState, state_to_document
from core.models.runner_state import SavedRunnerState, SavedService
from core.models.snapshot_documents import SnapshotPayload
from core.models.updates import _update_model


@dataclass(frozen=True)
class RestorePaths:
    """Target, workspace, cached snapshot, replacement, and prior-file restore paths."""
    target: Path
    work: Path
    cached: Path
    replacement: Path
    previous: Path


def _snapshot_state(
    state: RunnerState, snapshot_id: str, kind: str
) -> SavedRunnerState:
    saved = SavedRunnerState.model_validate(state_to_document(state))
    services = {
        key: _update_model(
            instance, freeze_id=None, prepared_freeze_id=None
        ).model_dump(exclude_unset=True)
        for key, instance in saved.services.items()
    }
    return _update_model(
        saved,
        phase=state.phase if kind == "final" else "waiting",
        mode="paused",
        pause_requested=False,
        stable_snapshot_id=snapshot_id,
        services=services,
    )


def _restored_saved_state(
    manifest: SnapshotPayload,
    stopped_state: SavedRunnerState,
    experiment_id: str,
    run_id: str,
) -> SavedRunnerState:
    saved = manifest.state
    last_decision = saved.last_dag_decision
    if (
        saved.phase == "stopped"
        and last_decision is not None
        and last_decision.decision.command == "stop"
    ):
        # The stop completed in this snapshot. A continuation resumes
        # after the condition, rather than issuing the stop again.
        last_decision = None
    origins = dict(saved.stage_result_origins)
    for stage_id in saved.stage_result_ids:
        origins.setdefault(stage_id, manifest.experiment_id)
    services = {
        key: _restored_service(instance).model_dump(exclude_unset=True)
        for key, instance in saved.services.items()
    }
    return _update_model(
        saved,
        experiment_id=experiment_id,
        run_id=run_id,
        template_path="experiment.yaml",
        mode="paused",
        phase="restoring",
        pause_requested=False,
        checkpoint_id=None,
        owner_identity=None,
        last_dag_decision=None
        if last_decision is None
        else last_decision.model_dump(exclude_unset=True),
        used_request_ids=sorted(
            set(saved.used_request_ids) | set(stopped_state.used_request_ids)
        ),
        stage_result_origins=origins,
        services=services,
    )


def _restored_service(instance: SavedService) -> SavedService:
    return _update_model(
        instance,
        process_identity=None,
        ready=False,
        ever_ready=False,
        started_at=None,
        start_deadline=None,
        last_status=None,
        stopping=False,
        stopped=True,
        blocked_action=None,
        failure=None,
        freeze_id=None,
        prepared_freeze_id=None,
        active_request=None,
        pending_requests=[],
    )
