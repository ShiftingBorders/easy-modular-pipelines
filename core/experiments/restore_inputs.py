"""Computed restoration paths and state-document preparation without I/O."""

from dataclasses import dataclass
from pathlib import Path

from core.primitives.json_values import JsonObject, copy_json_object


@dataclass(frozen=True)
class RestorePaths:
    target: Path
    work: Path
    cached: Path
    replacement: Path
    previous: Path


def _restored_state_document(
    manifest: JsonObject, stopped_state: JsonObject, experiment_id: str, run_id: str
) -> JsonObject:
    document = copy_json_object(manifest["state"], "restored state")
    document["experiment_id"] = experiment_id
    document["run_id"] = run_id
    document["template_path"] = "experiment.yaml"
    document["mode"], document["phase"], document["pause_requested"] = (
        "paused",
        "restoring",
        False,
    )
    document["checkpoint_id"] = None
    document["owner_identity"] = None
    if (
        manifest["state"]["phase"] == "stopped"
        and document.get("last_dag_decision") is not None
        and document["last_dag_decision"]["decision"]["command"] == "stop"
    ):
        # The stop completed in this snapshot. A continuation resumes
        # after the condition, rather than issuing the stop again.
        document["last_dag_decision"] = None
    document["used_request_ids"] = sorted(
        set(document["used_request_ids"])
        | set(stopped_state["used_request_ids"])
    )
    for stage_id in document["stage_result_ids"]:
        document["stage_result_origins"].setdefault(
            stage_id, manifest["experiment_id"]
        )
    for instance in document["services"].values():
        instance.update(
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
    return document
