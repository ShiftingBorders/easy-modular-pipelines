"""Plain execution state shared by the runner and its components."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from core.models.runner_state import (
    AttemptParameters,
    SavedAttempt,
    SavedRunnerState,
    SavedService,
    ServiceParameters,
    StateParameters,
)
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import (
    JsonObject,
    JsonValue,
    copy_json_object,
)

type ModuleRole = Literal["stage", "service"]
type RunnerMode = Literal["running", "paused"]
type RunnerPhase = Literal[
    "idle",
    "starting",
    "stage_running",
    "waiting",
    "snapshotting",
    "rebuilding",
    "restoring",
    "stopped",
    "completed",
    "failed",
]


class StageAttempt:
    """One stage attempt, distinct from its definition and automatic retry count."""

    attempt_id: str
    stage_id: str
    stage_execution_id: str
    cycle_number: int
    attempt_number: int
    artifacts_directory: Path
    input_data: JsonValue
    effective_settings: JsonObject
    process_identity: JsonObject | None
    started_at: str | None
    timeout_seconds: float | None
    result_request_id: str | None
    outcome: str | None

    def __init__(
        self,
        attempt_id: str,
        stage_id: str,
        stage_execution_id: str,
        cycle_number: int,
        attempt_number: int,
        artifacts_directory: Path,
        input_data: JsonValue,
        effective_settings: JsonObject,
        timeout_seconds: float | None,
    ) -> None:
        parameters = AttemptParameters(
            attempt_id=attempt_id,
            stage_id=stage_id,
            stage_execution_id=stage_execution_id,
            cycle_number=cycle_number,
            attempt_number=attempt_number,
            artifacts_directory=artifacts_directory,
            input_data=input_data,
            effective_settings=effective_settings,
            timeout_seconds=timeout_seconds,
        )
        self._configure(parameters)

    def _configure(self, parameters: AttemptParameters) -> None:
        self.attempt_id = parameters.attempt_id
        self.stage_id = parameters.stage_id
        self.stage_execution_id = parameters.stage_execution_id
        self.cycle_number = parameters.cycle_number
        self.attempt_number = parameters.attempt_number
        self.artifacts_directory = parameters.artifacts_directory
        self.input_data = parameters.input_data
        self.effective_settings = parameters.effective_settings
        self.timeout_seconds = parameters.timeout_seconds
        self.process_identity = None
        self.started_at = None
        self.result_request_id = None
        self.outcome = None
        self.executor_status: JsonObject | None = None
        self.request_id = parameters.attempt_id
        self.participant: JsonObject | None = None
        self.endpoint_path: Path | None = None
        self.service_id: str | None = None
        self.queued_monotonic: float | None = None
        self.queued_at: str | None = None


class ServiceInstance:
    """Observed service instance and its runner-owned persistent request queue."""

    service_id: str
    service_instance_id: str
    definition: JsonObject
    process_identity: JsonObject | None
    endpoint_path: Path | None
    ready: bool
    started_at: str | None
    last_status: JsonObject | None
    restart_count: int
    pending_requests: list[JsonObject]
    active_request: JsonObject | None

    def __init__(
        self,
        service_id: str,
        service_instance_id: str,
        definition: JsonObject,
    ) -> None:
        self._configure(
            ServiceParameters(
                service_id=service_id,
                service_instance_id=service_instance_id,
                definition=definition,
            )
        )

    def _configure(self, parameters: ServiceParameters) -> None:
        self.service_id = parameters.service_id
        self.service_instance_id = parameters.service_instance_id
        self.definition = parameters.definition
        self.process_identity = None
        self.endpoint_path = None
        self.ready = False
        self.started_at = None
        self.last_status = None
        self.restart_count = 0
        self.pending_requests = []
        self.active_request = None
        self.start_deadline: float | None = None
        self.ever_ready = False
        self.stopping = False
        self.stopped = False
        self.manually_stopped = False
        self.blocked_action: Literal["pause", "stop"] | None = None
        self.failure: JsonObject | None = None
        self.freeze_id: str | None = None
        self.prepared_freeze_id: str | None = None
        self.artifacts_directory: Path | None = None
        self.implementation: Literal["full", "action"] = "full"


class RunnerState:
    """Execution state persisted independently of optional experiment snapshots."""

    experiment_id: str
    experiment_directory: Path
    run_id: str
    template_path: Path
    template_revision_id: str
    template_yaml: str
    template: JsonObject
    mode: RunnerMode
    phase: RunnerPhase
    pause_requested: bool
    cycle_number: int
    stage_position: int
    active_attempt: StageAttempt | None
    stage_retry_counts: dict[str, int]
    stage_attempt_numbers: dict[str, int]
    services: dict[str, ServiceInstance]
    last_result: JsonValue
    last_result_id: str | None
    stage_result_ids: dict[str, str]
    unknown_state_recovery_count: int
    used_request_ids: set[str]
    stable_snapshot_id: str | None
    pending_rebuild: JsonObject | None

    def __init__(
        self,
        experiment_id: str,
        experiment_directory: Path,
        run_id: str,
        template_path: Path,
        template_revision_id: str,
        template_yaml: str,
        template: JsonObject,
        mode: RunnerMode,
    ) -> None:
        self._configure(
            StateParameters(
                experiment_id=experiment_id,
                experiment_directory=experiment_directory,
                run_id=run_id,
                template_path=template_path,
                template_revision_id=template_revision_id,
                template_yaml=template_yaml,
                template=template,
                mode=mode,
            )
        )

    def _configure(self, parameters: StateParameters) -> None:
        self.template = parameters.template
        self.experiment_id = parameters.experiment_id
        self.experiment_directory = parameters.experiment_directory
        self.run_id = parameters.run_id
        self.template_path = parameters.template_path
        self.template_revision_id = parameters.template_revision_id
        self.template_yaml = parameters.template_yaml
        self.mode = parameters.mode
        self.phase = "idle"
        self.pause_requested = False
        self.cycle_number = 1
        self.stage_position = 1
        self.active_attempt = None
        self.stage_retry_counts = {}
        self.stage_attempt_numbers = {}
        self.services = {}
        self.last_result = None
        self.last_result_id = None
        self.stage_result_ids = {}
        self.unknown_state_recovery_count = 0
        self.used_request_ids = set()
        self.stable_snapshot_id = None
        self.pending_rebuild = None
        self.pending_advance = False
        self.stage_result_origins: dict[str, str] = {}
        self.checkpoint_id: str | None = None
        self.owner_identity: JsonObject | None = None
        self.pending_input: JsonObject | None = None
        self.last_dag_decision: JsonObject | None = None
        self.retained_artifacts: list[str] = []


class StageOutcome:
    """Attempt outcome and requested DAG action; only the runner advances the DAG."""

    attempt: StageAttempt
    result: JsonObject | None
    action: Literal["advance", "pause", "stop"]

    def __init__(
        self,
        attempt: StageAttempt,
        result: JsonObject | None,
        action: Literal["advance", "pause", "stop"],
    ) -> None:
        if not isinstance(attempt, StageAttempt):
            raise TypeError("attempt must be a StageAttempt.")
        if action not in ("advance", "pause", "stop"):
            raise ValueError("action must be advance, pause, or stop.")
        self.attempt = attempt
        self.result = (
            None if result is None else copy_json_object(result, "stage result")
        )
        if self.result is not None and (
            self.result.get("result") not in ("success", "fail")
            or "data" not in self.result
        ):
            raise ValueError("Stage result must contain result=success/fail and data.")
        self.action = action


class RunnerStateStore:
    """Read and publish state.json; filesystem errors are reported to its caller."""

    def load(self, experiment_directory: Path) -> RunnerState:
        root = Path(experiment_directory)
        if not root.is_absolute():
            raise ValueError("experiment_directory must be absolute.")
        return state_from_document(
            root.resolve(), read_json(root / "runner" / "state.json")
        )

    def save(self, state: RunnerState) -> None:
        root = state.experiment_directory.resolve()
        document = state_to_document(state)
        state_from_document(root, document)
        destination = root / "runner" / "state.json"
        if not destination.parent.resolve().is_relative_to(root):
            raise ValueError("State directory escapes the experiment.")
        write_json(destination, document)


def _relative_state_path(path: Path, root: Path) -> Path:
    """Compare resolved paths using the same Windows namespace spelling."""
    if os.name == "nt":
        # Python 3.12 can retain this prefix when a file disappears between
        # realpath's native calls. Add it to both comparison paths; never strip
        # it from a resolved target or skip resolution of filesystem links.
        paths = []
        for value in (path, root):
            text = str(value)
            if not text.startswith("\\\\?\\"):
                text = (
                    "\\\\?\\UNC\\" + text[2:]
                    if text.startswith("\\\\")
                    else "\\\\?\\" + text
                )
            paths.append(Path(text))
        path, root = paths
    return path.relative_to(root)


def state_to_document(state: RunnerState) -> JsonObject:
    """Serialize known public records, never live tasks or control queues."""
    root = state.experiment_directory.resolve()
    document = dict(vars(state))
    document.pop("experiment_directory")
    document["schema_version"] = 4
    template_path = state.template_path.resolve()
    document["template_path"] = (
        template_path.relative_to(root).as_posix()
        if template_path.is_relative_to(root)
        else str(template_path)
    )
    document["last_result_id"] = state.last_result_id
    document["stage_result_ids"] = dict(state.stage_result_ids)
    document["used_request_ids"] = sorted(state.used_request_ids)
    document["services"] = {}
    for service_id, instance in state.services.items():
        saved = dict(vars(instance))
        for name in ("endpoint_path", "artifacts_directory"):
            path = saved[name]
            saved[name] = (
                None
                if path is None
                else _relative_state_path(path.resolve(), root).as_posix()
            )
        document["services"][service_id] = saved
    if state.active_attempt is not None:
        attempt = dict(vars(state.active_attempt))
        attempt["artifacts_directory"] = _relative_state_path(
            state.active_attempt.artifacts_directory.resolve(), root
        ).as_posix()
        endpoint = state.active_attempt.endpoint_path
        attempt["endpoint_path"] = (
            None
            if endpoint is None
            else _relative_state_path(endpoint.resolve(), root).as_posix()
        )
        document["active_attempt"] = attempt
    return copy_json_object(document, "runner state")


def state_from_document(root: Path, document: JsonObject) -> RunnerState:
    """Validate the entire document before allocating any runtime records."""
    if not root.is_absolute():
        raise ValueError("Experiment and template paths must be absolute.")
    return _restore_state(root, SavedRunnerState.model_validate(document))


def _saved_path(root: Path, relative: str, label: str) -> Path:
    path = Path(relative)
    resolved = (root / path).resolve()
    if path.anchor:
        raise ValueError(f"Saved {label} escapes the experiment.")
    try:
        _relative_state_path(resolved, root)
    except ValueError:
        raise ValueError(f"Saved {label} escapes the experiment.") from None
    return resolved


def _restore_state(root: Path, document: SavedRunnerState) -> RunnerState:
    values = document.model_dump(exclude_unset=True)
    values.pop("schema_version")
    template_path = Path(document.template_path)
    values["template_path"] = (
        template_path if template_path.is_absolute() else root / template_path
    )
    values["used_request_ids"] = set(document.used_request_ids)
    values["services"] = {
        key: _restore_service(root, service)
        for key, service in document.services.items()
    }
    values["active_attempt"] = (
        None
        if document.active_attempt is None
        else _restore_attempt(root, document.active_attempt)
    )
    artifacts = (root / "shared_artifacts").resolve()
    for relative in document.retained_artifacts:
        path = Path(relative)
        if path.anchor or not (root / path).resolve().is_relative_to(artifacts):
            raise ValueError("Retained artifact path escapes shared_artifacts.")
    # Every field is guaranteed by the saved-document model. Avoid revalidating
    # constructor inputs while restoring an already checked, complete record.
    state = RunnerState.__new__(RunnerState)
    state.experiment_directory = root
    for name, value in values.items():
        setattr(state, name, value)
    return state


def _restore_service(root: Path, document: SavedService) -> ServiceInstance:
    values = document.model_dump(exclude_unset=True)
    for name in ("endpoint_path", "artifacts_directory"):
        if values[name] is not None:
            values[name] = _saved_path(root, values[name], "service path")
    instance = ServiceInstance.__new__(ServiceInstance)
    for name, value in values.items():
        setattr(instance, name, value)
    return instance


def _restore_attempt(root: Path, document: SavedAttempt) -> StageAttempt:
    values = document.model_dump(exclude_unset=True)
    values["artifacts_directory"] = _saved_path(
        root, document.artifacts_directory, "attempt directory"
    )
    if document.endpoint_path is not None:
        values["endpoint_path"] = _saved_path(
            root, document.endpoint_path, "participant endpoint"
        )
    attempt = StageAttempt.__new__(StageAttempt)
    for name, value in values.items():
        setattr(attempt, name, value)
    return attempt


def _attempt_from_launch(
    definition: JsonObject, launched: JsonObject, directory: Path, root: Path
) -> StageAttempt:
    attempt = StageAttempt(
        launched["attempt_id"],
        launched["stage_id"],
        launched["stage_execution_id"],
        launched["cycle_number"],
        launched["attempt_number"],
        directory,
        {},
        {},
        definition["timeout_seconds"],
    )
    # Bind ownership before optional files are read so failure
    # handling still has to confirm this attempt's termination.
    attempt.request_id = launched["request_id"]
    attempt.participant = {
        key: launched[key]
        for key in (
            "experiment_id",
            "participant_id",
            "participant_instance_id",
        )
    }
    attempt.service_id = definition.get("service_id")
    attempt.endpoint_path = (
        root / "runner/endpoints" / f"{attempt.service_id}.json"
        if attempt.service_id is not None
        else directory / "executor.lock.json"
    )
    attempt.queued_monotonic = launched["queued_monotonic"]
    attempt.queued_at = launched["queued_at"]
    return attempt


def _apply_recovered_attempt_context(attempt: StageAttempt, context: JsonObject) -> None:
    attempt.input_data = context["input_data"]
    attempt.effective_settings = context["settings"]
    attempt.request_id = context["context"]["request_id"]
    attempt.participant = {
        key: context["context"][key]
        for key in (
            "experiment_id",
            "participant_id",
            "participant_instance_id",
        )
    }
    attempt.endpoint_path = Path(context["endpoint_path"])
    attempt.service_id = context["service_id"]
    attempt.queued_at = context["queued_at"]
    attempt.queued_monotonic = context["queued_monotonic"]
