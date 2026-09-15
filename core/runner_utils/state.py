"""Plain execution state shared by the runner and its components."""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from uuid import UUID

from core.logger_utils.events import copy_json_object, require_number, require_text
from core.runner_utils.runtimeio import read_json, write_json

type JsonValue = (
    None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
)
type JsonObject = dict[str, JsonValue]
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
    result_path: Path | None
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
        for name, value in (
            ("attempt_id", attempt_id),
            ("stage_id", stage_id),
            ("stage_execution_id", stage_execution_id),
        ):
            UUID(require_text(value, name))
        for name, value in (
            ("cycle_number", cycle_number),
            ("attempt_number", attempt_number),
        ):
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer.")
        artifacts_directory = Path(artifacts_directory)
        if not artifacts_directory.is_absolute():
            raise ValueError("artifacts_directory must be absolute.")
        if timeout_seconds is not None:
            require_number(timeout_seconds, "timeout_seconds")
            if timeout_seconds <= 0:
                raise ValueError("timeout_seconds must be positive or null.")
        payload = copy_json_object(
            {"input_data": input_data, "effective_settings": effective_settings},
            "attempt parameters",
        )
        if type(effective_settings) is not dict:
            raise TypeError("effective_settings must be a JSON object.")
        self.attempt_id = attempt_id
        self.stage_id = stage_id
        self.stage_execution_id = stage_execution_id
        self.cycle_number = cycle_number
        self.attempt_number = attempt_number
        self.artifacts_directory = artifacts_directory
        self.input_data = payload["input_data"]
        self.effective_settings = payload["effective_settings"]
        self.timeout_seconds = timeout_seconds
        self.process_identity = None
        self.started_at = None
        self.result_path = None
        self.outcome = None
        self.executor_status: JsonObject | None = None


class ServiceInstance:
    """Observed service instance and its runner-owned persistent request queue."""

    service_id: str
    service_instance_id: str
    definition: JsonObject
    interface: Literal["socket", "commands"]
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
        interface: Literal["socket", "commands"],
    ) -> None:
        UUID(require_text(service_id, "service_id"))
        UUID(require_text(service_instance_id, "service_instance_id"))
        if interface not in ("socket", "commands"):
            raise ValueError("interface must be socket or commands.")
        self.definition = copy_json_object(definition, "service definition")
        if self.definition.get("service_id") != service_id:
            raise ValueError("Service definition has a different service_id.")
        self.service_id = service_id
        self.service_instance_id = service_instance_id
        self.interface = interface
        self.process_identity = None
        self.endpoint_path = None
        self.ready = False
        self.started_at = None
        self.last_status = None
        self.restart_count = 0
        self.pending_requests = []
        self.active_request = None


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
    last_result_path: Path | None
    stage_result_paths: dict[str, Path]
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
        require_text(experiment_id, "experiment_id")
        require_text(run_id, "run_id")
        UUID(require_text(template_revision_id, "template_revision_id"))
        require_text(template_yaml, "template_yaml")
        experiment_directory = Path(experiment_directory)
        template_path = Path(template_path)
        if not experiment_directory.is_absolute() or not template_path.is_absolute():
            raise ValueError("Experiment and template paths must be absolute.")
        if mode not in ("running", "paused"):
            raise ValueError("mode must be running or paused.")
        self.template = copy_json_object(template, "template")
        self.experiment_id = experiment_id
        self.experiment_directory = experiment_directory
        self.run_id = run_id
        self.template_path = template_path
        self.template_revision_id = template_revision_id
        self.template_yaml = template_yaml
        self.mode = mode
        self.phase = "idle"
        self.pause_requested = False
        self.cycle_number = 1
        self.stage_position = 1
        self.active_attempt = None
        self.stage_retry_counts = {}
        self.stage_attempt_numbers = {}
        self.services = {}
        self.last_result = None
        self.last_result_path = None
        self.stage_result_paths = {}
        self.unknown_state_recovery_count = 0
        self.used_request_ids = set()
        self.stable_snapshot_id = None
        self.pending_rebuild = None


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


def state_to_document(state: RunnerState) -> JsonObject:
    """Serialize known public records, never live tasks or control queues."""
    if state.services:
        raise NotImplementedError(
            "Persistent services belong to the next runtime phase."
        )
    root = state.experiment_directory.resolve()
    document = dict(vars(state))
    document.pop("experiment_directory")
    document["schema_version"] = 1
    document["template_path"] = str(state.template_path)
    document["last_result_path"] = (
        None
        if state.last_result_path is None
        else state.last_result_path.resolve().relative_to(root).as_posix()
    )
    document["stage_result_paths"] = {
        key: value.resolve().relative_to(root).as_posix()
        for key, value in state.stage_result_paths.items()
    }
    document["used_request_ids"] = sorted(state.used_request_ids)
    if state.active_attempt is not None:
        attempt = dict(vars(state.active_attempt))
        attempt["artifacts_directory"] = (
            state.active_attempt.artifacts_directory.resolve()
            .relative_to(root)
            .as_posix()
        )
        attempt["result_path"] = (
            None
            if state.active_attempt.result_path is None
            else state.active_attempt.result_path.resolve().relative_to(root).as_posix()
        )
        document["active_attempt"] = attempt
    return copy_json_object(document, "runner state")


def state_from_document(root: Path, document: JsonObject) -> RunnerState:
    document = copy_json_object(document, "runner state")
    fields = {
        "schema_version",
        "experiment_id",
        "run_id",
        "template_path",
        "template_revision_id",
        "template_yaml",
        "template",
        "mode",
        "phase",
        "pause_requested",
        "cycle_number",
        "stage_position",
        "active_attempt",
        "stage_retry_counts",
        "stage_attempt_numbers",
        "services",
        "last_result",
        "last_result_path",
        "stage_result_paths",
        "unknown_state_recovery_count",
        "used_request_ids",
        "stable_snapshot_id",
        "pending_rebuild",
    }
    if (
        document.keys() != fields
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
    ):
        raise ValueError("Unsupported runner state schema.")
    template_path = Path(document["template_path"])
    if not template_path.is_absolute():
        template_path = root / template_path
    state = RunnerState(
        document["experiment_id"],
        root,
        document["run_id"],
        template_path,
        document["template_revision_id"],
        document["template_yaml"],
        document["template"],
        document["mode"],
    )
    if document["phase"] not in (
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
    ):
        raise ValueError("Invalid saved phase.")
    if type(document["pause_requested"]) is not bool or document["services"] != {}:
        raise ValueError("Invalid pause flag or unsupported service state.")
    for key in ("cycle_number", "stage_position", "unknown_state_recovery_count"):
        minimum = 0 if key == "unknown_state_recovery_count" else 1
        if type(document[key]) is not int or document[key] < minimum:
            raise ValueError(f"Invalid saved {key}.")
    for key in ("stage_retry_counts", "stage_attempt_numbers"):
        if type(document[key]) is not dict:
            raise TypeError(f"{key} must be an object.")
        for definition_id, count in document[key].items():
            UUID(definition_id)
            if type(count) is not int or count < 0:
                raise ValueError("Saved counts must be nonnegative integers.")
    for key in (
        "phase",
        "pause_requested",
        "cycle_number",
        "stage_position",
        "stage_retry_counts",
        "stage_attempt_numbers",
        "last_result",
        "unknown_state_recovery_count",
        "stable_snapshot_id",
        "pending_rebuild",
    ):
        setattr(state, key, document[key])
    if type(document["used_request_ids"]) is not list:
        raise TypeError("used_request_ids must be an array.")
    for request_id in document["used_request_ids"]:
        UUID(request_id)
    state.used_request_ids = set(document["used_request_ids"])
    if len(state.used_request_ids) != len(document["used_request_ids"]):
        raise ValueError("Duplicate saved request ID.")
    if type(document["stage_result_paths"]) is not dict:
        raise TypeError("stage_result_paths must be an object.")
    paths = dict(document["stage_result_paths"])
    if document["last_result_path"] is not None:
        paths["last_result_path"] = document["last_result_path"]
    for key, value in paths.items():
        relative = Path(require_text(value, key))
        resolved = (root / relative).resolve()
        if relative.anchor or not resolved.is_relative_to(root):
            raise ValueError("Saved result path escapes the experiment.")
        if key == "last_result_path":
            state.last_result_path = resolved
        else:
            UUID(key)
            state.stage_result_paths[key] = resolved
    if document["active_attempt"] is not None:
        attempt = copy_json_object(document["active_attempt"], "active_attempt")
        observed = {
            key: attempt.pop(key)
            for key in (
                "process_identity",
                "started_at",
                "result_path",
                "outcome",
                "executor_status",
            )
        }
        relative = Path(attempt["artifacts_directory"])
        attempt["artifacts_directory"] = (root / relative).resolve()
        if relative.anchor or not attempt["artifacts_directory"].is_relative_to(root):
            raise ValueError("Saved attempt directory escapes the experiment.")
        state.active_attempt = StageAttempt(**attempt)
        if observed["result_path"] is not None:
            relative = Path(observed["result_path"])
            observed["result_path"] = (root / relative).resolve()
            if relative.anchor or not observed["result_path"].is_relative_to(root):
                raise ValueError("Saved attempt result escapes the experiment.")
        for key, value in observed.items():
            setattr(state.active_attempt, key, value)
    return state
