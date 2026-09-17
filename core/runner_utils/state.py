"""Plain execution state shared by the runner and its components."""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from uuid import UUID

from core.logger_utils.events import copy_json_object, require_number, require_text
from core.runner_utils.protocol import participant_identity
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
        self.result_request_id = None
        self.outcome = None
        self.executor_status: JsonObject | None = None
        self.request_id = attempt_id
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
        UUID(require_text(service_id, "service_id"))
        UUID(require_text(service_instance_id, "service_instance_id"))
        self.definition = copy_json_object(definition, "service definition")
        if self.definition.get("service_id") != service_id:
            raise ValueError("Service definition has a different service_id.")
        self.service_id = service_id
        self.service_instance_id = service_instance_id
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
    root = state.experiment_directory.resolve()
    document = dict(vars(state))
    document.pop("experiment_directory")
    document["schema_version"] = 3
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
                None if path is None else path.resolve().relative_to(root).as_posix()
            )
        document["services"][service_id] = saved
    if state.active_attempt is not None:
        attempt = dict(vars(state.active_attempt))
        attempt["artifacts_directory"] = (
            state.active_attempt.artifacts_directory.resolve()
            .relative_to(root)
            .as_posix()
        )
        endpoint = state.active_attempt.endpoint_path
        attempt["endpoint_path"] = (
            None
            if endpoint is None
            else endpoint.resolve().relative_to(root).as_posix()
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
        "last_result_id",
        "stage_result_ids",
        "unknown_state_recovery_count",
        "used_request_ids",
        "stable_snapshot_id",
        "pending_rebuild",
        "pending_advance",
        "stage_result_origins",
        "checkpoint_id",
        "owner_identity",
    }
    if (
        document.keys() != fields
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 3
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
    if (
        type(document["pause_requested"]) is not bool
        or type(document["services"]) is not dict
    ):
        raise ValueError("Invalid pause flag or service state.")
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
    if type(document["pending_advance"]) is not bool:
        raise TypeError("pending_advance must be a boolean.")
    state.pending_advance = document["pending_advance"]
    if document["checkpoint_id"] is not None:
        UUID(require_text(document["checkpoint_id"], "checkpoint_id"))
    state.checkpoint_id = document["checkpoint_id"]
    if document["owner_identity"] is not None:
        owner = copy_json_object(document["owner_identity"], "runner owner")
        if owner.keys() != {"pid", "created_at_os", "host_id", "boot_id"}:
            raise ValueError("Runner owner requires complete OS identity.")
        for key in ("pid", "created_at_os"):
            if type(owner[key]) is not int or owner[key] < (1 if key == "pid" else 0):
                raise ValueError("Invalid runner owner identity.")
        require_text(owner["host_id"], "owner host_id")
        require_text(owner["boot_id"], "owner boot_id")
        state.owner_identity = owner
    origins = copy_json_object(document["stage_result_origins"], "stage result origins")
    if type(document["stage_result_ids"]) is not dict:
        raise TypeError("stage_result_ids must be an object.")
    if origins.keys() - document["stage_result_ids"].keys():
        raise ValueError("A result origin requires a saved stage result.")
    for stage_id, origin in origins.items():
        UUID(stage_id)
        require_text(origin, "result experiment ID")
    state.stage_result_origins = origins
    service_request_ids = set()
    for service_id, saved in document["services"].items():
        saved = copy_json_object(saved, "service state")
        instance = ServiceInstance(
            saved["service_id"],
            saved["service_instance_id"],
            saved["definition"],
        )
        if service_id != instance.service_id or saved.keys() != vars(instance).keys():
            raise ValueError("Invalid saved service fields or identity.")
        for key in ("ready", "ever_ready", "stopping", "stopped"):
            if type(saved[key]) is not bool:
                raise TypeError(f"service.{key} must be a boolean.")
        if type(saved["restart_count"]) is not int or saved["restart_count"] < 0:
            raise ValueError("Invalid service restart count.")
        if saved["blocked_action"] not in (None, "pause", "stop") or saved[
            "implementation"
        ] not in ("full", "action"):
            raise ValueError("Invalid saved service mode.")
        if saved["start_deadline"] is not None:
            require_number(saved["start_deadline"], "service start deadline")
        if saved["process_identity"] is not None:
            identity = copy_json_object(
                saved["process_identity"], "service process identity"
            )
            if identity.keys() != {"pid", "created_at_os", "host_id", "boot_id"}:
                raise ValueError("Service requires complete OS identity.")
            for name in ("pid", "created_at_os"):
                if type(identity[name]) is not int or identity[name] < (
                    1 if name == "pid" else 0
                ):
                    raise ValueError("Invalid service OS identity value.")
            for name in ("host_id", "boot_id"):
                require_text(identity[name], name)
        for name in ("started_at",):
            if saved[name] is not None:
                require_text(saved[name], name)
        if saved["failure"] is not None:
            failure = copy_json_object(saved["failure"], "service failure")
            if failure.keys() != {"code", "message"}:
                raise ValueError("Service failure requires code and message.")
            require_text(failure["code"], "failure code")
            require_text(failure["message"], "failure message")
        if saved["last_status"] is not None:
            copy_json_object(saved["last_status"], "service status")
        for key in ("freeze_id", "prepared_freeze_id"):
            if saved[key] is not None:
                UUID(saved[key])
        if type(saved["pending_requests"]) is not list:
            raise TypeError("Service pending_requests must be an array.")
        requests = [*saved["pending_requests"]]
        if saved["active_request"] is not None:
            requests.append(saved["active_request"])
        for index, request in enumerate(requests):
            request = copy_json_object(request, "service request")
            if request.get("owner") not in ("caller", "service"):
                raise ValueError("Saved request has an invalid policy owner.")
            require_number(request["queued_monotonic"], "request queue time")
            if request.get("deadline_monotonic") is not None:
                require_number(request["deadline_monotonic"], "request deadline")
            request_id = require_text(request["request_id"], "request_id")
            UUID(request_id)
            if (
                request_id not in state.used_request_ids
                or request_id in service_request_ids
            ):
                raise ValueError("Service request ID is missing or duplicated.")
            service_request_ids.add(request_id)
            require_text(request["command"], "service command")
            copy_json_object(request["args"], "service command arguments")
            if request["sent_monotonic"] is not None:
                require_number(request["sent_monotonic"], "service send time")
            if index < len(saved["pending_requests"]):
                if request["sent_monotonic"] is not None or request["timed_out"]:
                    raise ValueError(
                        "A sent service request cannot re-enter the pending queue."
                    )
            elif (
                request["sent_monotonic"] is None
                or request["service_instance_id"] != instance.service_instance_id
            ):
                raise ValueError(
                    "Active service request has no matching send identity/time."
                )
            if type(request["timed_out"]) is not bool:
                raise TypeError("Service timed_out must be a boolean.")
        for name in ("endpoint_path", "artifacts_directory"):
            if saved[name] is not None:
                relative = Path(require_text(saved[name], name))
                resolved = (root / relative).resolve()
                if relative.anchor or not resolved.is_relative_to(root):
                    raise ValueError("Saved service path escapes the experiment.")
                saved[name] = resolved
        for name, value in saved.items():
            setattr(instance, name, value)
        state.services[service_id] = instance
    for stage_id, request_id in document["stage_result_ids"].items():
        UUID(stage_id)
        UUID(require_text(request_id, "result request ID"))
        state.stage_result_ids[stage_id] = request_id
    if document["last_result_id"] is not None:
        UUID(require_text(document["last_result_id"], "last result ID"))
        state.last_result_id = document["last_result_id"]
    if document["active_attempt"] is not None:
        attempt = copy_json_object(document["active_attempt"], "active_attempt")
        observed = {
            key: attempt.pop(key)
            for key in (
                "process_identity",
                "started_at",
                "result_request_id",
                "outcome",
                "executor_status",
                "request_id",
                "participant",
                "endpoint_path",
                "service_id",
                "queued_monotonic",
                "queued_at",
            )
        }
        relative = Path(attempt["artifacts_directory"])
        attempt["artifacts_directory"] = (root / relative).resolve()
        if relative.anchor or not attempt["artifacts_directory"].is_relative_to(root):
            raise ValueError("Saved attempt directory escapes the experiment.")
        state.active_attempt = StageAttempt(**attempt)
        UUID(require_text(observed["request_id"], "request_id"))
        if observed["result_request_id"] is not None:
            UUID(require_text(observed["result_request_id"], "result request ID"))
        if observed["participant"] is not None:
            participant_identity(observed["participant"])
        else:
            raise ValueError("An active attempt requires a participant identity.")
        if observed["service_id"] is not None:
            UUID(require_text(observed["service_id"], "service ID"))
            if observed["participant"]["participant_id"] != observed["service_id"]:
                raise ValueError("Service attempt identity differs from its service.")
        if observed["queued_monotonic"] is not None:
            require_number(observed["queued_monotonic"], "queue time")
        if observed["endpoint_path"] is not None:
            relative = Path(observed["endpoint_path"])
            resolved = (root / relative).resolve()
            if relative.anchor or not resolved.is_relative_to(root):
                raise ValueError("Saved participant endpoint escapes the experiment.")
            observed["endpoint_path"] = resolved
        for key, value in observed.items():
            setattr(state.active_attempt, key, value)
    return state
