"""Runner inputs and persisted documents; path resolution remains an operation."""

from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ValidationInfo,
    model_validator,
)

from core.models.participant_identity import ParticipantIdentity
from core.models.process_identity import ProcessIdentity
from core.models.values import (
    AbsolutePath,
    Boolean,
    NonnegativeInteger,
    Number,
    PositiveInteger,
    PositiveNumber,
    Text,
    UUIDText,
)
from core.primitives.json_values import (
    JsonObject,
    JsonValue,
    copy_json_object,
)


def _object(value: object, info: ValidationInfo) -> JsonObject:
    return copy_json_object(value, info.field_name or "document")


Object = Annotated[JsonObject, BeforeValidator(_object)]


class _Input(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )


class _Document(_Input):
    @model_validator(mode="before")
    @classmethod
    def detach(cls, value: object) -> JsonObject:
        return copy_json_object(value, "runner document")


class AttemptParameters(_Input):
    attempt_id: UUIDText
    stage_id: UUIDText
    stage_execution_id: UUIDText
    cycle_number: PositiveInteger
    attempt_number: PositiveInteger
    artifacts_directory: AbsolutePath
    input_data: JsonValue
    effective_settings: Object
    timeout_seconds: PositiveNumber | None

    @model_validator(mode="before")
    @classmethod
    def detach_payload(cls, document: object) -> object:
        if (
            isinstance(document, dict)
            and {"input_data", "effective_settings"} <= document.keys()
        ):
            payload = copy_json_object(
                {name: document[name] for name in ("input_data", "effective_settings")},
                "attempt parameters",
            )
            return {**document, **payload}
        return document


class ServiceParameters(_Input):
    service_id: UUIDText
    service_instance_id: UUIDText
    definition: Object

    @model_validator(mode="after")
    def match_definition(self) -> Self:
        if self.definition.get("service_id") != self.service_id:
            raise ValueError("Service definition has a different service_id.")
        return self


class StateParameters(_Input):
    experiment_id: Text
    experiment_directory: AbsolutePath
    run_id: Text
    template_path: AbsolutePath
    template_revision_id: UUIDText
    template_yaml: Text
    template: Object
    mode: Literal["running", "paused"]


class PendingRebuild(_Document):
    operation_id: UUIDText
    snapshot_id: UUIDText
    template_revision_id: UUIDText
    run_id: Text


class PendingInput(_Document):
    request_id: UUIDText
    source_stage_id: UUIDText
    experiment_id: Text
    stage_id: str


class DagDecision(_Document):
    command: Literal["pause", "stop", "move"] | None
    stage_id: str | None = None

    @model_validator(mode="after")
    def match_target(self) -> Self:
        if (self.command == "move") != ("stage_id" in self.model_fields_set):
            raise ValueError("Invalid saved DAG decision fields.")
        return self


class LastDecision(_Document):
    request_id: UUIDText
    source_stage_id: UUIDText
    experiment_id: Text
    decision: DagDecision


class ServiceFailure(_Document):
    code: Text
    message: Text


class ServiceRequest(_Document):
    model_config = ConfigDict(extra="allow")

    owner: Literal["caller", "service"]
    request_id: UUIDText
    command: Text
    args: JsonObject
    queued_monotonic: Number
    deadline_monotonic: Number | None = None
    sent_monotonic: Number | None
    service_instance_id: str | None = None
    timed_out: Boolean


class SavedService(_Document):
    service_id: UUIDText
    service_instance_id: UUIDText
    definition: JsonObject
    process_identity: ProcessIdentity | None
    endpoint_path: Text | None
    ready: Boolean
    started_at: Text | None
    last_status: JsonObject | None
    restart_count: NonnegativeInteger
    pending_requests: list[ServiceRequest]
    active_request: ServiceRequest | None
    start_deadline: Number | None
    ever_ready: Boolean
    stopping: Boolean
    stopped: Boolean
    manually_stopped: Boolean = False
    blocked_action: Literal["pause", "stop"] | None
    failure: ServiceFailure | None
    freeze_id: UUIDText | None
    prepared_freeze_id: UUIDText | None
    artifacts_directory: Text | None
    implementation: Literal["full", "action"]

    @model_validator(mode="before")
    @classmethod
    def preserve_legacy_manual_stop(cls, document: object) -> object:
        if isinstance(document, dict):
            return {"manually_stopped": False, **document}
        return document

    @model_validator(mode="after")
    def match_requests(self) -> Self:
        if self.definition.get("service_id") != self.service_id:
            raise ValueError("Service definition has a different service_id.")
        for request in self.pending_requests:
            if request.sent_monotonic is not None or request.timed_out:
                raise ValueError(
                    "A sent service request cannot re-enter the pending queue."
                )
        if self.active_request is not None and (
            self.active_request.sent_monotonic is None
            or self.active_request.service_instance_id != self.service_instance_id
        ):
            raise ValueError(
                "Active service request has no matching send identity/time."
            )
        return self


class SavedAttempt(_Document):
    attempt_id: UUIDText
    stage_id: UUIDText
    stage_execution_id: UUIDText
    cycle_number: PositiveInteger
    attempt_number: PositiveInteger
    artifacts_directory: str
    input_data: JsonValue
    effective_settings: JsonObject
    timeout_seconds: PositiveNumber | None
    process_identity: JsonObject | None
    started_at: JsonValue
    result_request_id: UUIDText | None
    outcome: JsonValue
    executor_status: JsonValue
    request_id: UUIDText
    participant: ParticipantIdentity
    endpoint_path: str | None
    service_id: UUIDText | None
    queued_monotonic: Number | None
    queued_at: JsonValue

    @model_validator(mode="after")
    def match_participant(self) -> Self:
        if (
            self.service_id is not None
            and self.participant.participant_id != self.service_id
        ):
            raise ValueError("Service attempt identity differs from its service.")
        return self


class SavedStateMetadata(_Document):
    """Inspection header only; opaque state is not reconstructed or integrity checked."""

    model_config = ConfigDict(extra="allow")

    schema_version: Annotated[int, Field(ge=3, le=4)]
    experiment_id: Text


class SavedRunnerState(_Document):
    schema_version: Annotated[int, Field(ge=3, le=4)]
    experiment_id: Text
    run_id: Text
    template_path: str
    template_revision_id: UUIDText
    template_yaml: Text
    template: JsonObject
    mode: Literal["running", "paused"]
    phase: Literal[
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
    pause_requested: Boolean
    cycle_number: PositiveInteger
    stage_position: PositiveInteger
    active_attempt: SavedAttempt | None
    stage_retry_counts: dict[UUIDText, NonnegativeInteger]
    stage_attempt_numbers: dict[UUIDText, NonnegativeInteger]
    services: dict[str, SavedService]
    last_result: JsonValue
    last_result_id: UUIDText | None
    stage_result_ids: dict[UUIDText, UUIDText]
    unknown_state_recovery_count: NonnegativeInteger
    used_request_ids: list[UUIDText]
    stable_snapshot_id: JsonValue
    pending_rebuild: PendingRebuild | None
    pending_advance: Boolean
    stage_result_origins: dict[UUIDText, Text]
    checkpoint_id: UUIDText | None
    owner_identity: ProcessIdentity | None
    pending_input: PendingInput | None
    last_dag_decision: LastDecision | None
    retained_artifacts: list[Text]

    @model_validator(mode="before")
    @classmethod
    def migrate_schema_three(cls, document: object) -> object:
        if (
            isinstance(document, dict)
            and type(document.get("schema_version")) is int
            and document["schema_version"] == 3
        ):
            document = dict(document)
            for name, default in (
                ("pending_input", None),
                ("last_dag_decision", None),
                ("retained_artifacts", []),
            ):
                document.setdefault(name, default)
        return document

    @model_validator(mode="after")
    def validate_references(self) -> Self:
        if (
            self.pending_rebuild is not None
            and self.stable_snapshot_id != self.pending_rebuild.snapshot_id
        ):
            raise ValueError("Pending rebuild must retain its protective snapshot.")
        if len(self.used_request_ids) != len(set(self.used_request_ids)):
            raise ValueError("Duplicate saved request ID.")
        if self.stage_result_origins.keys() - self.stage_result_ids.keys():
            raise ValueError("A result origin requires a saved stage result.")
        if len(self.retained_artifacts) != len(set(self.retained_artifacts)):
            raise ValueError("retained_artifacts must be a unique array of paths.")
        seen = set()
        for identifier, service in self.services.items():
            if identifier != service.service_id:
                raise ValueError("Invalid saved service identity.")
            for request in (
                *service.pending_requests,
                *((service.active_request,) if service.active_request else ()),
            ):
                if (
                    request.request_id not in self.used_request_ids
                    or request.request_id in seen
                ):
                    raise ValueError("Service request ID is missing or duplicated.")
                seen.add(request.request_id)
        self._validate_transitions()
        return self

    def _validate_transitions(self) -> None:
        stages = self.template["stages"]
        identifiers = {item["stage_id"] for item in stages}
        for transfer in (self.pending_input, self.last_dag_decision):
            if transfer is not None and transfer.source_stage_id not in identifiers:
                raise ValueError("Saved transition refers to an unknown source stage.")
        if self.pending_input is not None:
            if self.pending_input.stage_id not in identifiers:
                raise ValueError("Pending input targets an unknown stage.")
            if (
                self.stage_position > len(stages)
                or self.pending_input.stage_id
                != stages[self.stage_position - 1]["stage_id"]
            ):
                raise ValueError("Pending input must belong to the current cursor.")
        if self.last_dag_decision is not None:
            decision = self.last_dag_decision.decision
            if decision.command == "move" and decision.stage_id not in identifiers:
                raise ValueError("Saved move targets an unknown stage.")
