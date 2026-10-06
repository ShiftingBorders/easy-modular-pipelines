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

from core.models.experiment_template import ExperimentTemplate, ServiceDefinition
from core.models.participant_identity import ParticipantIdentity
from core.models.participant_observations import (
    CommandWork,
    ExecutorCommandState,
    RetainedExecutorStatus,
)
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
    """Strict runner input whose runtime effects are handled separately."""
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )


class _Document(_Input):
    """Persisted runner document detached from caller-owned JSON data."""
    @model_validator(mode="before")
    @classmethod
    def detach(cls, value: object) -> object:
        """Return a validated JSON copy of a runner document.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(value, "runner document")


class AttemptParameters(_Input):
    """Fixed attempt identity, input, effective settings, and artifact directory.

    Args:
        attempt_id: UUID identifying one stage attempt.
        stage_id: Stable UUID of a node in the applied DAG.
        stage_execution_id: UUID identifying a stage visit; automatic retries
            retain this visit identity.
        cycle_number: One-based DAG cycle number.
        attempt_number: One-based attempt number for this stage in the current
            cycle.
        artifacts_directory: Absolute writable directory allocated for this
            invocation's artifacts.
        input_data: Application JSON input accepted for this call; explicit null
            is valid input.
        effective_settings: Detached effective settings after applying module
            defaults and template overrides.
        timeout_seconds: Positive attempt timeout in seconds, including queue
            time; None disables the deadline.
    """
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
        """Copy supplied input and effective settings before field validation.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
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


class RecoveryLaunchEvidence(_Document):
    """Selected committed start context; queue metadata retains its old values.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        participant_id: UUID of the stage or service represented by the
            participant.
        participant_instance_id: UUID distinguishing this particular participant
            process/attempt from replacements.
        attempt_id: UUID identifying one stage attempt.
        stage_id: Stable UUID of a node in the applied DAG.
        stage_execution_id: UUID identifying a stage visit; automatic retries
            retain this visit identity.
        cycle_number: One-based DAG cycle number.
        attempt_number: One-based attempt number for this stage in the current
            cycle.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        queued_at: Wall-clock time at which the attempt entered the queue.
        queued_monotonic: Local monotonic admission time in seconds; attempt
            deadlines include queue time.
    """

    model_config = ConfigDict(extra="allow")

    experiment_id: Text
    participant_id: UUIDText
    participant_instance_id: UUIDText
    attempt_id: UUIDText
    stage_id: UUIDText
    stage_execution_id: UUIDText
    cycle_number: PositiveInteger
    attempt_number: PositiveInteger
    request_id: JsonValue
    queued_at: JsonValue
    queued_monotonic: JsonValue


class RecoveryContextIdentity(ParticipantIdentity):
    """Reconstruction consumes identity plus the original request and attempt.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        participant_id: UUID of the stage or service represented by the
            participant.
        participant_instance_id: UUID distinguishing this particular participant
            process/attempt from replacements.
        request_id: Original request metadata retained for comparison during
            reconstruction.
        attempt_id: Original attempt metadata retained for comparison during
            reconstruction.
    """

    request_id: JsonValue
    attempt_id: JsonValue


class RecoveredAttemptContext(_Document):
    """Reconstruction fields only; unrelated module/process settings stay extras.

    Args:
        context: Original participant/request/attempt identity consumed during
            reconstruction.
        input_data: Application JSON input accepted for this call; explicit null
            is valid input.
        settings: JSON settings supplied for the operation.
        endpoint_path: Participant endpoint JSON path used to locate and
            authenticate the assigned process.
        service_id: Stable UUID of a declared service.
        queued_at: Wall-clock time at which the attempt entered the queue.
        queued_monotonic: Local monotonic admission time in seconds; attempt
            deadlines include queue time.
    """

    model_config = ConfigDict(extra="allow")

    context: RecoveryContextIdentity
    input_data: JsonValue
    settings: JsonValue
    endpoint_path: str
    service_id: JsonValue
    queued_at: JsonValue
    queued_monotonic: JsonValue


class ServiceParameters(_Input):
    """Service-instance identity paired with its validated template definition.

    Args:
        service_id: Stable UUID of a declared service.
        service_instance_id: UUID distinguishing one launch of a service from
            its previous instances.
        definition: Validated service definition; its service_id must match the
            surrounding record.
    """
    service_id: UUIDText
    service_instance_id: UUIDText
    definition: ServiceDefinition

    @model_validator(mode="after")
    def match_definition(self) -> Self:
        """Return parameters after requiring the definition's service ID to match."""
        if self.definition.service_id != self.service_id:
            raise ValueError("Service definition has a different service_id.")
        return self


class StateParameters(_Input):
    """Initial runner identity, absolute paths, applied template, and execution mode.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        experiment_directory: Absolute root of the experiment's runtime files.
        run_id: Logical run identity within experiment history, retained across
            the relevant execution scope.
        template_path: Applied template path used by the operation; runtime
            models require an absolute path.
        template_revision_id: UUID of the applied or candidate template
            revision.
        template_yaml: Original applied YAML retained for auditing and
            consistency checks.
        template: Validated applied experiment template.
        mode: Running or paused scheduler mode.
    """
    experiment_id: Text
    experiment_directory: AbsolutePath
    run_id: Text
    template_path: AbsolutePath
    template_revision_id: UUIDText
    template_yaml: Text
    template: ExperimentTemplate
    mode: Literal["running", "paused"]


class PendingRebuild(_Document):
    """Interrupted template rebuild and the protective snapshot required to undo it.

    Args:
        operation_id: Associated journal operation ID; None denotes an unscoped
            observation.
        snapshot_id: UUID of the snapshot selected or referenced by this
            document.
        template_revision_id: UUID of the applied or candidate template
            revision.
        run_id: Logical run identity within experiment history, retained across
            the relevant execution scope.
    """
    operation_id: UUIDText
    snapshot_id: UUIDText
    template_revision_id: UUIDText
    run_id: Text


class PendingInput(_Document):
    """Accepted result reference transferred from a source stage to a target stage.

    Args:
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        source_stage_id: UUID of the stage whose accepted result authorizes the
            transfer/decision.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        stage_id: Stable UUID of a node in the applied DAG.
    """
    request_id: UUIDText
    source_stage_id: UUIDText
    experiment_id: Text
    stage_id: str


class DagDecision(_Document):
    """Saved conditional DAG command with a target field only for moves.

    Args:
        command: Accepted pause, stop, or move decision, or None for ordinary
            advancement.
        stage_id: Target stage identifier; the field is present only for move
            decisions. Defaults to None.
    """
    command: Literal["pause", "stop", "move"] | None
    stage_id: str | None = None

    @model_validator(mode="after")
    def match_target(self) -> Self:
        """Return the decision after checking target-field presence matches a move."""
        if (self.command == "move") != ("stage_id" in self.model_fields_set):
            raise ValueError("Invalid saved DAG decision fields.")
        return self


class LastDecision(_Document):
    """Conditional command associated with its accepted request and source stage.

    Args:
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        source_stage_id: UUID of the stage whose accepted result authorizes the
            transfer/decision.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        decision: Validated conditional command retained with its accepted
            source result.
    """
    request_id: UUIDText
    source_stage_id: UUIDText
    experiment_id: Text
    decision: DagDecision

    @model_validator(mode="before")
    @classmethod
    def detach(cls, value: object) -> object:
        """Copy decision metadata while retaining an already validated DagDecision.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        if type(value) is dict and type(value.get("decision")) is DagDecision:
            document = dict(value)
            decision = document.pop("decision")
            detached: dict[str, object] = dict(
                copy_json_object(document, "runner document")
            )
            detached["decision"] = decision
            return detached
        return copy_json_object(value, "runner document")


class ServiceFailure(_Document):
    """Persisted service failure code and nonempty diagnostic message.

    Args:
        code: Machine-readable diagnostic code used to classify the failure.
        message: Human-readable diagnostic message.
    """
    code: Text
    message: Text


class ServiceFailureDetails(_Input):
    """Live errors preserve str(exception), including empty messages.

    The persisted ServiceFailure contract independently requires nonempty text.
    Keep that check at saving, where publication errors were already reported.

    Args:
        code: Machine-readable diagnostic code used to classify the failure.
        message: String representation of the live exception, including an empty
            message.
    """

    code: Text
    message: str


class ServiceRequest(_Document):
    """Persisted queued/sent service call with ownership and timeout metadata.

    Args:
        owner: Caller owns DAG acceptance; service assigns result/retry policy
            to ServiceManager.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        command: Command name selecting the operation to execute.
        args: JSON argument object supplied to the command handler.
        queued_monotonic: Local monotonic admission time in seconds; attempt
            deadlines include queue time.
        deadline_monotonic: Absolute local monotonic deadline in seconds; None
            means no request deadline. Defaults to None.
        sent_monotonic: Local monotonic send time in seconds; None means the
            request has not been sent.
        service_instance_id: UUID distinguishing one launch of a service from
            its previous instances. Defaults to None.
        timed_out: Whether the accepted timeout already prevents a late response
            from changing the outcome.
    """
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


class WorkingServiceRequest(_Input):
    """Live queue record; persisted constraints remain with ServiceRequest.

    Admission does not impose the saved nonnegative deadline constraint. Keep
    that validation at state publication, preserving expired deadline handling.
    Compatibility metadata remains in extras and is serialized with the record.

    Args:
        owner: Caller owns DAG acceptance; service assigns result/retry policy
            to ServiceManager.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        command: Command name selecting the operation to execute.
        args: JSON argument object supplied to the command handler.
        queued_monotonic: Local monotonic admission time in seconds; attempt
            deadlines include queue time.
        deadline_monotonic: Absolute local monotonic deadline in seconds; None
            means no request deadline. Defaults to None.
        sent_monotonic: Local monotonic send time in seconds; None means the
            request has not been sent.
        service_instance_id: UUID distinguishing one launch of a service from
            its previous instances. Defaults to None.
        timed_out: Whether the accepted timeout already prevents a late response
            from changing the outcome.
    """

    model_config = ConfigDict(extra="allow")

    owner: Literal["caller", "service"]
    request_id: UUIDText
    command: Text
    args: JsonObject
    queued_monotonic: int | float
    deadline_monotonic: int | float | bool | None = None
    sent_monotonic: int | float | None
    service_instance_id: str | None = None
    timed_out: Boolean


class ServiceCallExecutorStatus(_Input):
    """Runner observation of service work, with the live request retained.

    Args:
        participant: Identity of the participant that owns the fixed execution
            request.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        process: Full OS identity of the observed process, or None when no
            process is known.
        finished: Whether execution has been observed as finished.
        current: Live service queue record for this DAG call, or None when no
            current work is reported.
        pending: Ordered commands admitted but not yet executing. Defaults to a
            new empty list.
    """

    participant: ParticipantIdentity | None
    request_id: UUIDText
    process: ProcessIdentity | None
    finished: Boolean
    current: WorkingServiceRequest | None
    pending: list[CommandWork] = Field(default_factory=list)


type ExecutorStatus = (
    ExecutorCommandState | RetainedExecutorStatus | ServiceCallExecutorStatus
)


class SavedService(_Document):
    """Restorable service definition, process identity, queue, and lifecycle state.

    Args:
        service_id: Stable UUID of a declared service.
        service_instance_id: UUID distinguishing one launch of a service from
            its previous instances.
        definition: Validated service definition; its service_id must match the
            surrounding record.
        process_identity: Recorded full OS process identity, or None when not
            yet known.
        endpoint_path: Participant endpoint JSON path used to locate and
            authenticate the assigned process.
        ready: Whether this instance has confirmed readiness through its current
            observation.
        started_at: Recorded service wall-clock start time, or None before
            startup.
        last_status: Latest retained service response/observation, or None
            before one is available.
        restart_count: Automatic restarts consumed by this service in the
            current retry scope.
        pending_requests: Ordered unsent service requests; sent or timed-out
            entries are invalid here.
        active_request: Currently sent service request, or None when no request
            owns the service.
        start_deadline: Original absolute monotonic startup deadline in seconds.
        ever_ready: Whether this instance has ever returned a successful
            readiness heartbeat.
        stopping: Whether a shutdown operation is currently in progress.
        stopped: Whether shutdown of this instance has been confirmed.
        manually_stopped: Whether explicit service stop prevents automatic
            restart. Defaults to False.
        blocked_action: Pause or stop required by service policy; None when not
            blocked.
        failure: Current structured service failure, or None when no failure is
            recorded.
        freeze_id: Snapshot UUID whose write freeze was confirmed by this
            instance.
        prepared_freeze_id: Snapshot UUID whose freeze intent was recorded but
            may not yet be confirmed.
        artifacts_directory: Saved service-instance artifact directory, or None
            before preparation.
        implementation: Module implementation kind, full or action; does not
            change the participant role contract.
    """
    service_id: UUIDText
    service_instance_id: UUIDText
    definition: ServiceDefinition
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
        """Default an omitted legacy manually_stopped flag to False.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Copied mapping with False supplied only when manually_stopped was
            absent; nonmapping input passes to normal model validation.
        """
        if isinstance(document, dict):
            return {"manually_stopped": False, **document}
        return document

    @model_validator(mode="after")
    def match_requests(self) -> Self:
        """Return saved service state after checking definition and queue consistency.

        Raises:
            ValueError: The definition ID differs, pending work was already sent,
                or active work lacks this instance's send identity and timestamp.
        """
        if self.definition.service_id != self.service_id:
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
    """Persisted attempt inputs, participant identity, and execution observations.

    Args:
        attempt_id: UUID identifying one stage attempt.
        stage_id: Stable UUID of a node in the applied DAG.
        stage_execution_id: UUID identifying a stage visit; automatic retries
            retain this visit identity.
        cycle_number: One-based DAG cycle number.
        attempt_number: One-based attempt number for this stage in the current
            cycle.
        artifacts_directory: Absolute writable directory allocated for this
            invocation's artifacts.
        input_data: Application JSON input accepted for this call; explicit null
            is valid input.
        effective_settings: Detached effective settings after applying module
            defaults and template overrides.
        timeout_seconds: Positive attempt timeout in seconds, including queue
            time; None disables the deadline.
        process_identity: Recorded full OS process identity, or None when not
            yet known.
        started_at: Recorded wall-clock start time.
        result_request_id: Request ID of the accepted attempt result, or None
            before acceptance.
        outcome: Recorded command outcome, such as succeeded, failed, cancelled,
            or timed_out.
        executor_status: Retained executor/service execution observations,
            including compatible legacy metadata.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        participant: Identity of the participant that owns the fixed execution
            request.
        endpoint_path: Participant endpoint JSON path used to locate and
            authenticate the assigned process.
        service_id: Stable UUID of a declared service.
        queued_monotonic: Local monotonic admission time in seconds; attempt
            deadlines include queue time.
        queued_at: Wall-clock time at which the attempt entered the queue.
    """
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
        """Return the attempt after matching a service call to its participant ID."""
        if (
            self.service_id is not None
            and self.participant.participant_id != self.service_id
        ):
            raise ValueError("Service attempt identity differs from its service.")
        return self


class SavedStateMetadata(_Document):
    """Inspection header only; opaque state is not reconstructed or integrity checked.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        phase: Observed experiment lifecycle phase; it does not independently
            prove process liveness. Defaults to None.
        mode: Running or paused scheduler mode. Defaults to None.
        template: Sparse original template metadata; inspection does not
            reconstruct or verify runtime state. Defaults to None.
    """

    model_config = ConfigDict(extra="allow")

    schema_version: Annotated[int, Field(ge=3, le=4)]
    experiment_id: Text
    phase: JsonValue = None
    mode: JsonValue = None
    template: JsonValue = None


class SavedRunnerState(_Document):
    """Versioned checkpoint of runner cursor, results, services, and pending actions.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        run_id: Logical run identity within experiment history, retained across
            the relevant execution scope.
        template_path: Applied template path used by the operation; runtime
            models require an absolute path.
        template_revision_id: UUID of the applied or candidate template
            revision.
        template_yaml: Original applied YAML retained for auditing and
            consistency checks.
        template: Validated applied experiment template.
        mode: Running or paused scheduler mode.
        phase: Observed experiment lifecycle phase; it does not independently
            prove process liveness.
        pause_requested: Whether scheduling should pause at the next safe
            attempt boundary.
        cycle_number: One-based DAG cycle number.
        stage_position: One-based position of the selected stage in the applied
            DAG.
        active_attempt: Current unresolved/running stage attempt, or None at a
            stage-free boundary.
        stage_retry_counts: Stage UUIDs mapped to automatic retry counts for
            their current visits.
        stage_attempt_numbers: Stage UUIDs mapped to allocated attempt numbers
            in the current cycle.
        services: Service UUIDs mapped to persisted definitions, ownership,
            queue, and lifecycle state.
        last_result: Last retained accepted application payload, or None when no
            payload is retained.
        last_result_id: Journal request ID authorizing last_result, or None when
            absent.
        stage_result_ids: Stage UUIDs mapped to currently accepted result
            request IDs.
        unknown_state_recovery_count: Number of unresolved-state recovery
            attempts already consumed.
        used_request_ids: Request UUIDs already allocated; saved values must be
            unique.
        stable_snapshot_id: Latest retained protective/valid snapshot reference,
            if any.
        pending_rebuild: Unfinished template reload and its protective snapshot,
            or None.
        pending_advance: Whether the next scheduler transition should advance
            beyond the selected stage.
        stage_result_origins: Original experiment IDs for accepted results
            inherited through continuation.
        checkpoint_id: Identity of the authoritative journal checkpoint
            represented by saved state.
        owner_identity: Full OS identity of the owning runner, or None after
            explicit ownership release.
        pending_input: Accepted conditional-move result assigned to the current
            target stage, or None.
        last_dag_decision: Last applied conditional decision and its accepted
            source identity, or None.
        pending_dag_decision: Accepted decision deferred until snapshot
            publication; cannot coexist with active work. Defaults to None.
        retained_artifacts: Unique experiment-relative artifact paths protected
            from attempt pruning.
    """
    schema_version: Annotated[int, Field(ge=3, le=4)]
    experiment_id: Text
    run_id: Text
    template_path: str
    template_revision_id: UUIDText
    template_yaml: Text
    template: ExperimentTemplate
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
    pending_dag_decision: LastDecision | None = None
    retained_artifacts: list[Text]

    @model_validator(mode="before")
    @classmethod
    def migrate_schema_three(cls, document: object) -> object:
        """Supply missing conditional-transition defaults for schema-three documents.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Schema-three input with missing conditional-transition defaults
            supplied, or unchanged input for other versions.
        """
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
        """Return saved state after validating request, snapshot, and result references.

        Raises:
            ValueError: Identities repeat or disagree, required references are missing,
                or saved DAG transitions are inconsistent with the current cursor.
        """
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
        """Check saved DAG transitions against assigned IDs, cursor, and accepted results.

        Raises:
            ValueError: A transition references an unknown node or conflicts with
                the saved cursor, active attempt, or accepted source result.
        """
        stages = self.template.stages
        if any(item.stage_id is None for item in stages) or any(
            item.service_id is None for item in self.template.services
        ):
            raise ValueError("Saved runtime definitions require assigned IDs.")
        identifiers = {item.stage_id for item in stages}
        for transfer in (
            self.pending_input, self.last_dag_decision, self.pending_dag_decision
        ):
            if transfer is not None and transfer.source_stage_id not in identifiers:
                raise ValueError("Saved transition refers to an unknown source stage.")
        if self.pending_input is not None:
            if self.pending_input.stage_id not in identifiers:
                raise ValueError("Pending input targets an unknown stage.")
            if (
                self.stage_position > len(stages)
                or self.pending_input.stage_id
                != stages[self.stage_position - 1].stage_id
            ):
                raise ValueError("Pending input must belong to the current cursor.")
        if self.last_dag_decision is not None:
            decision = self.last_dag_decision.decision
            if decision.command == "move" and decision.stage_id not in identifiers:
                raise ValueError("Saved move targets an unknown stage.")
        if self.pending_dag_decision is not None:
            pending = self.pending_dag_decision
            if (
                self.active_attempt is not None
                or self.last_dag_decision is not None
                or self.last_result_id != pending.request_id
                or self.stage_result_ids.get(pending.source_stage_id) != pending.request_id
                or self.stage_position > len(stages)
                or stages[self.stage_position - 1].stage_id != pending.source_stage_id
            ):
                raise ValueError("Pending DAG command requires its accepted source result.")
            if pending.decision.command == "move" and pending.decision.stage_id not in identifiers:
                raise ValueError("Pending DAG move targets an unknown stage.")
