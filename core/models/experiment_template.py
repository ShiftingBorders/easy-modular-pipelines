"""Experiment template contracts, independent of files and running participants."""

import json
from pathlib import Path
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationInfo,
    field_validator,
    model_validator,
)

from core.models.journal_settings import JournalLimits
from core.models.values import Boolean, Number, PositiveInteger, PositiveNumber, Text
from core.modules.validation import _validate_module_hash
from core.primitives.json_values import (
    JsonObject,
    _validate_json,
    copy_json_object,
    require_text,
)


def _identifier(value: object, info: ValidationInfo) -> str:
    return str(UUID(require_text(value, info.field_name or "definition ID")))


def _boolean(value: object, info: ValidationInfo) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{info.field_name} must be a boolean.")
    return value


def _object(value: object, info: ValidationInfo) -> JsonObject:
    return copy_json_object(value, info.field_name or "settings")


def _hash(value: str) -> str:
    _validate_module_hash(value)
    return value


DefinitionID = Annotated[str | None, BeforeValidator(_identifier)]
OptionalBoolean = Annotated[bool | None, BeforeValidator(_boolean)]
SettingsObject = Annotated[JsonObject, BeforeValidator(_object)]
ModuleHash = Annotated[str, BeforeValidator(_hash)]
NonnegativeInteger = Annotated[int, Field(strict=True, ge=0)]


class _TemplateValue(BaseModel):
    """Strict immutable template value that rejects unknown fields."""
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        hide_input_in_errors=True,
    )


class ModuleReference(_TemplateValue):
    """Normalized module name and version with the required content hash.

    Args:
        name: Registered module name forming a portable directory component.
        version: Registered module version forming a portable directory
            component.
        hash: Expected SHA-256 module content digest.
    """
    name: Text
    version: Text
    hash: ModuleHash

    @field_validator("name", "version")
    @classmethod
    def normalize_component(cls, value: str) -> str:
        """Strip surrounding whitespace and reject unsafe module path components.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            Whitespace-trimmed name/version after rejecting unsafe path characters
            and dot segments.
        """
        value = value.strip()
        if value in (".", "..") or any(char in value for char in '/\\:*?"<>|'):
            raise ValueError("Unsafe module name or version.")
        return value


class ErrorPolicy(_TemplateValue):
    """Automatic retry count, delay in seconds, and exhaustion action.

    Args:
        retries: Number of automatic retries after the initial attempt.
        retry_delay_seconds: Nonnegative delay in seconds before an automatic
            retry.
        on_exhausted: Stop, pause, or skip after retry exhaustion; service skip
            bypasses its automatic restart-count limit.
    """
    retries: NonnegativeInteger
    retry_delay_seconds: Number
    on_exhausted: Literal["stop", "pause", "skip"]


class HeartbeatPolicy(_TemplateValue):
    """Service heartbeat interval and grace period in seconds.

    Args:
        interval_seconds: Positive interval in seconds between heartbeat probes.
        grace_seconds: Positive heartbeat grace period in seconds.
    """
    interval_seconds: PositiveNumber
    grace_seconds: PositiveNumber


class StageDefinition(_TemplateValue):
    """Stage module, per-attempt settings, timeout, and execution policies.

    Args:
        stage_id: Optional stable node UUID; assembly assigns one when omitted.
            Defaults to None.
        module: Validated name/version/hash reference selecting the module
            implementation.
        settings: Stage settings overrides recursively merged with module
            defaults.
        timeout_seconds: Positive attempt timeout in seconds, including queue
            time; None disables the deadline.
        errors: Automatic retry count, delay, and exhaustion action for this
            definition.
        returns_data: Conditional-stage payload contract: true requires data,
            false ignores it; ordinary nodes omit this field. Defaults to None.
        snapshot_after: Request a snapshot after successful execution when
            snapshot mode is after_epoch. Defaults to False.
    """
    stage_id: DefinitionID = None
    module: ModuleReference
    settings: SettingsObject
    timeout_seconds: PositiveNumber | None
    errors: ErrorPolicy
    returns_data: OptionalBoolean = None
    snapshot_after: Boolean = False


class ServiceCallDefinition(_TemplateValue):
    """DAG node invoking a declared service with per-call settings and policies.

    Args:
        stage_id: Optional stable DAG-node UUID; assembly assigns one when
            omitted. Defaults to None.
        service_id: Stable UUID of a declared service.
        settings: Per-call settings sent with input_data; service startup
            settings are not merged into them.
        timeout_seconds: Positive attempt timeout in seconds, including queue
            time; None disables the deadline.
        errors: Automatic retry count, delay, and exhaustion action for this
            definition.
        snapshot_after: Request a snapshot after successful execution when
            snapshot mode is after_epoch. Defaults to False.
    """
    stage_id: DefinitionID = None
    service_id: DefinitionID
    settings: SettingsObject
    timeout_seconds: PositiveNumber | None
    errors: ErrorPolicy
    snapshot_after: Boolean = False


class ServiceDefinition(_TemplateValue):
    """Long-lived service module with startup, heartbeat, and recovery settings.

    Args:
        service_id: Stable service UUID; assembly may assign an omitted ID only
            when no DAG node references it. Defaults to None.
        module: Validated name/version/hash reference selecting the module
            implementation.
        settings: Service startup overrides merged with module defaults.
        heartbeat: Service heartbeat interval and grace policy.
        command_timeout_seconds: Positive timeout in seconds for service-owned
            commands after sending.
        on_command_timeout: Action after service-owned command timeout: pause,
            restart, or stop.
        state_required: Whether snapshots/restoration require this service to
            provide a state export.
        errors: Automatic retry count, delay, and exhaustion action for this
            definition.
    """
    service_id: DefinitionID = None
    module: ModuleReference
    settings: SettingsObject
    heartbeat: HeartbeatPolicy
    command_timeout_seconds: PositiveNumber
    on_command_timeout: Literal["pause", "restart", "stop"]
    state_required: Annotated[bool, BeforeValidator(_boolean)]
    errors: ErrorPolicy


class ResourceDefinition(_TemplateValue):
    """Named resource with an optional hash and template-relative or absolute path.

    Args:
        name: Unique safe component naming the resource in the experiment.
        path: Source resource path; relative values resolve from the template
            file's directory.
        hash: Optional SHA-256 file/directory content hash; None omits resource
            hash checking.
    """
    name: Text
    path: Text
    hash: ModuleHash | None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Return a resource name after checking it is a safe path component.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            A resource name after checking it is a safe path component.
        """
        if value in (".", "..") or any(char in value for char in '/\\:*?"<>|'):
            raise ValueError("Resource names must be safe path components.")
        return value

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        """Return a resource path after rejecting drive-relative or rooted ambiguity.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            A resource path after rejecting drive-relative or rooted ambiguity.
        """
        path = Path(value)
        if not path.is_absolute() and (path.drive or path.root):
            raise ValueError("Ambiguous resource path.")
        return value


class UnknownStatePolicy(_TemplateValue):
    """Timeout and recovery limits for work whose outcome remains unresolved.

    Args:
        timeout_seconds: Positive seconds to await evidence resolving an unknown
            stage state.
        on_timeout: Stop, pause, rerun, or skip action for unresolved stage
            state after timeout.
        recovery_limit: Maximum unknown-state recovery count before applying
            on_recovery_limit.
        on_recovery_limit: Pause or stop action applied when the unknown-state
            recovery limit is reached.
    """
    timeout_seconds: PositiveNumber
    on_timeout: Literal["stop", "pause", "rerun", "skip"]
    recovery_limit: NonnegativeInteger
    on_recovery_limit: Literal["stop", "pause"]


class SnapshotPolicy(_TemplateValue):
    """Automatic snapshot timing and number of valid snapshots to retain.

    Args:
        mode: Automatic timing: off, after_stage, or after_epoch; finalization
            snapshots are separate.
        keep: Positive number of usable snapshot restoration points to retain.
    """
    mode: Literal["off", "after_stage", "after_epoch"]
    keep: PositiveInteger


class StoragePolicy(_TemplateValue):
    """Minimum free disk space in bytes reserved for snapshots."""
    min_snapshot_free_bytes: NonnegativeInteger


class TemplateLoggingPolicy(JournalLimits):
    """Journal storage limits and filtered-view refresh interval in seconds.

    Args:
        busy_timeout_seconds: SQLite lock-wait timeout in seconds, greater than
            zero and at most 60.
        max_event_bytes: Maximum encoded event bytes; None disables the
            additional event-size cap.
        min_free_bytes: Nonnegative free-space reserve in bytes required before
            storage writes.
        filtered_refresh_interval_seconds: Positive interval in seconds between
            filtered-view refreshes.
    """
    filtered_refresh_interval_seconds: PositiveNumber


class ExperimentTemplate(_TemplateValue):
    """Validated DAG definitions, resources, and experiment execution policies.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        name: Nonempty display name of the experiment.
        cycles: Positive number of complete passes through the DAG.
        keep_attempts: Number of recent attempt directories retained per
            stage/cycle, excluding protected artifacts.
        start_timeout: Positive startup/shutdown allowance in seconds for
            experiment participants.
        runner_timeout_margin_seconds: Additional seconds reserved for
            cancellation and confirming process termination.
        stages: Ordered DAG stage/service-call definitions.
        services: Declared service definitions available to DAG service-call
            nodes.
        resources: Static files/directories copied into the experiment,
            optionally hash-checked.
        unknown_state: Policy for unresolved stage outcomes and bounded recovery
            attempts.
        snapshots: Automatic snapshot timing and retention policy.
        storage: Free-space reserve for snapshot operations.
        logging: Experiment journal limits and filtered-view refresh settings.
    """
    schema_version: Annotated[int, Field(ge=2, le=2)]
    name: Text
    cycles: PositiveInteger
    keep_attempts: PositiveInteger
    start_timeout: PositiveNumber
    runner_timeout_margin_seconds: Number
    stages: list[StageDefinition | ServiceCallDefinition] = Field(min_length=1)
    services: list[ServiceDefinition]
    resources: list[ResourceDefinition]
    unknown_state: UnknownStatePolicy
    snapshots: SnapshotPolicy
    storage: StoragePolicy
    logging: TemplateLoggingPolicy

    @model_validator(mode="before")
    @classmethod
    def detach_document(cls, value: object) -> object:
        """Detach template JSON while retaining recognized validated submodels.

        Args:
            value: Template dictionary, optionally containing typed policy or
                definition models.

        Returns:
            Copied input preserving known models after whole-document JSON checks.
        """
        if type(value) is not dict:
            return copy_json_object(value, "experiment template")
        # Only known validated inputs may cross the internal model boundary.
        # All remaining data still goes through the original JSON checks.
        document = dict(value)
        models: dict[str, BaseModel] = {}
        for name, expected in (
            ("unknown_state", UnknownStatePolicy),
            ("snapshots", SnapshotPolicy),
            ("storage", StoragePolicy),
            ("logging", TemplateLoggingPolicy),
        ):
            if type(document.get(name)) is expected:
                _validate_template_input(document[name], depth=1)
                models[name] = document.pop(name)
        definitions: dict[str, dict[int, BaseModel]] = {}
        for name, expected in (
            ("stages", (StageDefinition, ServiceCallDefinition)),
            ("services", (ServiceDefinition,)),
            ("resources", (ResourceDefinition,)),
        ):
            items = document.get(name)
            if type(items) is list:
                retained: dict[int, BaseModel] = {
                    index: item
                    for index, item in enumerate(items)
                    if type(item) in expected
                }
                if retained:
                    for item in retained.values():
                        _validate_template_input(item, depth=2)
                    definitions[name] = retained
                    document[name] = [
                        None if index in retained else item
                        for index, item in enumerate(items)
                    ]
        detached: dict[str, object] = dict(
            copy_json_object(document, "experiment template")
        )
        detached.update(models)
        for name, retained in definitions.items():
            # Use detached JSON entries, preserving their caller ownership rules.
            json_items = detached[name]
            if isinstance(json_items, list):
                detached[name] = [
                    retained.get(index, item)
                    for index, item in enumerate(json_items)
                ]
        return detached

    @model_validator(mode="after")
    def validate_references(self) -> Self:
        """Return the template after checking IDs, service references, and resources.

        Raises:
            ValueError: Definition IDs or resource names repeat, or a service-call
                node references an undeclared service ID.
        """
        identifiers = [
            item.stage_id for item in self.stages if item.stage_id is not None
        ]
        identifiers.extend(
            item.service_id for item in self.services if item.service_id is not None
        )
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("Duplicate stage/service definition ID.")
        service_ids = {
            item.service_id for item in self.services if item.service_id is not None
        }
        for stage in self.stages:
            if (
                isinstance(stage, ServiceCallDefinition)
                and stage.service_id not in service_ids
            ):
                raise ValueError(
                    "A DAG service reference requires an explicit service_id in services."
                )
        names = [item.name for item in self.resources]
        if len(names) != len(set(names)):
            raise ValueError("Resource names must be unique safe path components.")
        return self

    def module_reference(
        self,
        definition: StageDefinition | ServiceCallDefinition | ServiceDefinition,
    ) -> ModuleReference:
        """Resolve the module used by a stage, service, or service-call definition.

        Args:
            definition: Definition belonging to this validated template.

        Returns:
            The direct module reference or the referenced service's module.
        """
        if isinstance(definition, ServiceCallDefinition):
            return next(
                service.module
                for service in self.services
                if service.service_id == definition.service_id
            )
        return definition.module


ResourceInputs = TypeAdapter(list[ResourceDefinition], config=ConfigDict(strict=True))


def _validate_template_input(model: BaseModel, *, depth: int) -> None:
    """Keep whole-document JSON limits when known nested inputs remain models."""
    for name in model.model_fields_set:
        value = getattr(model, name)
        if isinstance(value, (_TemplateValue, TemplateLoggingPolicy)):
            _validate_template_input(value, depth=depth + 1)
        else:
            _validate_json(value, depth + 1)
            try:
                json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
            except UnicodeEncodeError as error:
                raise ValueError("experiment template contains invalid Unicode.") from error


def _assigned_definition(
    definition: StageDefinition | ServiceCallDefinition | ServiceDefinition,
) -> StageDefinition | ServiceCallDefinition | ServiceDefinition:
    identifier = (
        definition.service_id
        if isinstance(definition, ServiceDefinition)
        else definition.stage_id
    )
    if identifier is None:
        raise ValueError("Launching a module requires an assigned definition ID.")
    return definition


LaunchInput = TypeAdapter(
    Annotated[
        StageDefinition | ServiceCallDefinition | ServiceDefinition,
        AfterValidator(_assigned_definition),
    ]
)
