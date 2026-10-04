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
from core.models.values import Number, PositiveInteger, PositiveNumber, Text
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
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        hide_input_in_errors=True,
    )


class ModuleReference(_TemplateValue):
    name: Text
    version: Text
    hash: ModuleHash

    @field_validator("name", "version")
    @classmethod
    def normalize_component(cls, value: str) -> str:
        value = value.strip()
        if value in (".", "..") or any(char in value for char in '/\\:*?"<>|'):
            raise ValueError("Unsafe module name or version.")
        return value


class ErrorPolicy(_TemplateValue):
    retries: NonnegativeInteger
    retry_delay_seconds: Number
    on_exhausted: Literal["stop", "pause", "skip"]


class HeartbeatPolicy(_TemplateValue):
    interval_seconds: PositiveNumber
    grace_seconds: PositiveNumber


class StageDefinition(_TemplateValue):
    stage_id: DefinitionID = None
    module: ModuleReference
    settings: SettingsObject
    timeout_seconds: PositiveNumber | None
    errors: ErrorPolicy
    returns_data: OptionalBoolean = None


class ServiceCallDefinition(_TemplateValue):
    stage_id: DefinitionID = None
    service_id: DefinitionID
    settings: SettingsObject
    timeout_seconds: PositiveNumber | None
    errors: ErrorPolicy


class ServiceDefinition(_TemplateValue):
    service_id: DefinitionID = None
    module: ModuleReference
    settings: SettingsObject
    heartbeat: HeartbeatPolicy
    command_timeout_seconds: PositiveNumber
    on_command_timeout: Literal["pause", "restart", "stop"]
    state_required: Annotated[bool, BeforeValidator(_boolean)]
    errors: ErrorPolicy


class ResourceDefinition(_TemplateValue):
    name: Text
    path: Text
    hash: ModuleHash | None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if value in (".", "..") or any(char in value for char in '/\\:*?"<>|'):
            raise ValueError("Resource names must be safe path components.")
        return value

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute() and (path.drive or path.root):
            raise ValueError("Ambiguous resource path.")
        return value


class UnknownStatePolicy(_TemplateValue):
    timeout_seconds: PositiveNumber
    on_timeout: Literal["stop", "pause", "rerun", "skip"]
    recovery_limit: NonnegativeInteger
    on_recovery_limit: Literal["stop", "pause"]


class SnapshotPolicy(_TemplateValue):
    mode: Literal["off", "after_stage", "after_epoch"]
    keep: PositiveInteger


class StoragePolicy(_TemplateValue):
    min_snapshot_free_bytes: NonnegativeInteger


class TemplateLoggingPolicy(JournalLimits):
    filtered_refresh_interval_seconds: PositiveNumber


class ExperimentTemplate(_TemplateValue):
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
