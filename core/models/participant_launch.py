"""Fixed module inputs validated before connecting or starting a process."""

import json
from pathlib import Path
from typing import Annotated, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializeAsAny,
    field_validator,
    model_validator,
)

from core.models.module_manifest import ModuleManifest
from core.models.participant_identity import ParticipantIdentity
from core.models.participant_protocol import ProtocolVersion
from core.models.values import AbsolutePath, Number, PositiveNumber, Text, UUIDText
from core.primitives.json_values import (
    JsonObject,
    JsonValue,
    _validate_json,
    copy_json_object,
)


def _detach_execution_inputs(document: object, fields: set[str]) -> object:
    if type(document) is not dict:
        return copy_json_object(document, "execution inputs")
    values = dict(document)
    retained: dict[str, object] = {}
    context = values.get("context")
    if isinstance(context, ParticipantIdentity) and type(context) in (
        ParticipantIdentity,
        StageExecutionIdentity,
    ):
        for name, value in context:
            _validate_json(value, depth=2)
            try:
                json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
            except UnicodeEncodeError as error:
                raise ValueError("execution inputs contain invalid Unicode.") from error
        retained["context"] = context
        values["context"] = None
    for name in (
        "experiment_directory",
        "resources_directory",
        "settings_directory",
        "module_data_directory",
        "artifacts_directory",
        "logging_config_path",
        "endpoint_path",
    ):
        if name not in fields:
            continue
        value = values.get(name)
        if isinstance(value, Path):
            copy_json_object({name: str(value)}, "execution path")
            retained[name] = value
            values[name] = None
    detached: dict[str, object] = dict(copy_json_object(values, "execution inputs"))
    detached.update(retained)
    return detached


def _same_execution_identity(
    left: ParticipantIdentity, right: ParticipantIdentity
) -> bool:
    """Compare identity values across base/extended models and compatible extras."""
    names = type(left).model_fields.keys() | type(right).model_fields.keys()
    left_extra = dict(left.model_extra or {})
    right_extra = dict(right.model_extra or {})
    for name in names:
        if name in type(left).model_fields:
            left_value = getattr(left, name)
        elif name in left_extra:
            left_value = left_extra.pop(name)
        else:
            return False
        if name in type(right).model_fields:
            right_value = getattr(right, name)
        elif name in right_extra:
            right_value = right_extra.pop(name)
        else:
            return False
        if left_value != right_value:
            return False
    return left_extra == right_extra


class ModulePreparation(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    context: SerializeAsAny[ParticipantIdentity]
    artifacts_directory: AbsolutePath
    input_data: JsonValue

    @field_validator("input_data", mode="before")
    @classmethod
    def detach_input(cls, value: object) -> JsonValue:
        return copy_json_object({"input_data": value}, "module input")["input_data"]


class ExecutionCall(BaseModel):
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    context: SerializeAsAny[ParticipantIdentity]
    input_data: JsonValue
    settings: JsonObject
    experiment_directory: AbsolutePath
    resources_directory: AbsolutePath
    settings_directory: AbsolutePath
    module_data_directory: AbsolutePath
    artifacts_directory: AbsolutePath

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> object:
        return _detach_execution_inputs(document, set(cls.model_fields))


class ModuleContext(ExecutionCall):
    protocol_version: ProtocolVersion
    logging_config_path: AbsolutePath
    endpoint_path: AbsolutePath
    control_timeout_seconds: PositiveNumber


class PreparedModuleContext(ExecutionCall):
    """Preparation also covers service calls without a module logger process."""

    protocol_version: ProtocolVersion
    logging_config_path: AbsolutePath | None
    endpoint_path: AbsolutePath
    control_timeout_seconds: PositiveNumber


class PreparedLaunch(BaseModel):
    """Internal preparation result; executor admission remains StageLaunch's job."""

    model_config = ConfigDict(extra="allow", strict=True, frozen=True)

    argv: list[Text]
    code_directory: AbsolutePath
    experiment_directory: AbsolutePath
    executor_logging_config: AbsolutePath | None
    module: ModuleManifest
    context: SerializeAsAny[ParticipantIdentity]
    runtime_context: PreparedModuleContext
    call: ExecutionCall
    effective_settings: JsonObject
    endpoint_path: AbsolutePath
    service_id: UUIDText | None
    timeout_seconds: PositiveNumber | None
    control_timeout_seconds: PositiveNumber
    stop_timeout_seconds: PositiveNumber
    runner_timeout_margin_seconds: Number


class StageExecutionIdentity(ParticipantIdentity):
    request_id: UUIDText


class AttemptContextHeader(BaseModel):
    """Recovery comparison header; the other context fields stay opaque here."""

    model_config = ConfigDict(extra="allow", strict=True, frozen=True)

    attempt_id: JsonValue


class AttemptContextObservation(BaseModel):
    """Only the original attempt inputs consumed by reconnect validation."""

    model_config = ConfigDict(extra="allow", strict=True, frozen=True)

    context: AttemptContextHeader
    input_data: JsonValue
    settings: JsonValue

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "attempt context")


class StageLaunch(BaseModel):
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    argv: Annotated[list[Text], Field(min_length=1)]
    code_directory: AbsolutePath
    executor_logging_config: AbsolutePath
    context: StageExecutionIdentity
    runtime_context: ModuleContext
    call: ExecutionCall
    endpoint_path: AbsolutePath
    control_timeout_seconds: PositiveNumber
    stop_timeout_seconds: PositiveNumber
    runner_timeout_margin_seconds: Number

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "stage launch")

    @model_validator(mode="after")
    def match_inputs(self) -> Self:
        if not _same_execution_identity(
            self.call.context, self.context
        ) or not _same_execution_identity(self.runtime_context.context, self.context):
            raise ValueError("Stage launch contexts must identify the same fixed call.")
        if any(
            getattr(self.call, name) != getattr(self.runtime_context, name)
            for name in ExecutionCall.model_fields
            if name != "context"
        ):
            raise ValueError("Stage call inputs differ from the module context.")
        if (
            self.endpoint_path != self.runtime_context.endpoint_path
            or self.control_timeout_seconds
            != self.runtime_context.control_timeout_seconds
        ):
            raise ValueError("Stage launch and module control settings differ.")
        return self
