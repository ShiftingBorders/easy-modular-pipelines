"""Fixed module inputs validated before connecting or starting a process."""

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.models.participant_identity import ParticipantIdentity
from core.models.participant_protocol import ProtocolVersion
from core.models.values import AbsolutePath, Number, PositiveNumber, Text, UUIDText
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


class ModulePreparation(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    context: ParticipantIdentity
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

    context: ParticipantIdentity
    input_data: JsonValue
    settings: JsonObject
    experiment_directory: AbsolutePath
    resources_directory: AbsolutePath
    settings_directory: AbsolutePath
    module_data_directory: AbsolutePath
    artifacts_directory: AbsolutePath

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "execution inputs")


class ModuleContext(ExecutionCall):
    protocol_version: ProtocolVersion
    logging_config_path: AbsolutePath
    endpoint_path: AbsolutePath
    control_timeout_seconds: PositiveNumber


class StageExecutionIdentity(ParticipantIdentity):
    request_id: UUIDText


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
        context = self.context.model_dump()
        if (
            self.call.context.model_dump() != context
            or self.runtime_context.context.model_dump() != context
        ):
            raise ValueError("Stage launch contexts must identify the same fixed call.")
        if any(
            getattr(self.call, name) != getattr(self.runtime_context, name)
            for name in ExecutionCall.model_fields
        ):
            raise ValueError("Stage call inputs differ from the module context.")
        if (
            self.endpoint_path != self.runtime_context.endpoint_path
            or self.control_timeout_seconds != self.runtime_context.control_timeout_seconds
        ):
            raise ValueError("Stage launch and module control settings differ.")
        return self
