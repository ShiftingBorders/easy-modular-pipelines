"""Participant observations checked before recovery mutates runtime state."""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from core.models.participant_protocol import ModuleProgress, ParticipantResponse
from core.models.process_identity import ProcessIdentity
from core.models.values import Boolean, Number, Text, UUIDText
from core.primitives.json_values import JsonObject, copy_json_object


class _Observation(BaseModel):
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "participant observation")


class CommandWork(_Observation):
    request_id: UUIDText
    command: Text


class CommandState(_Observation):
    current: CommandWork | None
    pending: list[CommandWork]


class CommandStateResponse(ParticipantResponse):
    result: Literal["success"]
    data: CommandState


class ExecutorCommandState(CommandState):
    process: ProcessIdentity | None
    started_at: Text | None
    started_monotonic: Number | None
    finished: Boolean
    exit_code: int | None
    progress: ModuleProgress | None
    module_state: JsonObject


class ExecutorCommandStateResponse(CommandStateResponse):
    data: ExecutorCommandState


class ServiceStateExport(_Observation):
    state_path: Text | None = None

    @field_validator("state_path")
    @classmethod
    def require_relative_path(cls, value: str | None) -> str | None:
        if value is not None and Path(value).anchor:
            raise ValueError("Service state_path must be experiment-relative.")
        return value
