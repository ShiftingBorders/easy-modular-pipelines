"""Participant observations checked before recovery mutates runtime state."""

from pathlib import Path
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    field_validator,
    model_validator,
)

from core.models.participant_protocol import (
    ModuleProgress,
    ParticipantResponse,
    ProtocolVersion,
)
from core.models.process_identity import ProcessIdentity
from core.models.values import Boolean, Number, Text, UUIDText
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


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


class ServiceObservation(_Observation):
    protocol_version: ProtocolVersion
    request_id: UUIDText
    result: Literal["success", "fail"]
    data: JsonValue
    message_type: Literal["response"] = "response"
    command: Text | None = None


class RetainedServiceStatus(_Observation):
    """Sparse retained status; recovery historically accepts partial JSON status."""

    request_id: JsonValue = None
    observed_at: JsonValue = None
    observed_monotonic: JsonValue = None

    @classmethod
    def from_observation(
        cls,
        observation: ServiceObservation,
        observed_at: str,
        observed_monotonic: float,
    ) -> "RetainedServiceStatus":
        values: dict[str, object] = dict(observation.model_extra or {})
        for name in (
            observation.model_fields_set & ServiceObservation.model_fields.keys()
        ):
            values[name] = getattr(observation, name)
        values.update(observed_at=observed_at, observed_monotonic=observed_monotonic)
        return cls.model_validate(values)


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


def _restoration_path(value: object) -> Path | None:
    if value is None:
        return None
    if not isinstance(value, (str, Path)):
        raise TypeError("Service restoration path must be a string or Path.")
    path = Path(value)
    if "\x00" in str(path):
        raise ValueError("Service restoration path contains a null character.")
    return path


class ServiceRestorationPaths(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    paths: dict[UUIDText, Annotated[Path | None, BeforeValidator(_restoration_path)]]
