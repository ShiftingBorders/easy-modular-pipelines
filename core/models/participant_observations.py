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
    """Detached participant observation preserving additional protocol fields."""
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of a participant observation.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "participant observation")


class CommandWork(_Observation):
    """Request ID and command name for current or queued participant work.

    Args:
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        command: Command name selecting the operation to execute.
    """
    request_id: UUIDText
    command: Text


class ServiceObservation(_Observation):
    """Service response and optional command name retained for supervision.

    Args:
        protocol_version: Participant wire-protocol version; the current strict
            protocol uses 2.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        result: Application outcome, success or fail.
        data: JSON application payload associated with the result.
        message_type: Wire-envelope discriminator identifying the kind of
            participant message. Defaults to response.
        command: Optional command name attached by the runner to interpret the
            response. Defaults to None.
    """
    protocol_version: ProtocolVersion
    request_id: UUIDText
    result: Literal["success", "fail"]
    data: JsonValue
    message_type: Literal["response"] = "response"
    command: Text | None = None


class RetainedServiceStatus(_Observation):
    """Sparse retained status; recovery historically accepts partial JSON status.

    Args:
        request_id: Identifier correlating one admitted request with its
            observations and outcome. Defaults to None.
        observed_at: Wall-clock timestamp at which this observation was
            recorded. Defaults to None.
        observed_monotonic: Local monotonic observation time in seconds, used
            for age and timeout comparisons. Defaults to None.
    """

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
        """Retain supplied response fields and attach local observation timestamps.

        Args:
            observation: Validated service response, including any extension fields.
            observed_at: Wall-clock observation timestamp.
            observed_monotonic: Local monotonic observation time in seconds.

        Returns:
            Validated status preserving optional-field absence in the response.
        """
        values: dict[str, object] = dict(observation.model_extra or {})
        for name in (
            observation.model_fields_set & ServiceObservation.model_fields.keys()
        ):
            values[name] = getattr(observation, name)
        values.update(observed_at=observed_at, observed_monotonic=observed_monotonic)
        return cls.model_validate(values)


class CommandState(_Observation):
    """Participant's current command and ordered pending commands.

    Args:
        current: Currently executing command, or None when the participant is
            idle.
        pending: Ordered commands admitted but not yet executing.
    """
    current: CommandWork | None
    pending: list[CommandWork]


class CommandStateResponse(ParticipantResponse):
    """Successful command-state response with typed current and pending work.

    Args:
        result: Success discriminator required for a usable command-state
            response.
        data: Current and pending command identities reported by the
            participant.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
        execution: Optional execution observations attached to the application
            result. Defaults to None.
        protocol_version: Participant wire-protocol version; the current strict
            protocol uses 2.
        message_type: Wire-envelope discriminator identifying the kind of
            participant message.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
    """
    result: Literal["success"]
    data: CommandState


class ExecutorCommandState(CommandState):
    """Executor command state, child process, progress, and module state.

    Args:
        current: Currently executing command, or None when the participant is
            idle.
        pending: Ordered commands admitted but not yet executing.
        process: Observed child-process OS identity, or None when no child
            identity is available.
        started_at: Recorded child-process wall-clock start time, or None before
            startup.
        started_monotonic: Local monotonic start time in seconds, or None before
            process startup.
        finished: Whether execution has been observed as finished.
        exit_code: Observed child-process exit code; None while no exit has been
            observed.
        progress: Latest module progress fraction/message, or None before a
            report.
        module_state: Latest module-reported JSON state; it does not itself
            establish process liveness.
    """
    process: ProcessIdentity | None
    started_at: Text | None
    started_monotonic: Number | None
    finished: Boolean
    exit_code: int | None
    progress: ModuleProgress | None
    module_state: JsonObject


class ExecutorCommandStateResponse(CommandStateResponse):
    """Successful command-state response carrying executor observations.

    Args:
        result: Success discriminator required for a usable command-state
            response.
        data: Executor's current/pending work, child-process identity, progress,
            and exit observations.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
        execution: Optional execution observations attached to the application
            result. Defaults to None.
        protocol_version: Participant wire-protocol version; the current strict
            protocol uses 2.
        message_type: Wire-envelope discriminator identifying the kind of
            participant message.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
    """
    data: ExecutorCommandState


class RetainedExecutorStatus(_Observation):
    """Partial execution metadata retains the historical saved-state contract.

    RPC observations use ExecutorCommandState. Accepted results and older state
    files can omit its fields or contain opaque execution metadata instead.

    Args:
        process: Legacy process metadata retained without asserting full OS-
            identity validity. Defaults to None.
        started_at: Recorded wall-clock start time. Defaults to None.
        finished: Whether execution has been observed as finished. Defaults to
            False.
        current: Legacy current-command observation, or None when omitted.
            Defaults to None.
        pending: Legacy pending-work observation, or None when omitted. Defaults
            to None.
    """

    process: JsonValue = None
    started_at: JsonValue = None
    finished: JsonValue = False
    current: JsonValue = None
    pending: JsonValue = None


class ServiceStateExport(_Observation):
    """Optional experiment-relative path to exported service state."""
    state_path: Text | None = None

    @field_validator("state_path")
    @classmethod
    def require_relative_path(cls, value: str | None) -> str | None:
        """Return a state path or None, rejecting a native absolute/rooted path.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            A state path or None, rejecting a native absolute/rooted path.
        """
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
    """Service UUIDs mapped to restoration paths or explicit stateless values."""
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    paths: dict[UUIDText, Annotated[Path | None, BeforeValidator(_restoration_path)]]
