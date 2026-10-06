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
    """Fixed participant context, artifact directory, and detached module input.

    Args:
        context: Fixed participant identity and associated attempt/call
            coordinates.
        artifacts_directory: Absolute writable directory allocated for this
            invocation's artifacts.
        input_data: Application JSON input accepted for this call; explicit null
            is valid input.
    """
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    context: SerializeAsAny[ParticipantIdentity]
    artifacts_directory: AbsolutePath
    input_data: JsonValue

    @field_validator("input_data", mode="before")
    @classmethod
    def detach_input(cls, value: object) -> JsonValue:
        """Return a JSON copy of the module's input payload.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object({"input_data": value}, "module input")["input_data"]


class ExecutionCall(BaseModel):
    """Fixed call inputs, settings, identity, and absolute runtime directories.

    Args:
        context: Fixed participant identity and associated attempt/call
            coordinates.
        input_data: Application JSON input accepted for this call; explicit null
            is valid input.
        settings: JSON settings supplied for the operation.
        experiment_directory: Absolute root of the experiment's runtime files.
        resources_directory: Absolute directory of copied experiment resources.
        settings_directory: Absolute directory of shared runtime settings.
        module_data_directory: Absolute writable persistent data directory for
            this stage/service owner.
        artifacts_directory: Absolute writable directory allocated for this
            invocation's artifacts.
    """
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
        """Copy execution input JSON while preserving recognized models and native paths.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return _detach_execution_inputs(document, set(cls.model_fields))


class ModuleContext(ExecutionCall):
    """Module call inputs plus endpoint, logger, and control timeout settings.

    Args:
        context: Fixed participant identity and associated attempt/call
            coordinates.
        input_data: Application JSON input accepted for this call; explicit null
            is valid input.
        settings: JSON settings supplied for the operation.
        experiment_directory: Absolute root of the experiment's runtime files.
        resources_directory: Absolute directory of copied experiment resources.
        settings_directory: Absolute directory of shared runtime settings.
        module_data_directory: Absolute writable persistent data directory for
            this stage/service owner.
        artifacts_directory: Absolute writable directory allocated for this
            invocation's artifacts.
        protocol_version: Participant wire-protocol version; the current strict
            protocol uses 2.
        logging_config_path: Logger configuration path for the existing shared
            journal.
        endpoint_path: Participant endpoint JSON path used to locate and
            authenticate the assigned process.
        control_timeout_seconds: Positive control/connection timeout in seconds.
    """
    protocol_version: ProtocolVersion
    logging_config_path: AbsolutePath
    endpoint_path: AbsolutePath
    control_timeout_seconds: PositiveNumber


class PreparedModuleContext(ExecutionCall):
    """Preparation also covers service calls without a module logger process.

    Args:
        context: Fixed participant identity and associated attempt/call
            coordinates.
        input_data: Application JSON input accepted for this call; explicit null
            is valid input.
        settings: JSON settings supplied for the operation.
        experiment_directory: Absolute root of the experiment's runtime files.
        resources_directory: Absolute directory of copied experiment resources.
        settings_directory: Absolute directory of shared runtime settings.
        module_data_directory: Absolute writable persistent data directory for
            this stage/service owner.
        artifacts_directory: Absolute writable directory allocated for this
            invocation's artifacts.
        protocol_version: Participant wire-protocol version; the current strict
            protocol uses 2.
        logging_config_path: Module logger config, or None for a service call
            that launches no module logger process.
        endpoint_path: Participant endpoint JSON path used to locate and
            authenticate the assigned process.
        control_timeout_seconds: Positive control/connection timeout in seconds.
    """

    protocol_version: ProtocolVersion
    logging_config_path: AbsolutePath | None
    endpoint_path: AbsolutePath
    control_timeout_seconds: PositiveNumber


class PreparedLaunch(BaseModel):
    """Internal preparation result; executor admission remains StageLaunch's job.

    Args:
        argv: Argument vector for direct process execution without a shell.
        code_directory: Absolute immutable module directory used as the child
            process working directory.
        experiment_directory: Absolute root of the experiment's runtime files.
        executor_logging_config: Executor logging config, or None when this
            preparation does not launch a stage executor.
        module: Validated installed module manifest used to prepare this
            invocation.
        context: Fixed participant identity and associated attempt/call
            coordinates.
        runtime_context: Fixed module context containing identity, inputs,
            paths, and connection settings.
        call: Fixed execution-call inputs that must agree with the module
            runtime context.
        effective_settings: Detached effective settings after applying module
            defaults and template overrides.
        endpoint_path: Participant endpoint JSON path used to locate and
            authenticate the assigned process.
        service_id: Target service UUID for a DAG service call, or None for a
            process launch.
        timeout_seconds: Positive attempt timeout in seconds, including queue
            time; None disables the deadline.
        control_timeout_seconds: Positive control/connection timeout in seconds.
        stop_timeout_seconds: Positive timeout in seconds for participant
            shutdown.
        runner_timeout_margin_seconds: Additional seconds reserved for
            cancellation and confirming process termination.
    """

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
    """Participant identity extended with the fixed execution request UUID.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        participant_id: UUID of the stage or service represented by the
            participant.
        participant_instance_id: UUID distinguishing this particular participant
            process/attempt from replacements.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
    """
    request_id: UUIDText


class AttemptContextHeader(BaseModel):
    """Recovery comparison header; the other context fields stay opaque here."""

    model_config = ConfigDict(extra="allow", strict=True, frozen=True)

    attempt_id: JsonValue


class AttemptContextObservation(BaseModel):
    """Only the original attempt inputs consumed by reconnect validation.

    Args:
        context: Fixed participant identity and associated attempt/call
            coordinates.
        input_data: Application JSON input accepted for this call; explicit null
            is valid input.
        settings: JSON settings supplied for the operation.
    """

    model_config = ConfigDict(extra="allow", strict=True, frozen=True)

    context: AttemptContextHeader
    input_data: JsonValue
    settings: JsonValue

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of a recorded attempt context.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "attempt context")


class StageLaunch(BaseModel):
    """Validated executor launch arguments and mutually consistent fixed call inputs.

    Args:
        argv: Argument vector for direct process execution without a shell.
        code_directory: Absolute immutable module directory used as the child
            process working directory.
        executor_logging_config: Absolute logger configuration path for the
            attempt's executor process.
        context: Fixed participant identity extended with the assigned execute
            request UUID.
        runtime_context: Fixed module context containing identity, inputs,
            paths, and connection settings.
        call: Fixed execution-call inputs that must agree with the module
            runtime context.
        endpoint_path: Participant endpoint JSON path used to locate and
            authenticate the assigned process.
        control_timeout_seconds: Positive control/connection timeout in seconds.
        stop_timeout_seconds: Positive timeout in seconds for participant
            shutdown.
        runner_timeout_margin_seconds: Additional seconds reserved for
            cancellation and confirming process termination.
    """
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
        """Return a validated JSON copy of a stage launch document.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "stage launch")

    @model_validator(mode="after")
    def match_inputs(self) -> Self:
        """Return the launch after checking identities, call inputs, and controls agree.

        Raises:
            ValueError: Launch, runtime context, and execution call describe different
                work or use different endpoint/control settings.
        """
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
