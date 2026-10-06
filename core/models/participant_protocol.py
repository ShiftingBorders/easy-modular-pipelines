"""Participant wire documents; authentication and live ownership stay outside."""

from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    model_validator,
)

from core.models.participant_identity import ParticipantIdentity
from core.models.process_identity import ProcessIdentity
from core.models.values import NormalizedUUIDText, Number, Text, UUIDText
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object

ProtocolVersion = Annotated[int, Field(strict=True, ge=2, le=2)]
RequestId = NormalizedUUIDText


class _Message(BaseModel):
    """Versioned participant message with detached JSON extension fields."""
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    protocol_version: ProtocolVersion

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of a participant message.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "participant message")


class ParticipantRequest(ParticipantIdentity):
    """Identified participant command with arguments and a monotonic deadline.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        participant_id: UUID of the stage or service represented by the
            participant.
        participant_instance_id: UUID distinguishing this particular participant
            process/attempt from replacements.
        protocol_version: Participant wire-protocol version; the current strict
            protocol uses 2.
        message_type: Wire-envelope discriminator identifying the kind of
            participant message.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        command: Command name selecting the operation to execute.
        args: JSON argument object supplied to the command handler.
        deadline_monotonic: Absolute local monotonic deadline in seconds; None
            means no request deadline. Defaults to None.
    """
    protocol_version: ProtocolVersion
    message_type: Literal["request"]
    request_id: RequestId
    command: Text
    args: JsonObject
    deadline_monotonic: Number | None = None

    @model_validator(mode="after")
    def validate_execute_context(self) -> Self:
        """Return the request after checking execute context is a JSON object."""
        if self.command == "execute":
            copy_json_object(self.args.get("context", {}), "call context")
        return self


class ParticipantResult(BaseModel):
    """Application outcome and data with optional error and execution metadata.

    Args:
        result: Application outcome, success or fail.
        data: JSON application payload associated with the result.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
        execution: Optional execution observations attached to the application
            result. Defaults to None.
    """
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    result: Literal["success", "fail"]
    data: JsonValue
    error: JsonValue = None
    execution: JsonValue = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Copy a result and require a supported outcome with an explicit data field.

        Args:
            document: Participant result object.

        Returns:
            Detached result with only supported envelope fields.

        Raises:
            ValueError: Fields are unknown, the outcome is invalid, or data is absent.
        """
        document = copy_json_object(document, "participant result")
        if document.keys() - cls.model_fields.keys():
            raise ValueError("Unknown result fields; domain output belongs in data.")
        if document.get("result") not in ("success", "fail") or "data" not in document:
            raise ValueError("A result requires result=success/fail and data.")
        return document


class ResultEnvelope(ParticipantResult):
    """Compatibility facade: optional envelope fields were not checked here.

    Args:
        result: Application outcome, success or fail.
        data: JSON application payload associated with the result.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
        execution: Optional execution observations attached to the application
            result. Defaults to None.
        protocol_version: Optional legacy transport metadata, preserved without
            strict protocol-version validation here. Defaults to None.
        message_type: Optional legacy transport discriminator, retained without
            strict envelope validation here. Defaults to None.
        request_id: Optional legacy request metadata, preserved without strict
            UUID validation here. Defaults to None.
    """

    protocol_version: JsonValue = None
    message_type: JsonValue = None
    request_id: JsonValue = None


class StageOutcomeResult(BaseModel):
    """Accepted stage envelope; StageOutcome historically preserves extra fields.

    Args:
        result: Application outcome, success or fail.
        data: JSON application payload associated with the result.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
        execution: Optional execution observations attached to the application
            result. Defaults to None.
    """

    model_config = ConfigDict(extra="allow", strict=True, frozen=True)

    result: Literal["success", "fail"]
    data: JsonValue
    error: JsonValue = None
    execution: JsonValue = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of an accepted stage result.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "stage result")


class StageResult(ParticipantResult):
    """Stdout contains only the application result, without transport metadata.

    Args:
        result: Application outcome, success or fail.
        data: JSON application payload associated with the result.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
        execution: Optional execution observations attached to the application
            result. Defaults to None.
    """

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Copy stage stdout after requiring exactly result and data fields.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        document = copy_json_object(document, "stage stdout")
        if document.keys() != {"result", "data"} or document["result"] not in (
            "success", "fail"
        ):
            raise ValueError("Stage stdout requires exactly result and data.")
        return document


class ModuleProgress(BaseModel):
    """Progress fraction from zero to one with an optional message.

    Args:
        value: Finite progress fraction from zero to one.
        message: Optional nonempty description of current stage work. Defaults
            to None.
    """
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    value: Annotated[Number, Field(le=1)]
    message: Text | None = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of reported module progress.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "module progress")


class ParticipantResponse(ParticipantResult):
    """Versioned participant result associated with its original request UUID.

    Args:
        result: Application outcome, success or fail.
        data: JSON application payload associated with the result.
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
    protocol_version: ProtocolVersion
    message_type: Literal["response"]
    request_id: UUIDText


class ParticipantNotification(_Message):
    """Unsolicited participant command notification with JSON object data.

    Args:
        protocol_version: Participant wire-protocol version; the current strict
            protocol uses 2.
        message_type: Wire-envelope discriminator identifying the kind of
            participant message.
        command: Command name selecting the operation to execute.
        data: JSON notification details, such as the reason for cooperative
            cancellation.
    """
    message_type: Literal["notification"]
    command: Text
    data: JsonObject


class EndpointAddress(BaseModel):
    """Loopback TCP endpoint and authentication token-file location.

    Args:
        host: Loopback listener address, fixed to 127.0.0.1.
        port: Published TCP port; actual connection availability is checked by
            the client.
        token_file: Token file path; relative values are resolved from the
            endpoint document's directory.
    """
    model_config = ConfigDict(extra="allow", strict=True, frozen=True)

    host: Literal["127.0.0.1"]
    port: Annotated[int, Field(strict=True)]
    token_file: str


class ParticipantEndpoint(ParticipantIdentity):
    """Participant identity, OS process identity, and published connection details.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        participant_id: UUID of the stage or service represented by the
            participant.
        participant_instance_id: UUID distinguishing this particular participant
            process/attempt from replacements.
        protocol_version: Participant wire-protocol version; the current strict
            protocol uses 2.
        process: Required complete OS identity of the published participant
            process.
        endpoint: Loopback TCP address and token-file location published by the
            participant.
    """
    protocol_version: ProtocolVersion
    process: ProcessIdentity
    endpoint: EndpointAddress


class ParticipantHello(_Message):
    """Connection handshake identifying the client role and authentication token.

    Args:
        protocol_version: Participant wire-protocol version; the current strict
            protocol uses 2.
        message_type: Wire-envelope discriminator identifying the kind of
            participant message.
        identity: Expected experiment, participant, and instance identity
            presented during authentication.
        role: Client role, runner or module; module clients require a configured
            module handler.
        token: Secret token used to authenticate this handshake; must not be
            journaled.
    """
    message_type: Literal["hello"]
    identity: JsonObject
    role: Literal["runner", "module"]
    token: str


class ParticipantHelloReply(_Message):
    """Successful versioned handshake response with participant metadata.

    Args:
        protocol_version: Participant wire-protocol version; the current strict
            protocol uses 2.
        message_type: Wire-envelope discriminator identifying the kind of
            participant message.
        result: Success discriminator for an accepted handshake.
        data: Participant identity echoed for comparison with the expected
            identity.
    """
    message_type: Literal["hello"]
    result: Literal["success"]
    data: JsonObject


ParticipantReply = TypeAdapter(
    Annotated[
        ParticipantResponse | ParticipantNotification,
        Field(discriminator="message_type"),
    ]
)
