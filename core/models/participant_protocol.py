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
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    protocol_version: ProtocolVersion

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "participant message")


class ParticipantRequest(ParticipantIdentity):
    protocol_version: ProtocolVersion
    message_type: Literal["request"]
    request_id: RequestId
    command: Text
    args: JsonObject
    deadline_monotonic: Number | None = None

    @model_validator(mode="after")
    def validate_execute_context(self) -> Self:
        if self.command == "execute":
            copy_json_object(self.args.get("context", {}), "call context")
        return self


class ParticipantResult(BaseModel):
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
        document = copy_json_object(document, "participant result")
        if document.keys() - cls.model_fields.keys():
            raise ValueError("Unknown result fields; domain output belongs in data.")
        if document.get("result") not in ("success", "fail") or "data" not in document:
            raise ValueError("A result requires result=success/fail and data.")
        return document


class ResultEnvelope(ParticipantResult):
    """Compatibility facade: optional envelope fields were not checked here."""

    protocol_version: JsonValue = None
    message_type: JsonValue = None
    request_id: JsonValue = None


class StageOutcomeResult(BaseModel):
    """Accepted stage envelope; StageOutcome historically preserves extra fields."""

    model_config = ConfigDict(extra="allow", strict=True, frozen=True)

    result: Literal["success", "fail"]
    data: JsonValue
    error: JsonValue = None
    execution: JsonValue = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "stage result")


class StageResult(ParticipantResult):
    """Stdout contains only the application result, without transport metadata."""

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        document = copy_json_object(document, "stage stdout")
        if document.keys() != {"result", "data"} or document["result"] not in (
            "success", "fail"
        ):
            raise ValueError("Stage stdout requires exactly result and data.")
        return document


class ModuleProgress(BaseModel):
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    value: Annotated[Number, Field(le=1)]
    message: Text | None = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "module progress")


class ParticipantResponse(ParticipantResult):
    protocol_version: ProtocolVersion
    message_type: Literal["response"]
    request_id: UUIDText


class ParticipantNotification(_Message):
    message_type: Literal["notification"]
    command: Text
    data: JsonObject


class EndpointAddress(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True, frozen=True)

    host: Literal["127.0.0.1"]
    port: Annotated[int, Field(strict=True)]
    token_file: str


class ParticipantEndpoint(ParticipantIdentity):
    protocol_version: ProtocolVersion
    process: ProcessIdentity
    endpoint: EndpointAddress


class ParticipantHello(_Message):
    message_type: Literal["hello"]
    identity: JsonObject
    role: Literal["runner", "module"]
    token: str


class ParticipantHelloReply(_Message):
    message_type: Literal["hello"]
    result: Literal["success"]
    data: JsonObject


ParticipantReply = TypeAdapter(
    Annotated[
        ParticipantResponse | ParticipantNotification,
        Field(discriminator="message_type"),
    ]
)
