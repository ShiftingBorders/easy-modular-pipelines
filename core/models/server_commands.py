"""Server wire documents; admission, mode and queue ownership stay in runtime."""

from typing import Annotated, Literal, Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.json_schema import SkipJsonSchema

from core.models.values import (
    NormalizedUUIDText,
    PositiveInteger,
    SchemaVersionOne,
    Text,
    UUIDText,
)
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object

type CommandState = Literal[
    "pending", "unknown", "unavailable", "succeeded", "failed", "cancelled"
]


class _Document(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "server document")


class CommandTarget(_Document):
    kind: Literal["stage", "service"]
    position: PositiveInteger


class ServerCommand(_Document):
    api_version: SchemaVersionOne = 1
    command_id: NormalizedUUIDText = Field(default_factory=lambda: str(uuid4()))
    command: Text
    args: JsonObject = Field(default_factory=dict)
    target: CommandTarget | SkipJsonSchema[None] = Field(
        default=None, json_schema_extra=lambda schema: schema.pop("default", None)
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        document = copy_json_object(document, "command")
        if document.keys() - cls.model_fields.keys():
            raise ValueError("Unknown command fields.")
        if "target" in document:
            target = copy_json_object(document["target"], "target")
            if not target:
                document.pop("target")
        return document

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if not self.command.startswith("server."):
            return self
        if self.command not in ("server.restart", "server.mode", "server.shutdown"):
            raise ValueError("Unsupported server lifecycle command.")
        if self.target is not None:
            raise ValueError("Runtime lifecycle commands do not accept chains or targets.")
        if self.command in ("server.restart", "server.shutdown") and self.args:
            raise ValueError(f"{self.command} does not accept arguments.")
        if self.command == "server.mode" and (
            self.args.keys() != {"mode"} or self.args["mode"] not in ("run", "maintenance")
        ):
            raise ValueError("server.mode requires mode=run|maintenance.")
        return self


class ServerChain(_Document):
    api_version: SchemaVersionOne = 1
    chain_id: NormalizedUUIDText = Field(default_factory=lambda: str(uuid4()))
    commands: Annotated[list[ServerCommand], Field(min_length=1)]

    @model_validator(mode="after")
    def exclude_lifecycle(self) -> Self:
        if any(command.command.startswith("server.") for command in self.commands):
            raise ValueError("Runtime lifecycle commands do not accept chains or targets.")
        return self


class ControllerCommand(_Document):
    api_version: SchemaVersionOne
    command_id: UUIDText
    command: Text
    args: JsonObject = Field(default_factory=dict)
    target: JsonObject = Field(default_factory=dict)
    chain_id: UUIDText | None = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        document = copy_json_object(document, "controller command")
        if "commands" in document:
            raise ValueError("Nested chains are not supported.")
        return document


class ControllerChain(_Document):
    api_version: SchemaVersionOne
    chain_id: UUIDText
    commands: Annotated[list[ControllerCommand], Field(min_length=1)]

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        document = copy_json_object(document, "controller chain")
        if isinstance(document.get("commands"), list):
            document["commands"] = [
                {
                    **copy_json_object(command, "chain command"),
                    "api_version": 1,
                    "chain_id": document.get("chain_id"),
                }
                for command in document["commands"]
            ]
        return document


class RuntimeReady(_Document):
    """Ready metadata is an object; actual ownership comes from the process handle."""

    model_config = ConfigDict(extra="allow")

    kind: Literal["ready"] = Field(alias="_runtime")
    process: JsonObject
    storage: JsonObject = Field(default_factory=dict)


class RuntimeStopped(_Document):
    model_config = ConfigDict(extra="allow")

    kind: Literal["stopped", "error"] = Field(alias="_runtime")
    message: JsonValue = "Controller stopped."


class CommandStatus(_Document):
    state: CommandState
    result: Literal["success", "fail"] | None

    @model_validator(mode="after")
    def match_result(self) -> Self:
        expected = (
            "success" if self.state == "succeeded"
            else "fail" if self.state in ("failed", "cancelled") else None
        )
        if self.result != expected:
            raise ValueError("Invalid controller command outcome.")
        return self


class ControllerOutcome(CommandStatus):
    """Optional payload fields retain the existing partial-outcome contract."""

    model_config = ConfigDict(extra="allow")

    command_id: UUIDText
    state: Literal["succeeded", "failed", "cancelled"]
    result: Literal["success", "fail"]
    data: JsonValue = None
    error: JsonValue = None


class ControllerRejection(_Document):
    """Intake errors can lack a valid command ID, unlike admitted outcomes."""

    model_config = ConfigDict(extra="allow")

    command_id: JsonValue
    state: Literal["failed", "cancelled"]
    result: Literal["fail"]
    data: JsonValue = None
    error: JsonValue = None
