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
    """Strict server wire document detached from caller-owned JSON data."""
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of a server document.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "server document")


class CommandTarget(_Document):
    """Stage or service target addressed by a one-based position.

    Args:
        kind: Stage or service definition list to which position refers.
        position: One-based index in the selected definition list.
    """
    kind: Literal["stage", "service"]
    position: PositiveInteger


class ServerCommand(_Document):
    """Versioned command envelope with request identity, arguments, and target.

    Args:
        api_version: System API envelope version, currently 1. Defaults to 1.
        command_id: Normalized command UUID for receipt lookup/replay detection;
            generated when omitted.
        command: Command name selecting the operation to execute.
        args: JSON argument object supplied to the command handler. Defaults to
            a new empty dict.
        target: Optional stage/service target; server lifecycle commands reject
            targets. Defaults to None.
    """
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
        """Copy supported command fields and remove an explicitly empty target.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
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
        """Return the command after checking runtime lifecycle arguments and target.

        Raises:
            ValueError: A server lifecycle command is unsupported, carries a target,
                or supplies arguments inconsistent with restart, shutdown, or mode.
        """
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
    """Nonempty ordered command chain with its own normalized UUID.

    Args:
        api_version: System API envelope version, currently 1. Defaults to 1.
        chain_id: Normalized ordered-chain UUID; generated when omitted.
        commands: Nonempty ordered commands; server lifecycle commands cannot
            belong to chains.
    """
    api_version: SchemaVersionOne = 1
    chain_id: NormalizedUUIDText = Field(default_factory=lambda: str(uuid4()))
    commands: Annotated[list[ServerCommand], Field(min_length=1)]

    @model_validator(mode="after")
    def exclude_lifecycle(self) -> Self:
        """Return the chain after rejecting runtime lifecycle commands."""
        if any(command.command.startswith("server.") for command in self.commands):
            raise ValueError("Runtime lifecycle commands do not accept chains or targets.")
        return self


class ControllerCommand(_Document):
    """Command envelope admitted for delivery to the controller process.

    Args:
        api_version: System API envelope version, currently 1.
        command_id: Command UUID used for receipt lookup and duplicate-
            submission detection.
        command: Command name selecting the operation to execute.
        args: JSON argument object supplied to the command handler. Defaults to
            a new empty dict.
        target: Detached target object; an empty object means no target.
            Defaults to a new empty dict.
        chain_id: UUID identifying the ordered command chain. Defaults to None.
    """
    api_version: SchemaVersionOne
    command_id: UUIDText
    command: Text
    args: JsonObject = Field(default_factory=dict)
    target: JsonObject = Field(default_factory=dict)
    chain_id: UUIDText | None = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Copy controller command JSON and reject nested chains.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        document = copy_json_object(document, "controller command")
        if "commands" in document:
            raise ValueError("Nested chains are not supported.")
        return document


class ControllerChain(_Document):
    """Nonempty command chain delivered to a single controller process.

    Args:
        api_version: System API envelope version, currently 1.
        chain_id: UUID identifying the ordered command chain.
        commands: Nonempty ordered admitted commands; each receives the
            enclosing chain ID.
    """
    api_version: SchemaVersionOne
    chain_id: UUIDText
    commands: Annotated[list[ControllerCommand], Field(min_length=1)]

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Copy chain JSON and bind each child to its chain ID and API version.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
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
    """Ready metadata is an object; actual ownership comes from the process handle.

    Args:
        kind: Private IPC discriminator, fixed to ready. Input alias: _runtime.
        process: Controller process metadata; the parent process handle
            establishes actual ownership.
        storage: Storage initialization details reported by the controller.
            Defaults to a new empty dict.
    """

    model_config = ConfigDict(extra="allow")

    kind: Literal["ready"] = Field(alias="_runtime")
    process: JsonObject
    storage: JsonObject = Field(default_factory=dict)


class RuntimeStopped(_Document):
    """Controller termination or startup-error notification.

    Args:
        kind: Private IPC discriminator, stopped or error. Input alias:
            _runtime.
        message: Termination/startup diagnostic preserved as JSON. Defaults to
            Controller stopped..
    """
    model_config = ConfigDict(extra="allow")

    kind: Literal["stopped", "error"] = Field(alias="_runtime")
    message: JsonValue = "Controller stopped."


class CommandStatus(_Document):
    """Command state paired with its corresponding success/failure result.

    Args:
        state: Command lifecycle state, distinct from experiment phase.
        result: Success for succeeded, fail for failed/cancelled, otherwise
            None.
    """
    state: CommandState
    result: Literal["success", "fail"] | None

    @model_validator(mode="after")
    def match_result(self) -> Self:
        """Return status after checking that result agrees with terminal/pending state."""
        expected = (
            "success" if self.state == "succeeded"
            else "fail" if self.state in ("failed", "cancelled") else None
        )
        if self.result != expected:
            raise ValueError("Invalid controller command outcome.")
        return self


class ControllerOutcome(CommandStatus):
    """Optional payload fields retain the existing partial-outcome contract.

    Args:
        state: Command lifecycle state, distinct from experiment phase.
        result: Application outcome, success or fail.
        command_id: Command UUID used for receipt lookup and duplicate-
            submission detection.
        data: JSON application payload associated with the result. Defaults to
            None.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
    """

    model_config = ConfigDict(extra="allow")

    command_id: UUIDText
    state: Literal["succeeded", "failed", "cancelled"]
    result: Literal["success", "fail"]
    data: JsonValue = None
    error: JsonValue = None


class ControllerRejection(_Document):
    """Intake errors can lack a valid command ID, unlike admitted outcomes.

    Args:
        command_id: Correlation value copied from rejected intake; it may be
            absent or not a valid UUID.
        state: Command lifecycle state, distinct from experiment phase.
        result: Application outcome, success or fail.
        data: JSON application payload associated with the result. Defaults to
            None.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
    """

    model_config = ConfigDict(extra="allow")

    command_id: JsonValue
    state: Literal["failed", "cancelled"]
    result: Literal["fail"]
    data: JsonValue = None
    error: JsonValue = None


class RuntimeCommandOutcome(CommandStatus):
    """Runtime-generated pending, unknown and oversized-response observations.

    Args:
        state: Command lifecycle state, distinct from experiment phase.
        result: Success for succeeded, fail for failed/cancelled, otherwise
            None.
        command_id: Command UUID used for receipt lookup and duplicate-
            submission detection.
        data: JSON application payload associated with the result. Defaults to
            None.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
    """

    model_config = ConfigDict(extra="allow")

    command_id: UUIDText
    data: JsonValue = None
    error: JsonValue = None
