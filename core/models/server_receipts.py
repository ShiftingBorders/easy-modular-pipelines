"""Command and chain receipts; correlation with a submitted request stays outside."""

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.models.server_commands import CommandStatus
from core.models.values import UUIDText
from core.primitives.json_values import JsonObject, copy_json_object


class CommandReceipt(CommandStatus):
    model_config = ConfigDict(extra="allow")

    command_id: UUIDText
    server_instance_id: UUIDText
    chain_id: UUIDText | None = None


class ChainReceipt(BaseModel):
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    chain_id: UUIDText
    server_instance_id: UUIDText
    commands: Annotated[list[CommandReceipt], Field(min_length=1)]

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        if type(document) is not dict:
            raise TypeError("A chain receipt must be a JSON object.")
        # Each command has its own validated JSON envelope and depth allowance.
        header = copy_json_object(
            {key: value for key, value in document.items() if key != "commands"},
            "chain receipt",
        )
        if "commands" in document:
            header["commands"] = document["commands"]
        return header

    @model_validator(mode="after")
    def match_instance(self) -> Self:
        if any(
            receipt.server_instance_id != self.server_instance_id
            or receipt.chain_id not in (None, self.chain_id)
            for receipt in self.commands
        ):
            raise ValueError("Chain receipts must belong to one chain and server instance.")
        return self
