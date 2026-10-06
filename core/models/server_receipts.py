"""Command and chain receipts; correlation with a submitted request stays outside."""

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.models.server_commands import CommandStatus
from core.models.values import UUIDText
from core.primitives.json_values import copy_json_object


class CommandReceipt(CommandStatus):
    """Command status associated with a server instance and optional chain.

    Args:
        state: Command lifecycle state, distinct from experiment phase.
        result: Success for succeeded, fail for failed/cancelled, otherwise
            None.
        command_id: Command UUID used for receipt lookup and duplicate-
            submission detection.
        server_instance_id: UUID of the HTTP runtime instance that owns these
            receipts or observations.
        chain_id: UUID identifying the ordered command chain. Defaults to None.
    """
    model_config = ConfigDict(extra="allow")

    command_id: UUIDText
    server_instance_id: UUIDText
    chain_id: UUIDText | None = None


class ChainReceipt(BaseModel):
    """Nonempty list of command receipts belonging to one server and chain.

    Args:
        chain_id: UUID identifying the ordered command chain.
        server_instance_id: UUID of the HTTP runtime instance that owns these
            receipts or observations.
        commands: Nonempty command receipts with the same server identity and
            compatible chain IDs.
    """
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    chain_id: UUIDText
    server_instance_id: UUIDText
    commands: Annotated[list[CommandReceipt], Field(min_length=1)]

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> dict[str, object]:
        """Copy receipt metadata while allowing each command its own JSON depth budget.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        if type(document) is not dict:
            raise TypeError("A chain receipt must be a JSON object.")
        # Each command has its own validated JSON envelope and depth allowance.
        header: dict[str, object] = dict(
            copy_json_object(
                {key: value for key, value in document.items() if key != "commands"},
                "chain receipt",
            )
        )
        if "commands" in document:
            header["commands"] = document["commands"]
        return header

    @model_validator(mode="after")
    def match_instance(self) -> Self:
        """Return receipts after matching all child server and optional chain IDs."""
        if any(
            receipt.server_instance_id != self.server_instance_id
            or receipt.chain_id not in (None, self.chain_id)
            for receipt in self.commands
        ):
            raise ValueError("Chain receipts must belong to one chain and server instance.")
        return self
