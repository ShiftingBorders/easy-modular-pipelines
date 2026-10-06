"""Conditional output structure; target membership belongs to the current DAG."""

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.values import NormalizedUUIDText
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


class ConditionalDecision(BaseModel):
    """Conditional stage output with an optional DAG command and payload.

    Args:
        command: Optional pause, stop, or move decision; None advances normally.
            Defaults to None.
        stage_id: Target node UUID, required only for move and forbidden on
            other commands. Defaults to None.
        data: Conditional application payload; field presence distinguishes
            omitted data from explicit null. Defaults to None.
    """
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    command: Literal["pause", "stop", "move"] | None = None
    stage_id: NormalizedUUIDText | None = None
    data: JsonValue = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, value: object) -> JsonObject:
        """Normalize null to an empty decision and copy supported decision fields.

        Args:
            value: Null or a JSON decision object returned by a conditional stage.

        Returns:
            Detached decision data for field validation.

        Raises:
            ValueError: The input, command, or supplied fields violate the contract.
        """
        if value is None:
            return {}
        if type(value) is not dict:
            raise ValueError("A conditional result must be null or a decision object.")
        document = copy_json_object(value, "conditional decision")
        if document.keys() - cls.model_fields.keys():
            raise ValueError("Unknown conditional decision fields.")
        if document.get("command") not in (None, "pause", "stop", "move"):
            raise ValueError("Conditional commands are pause, stop, or move.")
        if document.get("command") != "move" and "stage_id" in document:
            raise ValueError("Only move accepts a target stage_id.")
        return document

    @model_validator(mode="after")
    def require_move_target(self) -> Self:
        """Return the decision, requiring a target stage ID for a move command."""
        if self.command == "move" and self.stage_id is None:
            raise ValueError("Conditional move requires a target stage_id.")
        return self
