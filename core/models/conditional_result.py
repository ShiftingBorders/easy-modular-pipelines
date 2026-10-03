"""Conditional output structure; target membership belongs to the current DAG."""

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.values import NormalizedUUIDText
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


class ConditionalDecision(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    command: Literal["pause", "stop", "move"] | None = None
    stage_id: NormalizedUUIDText | None = None
    data: JsonValue = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, value: object) -> JsonObject:
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
        if self.command == "move" and self.stage_id is None:
            raise ValueError("Conditional move requires a target stage_id.")
        return self
