"""HTTP-only query representation; library arguments reuse their existing models."""

import json
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.models.values import PositiveInteger, Text
from core.primitives.json_values import JsonObject, copy_json_object


class ShutdownQuery(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    wait: Literal["true", "false"] = "false"


class EventQuery(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    limit: Annotated[PositiveInteger, Field(le=1000)] = 100
    cursor: JsonObject | None = None
    experiment_id: Text

    @field_validator("cursor", mode="before")
    @classmethod
    def decode_cursor(cls, value: object) -> JsonObject | None:
        if value is None:
            return None
        if type(value) is not str:
            raise TypeError("Journal cursor must be encoded JSON.")
        if len(value) > 4096:
            raise ValueError("Journal cursor is too large.")
        return copy_json_object(json.loads(value), "cursor")
