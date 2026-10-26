"""HTTP-only query representation; library arguments reuse their existing models."""

import json
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.models.values import PositiveInteger, Text
from core.primitives.json_values import JsonObject, copy_json_object


class ShutdownQuery(BaseModel):
    """HTTP shutdown option using explicit true/false text."""
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    wait: Literal["true", "false"] = "false"


class EventQuery(BaseModel):
    """Experiment event query with a bounded page size and decoded checkpoint.

    Args:
        limit: Maximum page item count, from 1 through 1000. Defaults to 100.
        cursor: JSON-encoded journal checkpoint text, at most 4096 characters,
            decoded to an object; None starts reading. Defaults to None.
        experiment_id: Experiment identifier associating this document with its
            execution history.
    """
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    limit: Annotated[PositiveInteger, Field(le=1000)] = 100
    cursor: JsonObject | None = None
    experiment_id: Text

    @field_validator("cursor", mode="before")
    @classmethod
    def decode_cursor(cls, value: object) -> JsonObject | None:
        """Decode a JSON checkpoint from query text, allowing an omitted cursor.

        Args:
            value: JSON text of at most 4096 characters, or None.

        Returns:
            A detached checkpoint object, or None.

        Raises:
            TypeError: A supplied cursor is not text.
            ValueError: The text is too large or does not encode a valid JSON object.
        """
        if value is None:
            return None
        if type(value) is not str:
            raise TypeError("Journal cursor must be encoded JSON.")
        if len(value) > 4096:
            raise ValueError("Journal cursor is too large.")
        return copy_json_object(json.loads(value), "cursor")
