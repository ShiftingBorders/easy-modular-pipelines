"""Sparse saved metadata and compact history inputs, distinct from runtime state."""

from copy import deepcopy
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field

from core.primitives.json_values import JsonObject, JsonValue


class SchedulingMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    # Old scheduling documents may lack schema, identity and runtime definitions.
    template: JsonObject = Field(default_factory=dict)
    phase: JsonValue = "unknown"
    mode: JsonValue = None


class CompactTemplate(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    name: JsonValue = None
    cycles: JsonValue = None
    stages: list[JsonObject] = Field(default_factory=list)
    services: list[JsonObject] = Field(default_factory=list)


@dataclass(frozen=True)
class SchedulingState:
    """A sparse metadata observation and its owner-computed display/error fields."""

    metadata: SchedulingMetadata | None = None
    name: JsonValue = None
    error: str | None = None

    @property
    def phase(self) -> JsonValue:
        return self.metadata.phase if self.metadata is not None else "unknown"

    def document(self) -> JsonObject:
        if self.metadata is None:
            return {"phase": "unknown", "error": self.error}
        return deepcopy(
            {
                "phase": self.metadata.phase,
                "mode": self.metadata.mode,
                "name": self.name,
            }
        )
