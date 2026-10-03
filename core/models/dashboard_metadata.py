"""Sparse saved metadata and compact history inputs, distinct from runtime state."""

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
