"""Sparse saved metadata and compact history inputs, distinct from runtime state."""

from copy import deepcopy
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator

from core.primitives.json_values import JsonObject, JsonValue


class SchedulingMetadata(BaseModel):
    """Sparse saved scheduling fields, including legacy documents.

    Args:
        template: Sparse recorded template JSON; full runtime validation is
            performed elsewhere. Defaults to a new empty dict.
        phase: Observed experiment lifecycle phase; it does not independently
            prove process liveness. Defaults to unknown.
        mode: Running or paused scheduler mode. Defaults to None.
    """
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    # Old scheduling documents may lack schema, identity and runtime definitions.
    template: JsonObject = Field(default_factory=dict)
    phase: JsonValue = "unknown"
    mode: JsonValue = None


class CompactTemplate(BaseModel):
    """Template fields needed by dashboard history projections.

    Args:
        name: Optional recorded experiment display name; historical metadata may
            be sparse. Defaults to None.
        cycles: Positive number of complete passes through the DAG. Defaults to
            None.
        stages: Recorded stage dictionaries used to build compact dashboard
            projections. Defaults to a new empty list.
        services: Recorded service dictionaries used to build compact dashboard
            projections. Defaults to a new empty list.
    """
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    name: JsonValue = None
    cycles: JsonValue = None
    stages: list[JsonObject] = Field(default_factory=list)
    services: list[JsonObject] = Field(default_factory=list)


class RecordedReaderLogging(RootModel[JsonObject]):
    """Recorded logging object before materializing the existing reader file.

    Full option validation and path resolution remain with the logging loader;
    this sparse check preserves unknown recorded fields and their original order.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    @model_validator(mode="before")
    @classmethod
    def require_logging_object(cls, value: object) -> object:
        """Return a logging dictionary or raise TypeError when it is unavailable.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            A logging dictionary or raise TypeError when it is unavailable.
        """
        if not isinstance(value, dict):
            raise TypeError("Recorded logging settings are missing.")
        return value


@dataclass(frozen=True)
class SchedulingState:
    """A sparse metadata observation and its owner-computed display/error fields."""

    metadata: SchedulingMetadata | None = None
    name: JsonValue = None
    error: str | None = None

    @property
    def phase(self) -> JsonValue:
        """Observed phase, or unknown when scheduling metadata is unavailable."""
        return self.metadata.phase if self.metadata is not None else "unknown"

    def document(self) -> JsonObject:
        """Return detached display fields or the error for unavailable metadata."""
        if self.metadata is None:
            return {"phase": "unknown", "error": self.error}
        return deepcopy(
            {
                "phase": self.metadata.phase,
                "mode": self.metadata.mode,
                "name": self.name,
            }
        )
