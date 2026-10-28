"""Validated checkpoint request-ID representations; no database or runner I/O."""

from itertools import pairwise
from typing import Annotated, Literal, Self

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

CanonicalRequestId = Annotated[
    str,
    Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
]
CheckpointType = Literal["runner.checkpoint", "rebuild.checkpoint"]


def _sorted_unique(values: list[str]) -> list[str]:
    if any(previous >= current for previous, current in pairwise(values)):
        raise ValueError("Checkpoint request IDs must be sorted and unique.")
    return values


SortedRequestIds = Annotated[list[CanonicalRequestId], AfterValidator(_sorted_unique)]


class CheckpointIdsInput(BaseModel):
    """Eligibility fields only; other checkpoint data is preserved verbatim."""

    model_config = ConfigDict(strict=True, frozen=True, extra="ignore")

    schema_version: Literal[3, 4]
    experiment_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    used_request_ids: SortedRequestIds


class _StoredCheckpointEnvelope(BaseModel):
    """Only the fields needed to bind a stored placeholder to its history."""

    model_config = ConfigDict(strict=True, frozen=True, extra="ignore")

    event_id: str
    event_type: CheckpointType
    data: CheckpointIdsInput


class CheckpointIdHistory(BaseModel):
    """A full array at depth zero, or additions to an earlier scoped array."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    event_id: str = Field(min_length=1)
    experiment_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    event_type: CheckpointType
    generation: str = Field(pattern=r"^[0-9a-f]{32}$")
    base_event_id: str | None = Field(default=None, min_length=1)
    depth: int = Field(ge=0, lt=32)
    ids: SortedRequestIds
    item_count: int = Field(ge=0)
    event_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_anchor(self) -> Self:
        if (self.base_event_id is None) != (self.depth == 0):
            raise ValueError("Checkpoint base and depth disagree.")
        if self.base_event_id == self.event_id:
            raise ValueError("A checkpoint cannot reference itself.")
        if self.item_count < len(self.ids) or (
            self.base_event_id is None and self.item_count != len(self.ids)
        ):
            raise ValueError("Checkpoint request-ID count is inconsistent.")
        return self

    def scope(self) -> tuple[str, str, str, str]:
        """Historical scope, independent of the journal's current generation."""
        return self.experiment_id, self.run_id, self.event_type, self.generation

    def validate_envelope(self, document: object) -> None:
        """Require an empty placeholder and the same event and state identity."""
        event = _StoredCheckpointEnvelope.model_validate(document)
        if (
            event.event_id != self.event_id
            or event.event_type != self.event_type
            or event.data.experiment_id != self.experiment_id
            or event.data.run_id != self.run_id
            or event.data.used_request_ids
        ):
            raise ValueError("Compact checkpoint envelope disagrees with its history.")
