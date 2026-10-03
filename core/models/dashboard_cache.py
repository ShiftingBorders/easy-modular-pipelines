"""Cache process results and persisted publications; live source checks stay outside."""

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.models.journal_cache import CacheCheckpoint, CacheIdentity, JournalBoundary
from core.models.values import NonnegativeInteger, Number, PositiveInteger, Text
from core.primitives.json_values import JsonObject, copy_json_object


class _Document(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "dashboard cache document")


class CacheWorkerError(_Document):
    code: str
    message: str


class CacheWorkerResult(_Document):
    experiment_id: Text
    pid: PositiveInteger
    complete: bool
    cached_through: CacheCheckpoint | None = None
    target_boundary: JournalBoundary | None = None
    error: CacheWorkerError | None = None
    modules_published: bool | None = None
    modules_error: str | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.error is not None:
            if self.complete:
                raise ValueError("A failed cache job cannot report completion.")
            return self
        if self.cached_through is None or self.target_boundary is None:
            raise ValueError("Successful cache progress requires its checkpoints.")
        if (
            self.cached_through.journal_id != self.target_boundary.journal_id
            or self.cached_through.generation != self.target_boundary.generation
        ):
            raise ValueError("Cache progress checkpoints belong to different journals.")
        if self.complete and (
            self.cached_through.cursor < self.target_boundary.cursor
            or self.cached_through.change_cursor < self.target_boundary.change_cursor
        ):
            raise ValueError("Completed cache progress has not reached its target.")
        return self


class ModuleCacheSource(_Document):
    directory: str
    complete: bool
    error: str | None = None
    journal: CacheIdentity | None = None
    file_key: (
        Annotated[list[NonnegativeInteger], Field(min_length=2, max_length=2)] | None
    ) = None
    cache_schema_version: Annotated[int, Field(ge=1, le=1)] | None = None
    version: NonnegativeInteger | None = None
    cached_through: CacheCheckpoint | None = None


class ModuleStatistics(_Document):
    model_config = ConfigDict(extra="allow")

    module_id: str
    name: str
    version: str | None
    module_hash: str | None
    runs: NonnegativeInteger
    error_count: NonnegativeInteger
    restarts: NonnegativeInteger
    recent_attempts: list[JsonObject]
    complete: bool
    p50_seconds: Number | None
    p95_seconds: Number | None
    experiment_name: str


class ModulePublication(_Document):
    schema_version: Annotated[int, Field(ge=2, le=2)]
    project_root: str
    sources: dict[str, ModuleCacheSource]
    items: list[ModuleStatistics]
    complete: bool
    error: str | None
    published_at: str

    @model_validator(mode="after")
    def validate_completeness(self) -> Self:
        if self.complete and any(not source.complete for source in self.sources.values()):
            raise ValueError("Complete module statistics require complete source caches.")
        return self
