"""Cache process results and persisted publications; live source checks stay outside."""

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.models.journal_cache import CacheCheckpoint, CacheIdentity, JournalBoundary
from core.models.values import NonnegativeInteger, Number, PositiveInteger, Text
from core.primitives.json_values import JsonObject, copy_json_object


class _Document(BaseModel):
    """Strict detached JSON document exchanged by dashboard cache workers."""
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of the cache document.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "dashboard cache document")


class CacheWorkerError(_Document):
    """Machine-readable code and message describing a failed cache job.

    Args:
        code: Machine-readable diagnostic code used to classify the failure.
        message: Human-readable diagnostic message.
    """
    code: str
    message: str


class CacheWorkerResult(_Document):
    """Worker progress with source checkpoints and optional publication errors.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        pid: Operating-system process identifier; additional identity fields are
            needed to prove ownership.
        complete: Whether the represented cache data covers its advertised
            source boundary.
        cached_through: Journal identity and event/change cursors whose derived
            projections are complete. Defaults to None.
        target_boundary: Source boundary that the current cache task aims to
            make fully available. Defaults to None.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
        modules_published: Whether project-wide module statistics were published
            after this cache task. Defaults to None.
        modules_error: Module-statistics publication error, independent of the
            history-cache result. Defaults to None.
    """
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
        """Return progress after checking completion against its journal boundary.

        Raises:
            ValueError: Failed work claims completion, checkpoints are missing or
                inconsistent, or completed work has not reached its target.
        """
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
    """Source cache identity, publication version, and completeness metadata.

    Args:
        directory: Experiment directory whose derived cache contributed this
            publication.
        complete: Whether the represented cache data covers its advertised
            source boundary.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
        journal: Source journal/generation identity, or None when unavailable.
            Defaults to None.
        file_key: Filesystem device/inode pair used to detect replacement of the
            source journal. Defaults to None.
        cache_schema_version: Version of the disposable cache schema, separate
            from the source journal schema. Defaults to None.
        version: Derived-cache publication version; None when no version was
            observed. Defaults to None.
        cached_through: Journal identity and event/change cursors whose derived
            projections are complete. Defaults to None.
    """
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
    """Module execution counts and duration percentiles in seconds.

    Args:
        module_id: Stable identity used to group this module's execution
            statistics.
        name: Module name represented by this aggregate.
        version: Recorded module version, or None when absent from historical
            metadata.
        module_hash: Recorded module content hash, or None when unavailable in
            historical data.
        runs: Number of represented module executions.
        error_count: Number of recorded module errors in this aggregate.
        restarts: Number of recorded module restarts in this aggregate.
        recent_attempts: Recent attempt summaries retained alongside full
            aggregate statistics.
        complete: Whether the represented cache data covers its advertised
            source boundary.
        p50_seconds: Median execution duration in seconds, or None without
            duration observations.
        p95_seconds: 95th percentile execution duration in seconds, or None
            without observations.
        experiment_name: Display name of the experiment contributing these
            statistics.
    """
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
    """Project-wide module statistics with their source cache boundaries.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        project_root: Absolute root of the project containing registered modules
            and experiments.
        sources: Experiment IDs mapped to the source cache publication
            identities and completeness.
        items: Published module-statistics records.
        complete: Whether the represented cache data covers its advertised
            source boundary.
        error: Failure details, or None when no failure is reported.
        published_at: Wall-clock timestamp of publication.
    """
    schema_version: Annotated[int, Field(ge=2, le=2)]
    project_root: str
    sources: dict[str, ModuleCacheSource]
    items: list[ModuleStatistics]
    complete: bool
    error: str | None
    published_at: str

    @model_validator(mode="after")
    def validate_completeness(self) -> Self:
        """Return the publication, rejecting complete output from incomplete sources."""
        if self.complete and any(not source.complete for source in self.sources.values()):
            raise ValueError("Complete module statistics require complete source caches.")
        return self
