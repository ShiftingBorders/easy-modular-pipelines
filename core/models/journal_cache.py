"""Derived-cache inputs and documents; source correspondence remains an operation."""

from typing import Annotated

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationInfo,
    field_validator,
    model_validator,
)

from core.models.journal_records import SQLitePosition, UTCText, _Document
from core.models.values import (
    AbsolutePath,
    Boolean,
    NonnegativeInteger,
    PositiveInteger,
    SchemaVersionOne,
    Text,
    UUIDText,
    _absolute_path,
)
from core.primitives.json_values import JsonObject, copy_json_object


class CacheIdentity(_Document):
    # Cache comparisons preserve the caller's identity spelling, unlike journal opening.
    journal_id: UUIDText
    generation: UUIDText


class HistoryCacheParameters(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, hide_input_in_errors=True)

    path: AbsolutePath
    config_path: AbsolutePath
    identity: CacheIdentity
    file_key: tuple[NonnegativeInteger, NonnegativeInteger]
    experiment_id: Text
    window_events: PositiveInteger
    max_bytes: PositiveInteger
    max_events: PositiveInteger = 100000


class CacheSource(_Document):
    version: SchemaVersionOne
    identity: CacheIdentity
    file_key: Annotated[list[NonnegativeInteger], Field(min_length=2, max_length=2)]
    experiment_id: Text


class CacheChangeCheckpoint(CacheIdentity):
    change_cursor: SQLitePosition


class CacheCheckpoint(CacheChangeCheckpoint):
    cursor: SQLitePosition


class JournalBoundary(CacheCheckpoint):
    schema_version: Annotated[int, Field(strict=True, ge=2, le=2)]
    event_count: SQLitePosition


class CacheGap(_Document):
    after: SQLitePosition
    before: SQLitePosition


class CachePublication(_Document):
    complete: Boolean
    version: NonnegativeInteger
    observed_at: Text | None
    cached_through: CacheCheckpoint
    boundary: JournalBoundary | None = None
    window_count: NonnegativeInteger | None = None
    cache_available: Boolean | None = None
    window_start_cursor: SQLitePosition | None = None
    window_predecessor_cursor: SQLitePosition | None = None
    gap: CacheGap | None = None
    target_boundary: JournalBoundary | None = None


class FilteredPublication(JournalBoundary):
    publication_id: UUIDText
    published_at: UTCText


class CacheReaderContext(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True, frozen=True, hide_input_in_errors=True)

    directory: Text
    project_root: Text
    state: JsonObject
    _field_order: tuple[str, ...] = PrivateAttr()

    @model_validator(mode="wrap")
    @classmethod
    def preserve_field_order(cls, document: object, handler):
        model = handler(document)
        if not isinstance(document, cls):
            model._field_order = tuple(document)
        return model

    def document(self) -> JsonObject:
        """Keep the original JSON order used by the publication fingerprint."""
        fields = self.model_dump()
        return {key: fields[key] for key in self._field_order}

    @field_validator("directory", "project_root")
    @classmethod
    def absolute_path(cls, value: str, info: ValidationInfo) -> str:
        # Preserve path spelling and publication hashes while checking the path.
        _absolute_path(value, info)
        return value

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        if type(document) is not dict:
            raise TypeError("Cache reader context must be a JSON object.")
        # State has its own depth allowance; the publication wrapper adds no level.
        context = copy_json_object(
            {key: value for key, value in document.items() if key != "state"},
            "cache reader context",
        )
        if "state" in document:
            context["state"] = copy_json_object(document["state"], "cache reader state")
        return context


class HistoryCacheRefresh(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    state: JsonObject
    target: JournalBoundary | None = None
    window: Boolean = True
    reader_context: CacheReaderContext | None = None

    @field_validator("state", mode="before")
    @classmethod
    def detach_state(cls, value: object) -> JsonObject:
        return copy_json_object(value, "cache reader state")
