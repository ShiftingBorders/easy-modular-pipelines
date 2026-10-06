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

from core.models.journal_records import (
    JournalCheckpoint,
    SQLitePosition,
    UTCText,
    _Document,
)
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
    """Journal and generation UUIDs preserving the source's original spelling.

    Args:
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
    """
    journal_id: UUIDText
    generation: UUIDText


class HistoryCacheParameters(BaseModel):
    """Absolute cache paths, source identity, and bounded history working limits.

    Args:
        path: Absolute path to the disposable derived-cache SQLite database.
        config_path: Absolute source logger configuration path used for read-
            only history access.
        identity: Expected source journal and generation.
        file_key: Filesystem device/inode pair used to detect replacement of the
            source journal.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        window_events: Maximum number of raw events retained in the active RAM
            window.
        max_bytes: Byte budget for the active window and each projection working
            set.
        max_events: Maximum effective events loaded for one projection working
            set. Defaults to 100000.
    """
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
    """Persisted source identity and file key for a derived experiment cache.

    Args:
        version: Source-descriptor schema version, currently 1.
        identity: Expected source journal and generation.
        file_key: Filesystem device/inode pair used to detect replacement of the
            source journal.
        experiment_id: Experiment identifier associating this document with its
            execution history.
    """
    version: SchemaVersionOne
    identity: CacheIdentity
    file_key: Annotated[list[NonnegativeInteger], Field(min_length=2, max_length=2)]
    experiment_id: Text

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> dict[str, object]:
        """Copy source JSON while retaining a validated CacheIdentity instance.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        if type(document) is dict and type(document.get("identity")) is CacheIdentity:
            identity = document["identity"]
            values = dict(document)
            values["identity"] = {
                "journal_id": identity.journal_id,
                "generation": identity.generation,
            }
            detached: dict[str, object] = dict(
                copy_json_object(values, "journal document")
            )
            detached["identity"] = identity
            return detached
        return dict(copy_json_object(document, "journal document"))


class CacheChangeCheckpoint(CacheIdentity):
    """Journal identity and last consumed change cursor.

    Args:
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
        change_cursor: Nonnegative SQLite change-feed position, independent of
            the event cursor.
    """
    change_cursor: SQLitePosition


class CacheCheckpoint(CacheChangeCheckpoint):
    """Journal identity and last consumed event and change cursors.

    Args:
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
        change_cursor: Nonnegative SQLite change-feed position, independent of
            the event cursor.
        cursor: Nonnegative event cursor identifying a position in the source
            journal.
    """
    cursor: SQLitePosition


class JournalBoundary(CacheCheckpoint):
    """Observed journal schema, event count, and event/change cursor boundary.

    Args:
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
        change_cursor: Nonnegative SQLite change-feed position, independent of
            the event cursor.
        cursor: Nonnegative event cursor identifying a position in the source
            journal.
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        event_count: Number of source events represented by the recorded
            boundary or export.
    """
    schema_version: Annotated[int, Field(strict=True, ge=2, le=2)]
    event_count: SQLitePosition


class CacheGap(_Document):
    """Exclusive cursor endpoints delimiting a gap in cached history.

    Args:
        after: Last cached source cursor before the gap, excluded from the gap.
        before: First source cursor after the gap, excluded from the gap.
    """
    after: SQLitePosition
    before: SQLitePosition


class CachePublication(_Document):
    """Published cache progress, RAM window metadata, and observed boundaries.

    Args:
        complete: Whether the represented cache data covers its advertised
            source boundary.
        version: Monotonic version of the derived publication, independent of
            schema version.
        observed_at: Wall-clock timestamp at which this observation was
            recorded.
        cached_through: Journal identity and event/change cursors whose derived
            projections are complete.
        boundary: Observed source journal boundary associated with this
            page/publication. Defaults to None.
        window_count: Number of raw source events retained in the active RAM
            window. Defaults to None.
        cache_available: Whether a compatible derived database publication is
            available. Defaults to None.
        window_start_cursor: Cursor of the oldest event in the active RAM
            window. Defaults to None.
        window_predecessor_cursor: Last source cursor before the active RAM
            window, used to delimit an uncached gap. Defaults to None.
        gap: Optional uncached interval between completed disk history and the
            RAM window. Defaults to None.
        target_boundary: Source boundary that the current cache task aims to
            make fully available. Defaults to None.
    """
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

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> dict[str, object]:
        """Copy publication JSON while retaining recognized checkpoint and gap models.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        if type(document) is not dict:
            return dict(copy_json_object(document, "journal document"))
        values = dict(document)
        retained = {}
        expected = {
            "cached_through": CacheCheckpoint,
            "boundary": JournalBoundary,
            "target_boundary": JournalBoundary,
            "gap": CacheGap,
        }
        for name, model in expected.items():
            if type(values.get(name)) is model:
                value = values[name]
                values[name] = dict(value)
                retained[name] = value
        detached: dict[str, object] = dict(copy_json_object(values, "journal document"))
        detached.update(retained)
        return detached


class FilteredPublication(JournalBoundary):
    """Filtered journal boundary with publication identity and UTC timestamp.

    Args:
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
        change_cursor: Nonnegative SQLite change-feed position, independent of
            the event cursor.
        cursor: Nonnegative event cursor identifying a position in the source
            journal.
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        event_count: Number of source events represented by the recorded
            boundary or export.
        publication_id: Identity tying pagination to one immutable filtered
            publication.
        published_at: Wall-clock timestamp of publication.
    """
    publication_id: UUIDText
    published_at: UTCText


class FilteredCheckpoint(_Document):
    """Filtered cursor accepts UUID publications and synthetic source publications.

    Args:
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
        cursor: Nonnegative event cursor identifying a position in the source
            journal.
        publication_id: Identity tying pagination to one immutable filtered
            publication.
    """

    journal_id: str
    generation: str
    cursor: SQLitePosition
    publication_id: str

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Validate publication fields and normalize the embedded journal checkpoint.

        Args:
            document: Journal identity, event cursor, and nonempty publication ID.

        Returns:
            Detached checkpoint with normalized journal identity.

        Raises:
            ValueError: Required fields, publication ID, or journal identity are invalid.
        """
        document = copy_json_object(document, "publication checkpoint")
        if document.keys() != {"journal_id", "generation", "cursor", "publication_id"}:
            raise ValueError("Publication checkpoint fields do not match.")
        if (
            type(document["publication_id"]) is not str
            or not document["publication_id"]
        ):
            raise ValueError("publication_id must be a nonempty string.")
        checkpoint = JournalCheckpoint.model_validate(
            {
                name: value
                for name, value in document.items()
                if name != "publication_id"
            },
            context={"key": "cursor"},
        )
        return {
            "journal_id": checkpoint.journal_id,
            "generation": checkpoint.generation,
            "cursor": checkpoint.position,
            "publication_id": document["publication_id"],
        }


class CacheReaderContext(BaseModel):
    """Saved reader paths and state retaining field order for fingerprinting.

    Args:
        directory: Absolute experiment directory, preserving the original path
            spelling.
        project_root: Absolute root of the project containing registered modules
            and experiments.
        state: Reader projection state copied separately with its own JSON depth
            allowance.
    """
    model_config = ConfigDict(extra="allow", strict=True, frozen=True, hide_input_in_errors=True)

    directory: Text
    project_root: Text
    state: JsonObject
    _field_order: tuple[str, ...] = PrivateAttr()

    @model_validator(mode="wrap")
    @classmethod
    def preserve_field_order(cls, document: object, handler):
        """Run model validation and retain original input order on newly parsed models.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.
            handler: Pydantic continuation that validates the wrapped input and
                returns the model.

        Returns:
            Validated reader context with original field order retained for stable
            publication fingerprinting.
        """
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
        """Check that a path is absolute while preserving its original spelling.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.
            info: Pydantic validation context identifying the field and any explicit
                caller context.

        Returns:
            The original absolute path string without changing its spelling or
            publication fingerprint.
        """
        _absolute_path(value, info)
        return value

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Copy reader context and state with separate JSON depth allowances.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
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
    """Requested cache boundary, reader state, and RAM window refresh options.

    Args:
        state: Current reader state supplied to projection callbacks.
        target: Optional fixed source boundary; None observes a boundary when
            refresh starts. Defaults to None.
        window: Whether refresh should also update the active raw-event RAM
            window. Defaults to True.
        reader_context: Optional paths/state to publish alongside the refreshed
            projections. Defaults to None.
    """
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
        """Return a validated JSON copy of the reader state.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(value, "cache reader state")
