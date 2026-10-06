"""Query and cursor syntax; publication lifetime and source identity remain live checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, SupportsIndex, SupportsInt

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    RootModel,
    field_validator,
    model_validator,
)

from core.models.values import NonnegativeInteger
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


class DashboardQuery(BaseModel):
    """Supported dashboard HTTP query parameters before view-specific binding.

    Args:
        run_id: Optional logical-run filter in query text. Defaults to None.
        view: Requested raw/effective event view; the consuming view applies its
            defaults. Defaults to None.
        cursor: Opaque serialized continuation cursor; publication lifetime is
            checked by the reader. Defaults to None.
        limit: Page-size text convertible to an integer from 1 through 1000.
            Defaults to None.
        q: Optional text-search expression consumed by the selected dashboard
            view. Defaults to None.
        module: Optional module filter. Defaults to None.
        metric: Optional resource metric filter. Defaults to None.
        since: Optional lower time boundary in query text. Defaults to None.
        until: Optional upper time boundary in query text. Defaults to None.
        revision: Optional applied-template revision filter. Defaults to None.
        ref: Encoded reference selecting original events for detail inspection.
            Defaults to None.
        compact: Text flag selecting the view's compact response representation.
            Defaults to None.
    """
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    run_id: str | None = None
    view: str | None = None
    cursor: str | None = None
    limit: str | None = None
    q: str | None = None
    module: str | None = None
    metric: str | None = None
    since: str | None = None
    until: str | None = None
    revision: str | None = None
    ref: str | None = None
    compact: str | None = None

    @model_validator(mode="before")
    @classmethod
    def known_fields(cls, document: object) -> object:
        """Return query input after rejecting unknown parameter names.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Query input after rejecting unknown parameter names.
        """
        if isinstance(document, dict) and document.keys() - cls.model_fields.keys():
            raise ValueError("Unknown query parameter.")
        return document

    @field_validator("*", mode="before")
    @classmethod
    def bounded_text(cls, value: object) -> object:
        """Return a query value after rejecting text longer than 4096 characters.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            A query value after rejecting text longer than 4096 characters.
        """
        if isinstance(value, str) and len(value) > 4096:
            raise ValueError("Query parameter is too long.")
        return value

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, value: str | None) -> str | None:
        """Return limit text after checking its integer value is from 1 to 1000.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            Limit text after checking its integer value is from 1 to 1000.
        """
        if value is not None:
            try:
                PageLimit.model_validate(value)
            except ValueError as error:
                try:
                    int(value)
                except ValueError:
                    raise ValueError("limit must be an integer.") from error
                raise ValueError("limit must be between 1 and 1000.") from error
        return value


@dataclass(frozen=True)
class ViewQuery:
    """Bound operation arguments; native values keep their existing local checks.

    HTTP text is validated by DashboardQuery. Library callers retain int()
    conversion for limits and instant() handling of non-text range values.
    Presence matters for explicit nulls versus omitted defaults.
    """

    run_id: str | None = None
    view: str | None = "effective"
    cursor: str | bytes | bytearray | None = None
    limit: object = 200
    since: object = None
    until: object = None
    revision: str | None = None
    ref: str | bytes | bytearray | None = "{}"
    compact: str | None = None
    has_range: bool = False

    @classmethod
    def from_query(cls, query: DashboardQuery | dict | ViewQuery) -> ViewQuery:
        """Bind HTTP or library query values while preserving explicit nulls.

        Args:
            query: Validated HTTP query, native argument dictionary, or bound query.

        Returns:
            Bound view arguments; an existing ViewQuery is returned unchanged.
        """
        if isinstance(query, ViewQuery):
            return query
        if isinstance(query, DashboardQuery):
            fields = query.model_fields_set
            return cls(
                run_id=query.run_id,
                view=query.view if "view" in fields else "effective",
                cursor=query.cursor,
                limit=query.limit if "limit" in fields else 200,
                since=query.since,
                until=query.until,
                revision=query.revision,
                ref=query.ref if "ref" in fields else "{}",
                compact=query.compact,
                has_range=bool(fields & {"since", "until"}),
            )
        return cls(
            run_id=query.get("run_id"),
            view=query.get("view", "effective"),
            cursor=query.get("cursor"),
            limit=query.get("limit", 200),
            since=query.get("since"),
            until=query.get("until"),
            revision=query.get("revision"),
            ref=query.get("ref", "{}"),
            compact=query.get("compact"),
            has_range="since" in query or "until" in query,
        )


class PageLimit(RootModel[Annotated[int, Field(ge=1, le=1000)]]):
    """Page size from 1 to 1000 using the library's explicit int conversion."""
    model_config = ConfigDict(strict=True)

    @model_validator(mode="before")
    @classmethod
    def parse_integer(
        cls, value: str | bytes | bytearray | SupportsInt | SupportsIndex
    ) -> int:
        # Preserve the existing explicit int() conversion for library query values.
        """Convert a library page-limit value with int before range validation.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            The result of explicit int conversion; the enclosing model subsequently
            enforces the 1 through 1000 range.
        """
        return int(value)


class _Cursor(BaseModel):
    """Detached history cursor that tolerates additional recorded fields."""
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of the history cursor.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "history cursor")


class _PositionCursor(_Cursor):
    """History cursor with a three-part sortable position."""
    position: tuple[int, str, str]

    @field_validator("position", mode="before")
    @classmethod
    def json_position(cls, value: object) -> tuple:
        """Convert a JSON position list to a tuple or raise TypeError.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            Tuple of the supplied array elements; subsequent field validation checks
            its three-item types.
        """
        if not isinstance(value, list):
            raise TypeError("Cursor position must be a three-item JSON array.")
        return tuple(value)


class KeysetCursor(_PositionCursor):
    """Position cursor bound to a query scope, version, and observation time.

    Args:
        position: Three-part sortable position encoded as a JSON array and
            retained as a tuple.
        scope: Query-scope values that must match the requested page.
        version: Cache publication version against which this position was
            issued.
        at: Finite observation time used to check cursor lifetime.
    """
    scope: list[JsonValue]
    version: NonnegativeInteger
    at: FiniteFloat


class PublicationCursor(_PositionCursor):
    """Position cursor tied to a particular history publication.

    Args:
        position: Three-part sortable position encoded as a JSON array and
            retained as a tuple.
        publication: Identity of the cached publication to which this cursor
            belongs.
    """
    publication: str


class OffsetCursor(_Cursor):
    """Nonnegative offset into a particular history publication.

    Args:
        publication: Identity of the cached publication to which this cursor
            belongs.
        offset: Nonnegative offset into the selected cached publication.
    """
    publication: str
    offset: NonnegativeInteger


class DetailIdentity(BaseModel):
    """Optional journal identity supplied with a historical detail reference.

    Args:
        journal_id: Identity of the source journal, distinct from its
            restoration generation. Defaults to None.
        generation: Journal generation used to reject checkpoints from
            superseded history. Defaults to None.
    """
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    journal_id: JsonValue = None
    generation: JsonValue = None


class DetailReference(DetailIdentity):
    """Journal detail reference containing between 1 and 1000 event IDs.

    Args:
        journal_id: Identity of the source journal, distinct from its
            restoration generation. Defaults to None.
        generation: Journal generation used to reject checkpoints from
            superseded history. Defaults to None.
        event_ids: Between 1 and 1000 nonempty source event IDs to inspect.
        kind: Optional detail type retained for interpretation by the selected
            reader. Defaults to None.
    """
    event_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]], Field(min_length=1, max_length=1000)
    ]
    kind: JsonValue = None
