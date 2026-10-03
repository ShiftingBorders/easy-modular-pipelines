"""Query and cursor syntax; publication lifetime and source identity remain live checks."""

from typing import Annotated

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
        if isinstance(document, dict) and document.keys() - cls.model_fields.keys():
            raise ValueError("Unknown query parameter.")
        return document

    @field_validator("*", mode="before")
    @classmethod
    def bounded_text(cls, value: object) -> object:
        if isinstance(value, str) and len(value) > 4096:
            raise ValueError("Query parameter is too long.")
        return value

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, value: str | None) -> str | None:
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


class PageLimit(RootModel[Annotated[int, Field(ge=1, le=1000)]]):
    model_config = ConfigDict(strict=True)

    @model_validator(mode="before")
    @classmethod
    def parse_integer(cls, value: object) -> int:
        # Preserve the existing explicit int() conversion for library query values.
        return int(value)


class _Cursor(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "history cursor")


class _PositionCursor(_Cursor):
    position: tuple[int, str, str]

    @field_validator("position", mode="before")
    @classmethod
    def json_position(cls, value: object) -> tuple:
        if not isinstance(value, list):
            raise TypeError("Cursor position must be a three-item JSON array.")
        return tuple(value)


class KeysetCursor(_PositionCursor):
    scope: list[JsonValue]
    version: NonnegativeInteger
    at: FiniteFloat


class PublicationCursor(_PositionCursor):
    publication: str


class OffsetCursor(_Cursor):
    publication: str
    offset: NonnegativeInteger


class DetailIdentity(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    journal_id: JsonValue = None
    generation: JsonValue = None


class DetailReference(DetailIdentity):
    event_ids: Annotated[
        list[Annotated[str, Field(min_length=1)]], Field(min_length=1, max_length=1000)
    ]
    kind: JsonValue = None
