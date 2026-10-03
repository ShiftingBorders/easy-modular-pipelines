"""Collector IPC inputs, validated before updating history or journal selection."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from core.models.resource_target import ResourceContext, ResourceTargetDocument
from core.models.values import (
    AbsolutePath,
    Boolean,
    NonnegativeInteger,
    Number,
    PositiveInteger,
    Text,
    UUIDText,
)
from core.primitives.json_values import JsonObject, copy_json_object


class _Document(BaseModel):
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "collector document")


class CollectorSnapshot(_Document):
    context: ResourceContext
    logging_config_path: AbsolutePath | None
    targets: list[ResourceTargetDocument]


class CollectorRestart(_Document):
    reason: Text
    error: str
    previous_collector_id: UUIDText | None
    context: ResourceContext


class CollectorUpdate(_Document):
    command: Literal["update"]
    revision: NonnegativeInteger
    snapshot: CollectorSnapshot
    restart_notice: CollectorRestart | None = None


class CollectorStop(_Document):
    command: Literal["stop"]


class ResourceMeasurement(_Document):
    value: int | float | None
    unit: Text
    kind: Literal["gauge"]
    scope: Text
    estimated: Boolean
    attributes: JsonObject


class ResourceSample(_Document):
    series_id: Text
    context: ResourceContext
    observed_at: Text
    observed_monotonic: Number
    resources: dict[Text, ResourceMeasurement]


class CollectorPacket(_Document):
    collector_id: UUIDText
    pid: PositiveInteger
    revision: Annotated[int, Field(ge=-1)]
    journal_closed: Boolean
    journal_error: str | None
    unconfirmed_samples: NonnegativeInteger
    samples: list[ResourceSample]


CollectorCommand = TypeAdapter(
    Annotated[CollectorUpdate | CollectorStop, Field(discriminator="command")]
)
