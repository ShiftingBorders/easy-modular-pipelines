"""Collector read projections; their partial metadata differs from full IPC packets."""

from datetime import datetime

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from core.models.values import NonnegativeInteger
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


class _Document(BaseModel):
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "collector read document")


class CollectorMetric(_Document):
    value: int | float | None = None
    attributes: JsonObject = Field(default_factory=dict)


class MetricFreshness(_Document):
    fresh: bool


class CollectorSample(_Document):
    series_id: str
    observed_at: str
    resources: dict[str, CollectorMetric]
    freshness: dict[str, MetricFreshness] = Field(default_factory=dict)

    @field_validator("observed_at")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        # The existing read contract accepts both aware and local naive timestamps.
        datetime.fromisoformat(value).timestamp()
        return value


class CollectorStatus(_Document):
    latest: list[CollectorSample]
    history_id: JsonValue = None


class CollectorHistoryPage(_Document):
    samples: list[CollectorSample]
    cursor: NonnegativeInteger
    history_id: JsonValue
    gap: bool


CollectorSamples = TypeAdapter(list[CollectorSample])
