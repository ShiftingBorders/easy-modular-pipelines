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
    """Detached collector read document retaining additional projection fields."""
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of the collector read document.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "collector read document")


class CollectorMetric(_Document):
    """Optional numerical resource value and its measurement attributes.

    Args:
        value: Observed numeric metric value, or None when unavailable. Defaults
            to None.
        attributes: JSON metadata describing the measurement or resource, such
            as provenance and availability. Defaults to a new empty dict.
    """
    value: int | float | None = None
    attributes: JsonObject = Field(default_factory=dict)


class MetricFreshness(_Document):
    """Freshness flag for one projected resource metric."""
    fresh: bool


class CollectorSample(_Document):
    """Timestamped resource values and freshness metadata for one series.

    Args:
        series_id: Identity of the monitored resource time series.
        observed_at: Wall-clock timestamp at which this observation was
            recorded.
        resources: Resource names mapped to numerical observations and
            attributes.
        freshness: Per-metric freshness observations keyed by resource name.
            Defaults to a new empty dict.
        fresh: Whether this observation reflects current data rather than
            retained stale history. Defaults to False.
    """
    series_id: str
    observed_at: str
    resources: dict[str, CollectorMetric]
    freshness: dict[str, MetricFreshness] = Field(default_factory=dict)
    fresh: JsonValue = False

    @field_validator("observed_at")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        # The existing read contract accepts both aware and local naive timestamps.
        """Return an ISO timestamp after checking it can represent a local instant.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            An ISO timestamp after checking it can represent a local instant.
        """
        datetime.fromisoformat(value).timestamp()
        return value


class CollectorStatus(_Document):
    """Latest collector samples and optional runtime or storage errors.

    Args:
        latest: Latest available sample for each resource series.
        history_id: Identity of the collector history, allowing detection of
            cursor reset. Defaults to None.
        state: Collector supervision state as reported by the runtime. Defaults
            to None.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
        journal_error: Latest telemetry journal failure, or None when no error
            is reported. Defaults to None.
        history_error: History retrieval/storage diagnostic, when available.
            Defaults to None.
    """
    latest: list[CollectorSample]
    history_id: JsonValue = None
    state: JsonValue = None
    error: JsonValue = None
    journal_error: JsonValue = None
    history_error: JsonValue = None


class CollectorHistoryPage(_Document):
    """Collector history samples with a continuation cursor and gap indicator.

    Args:
        samples: Ordered resource sample documents carried by the packet/page.
        cursor: Exclusive sample continuation position in this collector
            history.
        history_id: Identity of the collector history, allowing detection of
            cursor reset.
        gap: Whether eviction, reset, or missing sequence numbers interrupted
            the requested history.
    """
    samples: list[CollectorSample]
    cursor: NonnegativeInteger
    history_id: JsonValue
    gap: bool


CollectorSamples = TypeAdapter(list[CollectorSample])
