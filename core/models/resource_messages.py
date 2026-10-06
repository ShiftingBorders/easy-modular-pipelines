"""Collector IPC inputs, validated before updating history or journal selection."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from core.models.journal_records import JournalContext
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
    """Detached collector IPC document preserving additional message fields."""
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of a collector document.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "collector document")


class CollectorSnapshot(_Document):
    """Selected journal context, logging configuration, and monitored processes.

    Args:
        context: Validated journal context identifying the experiment and
            participant scope.
        logging_config_path: Logger configuration path for the existing shared
            journal.
        targets: Processes to monitor, each with a full expected identity and
            journal context.
    """
    context: ResourceContext
    logging_config_path: AbsolutePath | None
    targets: list[ResourceTargetDocument]


class CollectorRestart(_Document):
    """Restart reason, previous collector identity, and diagnostic context.

    Args:
        reason: Machine-readable explanation for collector replacement.
        error: Human-readable diagnostic from the failed collector/supervisor.
        previous_collector_id: Previous collector UUID, or None when no earlier
            identity is known.
        context: Validated journal context identifying the experiment and
            participant scope.
    """
    reason: Text
    error: str
    previous_collector_id: UUIDText | None
    context: ResourceContext


class CollectorUpdate(_Document):
    """Revisioned collector configuration update with an optional restart notice.

    Args:
        command: IPC discriminator, fixed to update.
        revision: Nonnegative target revision used to reject obsolete samples.
        snapshot: Selected journal, context, and monitoring targets for this
            revision.
        restart_notice: Optional diagnostic describing why the collector process
            was replaced. Defaults to None.
    """
    command: Literal["update"]
    revision: NonnegativeInteger
    snapshot: CollectorSnapshot
    restart_notice: CollectorRestart | None = None


class CollectorStop(_Document):
    """IPC command requesting collector shutdown."""
    command: Literal["stop"]


class ResourceMeasurement(_Document):
    """Gauge value, unit, scope, and estimation metadata for a sampled resource.

    Args:
        value: Numeric observation, or None when unavailable.
        unit: Nonempty measurement unit, such as byte, percent, or second.
        kind: Gauge discriminator; resource sampling does not report additive
            deltas here.
        scope: Owner scope of the resource measurement, typically host or
            process.
        estimated: Whether the measurement is an estimate rather than a direct
            observation.
        attributes: JSON metadata describing the measurement or resource, such
            as provenance and availability.
    """
    value: int | float | None
    unit: Text
    kind: Literal["gauge"]
    scope: Text
    estimated: Boolean
    attributes: JsonObject


class ResourceSample(_Document):
    """Timestamped resource measurements for one monitored series and context.

    Args:
        series_id: Identity of the monitored resource time series.
        context: Validated journal context identifying the experiment and
            participant scope.
        observed_at: Wall-clock timestamp at which this observation was
            recorded.
        observed_monotonic: Local monotonic observation time in seconds, used
            for age and timeout comparisons.
        resources: Metric names mapped to observed gauge values and provenance
            metadata.
    """
    series_id: Text
    context: ResourceContext
    observed_at: Text
    observed_monotonic: Number
    resources: dict[Text, ResourceMeasurement]

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> object:
        """Copy sample JSON while retaining typed context and measurement instances.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        if not isinstance(document, dict):
            return copy_json_object(document, "collector document")
        values = dict(document)
        context = values.get("context")
        if isinstance(context, JournalContext):
            values["context"] = context.root
        resources = values.get("resources")
        retained: dict[str, ResourceMeasurement] = {}
        if isinstance(resources, dict):
            retained = {
                name: item
                for name, item in resources.items()
                if type(item) is ResourceMeasurement
            }
            values["resources"] = {
                name: item.model_dump(exclude_unset=True) if name in retained else item
                for name, item in resources.items()
            }
        detached: dict[str, object] = dict(
            copy_json_object(values, "collector document")
        )
        if isinstance(context, JournalContext):
            detached["context"] = context
        detached_resources = detached.get("resources")
        if retained and isinstance(detached_resources, dict):
            detached["resources"] = {**detached_resources, **retained}
        return detached


class CollectorPacket(_Document):
    """Collector status, journal delivery state, and a batch of resource samples.

    Args:
        collector_id: UUID of the collector process instance producing this
            packet.
        pid: Operating-system process identifier; additional identity fields are
            needed to prove ownership.
        revision: Last target revision applied by the worker; -1 means no update
            has been applied.
        journal_closed: Whether the collector has closed its journal writer.
        journal_error: Latest telemetry journal failure, or None when no error
            is reported.
        unconfirmed_samples: Count of samples whose journal delivery is not
            confirmed and will not be replayed.
        samples: Ordered resource sample documents carried by the packet/page.
    """
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
