"""Computed OS observations; transport and journal recipients validate their JSON."""

from dataclasses import dataclass

from core.models.journal_records import JournalContext
from core.primitives.json_values import JsonObject


@dataclass(frozen=True)
class MeasuredResource:
    """Keep native values until the original output/validation boundary."""

    value: int | float | None
    unit: str
    scope: str
    attributes: JsonObject
    kind: str = "gauge"
    estimated: bool = False

    def document(self) -> JsonObject:
        """Return the resource gauge in the journal measurement envelope.

        Returns:
            The resource gauge in the journal measurement envelope.
        """
        return {
            "value": self.value,
            "unit": self.unit,
            "kind": self.kind,
            "scope": self.scope,
            "estimated": self.estimated,
            "attributes": self.attributes,
        }


@dataclass(frozen=True)
class ResourceObservation:
    """One host or process sample with wall-clock and monotonic observation times."""
    series_id: str
    context: JournalContext | JsonObject
    observed_at: str
    observed_monotonic: float
    resources: dict[str, MeasuredResource]

    def document(self) -> JsonObject:
        """Serialize context and resource measurements into the collector sample envelope.

        Returns:
            Sample envelope with timestamps, serialized context, and resource
            measurement dictionaries; this does not itself perform transport
            validation.
        """
        return {
            "series_id": self.series_id,
            "context": self.context.model_dump()
            if isinstance(self.context, JournalContext)
            else self.context,
            "observed_at": self.observed_at,
            "observed_monotonic": self.observed_monotonic,
            "resources": {
                name: item.document() for name, item in self.resources.items()
            },
        }
