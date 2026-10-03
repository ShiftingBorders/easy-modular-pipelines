"""Resource measurement validation and normalization."""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.primitives.json_values import (
    JsonObject,
    require_text,
)

if TYPE_CHECKING:
    from core.journal.logger import Operation


def _validate_measurement(
    name: str, measurement: JsonObject, operation: Operation | None
) -> None:
    from core.models.journal_records import JournalMeasurement

    require_text(name, "resource name")
    validated = JournalMeasurement.model_validate(measurement)
    if validated.scope == "operation" and operation is None:
        raise ValueError("Operation-scoped resources require an operation handle.")
    excluded = {"attributes"} if "attributes" not in validated.model_fields_set else set()
    measurement.update(validated.model_dump(exclude=excluded))
