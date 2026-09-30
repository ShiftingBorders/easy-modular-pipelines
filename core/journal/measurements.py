"""Resource measurement validation and normalization."""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.primitives.json_values import (
    JsonObject,
    copy_json_object,
    require_number,
    require_text,
)

if TYPE_CHECKING:
    from core.journal.logger import Operation


def _validate_measurement(
    name: str, measurement: JsonObject, operation: Operation | None
) -> None:
    require_text(name, "resource name")
    if not isinstance(measurement, dict):
        raise TypeError("Each resource must be a JSON measurement object.")
    if measurement.keys() - {
        "value",
        "unit",
        "kind",
        "scope",
        "estimated",
        "attributes",
    }:
        raise ValueError("Unknown resource measurement fields.")
    if measurement.get("value") is not None:
        require_number(measurement.get("value"), f"{name}.value")
    else:
        measurement["value"] = None
    require_text(measurement.get("unit"), f"{name}.unit")
    measurement.setdefault("kind", "delta")
    measurement.setdefault("scope", "operation")
    measurement.setdefault("estimated", False)
    if measurement["kind"] not in ("delta", "total", "gauge", "peak"):
        raise ValueError("Resource kind must be delta, total, gauge, or peak.")
    scopes = ("operation", "process", "service", "host")
    if measurement["scope"] not in scopes:
        raise ValueError(f"Resource scope must be one of: {', '.join(scopes)}.")
    if measurement["scope"] == "operation" and operation is None:
        raise ValueError("Operation-scoped resources require an operation handle.")
    if type(measurement["estimated"]) is not bool:
        raise TypeError("Resource estimated must be a boolean.")
    if "attributes" in measurement:
        copy_json_object(measurement["attributes"], "resource attributes")
