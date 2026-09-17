"""Read validated, journal-backed call results without a second result store."""

from __future__ import annotations

from core.logger_utils.events import JsonObject
from core.runner_utils.protocol import validate_response


def read_result(
    reader, request_id: str, *, expected: JsonObject, accepted: bool = False
) -> JsonObject | None:
    record = reader.read_command_result(request_id)
    if record is None:
        return None
    context = record["event"]["context"]
    for name, value in expected.items():
        if context.get(name) != value:
            raise ValueError(f"Journal result identity mismatch: {name}.")
    validate_response(record["response"])
    if accepted and record["author"] != "runner":
        raise ValueError("The journal result has not been accepted by runner.")
    return record
