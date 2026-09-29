"""Validate and normalize journal-backed stage results."""

from __future__ import annotations

from uuid import UUID

from core.logger_utils.events import JsonObject, JsonValue, require_text
from core.runner_utils.protocol import validate_response


class MissingConditionalDataError(ValueError):
    """A data-returning conditional stage omitted its required payload."""


def normalize_conditional_result(
    response: JsonObject,
    input_data: JsonValue,
    definition: JsonObject,
    template: JsonObject,
) -> JsonObject:
    """Separate a conditional decision from its accepted application output."""
    decision = response["data"]
    if decision is None:
        decision = {}
    if type(decision) is not dict:
        raise ValueError("A conditional result must be null or a decision object.")
    if decision.keys() - {"command", "stage_id", "data"}:
        raise ValueError("Unknown conditional decision fields.")
    command = decision.get("command")
    if command not in (None, "pause", "stop", "move"):
        raise ValueError("Conditional commands are pause, stop, or move.")
    control = {"command": command}
    if command == "move":
        target = str(UUID(require_text(decision.get("stage_id"), "move.stage_id")))
        if target not in {stage["stage_id"] for stage in template["stages"]}:
            raise ValueError("Conditional move target is not a node in this DAG.")
        control["stage_id"] = target
    elif "stage_id" in decision:
        raise ValueError("Only move accepts a target stage_id.")
    if definition["returns_data"] and "data" not in decision:
        raise MissingConditionalDataError(
            "Conditional stage with returns_data=true must return a data field."
        )
    # Without a command this stage simply forwards its input. Explicit null is
    # a payload only when data is enabled and a command is present.
    output = input_data
    if command is not None and definition["returns_data"]:
        output = decision["data"]
    return {
        **response,
        "data": output,
        "execution": {**response.get("execution", {}), "dag_decision": control},
    }


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
