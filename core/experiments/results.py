"""Validate and normalize journal-backed stage results."""

from __future__ import annotations

from core.models.conditional_result import ConditionalDecision
from core.models.experiment_template import ExperimentTemplate, StageDefinition
from core.participants.protocol import validate_response
from core.primitives.json_values import JsonObject, JsonValue


class MissingConditionalDataError(ValueError):
    """A data-returning conditional stage omitted its required payload."""


def normalize_conditional_result(
    response: JsonObject,
    input_data: JsonValue,
    definition: StageDefinition | JsonObject,
    template: ExperimentTemplate | JsonObject,
) -> JsonObject:
    """Separate a conditional decision from its accepted application output."""
    decision = ConditionalDecision.model_validate(response["data"])
    return _apply_conditional_decision(
        decision,
        response,
        input_data,
        definition.returns_data
        if isinstance(definition, StageDefinition)
        else definition["returns_data"],
        template,
    )


def _apply_conditional_decision(
    decision: ConditionalDecision,
    response: JsonObject,
    input_data: JsonValue,
    returns_data: bool,
    template: ExperimentTemplate | JsonObject,
) -> JsonObject:
    control = {"command": decision.command}
    if decision.command == "move":
        identifiers = (
            {stage.stage_id for stage in template.stages}
            if isinstance(template, ExperimentTemplate)
            else {stage["stage_id"] for stage in template["stages"]}
        )
        if decision.stage_id not in identifiers:
            raise ValueError("Conditional move target is not a node in this DAG.")
        control["stage_id"] = decision.stage_id
    if returns_data and "data" not in decision.model_fields_set:
        raise MissingConditionalDataError(
            "Conditional stage with returns_data=true must return a data field."
        )
    # Without a command this stage simply forwards its input. Explicit null is
    # a payload only when data is enabled and a command is present.
    output = input_data
    if decision.command is not None and returns_data:
        output = decision.data
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
