"""Validate and normalize journal-backed stage results."""

from __future__ import annotations

from core.models.conditional_result import ConditionalDecision
from core.models.experiment_template import ExperimentTemplate, StageDefinition
from core.models.participant_identity import ParticipantIdentity
from core.models.participant_protocol import StageOutcomeResult
from core.models.updates import _update_model
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
    control, output = _conditional_output(decision, input_data, returns_data, template)
    return {
        **response,
        "data": output,
        "execution": {**response.get("execution", {}), "dag_decision": control},
    }


def _normalize_conditional_result(
    response: StageOutcomeResult,
    input_data: JsonValue,
    definition: StageDefinition,
    template: ExperimentTemplate,
) -> StageOutcomeResult:
    """Keep the accepted envelope typed until its journal or public boundary."""
    decision = ConditionalDecision.model_validate(response.data)
    control, output = _conditional_output(
        decision, input_data, definition.returns_data, template
    )
    execution = response.execution if "execution" in response.model_fields_set else {}
    return _update_model(
        response,
        data=output,
        execution={**execution, "dag_decision": control},
    )


def _conditional_output(
    decision: ConditionalDecision,
    input_data: JsonValue,
    returns_data: bool,
    template: ExperimentTemplate | JsonObject,
) -> tuple[JsonObject, JsonValue]:
    """Check the current DAG target and select the application output."""
    control: JsonObject = {"command": decision.command}
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
    return control, output


def read_result(
    reader,
    request_id: str,
    *,
    expected: ParticipantIdentity | JsonObject,
    accepted: bool = False,
) -> JsonObject | None:
    record = reader.read_command_result(request_id)
    if record is None:
        return None
    context = record["event"]["context"]
    fields = expected if isinstance(expected, ParticipantIdentity) else expected.items()
    for name, value in fields:
        if context.get(name) != value:
            raise ValueError(f"Journal result identity mismatch: {name}.")
    validate_response(record["response"])
    if accepted and record["author"] != "runner":
        raise ValueError("The journal result has not been accepted by runner.")
    return record
