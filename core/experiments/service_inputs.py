"""Service restoration paths and journal context."""

from __future__ import annotations

from pathlib import Path

from core.experiments.state import RunnerState
from core.models.experiment_template import ServiceDefinition
from core.models.participant_observations import ServiceRestorationPaths
from core.primitives.json_values import JsonObject


def _load_state_paths(
    state: RunnerState, service_states: dict[str, Path], selected: set[str]
) -> dict[str, str]:
    # Unused supplied entries retain the existing ignore behavior.
    supplied_paths = ServiceRestorationPaths.model_validate(
        {
            "paths": {
                service_id: service_states.get(service_id)
                for service_id in state.services
                if service_id in selected
            }
        }
    )
    paths = {}
    root = state.experiment_directory.resolve()
    for service_id, instance in state.services.items():
        if service_id not in selected:
            continue
        supplied = supplied_paths.paths[service_id]
        if supplied is None:
            if instance.definition.state_required:
                raise ValueError(f"Required service state is missing: {service_id}")
            continue
        path = (root / supplied).resolve()
        if not path.is_relative_to(root) or not path.exists():
            raise ValueError("Restored service state must exist inside the experiment.")
        paths[service_id] = path.relative_to(root).as_posix()
    return paths


def _context(state: RunnerState, service_id: str, instance_id: str) -> JsonObject:
    context = {
        "experiment_id": state.experiment_id,
        "run_id": state.run_id,
        "service_id": service_id,
        "service_instance_id": instance_id,
        "participant_id": service_id,
        "participant_instance_id": instance_id,
    }
    if state.pending_rebuild is not None:
        context["parent_operation_id"] = state.pending_rebuild.operation_id
    return context


def _service_definition(state: RunnerState, position: int) -> ServiceDefinition:
    if type(position) is not int or not 1 <= position <= len(state.template.services):
        raise ValueError("Service position is outside the template.")
    return state.template.services[position - 1]
