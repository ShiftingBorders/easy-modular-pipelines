"""Preparation and change descriptions for template reload."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from core.experiments.state import RunnerState, ServiceInstance
from core.experiments.template_validation import _assign_definition_ids
from core.journal.logger import Operation, OperationLogger
from core.models.experiment_template import (
    ExperimentTemplate,
    ServiceCallDefinition,
    ServiceDefinition,
    StageDefinition,
)
from core.models.updates import _update_model
from core.primitives.json_values import JsonObject


@dataclass(frozen=True)
class ReloadLayout:
    """Compared service/stage definitions, preserved prefix, and next DAG position."""
    old_services: dict[str, ServiceDefinition]
    new_services: dict[str, ServiceDefinition]
    changed_services: set[str]
    old_stages: list[StageDefinition | ServiceCallDefinition]
    new_stages: list[StageDefinition | ServiceCallDefinition]
    prefix: int
    next_position: int


@dataclass(frozen=True)
class ReloadCursor:
    """Reload cursor decision and stage IDs whose current results remain valid."""
    next_position: int
    stage_position: int
    pending_advance: bool
    rewind: bool
    preserved: set[str]


@dataclass
class ReloadApplication:
    """Working reload state, audit operation, workspace, and commit/cleanup progress."""
    template: ExperimentTemplate
    template_yaml: str
    previous: ExperimentTemplate
    layout: ReloadLayout
    cursor: ReloadCursor
    completed: set[str]
    previous_revision: str
    candidate_revision: str
    candidate_run: str
    workspace: Path
    logger: OperationLogger
    operation: Operation
    result: JsonObject
    prior_instances: dict[str, str]
    prior_service_retries: dict[str, int]
    parameters: str | None = None
    snapshot_id: str | None = None
    detached: bool = False
    committed: bool = False


def _definition_fingerprint(
    definition: StageDefinition | ServiceCallDefinition | ServiceDefinition | None,
    *,
    ignore_snapshot_policy: bool = False,
) -> str:
    """Retain JSON equality, including omitted defaults and numeric spelling."""
    document = None if definition is None else definition.model_dump(exclude_unset=True)
    if (
        document is not None
        and isinstance(definition, (StageDefinition, ServiceCallDefinition))
        and (ignore_snapshot_policy or not definition.snapshot_after)
    ):
        document.pop("snapshot_after", None)
    return json.dumps(document, sort_keys=True)


def _template_fingerprint(template: ExperimentTemplate) -> str:
    document = template.model_dump(exclude_unset=True)
    for definition in document["stages"]:
        if not definition.get("snapshot_after", False):
            definition.pop("snapshot_after", None)
    return json.dumps(document, sort_keys=True)


def _reload_layout(
    previous: ExperimentTemplate,
    candidate: ExperimentTemplate,
    stage_position: int,
    pending_advance: bool,
) -> ReloadLayout:
    old_services = {item.service_id: item for item in previous.services}
    new_services = {item.service_id: item for item in candidate.services}
    changed_services = {
        service_id
        for service_id in old_services.keys() | new_services.keys()
        if _definition_fingerprint(old_services.get(service_id))
        != _definition_fingerprint(new_services.get(service_id))
    }
    old_stages, new_stages = previous.stages, candidate.stages
    prefix = 0
    for before, after in zip(old_stages, new_stages):
        if _definition_fingerprint(before, ignore_snapshot_policy=True) != _definition_fingerprint(
            after, ignore_snapshot_policy=True
        ) or (
            isinstance(before, ServiceCallDefinition)
            and before.service_id in changed_services
        ):
            break
        prefix += 1
    old_next = stage_position - 1 + int(pending_advance)
    if not 0 <= old_next <= len(old_stages):
        raise ValueError("Saved cursor is outside the applied DAG.")
    new_positions = {item.stage_id: index for index, item in enumerate(new_stages)}
    next_position = len(new_stages)
    for definition in old_stages[old_next:]:
        if definition.stage_id in new_positions:
            next_position = new_positions[definition.stage_id]
            break
    if old_next == prefix:
        next_position = prefix
    return ReloadLayout(
        old_services,
        new_services,
        changed_services,
        old_stages,
        new_stages,
        prefix,
        next_position,
    )


def _reload_cursor(layout: ReloadLayout, completed: set[str]) -> ReloadCursor:
    invalidated = {item.stage_id for item in layout.old_stages[layout.prefix :]}
    rewind = bool(completed & invalidated) and layout.prefix < layout.next_position
    next_position = layout.prefix if rewind else layout.next_position
    next_position = min(next_position, len(layout.new_stages))
    pending = next_position == len(layout.new_stages)
    position = len(layout.new_stages) if pending else next_position + 1
    preserved = {item.stage_id for item in layout.new_stages[: layout.prefix]}
    return ReloadCursor(next_position, position, pending, rewind, preserved)


def _prepare_reload_candidate(
    state: RunnerState, template: ExperimentTemplate
) -> ExperimentTemplate:
    # Equivalent file-relative spellings are not a resource change. Keep
    # the applied spelling, including configured absolute paths.
    if len(template.resources) == len(state.template.resources) and all(
        before.name == after.name
        and before.hash == after.hash
        and Path(before.path).resolve() == Path(after.path).resolve()
        for before, after in zip(state.template.resources, template.resources)
    ):
        template = _update_model(template, resources=state.template.resources)
    previous = state.template.model_dump(exclude_unset=True)
    candidate = template.model_dump(exclude_unset=True)
    for key in previous.keys() - {"stages", "services"}:
        if json.dumps(candidate[key], sort_keys=True) != json.dumps(
            previous[key], sort_keys=True
        ):
            raise ValueError(f"reload_template cannot change {key}.")
    old_roles = {item.stage_id: "stage" for item in state.template.stages}
    old_roles.update({item.service_id: "service" for item in state.template.services})
    template = _assign_definition_ids(template)
    for role, entries in (("stage", template.stages), ("service", template.services)):
        for item in entries:
            identifier = (
                item.service_id
                if isinstance(item, ServiceDefinition)
                else item.stage_id
            )
            if old_roles.get(identifier, role) != role:
                raise ValueError("A stable definition ID cannot change its role.")
    return template


def _definition_change(
    role: str,
    identity: str,
    before: dict[
        str, tuple[int, StageDefinition | ServiceCallDefinition | ServiceDefinition]
    ],
    after: dict[
        str, tuple[int, StageDefinition | ServiceCallDefinition | ServiceDefinition]
    ],
) -> JsonObject:
    change = {
        "kind": role,
        "id": identity,
        "before": None
        if identity not in before
        else before[identity][1].model_dump(exclude_unset=True),
        "after": None
        if identity not in after
        else after[identity][1].model_dump(exclude_unset=True),
        "old_position": None if identity not in before else before[identity][0],
        "new_position": None if identity not in after else after[identity][0],
    }
    fields = []
    pending_fields = [
        (
            [],
            change["before"],
            change["after"],
            identity in before,
            identity in after,
        )
    ]
    while pending_fields:
        path, old_value, new_value, old_present, new_present = pending_fields.pop()
        if old_present == new_present and json.dumps(
            old_value, sort_keys=True
        ) == json.dumps(new_value, sort_keys=True):
            continue
        if isinstance(old_value, dict) and isinstance(new_value, dict):
            for field in sorted(old_value.keys() | new_value.keys(), reverse=True):
                pending_fields.append(
                    (
                        [*path, field],
                        old_value.get(field),
                        new_value.get(field),
                        field in old_value,
                        field in new_value,
                    )
                )
        else:
            fields.append(
                {
                    "path": path,
                    "before_present": old_present,
                    "after_present": new_present,
                    "before": old_value,
                    "after": new_value,
                }
            )
    change["fields"] = fields
    return change


def _service_state(
    position: int, definition: ServiceDefinition, instance: ServiceInstance | None
) -> JsonObject:
    active = None if instance is None else instance.active_request
    return {
        "position": position,
        "service_id": definition.service_id,
        "module": definition.module.model_dump(exclude_unset=True),
        "service_instance_id": None
        if instance is None
        else instance.service_instance_id,
        "implementation": None if instance is None else instance.implementation,
        "ready": instance is not None and instance.ready,
        "stopping": instance is not None and instance.stopping,
        "stopped": instance is None or instance.stopped,
        "manually_stopped": instance is not None and instance.manually_stopped,
        "restart_count": 0 if instance is None else instance.restart_count,
        "blocked_action": None if instance is None else instance.blocked_action,
        "failure": None
        if instance is None or instance.failure is None
        else instance.failure.model_dump(exclude_unset=True),
        "process": None
        if instance is None or instance.process_identity is None
        else instance.process_identity.model_dump(),
        "last_status": None
        if instance is None or instance.last_status is None
        else instance.last_status.model_dump(exclude_unset=True),
        "active_request": None
        if active is None
        else {
            "request_id": active.request_id,
            "command": active.command,
            "timed_out": active.timed_out,
        },
        "pending_requests": 0 if instance is None else len(instance.pending_requests),
    }
