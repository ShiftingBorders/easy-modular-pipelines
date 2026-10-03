"""Preparation and change descriptions for template reload."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from core.experiments.state import RunnerState, ServiceInstance
from core.journal.logger import Operation, OperationLogger
from core.primitives.json_values import JsonObject


@dataclass(frozen=True)
class ReloadLayout:
    old_services: dict[str, JsonObject]
    new_services: dict[str, JsonObject]
    changed_services: set[str]
    old_stages: list[JsonObject]
    new_stages: list[JsonObject]
    prefix: int
    next_position: int


@dataclass(frozen=True)
class ReloadCursor:
    next_position: int
    stage_position: int
    pending_advance: bool
    rewind: bool
    preserved: set[str]


@dataclass
class ReloadApplication:
    template: JsonObject
    template_yaml: str
    previous: JsonObject
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


def _reload_layout(
    previous: JsonObject, candidate: JsonObject, stage_position: int, pending_advance: bool
) -> ReloadLayout:
    old_services = {item["service_id"]: item for item in previous["services"]}
    new_services = {item["service_id"]: item for item in candidate["services"]}
    changed_services = {
        service_id for service_id in old_services.keys() | new_services.keys()
        if json.dumps(old_services.get(service_id), sort_keys=True)
        != json.dumps(new_services.get(service_id), sort_keys=True)
    }
    old_stages, new_stages = previous["stages"], candidate["stages"]
    prefix = 0
    for before, after in zip(old_stages, new_stages):
        if (
            json.dumps(before, sort_keys=True) != json.dumps(after, sort_keys=True)
            or ("service_id" in before and before["service_id"] in changed_services)
        ):
            break
        prefix += 1
    old_next = stage_position - 1 + int(pending_advance)
    if not 0 <= old_next <= len(old_stages):
        raise ValueError("Saved cursor is outside the applied DAG.")
    new_positions = {item["stage_id"]: index for index, item in enumerate(new_stages)}
    next_position = len(new_stages)
    for definition in old_stages[old_next:]:
        if definition["stage_id"] in new_positions:
            next_position = new_positions[definition["stage_id"]]
            break
    if old_next == prefix:
        next_position = prefix
    return ReloadLayout(
        old_services, new_services, changed_services, old_stages, new_stages,
        prefix, next_position,
    )


def _reload_cursor(layout: ReloadLayout, completed: set[str]) -> ReloadCursor:
    invalidated = {item["stage_id"] for item in layout.old_stages[layout.prefix:]}
    rewind = bool(completed & invalidated) and layout.prefix < layout.next_position
    next_position = layout.prefix if rewind else layout.next_position
    next_position = min(next_position, len(layout.new_stages))
    pending = next_position == len(layout.new_stages)
    position = len(layout.new_stages) if pending else next_position + 1
    preserved = {item["stage_id"] for item in layout.new_stages[:layout.prefix]}
    return ReloadCursor(next_position, position, pending, rewind, preserved)


def _prepare_reload_candidate(state: RunnerState, template: JsonObject) -> None:
    # Equivalent file-relative spellings are not a resource change. Keep
    # the applied spelling, including configured absolute paths.
    if len(template["resources"]) == len(state.template["resources"]) and all(
        before["name"] == after["name"]
        and before["hash"] == after["hash"]
        and Path(before["path"]).resolve() == Path(after["path"]).resolve()
        for before, after in zip(state.template["resources"], template["resources"])
    ):
        template["resources"] = [dict(item) for item in state.template["resources"]]
    for key in state.template.keys() - {"stages", "services"}:
        if json.dumps(template[key], sort_keys=True) != json.dumps(
            state.template[key], sort_keys=True
        ):
            raise ValueError(f"reload_template cannot change {key}.")
    old_roles = {
        item[f"{role}_id"]: role
        for role in ("stage", "service")
        for item in state.template[f"{role}s"]
    }
    for role in ("stage", "service"):
        for item in template[f"{role}s"]:
            item.setdefault(f"{role}_id", str(uuid4()))
            if old_roles.get(item[f"{role}_id"], role) != role:
                raise ValueError("A stable definition ID cannot change its role.")


def _definition_change(
    role: str,
    identity: str,
    before: dict[str, tuple[int, JsonObject]],
    after: dict[str, tuple[int, JsonObject]],
) -> JsonObject:
    change = {
        "kind": role,
        "id": identity,
        "before": None if identity not in before else before[identity][1],
        "after": None if identity not in after else after[identity][1],
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
    position: int, definition: JsonObject, instance: ServiceInstance | None
) -> JsonObject:
    active = None if instance is None else instance.active_request
    return {
        "position": position,
        "service_id": definition["service_id"],
        "module": definition["module"],
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
        "failure": None if instance is None else instance.failure,
        "process": None if instance is None else instance.process_identity,
        "last_status": None if instance is None else instance.last_status,
        "active_request": None
        if active is None
        else {key: active[key] for key in ("request_id", "command", "timed_out")},
        "pending_requests": 0 if instance is None else len(instance.pending_requests),
    }
