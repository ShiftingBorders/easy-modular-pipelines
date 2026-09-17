"""Dashboard views derived from recorded facts, without modifying runtime state."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import datetime
from itertools import pairwise


def instant(value: object) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return None


def elapsed(start: object, finish: object) -> float | None:
    first, last = instant(start), instant(finish)
    return None if first is None or last is None or last < first else last - first


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[
        max(0, min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1))
    ]


def experiment_views(
    dataset: dict, live: dict | None = None, run_id: str | None = None
) -> dict:
    identifier = dataset["experiment_id"]
    saved = dataset.get("state", {})
    events = [
        entry
        for entry in dataset["entries"]
        if run_id is None or entry["context"].get("run_id") == run_id
    ]
    effective = [
        entry
        for entry in events
        if entry.get("effective", True) and not entry.get("ignored")
    ]
    revisions = []
    attempts: dict[str, dict] = {}
    operations: dict[str, dict] = {}
    runs = {}
    errors, artifacts, commands = [], [], {}
    checkpoints = []
    for event in effective:
        context, data, kind = event["context"], event["data"], event["event_type"]
        run = context.get("run_id")
        if run:
            runs.setdefault(
                run,
                {
                    "run_id": run,
                    "started_at": event["occurred_at"],
                    "status": "recorded",
                    "template_revision_id": context.get("template_revision_id"),
                },
            )
        if kind == "template.applied":
            revision = {**data, "run_id": run, "occurred_at": event["occurred_at"]}
            revisions.append(revision)
            if run:
                runs[run]["template_revision_id"] = data["template_revision_id"]
        elif kind == "attempt.parameters":
            attempt_id = context.get("attempt_id")
            if attempt_id:
                attempts[attempt_id] = {
                    **context,
                    "recorded_at": event["occurred_at"],
                    "effective_settings": data["effective_settings"],
                    "template": data["template"],
                    "template_yaml": data["template_yaml"],
                    "status": "prepared",
                    "started_at": None,
                    "finished_at": None,
                    "duration_seconds": None,
                }
        elif kind == "stage.process_started":
            attempt = attempts.setdefault(context.get("attempt_id"), dict(context))
            attempt.update(
                started_at=data.get("started_at", event["occurred_at"]),
                process=data.get("identity"),
                status="running",
            )
        elif kind == "stage.finished":
            attempt = attempts.setdefault(context.get("attempt_id"), dict(context))
            attempt.update(context)
            attempt.update(
                finished_at=event["occurred_at"],
                status=data.get("outcome", "unknown"),
                result=data.get("result"),
                result_path=data.get("result_path"),
            )
            attempt["duration_seconds"] = elapsed(
                attempt.get("started_at"), attempt["finished_at"]
            )
            if data.get("result_path"):
                artifacts.append(
                    {
                        **context,
                        "artifact_id": event["event_id"],
                        "path": data["result_path"],
                        "path_base": "experiment",
                        "purpose": "stage_result",
                        "size_bytes": None,
                        "occurred_at": event["occurred_at"],
                    }
                )
        elif kind == "operation.started":
            operations[event["operation_id"]] = {
                **context,
                "operation_id": event["operation_id"],
                "parent_operation_id": data.get("parent_operation_id"),
                "operation_type": data["operation_type"],
                "name": data["operation_name"],
                "started_at": event["occurred_at"],
                "finished_at": None,
                "status": "unconfirmed",
                "attributes": data.get("attributes", {}),
            }
        elif kind == "operation.finished":
            operation = operations.setdefault(
                event["operation_id"],
                {
                    **context,
                    "operation_id": event["operation_id"],
                    "started_at": None,
                    "name": "Missing start",
                },
            )
            operation.update(
                finished_at=event["occurred_at"],
                status=data["status"],
                duration_seconds=data.get("duration_ms", 0) / 1000,
                reason_code=data.get("reason_code"),
            )
        elif kind == "error.recorded":
            errors.append(
                {
                    **data,
                    **context,
                    "event_id": event["event_id"],
                    "type": data["error_type"],
                    "phase": context.get(
                        "phase", "execution" if context.get("attempt_id") else "runtime"
                    ),
                    "occurred_at": event["occurred_at"],
                }
            )
        elif kind == "artifact.recorded":
            artifacts.append(
                {
                    **data,
                    **context,
                    "path_base": "attempt",
                    "occurred_at": event["occurred_at"],
                }
            )
        elif kind == "runner.checkpoint":
            checkpoints.append((event, data))
        if kind in {
            "control.intent",
            "control.result",
            "control.reconciled",
            "command.result",
        }:
            command_key = (
                context.get("request_id")
                or data.get("request_id")
                or data.get("intent_event_id")
                or event["event_id"]
            )
            command = commands.setdefault(
                command_key,
                {
                    "request_id": command_key,
                    "sent_at": event["occurred_at"],
                    "observations": [],
                    "run_id": run,
                },
            )
            command["observations"].append(event)
            command.update(
                command=data.get("action")
                or data.get("command")
                or command.get("command")
                or "Service result",
                target=context.get("service_id")
                or context.get("module_name")
                or "Runner",
                kind="service" if context.get("service_id") else "control",
                status=data.get("outcome")
                or data.get("status")
                or ("pending" if kind == "control.intent" else "recorded"),
            )
    template = revisions[-1]["template"] if revisions else saved.get("template", {})
    template_yaml = (
        revisions[-1]["template_yaml"] if revisions else saved.get("template_yaml", "")
    )
    revision_id = (
        revisions[-1]["template_revision_id"]
        if revisions
        else saved.get("template_revision_id")
    )
    current_run = run_id or saved.get("run_id") or next(reversed(runs), None)
    fresh = bool(
        live
        and live.get("experiment_id") == identifier
        and live.get("fresh")
        and (run_id is None or run_id == saved.get("run_id"))
    )
    recorded_state = next(
        (
            event["data"]
            for event in reversed(effective)
            if event["event_type"] == "experiment.state"
        ),
        saved,
    )
    observed = live if fresh else recorded_state
    phase = observed.get("phase", "unknown")
    if not fresh and phase not in {"completed", "failed", "stopped"}:
        status = "unknown"
    else:
        status = (
            "paused"
            if phase == "waiting" and observed.get("mode") == "paused"
            else "running"
            if phase == "stage_running"
            else phase
        )
    if current_run in runs:
        runs[current_run]["status"] = status
    stages = template.get("stages", [])
    nodes = []
    for position, definition in enumerate(stages, 1):
        node = {
            **definition,
            "name": definition.get("name") or definition["module"]["name"],
            "position": position,
        }
        stage_attempts = [
            attempt
            for attempt in attempts.values()
            if attempt.get("stage_id") == definition["stage_id"]
        ]
        node["status"] = (
            stage_attempts[-1].get("status", "unknown") if stage_attempts else "pending"
        )
        if fresh and observed.get("stage_position") == position:
            node["status"] = "running" if phase == "stage_running" else "ready"
        nodes.append(node)
    edges = [
        {"from": left["stage_id"], "to": right["stage_id"]}
        for left, right in pairwise(stages)
    ]
    if stages and isinstance(template.get("cycles"), int) and template["cycles"] > 1:
        edges.append(
            {
                "from": stages[-1]["stage_id"],
                "to": stages[0]["stage_id"],
                "condition": f"completed cycles < {template['cycles']}",
            }
        )
    # Attempts are explicit execution scopes even if a module records no nested operations.
    for attempt_id, attempt in attempts.items():
        if not attempt_id or not attempt.get("started_at"):
            continue
        if attempt.get("status") == "running" and not fresh:
            attempt["status"] = "unconfirmed"
        scope = f"attempt:{attempt_id}"
        operations[scope] = {
            **attempt,
            "operation_id": scope,
            "parent_operation_id": f"run:{attempt.get('run_id')}",
            "name": f"{attempt.get('module_name', 'Stage')} · cycle {attempt.get('cycle_number')} · attempt {attempt.get('attempt_number')}",
        }
        for operation in list(operations.values()):
            if (
                operation["operation_id"] != scope
                and operation.get("attempt_id") == attempt_id
                and not operation.get("parent_operation_id")
            ):
                operation["parent_operation_id"] = scope
    for run, details in runs.items():
        children = [
            operation
            for operation in operations.values()
            if operation.get("run_id") == run
        ]
        finishes = [
            operation.get("finished_at")
            for operation in children
            if operation.get("finished_at")
        ]
        operations[f"run:{run}"] = {
            "operation_id": f"run:{run}",
            "run_id": run,
            "parent_operation_id": None,
            "name": f"Logical run {run}",
            "started_at": details["started_at"],
            "finished_at": max(finishes)
            if finishes
            and all(item.get("finished_at") for item in children)
            and status in {"completed", "stopped", "failed"}
            else None,
            "status": details["status"],
        }
    for record in [*errors, *artifacts]:
        attempt = attempts.get(record.get("attempt_id"), {})
        for key in ("module_name", "module_version", "stage_id", "cycle_number"):
            if record.get(key) is None:
                record[key] = attempt.get(key)
    valid_cycles = {}
    grouped = defaultdict(list)
    for attempt in attempts.values():
        if (
            attempt.get("template_revision_id") == revision_id
            and attempt.get("run_id") == current_run
        ):
            grouped[attempt.get("cycle_number")].append(attempt)
    stage_ids = {stage["stage_id"] for stage in stages}
    for cycle, members in grouped.items():
        last_attempts = {item.get("stage_id"): item for item in members}
        successful = {
            key
            for key, item in last_attempts.items()
            if item.get("status") == "succeeded"
        }
        if cycle is None or not stage_ids or not stage_ids.issubset(successful):
            continue
        starts = [item["started_at"] for item in members if item.get("started_at")]
        ends = [item["finished_at"] for item in members if item.get("finished_at")]
        if starts and ends:
            wall = elapsed(min(starts), max(ends))
            if wall is not None:
                valid_cycles[cycle] = {
                    "duration": wall,
                    "attempts": members,
                    "started_at": min(starts),
                    "finished_at": max(ends),
                }
    measurements = cycle_measurements(
        effective, attempts, operations, valid_cycles, revision_id, current_run
    )
    total_cycles = template.get("cycles")
    completed = max(0, (observed.get("cycle_number") or 1) - 1)
    if phase == "completed" and isinstance(total_cycles, int):
        completed = total_cycles
    completed = max(completed, max(valid_cycles, default=0))
    cohort = [
        item
        for item in valid_cycles.values()
        if not any(
            data.get("mode") == "paused"
            and data.get("phase") == "waiting"
            and (instant(item["started_at"]) or 0)
            < (instant(event["occurred_at"]) or 0)
            < (instant(item["finished_at"]) or 0)
            for event, data in checkpoints
        )
    ]
    durations = [cycle["duration"] for cycle in cohort]
    mean = statistics.mean(durations) if durations else None
    remaining = (
        max(0, total_cycles - completed) if isinstance(total_cycles, int) else None
    )
    active_cycle = observed.get("cycle_number")
    started = [
        attempt.get("started_at")
        for attempt in grouped.get(active_cycle, [])
        if attempt.get("started_at")
    ]
    spent = (
        elapsed(min(started), observed.get("observed_at"))
        if started and active_cycle not in valid_cycles and fresh
        else 0
    )
    usable = dataset["complete"] and mean is not None and remaining is not None
    eta = max(0, remaining * mean - (spent or 0)) if usable else None
    components = []
    for definition in stages:
        times = []
        for cycle in cohort:
            selected = [
                attempt
                for attempt in cycle["attempts"]
                if attempt.get("stage_id") == definition["stage_id"]
            ]
            starts = [item["started_at"] for item in selected if item.get("started_at")]
            ends = [item["finished_at"] for item in selected if item.get("finished_at")]
            value = elapsed(min(starts), max(ends)) if starts and ends else None
            if value is not None:
                times.append(value)
        components.append(
            {
                "stage_id": definition["stage_id"],
                "module_name": definition["module"]["name"],
                "mean_seconds": statistics.mean(times) if times else None,
            }
        )
    started_at = min((row["started_at"] for row in runs.values()), default=None)
    summary = {
        "experiment_id": identifier,
        "name": template.get("name") or identifier,
        "status": status,
        "phase": phase,
        "fresh": fresh,
        "observed_at": observed.get("observed_at"),
        "last_recorded_status": phase,
        "run_id": current_run,
        "template_revision_id": revision_id,
        "completed_cycles": completed,
        "total_cycles": total_cycles,
        "started_at": started_at,
        "error_count": len(errors) if dataset["complete"] else None,
        "services": observed.get("services", []),
        "complete": dataset["complete"],
        "error": dataset.get("error"),
    }
    return {
        "summary": summary,
        "runs": list(runs.values()),
        "operations": list(operations.values()),
        "events": events,
        "effective": effective,
        "errors": errors,
        "parameters": list(attempts.values()),
        "measurements": measurements,
        "template": {
            "template": template,
            "template_yaml": template_yaml,
            "nodes": nodes,
            "edges": edges,
            "revisions": revisions,
        },
        "commands": list(commands.values()),
        "artifacts": artifacts,
        "forecast": {
            "completed_cycles": completed,
            "total_cycles": total_cycles,
            "eta_seconds": eta,
            "paused": status == "paused",
            "sample_mean_seconds": mean,
            "sample_cycles": len(durations),
            "eta_low_seconds": max(0, remaining * min(durations) - (spent or 0))
            if usable
            else None,
            "eta_high_seconds": max(0, remaining * max(durations) - (spent or 0))
            if usable
            else None,
            "measurements": measurements,
            "component_durations": components,
            "template_revision_id": revision_id,
            "complete": dataset["complete"],
        },
    }


def cycle_measurements(
    events: list[dict],
    attempts: dict,
    operations: dict,
    cycles: dict,
    revision_id: str | None,
    run_id: str | None = None,
) -> list[dict]:
    buckets = defaultdict(list)
    for event in events:
        if event["event_type"] != "resources.recorded":
            continue
        context = {
            **attempts.get(event["context"].get("attempt_id"), {}),
            **event["context"],
        }
        if (
            context.get("cycle_number") not in cycles
            or context.get("template_revision_id") != revision_id
            or not context.get("module_name")
            or (run_id is not None and context.get("run_id") != run_id)
        ):
            continue
        for name, measurement in event["data"]["resources"].items():
            if measurement["scope"] not in {"operation", "process"}:
                continue
            key = (
                context["module_name"],
                context.get("module_version"),
                name,
                measurement["unit"],
                context["cycle_number"],
                measurement["scope"],
            )
            buckets[key].append((event, measurement, context))
    result = []
    for (module, version, name, unit, cycle, scope), records in buckets.items():
        streams = defaultdict(list)
        complete = True
        estimated = False
        for event, metric, context in records:
            stream = (
                event.get("operation_id")
                or context.get("attempt_id")
                or event["producer_instance_id"]
            )
            streams[stream].append(metric)
            complete = complete and metric.get("value") is not None
            estimated = estimated or metric.get("estimated", False)
        # Hierarchical instrumentation is not proof that independently reported totals are disjoint.
        overlap = False
        for key in streams:
            seen = {key}
            parent = operations.get(key, {}).get("parent_operation_id")
            while parent and parent not in seen:
                overlap = overlap or parent in streams
                seen.add(parent)
                parent = operations.get(parent, {}).get("parent_operation_id")
        values = []
        kinds = {metric["kind"] for _, metric, _ in records}
        for metrics in streams.values():
            available = [
                metric
                for metric in metrics
                if isinstance(metric.get("value"), (int, float))
                and not isinstance(metric["value"], bool)
            ]
            totals = [metric for metric in available if metric["kind"] == "total"]
            if totals:
                values.append(totals[-1]["value"])
            elif available and all(metric["kind"] == "delta" for metric in available):
                values.append(sum(metric["value"] for metric in available))
            elif available and all(metric["kind"] == "peak" for metric in available):
                values.append(max(metric["value"] for metric in available))
            elif available and all(metric["kind"] == "gauge" for metric in available):
                weights = [
                    (
                        metric["value"],
                        metric.get("attributes", {}).get("interval_seconds") or 1,
                    )
                    for metric in available
                ]
                values.append(
                    sum(value * weight for value, weight in weights)
                    / sum(weight for _, weight in weights)
                )
            else:
                complete = False
        value = None
        compatible = kinds in ({"total"}, {"delta"}, {"peak"}, {"gauge"})
        if values and not overlap and compatible:
            value = (
                max(values)
                if kinds == {"peak"}
                else statistics.mean(values)
                if kinds == {"gauge"}
                else sum(values)
            )
        covered = {context.get("attempt_id") for _, _, context in records}
        expected = {
            attempt.get("attempt_id")
            for attempt in cycles[cycle]["attempts"]
            if attempt.get("module_name") == module
        }
        complete = (
            complete and expected.issubset(covered) and not overlap and compatible
        )
        result.append(
            {
                "module_name": module,
                "module_version": version,
                "metric": name,
                "unit": unit,
                "cycle_number": cycle,
                "value": value,
                "scope": scope,
                "aggregation": "mean"
                if kinds == {"gauge"}
                else "max"
                if kinds == {"peak"}
                else "sum"
                if compatible
                else None,
                "complete": complete,
                "estimated": estimated,
                "template_revision_id": revision_id,
                "reason": "overlapping_operation_measurements" if overlap else None,
                "samples": len(records),
            }
        )
    return result
