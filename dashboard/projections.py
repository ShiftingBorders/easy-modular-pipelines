"""Dashboard views derived from recorded facts, without modifying runtime state."""

from __future__ import annotations

import heapq
import json
import math
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime
from itertools import pairwise


def compact_event(event: dict) -> dict:
    """Keep exact calculation inputs; original diagnostic payloads stay in the journal."""
    fields = {
        "attempt.parameters": (),
        "stage.process_started": ("started_at", "process"),
        "stage.finished": ("outcome", "result_request_id"),
        "operation.started": (
            "parent_operation_id",
            "operation_type",
            "operation_name",
        ),
        "operation.finished": ("status", "duration_ms", "reason_code"),
        "error.recorded": ("error_id", "error_type", "message"),
        "artifact.recorded": (
            "artifact_id",
            "path",
            "purpose",
            "name",
            "size_bytes",
            "content_hash",
        ),
        "runner.checkpoint": ("mode", "phase"),
        "experiment.state": (
            "phase",
            "mode",
            "cycle_number",
            "observed_at",
            "services",
            "stage_position",
        ),
        "control.intent": (
            "action",
            "command",
            "outcome",
            "status",
            "intent_event_id",
            "request_id",
        ),
        "control.result": (
            "action",
            "command",
            "outcome",
            "status",
            "intent_event_id",
            "request_id",
        ),
        "control.reconciled": (
            "action",
            "command",
            "outcome",
            "status",
            "intent_event_id",
            "request_id",
        ),
        "command.result": (
            "action",
            "command",
            "outcome",
            "status",
            "intent_event_id",
            "request_id",
        ),
    }
    kind, data = event["event_type"], event["data"]
    reduced = {key: data[key] for key in fields.get(kind, ()) if key in data}
    if kind == "attempt.parameters":
        reduced.update(effective_settings={}, template={}, template_yaml="")
    if kind == "resources.recorded":
        reduced["resources"] = {
            name: {
                **{key: value for key, value in metric.items() if key != "attributes"},
                "attributes": {
                    "interval_seconds": metric.get("attributes", {}).get(
                        "interval_seconds"
                    )
                },
            }
            for name, metric in data["resources"].items()
            if metric["scope"] in {"operation", "process"}
        }
    if kind == "template.applied":
        reduced = {
            "template_revision_id": data["template_revision_id"],
            "template": compact_template(data["template"]),
            "template_yaml": "",
        }
    return {**event, "data": reduced}


def compact_template(template: dict) -> dict:
    result = {key: template[key] for key in ("name", "cycles") if key in template}
    result["stages"] = [
        {
            key: stage[key]
            for key in ("stage_id", "name", "module", "service_id")
            if key in stage
        }
        for stage in template.get("stages", [])
    ]
    result["services"] = [
        {key: service[key] for key in ("service_id", "module") if key in service}
        for service in template.get("services", [])
    ]
    return result


def project_scope(dataset: dict, run_id: str | None, cycle: int | None) -> dict:
    """Recompute one affected execution cycle using the established semantics."""
    state = {
        **dataset["state"],
        "template": compact_template(dataset["state"].get("template", {})),
        "template_yaml": "",
    }
    model = experiment_views({**dataset, "state": state}, run_id=run_id)
    events = dataset["entries"]
    by_attempt, by_operation = defaultdict(list), defaultdict(list)
    by_artifact = defaultdict(list)
    positions = {event["event_id"]: event["cursor"] for event in events}
    for event in events:
        kind = event["event_type"]
        if kind in {
            "attempt.parameters",
            "stage.process_started",
            "stage.finished",
            "call.started",
        }:
            by_attempt[event["context"].get("attempt_id")].append(event["event_id"])
        if kind in {"operation.started", "operation.finished"}:
            by_operation[event.get("operation_id")].append(event["event_id"])
        artifact_id = event["data"].get("artifact_id")
        if isinstance(artifact_id, str):
            by_artifact[artifact_id].append(event["event_id"])
    records = {}
    identity_fields = {
        "parameters": "attempt_id",
        "operations": "operation_id",
        "errors": "event_id",
        "artifacts": "artifact_id",
        "commands": "request_id",
    }
    for kind, identity_field in identity_fields.items():
        records[kind] = []
        for row in model[kind]:
            key = row.get(identity_field)
            if key is None or kind == "operations" and str(key).startswith("run:"):
                continue
            sources = projection_sources(kind, row, by_artifact, by_attempt, by_operation)
            if not sources:
                continue
            row = {
                key: value
                for key, value in row.items()
                if key
                not in {
                    "template",
                    "template_yaml",
                    "effective_settings",
                    "observations",
                    "attributes",
                    "result",
                }
            }
            row.update(
                _record_key=str(key),
                _position=min(positions[source] for source in sources),
                detail_ref={"event_ids": sources, "kind": kind},
            )
            records[kind].append(row)
    records["measurements"] = []
    for index, row in enumerate(model["measurements"]):
        key = json.dumps(
            [
                row.get(field)
                for field in (
                    "module_name",
                    "module_version",
                    "metric",
                    "unit",
                    "scope",
                    "cycle_number",
                )
            ]
        )
        records["measurements"].append({**row, "_record_key": key, "_position": index})
    forecast = model["forecast"]
    attempts = model["parameters"]
    stage_ids = {
        stage["stage_id"] for stage in model["template"]["template"].get("stages", [])
    }
    latest_attempts = {row.get("stage_id"): row for row in attempts}
    valid = bool(stage_ids) and all(
        latest_attempts.get(stage, {}).get("status") == "succeeded"
        for stage in stage_ids
    )
    return {
        "records": records,
        "summary": {
            "cycle": cycle,
            "duration": forecast["sample_mean_seconds"],
            "sample_cycles": forecast["sample_cycles"],
            "components": forecast["component_durations"],
            "valid": valid,
            "first_started": min(
                (row["started_at"] for row in attempts if row.get("started_at")),
                default=None,
            ),
            "last_finished": max(
                (row["finished_at"] for row in attempts if row.get("finished_at")),
                default=None,
            ),
            "all_finished": bool(attempts)
            and all(row.get("finished_at") for row in attempts),
        },
    }


def window_experiment_views(
    dataset: dict, live: dict, run_id: str | None = None
) -> dict:
    """Project a complete RAM window while preserving original event contexts."""
    attempts = {
        event["context"].get("attempt_id"): {
            key: value
            for key, value in event["context"].items()
            if key in ("run_id", "template_revision_id", "cycle_number")
            and value is not None
        }
        for event in dataset["entries"]
        if event["event_type"] == "attempt.parameters"
        and event["context"].get("attempt_id")
    }
    entries = [
        {
            **event,
            "context": {
                **attempts.get(event["context"].get("attempt_id"), {}),
                **event["context"],
            },
        }
        for event in dataset["entries"]
    ]
    model = experiment_views({**dataset, "entries": entries}, live, run_id)
    selected = {event["event_id"] for event in model["events"]}
    model["events"] = [
        event for event in dataset["entries"] if event["event_id"] in selected
    ]
    model["effective"] = [
        event
        for event in model["events"]
        if event["effective"] and not event["ignored"]
    ]
    return model


def observed_attempt_status(attempt: dict, fresh: bool) -> str:
    """Apply runtime freshness without changing recorded completion outcomes."""
    status = attempt.get("status", "unknown")
    if (
        status in {"running", "unconfirmed"}
        and attempt.get("started_at")
        and not attempt.get("finished_at")
    ):
        return "running" if fresh else "unconfirmed"
    return status


def projection_sources(
    kind: str, row: dict, by_artifact: dict, by_attempt: dict, by_operation: dict
) -> list[str]:
    if kind == "errors":
        return [row["event_id"]]
    if kind == "artifacts":
        return by_artifact.get(row["artifact_id"], [])
    if kind == "commands":
        return [event["event_id"] for event in row["observations"]]
    if kind == "parameters" or str(row.get("operation_id", "")).startswith("attempt:"):
        return by_attempt.get(row.get("attempt_id"), [])
    return by_operation.get(row.get("operation_id"), [])


def cached_experiment_views(
    dataset: dict, live: dict, run_id: str | None = None
) -> dict:
    """Build small screen summaries from indexed projections, not historical payloads."""
    cache = dataset["cache"]
    selection = " AND run_id=?" if run_id else ""
    args = (run_id,) if run_id else ()
    latest = cache.query(
        "SELECT compact FROM facts WHERE kind='template.applied' AND effective=1"
        + selection
        + " ORDER BY cursor DESC LIMIT 1",
        args,
    )
    recorded = cache.query(
        "SELECT compact FROM facts WHERE kind='experiment.state' AND effective=1"
        + selection
        + " ORDER BY cursor DESC LIMIT 1",
        args,
    )
    entries = [json.loads(row[0]) for row in [*latest, *recorded]]
    saved = {
        **dataset["state"],
        "template": compact_template(dataset["state"].get("template", {})),
        "template_yaml": "",
    }
    model = experiment_views(
        {**dataset, "state": saved, "entries": entries}, live, run_id
    )
    summary = model["summary"]
    current_run = run_id or summary["run_id"]
    revision = summary["template_revision_id"]
    counts = cache.query(
        "SELECT COUNT(*), MIN(occurred_at) FROM facts WHERE kind='error.recorded' AND effective=1"
        + selection,
        args,
    )[0]
    summary["error_count"] = counts[0] if dataset["complete"] else None
    first = (
        cache.query("SELECT MIN(started_at) FROM runs WHERE run_id=?", (run_id,))
        if run_id
        else cache.query("SELECT MIN(started_at) FROM runs")
    )
    summary["started_at"] = first[0][0]
    cycle_stats = cache.query(
        "SELECT COUNT(*), exact_mean(json_extract(summary,'$.duration')), MIN(json_extract(summary,'$.duration')), MAX(json_extract(summary,'$.duration')), MAX(cycle) FROM scopes WHERE run_id IS ? AND revision IS ? AND cycle IS NOT NULL AND json_extract(summary,'$.sample_cycles')>0",
        (current_run, revision),
    )[0]
    count, mean, low, high, _completed = cycle_stats
    valid_cycle = cache.query(
        "SELECT MAX(cycle) FROM scopes WHERE run_id IS ? AND revision IS ? AND json_extract(summary,'$.valid')=1",
        (current_run, revision),
    )[0][0]
    summary["completed_cycles"] = max(summary["completed_cycles"], valid_cycle or 0)
    forecast = model["forecast"]
    forecast.update(
        completed_cycles=summary["completed_cycles"],
        sample_cycles=count,
        sample_mean_seconds=mean,
    )
    remaining = (
        max(0, summary["total_cycles"] - summary["completed_cycles"])
        if type(summary["total_cycles"]) is int
        else None
    )
    usable = dataset["complete"] and count > 0 and remaining is not None
    observed = live if summary["fresh"] else {}
    active = cache.query(
        "SELECT summary FROM scopes WHERE run_id IS ? AND revision IS ? AND cycle IS ?",
        (current_run, revision, observed.get("cycle_number")),
    )
    spent = (
        elapsed(
            json.loads(active[0][0]).get("first_started"), observed.get("observed_at")
        )
        if active and not json.loads(active[0][0])["valid"]
        else 0
    )
    forecast.update(
        eta_seconds=max(0, remaining * mean - (spent or 0)) if usable else None,
        eta_low_seconds=max(0, remaining * low - (spent or 0)) if usable else None,
        eta_high_seconds=max(0, remaining * high - (spent or 0)) if usable else None,
    )
    components = cache.query(
        "SELECT json_extract(component.value,'$.stage_id'), json_extract(component.value,'$.module_name'), exact_mean(json_extract(component.value,'$.mean_seconds')) FROM scopes, json_each(scopes.summary,'$.components') AS component WHERE run_id IS ? AND revision IS ? AND json_extract(summary,'$.sample_cycles')>0 GROUP BY json_extract(component.value,'$.stage_id')",
        (current_run, revision),
    )
    durations = {stage: duration for stage, _module, duration in components}
    forecast["component_durations"] = [
        {**component, "mean_seconds": durations.get(component["stage_id"])}
        for component in forecast["component_durations"]
    ]
    dag_state = (
        live
        if summary["fresh"]
        else (json.loads(recorded[0][0])["data"] if recorded else saved)
    )
    dag_cycle = dag_state.get("cycle_number")
    for node in model["template"]["nodes"]:
        row = cache.query(
            "SELECT payload FROM records WHERE kind='parameters' AND run_id IS ? AND revision IS ? AND (? IS NULL OR cycle=?) AND json_extract(payload,'$.stage_id')=? ORDER BY position DESC LIMIT 1",
            (current_run, revision, dag_cycle, dag_cycle, node["stage_id"]),
        )
        if row and node["status"] not in {"running", "ready"}:
            node["status"] = json.loads(row[0][0])["status"]
    # Large lists have separate indexed page endpoints; summaries remain small.
    return model


def cached_metrics(dataset: dict, model: dict) -> dict:
    cache = dataset["cache"]
    summary = model["summary"]
    args = (summary["run_id"], summary["template_revision_id"])
    base = "kind='measurements' AND run_id IS ? AND revision IS ?"
    groups = cache.query(
        """
        SELECT json_extract(payload,'$.module_name'), json_extract(payload,'$.module_version'),
               json_extract(payload,'$.metric'), MIN(json_extract(payload,'$.unit')),
               COUNT(*), COUNT(DISTINCT cycle), COUNT(DISTINCT json_extract(payload,'$.unit')),
               COUNT(json_extract(payload,'$.value')), exact_mean(json_extract(payload,'$.value')),
               SUM(json_extract(payload,'$.value')),
               MAX(json_extract(payload,'$.complete')=0 OR json_extract(payload,'$.estimated')=1),
               MIN(json_extract(payload,'$.aggregation')='sum')
        FROM records WHERE """
        + base
        + " GROUP BY json_extract(payload,'$.module_name'), json_extract(payload,'$.module_version'), json_extract(payload,'$.metric')",
        args,
    )
    summaries = []
    for (
        name,
        version,
        metric,
        unit,
        count,
        cycles,
        units,
        known,
        mean,
        total,
        incomplete,
        additive,
    ) in groups:
        compatible = units == 1 and cycles == count
        summaries.append(
            {
                "module_name": name,
                "module_version": version,
                "metric": metric,
                "unit": unit if units == 1 else "Mixed units",
                "sample_cycles": cycles,
                "mean": mean if compatible and known == count else None,
                "total": total,
                "incomplete": not compatible
                or known != count
                or bool(incomplete)
                or not dataset["complete"],
                "additive": bool(additive),
            }
        )
    recent = cache.query(
        """
        SELECT payload FROM (
            SELECT payload, cycle, ROW_NUMBER() OVER (
                PARTITION BY json_extract(payload,'$.module_name'), json_extract(payload,'$.module_version'), json_extract(payload,'$.metric')
                ORDER BY cycle DESC, position DESC, record_key DESC
            ) AS number FROM records WHERE """
        + base
        + ") WHERE number<=20 ORDER BY cycle",
        args,
    )
    cycles = cache.query(
        "SELECT COUNT(DISTINCT cycle) FROM records WHERE " + base, args
    )[0][0]
    return {
        "metric_summaries": summaries,
        "measurements": [json.loads(row[0]) for row in recent],
        "measurement_cycles": cycles,
    }


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
        elif kind == "call.started" and context.get("attempt_id"):
            attempt = attempts.setdefault(context["attempt_id"], dict(context))
            attempt.update(started_at=event["occurred_at"], status="running")
        elif kind == "stage.process_started":
            attempt = attempts.setdefault(context.get("attempt_id"), dict(context))
            attempt.update(
                started_at=data.get("started_at", event["occurred_at"]),
                process=data.get("process"),
                status="running",
            )
        elif kind == "stage.finished":
            attempt = attempts.setdefault(context.get("attempt_id"), dict(context))
            attempt.update(context)
            attempt.update(
                finished_at=event["occurred_at"],
                status=data.get("outcome", "unknown"),
                result=data.get("result"),
                result_request_id=data.get("result_request_id"),
            )
            attempt["duration_seconds"] = elapsed(
                attempt.get("started_at"), attempt["finished_at"]
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
    # DAG nodes and timeline scopes must use the same observed attempt status.
    for attempt in attempts.values():
        attempt["status"] = observed_attempt_status(attempt, fresh)
    stages = template.get("stages", [])
    service_modules = {
        service["service_id"]: service["module"]
        for service in template.get("services", [])
    }
    nodes = []
    dag_cycle = observed.get("cycle_number")
    for position, definition in enumerate(stages, 1):
        node = {
            **definition,
            "name": definition.get("name")
            or (
                definition["module"]
                if "module" in definition
                else service_modules[definition["service_id"]]
            )["name"],
            "position": position,
        }
        stage_attempts = [
            attempt
            for attempt in attempts.values()
            if attempt.get("stage_id") == definition["stage_id"]
            and attempt.get("run_id") == current_run
            and attempt.get("template_revision_id") == revision_id
            and (dag_cycle is None or attempt.get("cycle_number") == dag_cycle)
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
        effective,
        attempts,
        {**operations, **dataset.get("operation_ancestors", {})},
        valid_cycles,
        revision_id,
        current_run,
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
                "module_name": (
                    definition["module"]
                    if "module" in definition
                    else service_modules[definition["service_id"]]
                )["name"],
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


def module_statistics(snapshots: list[tuple[dict, sqlite3.Connection]]) -> list[dict]:
    """Exact project statistics from caller-owned SQLite read transactions."""
    groups = {}
    module_fields = "json_extract(payload,'$.module_name'), json_extract(payload,'$.module_version'), json_extract(payload,'$.module_hash')"
    for dataset, connection in snapshots:
        for name, version, digest in connection.execute(
            "SELECT DISTINCT " + module_fields + " FROM records WHERE kind='parameters'"
        ):
            if not name:
                continue
            key = (name, version, digest)
            group = groups.setdefault(
                key,
                {
                    "module_id": "/".join(str(part or "") for part in key),
                    "name": name,
                    "version": version,
                    "module_hash": digest,
                    "runs": 0,
                    "error_count": 0,
                    "restarts": 0,
                    "recent_attempts": [],
                    "complete": True,
                    "experiments": set(),
                    "caches": [],
                },
            )
            selection = "kind='parameters' AND json_extract(payload,'$.module_name') IS ? AND json_extract(payload,'$.module_version') IS ? AND json_extract(payload,'$.module_hash') IS ?"
            counts = connection.execute(
                "SELECT COUNT(*) FROM records WHERE "
                + selection
                + " AND json_extract(payload,'$.started_at') IS NOT NULL",
                key,
            ).fetchone()[0]
            restarts = connection.execute(
                "SELECT COALESCE(SUM(MAX(0,n-1)),0) FROM (SELECT COUNT(*) AS n FROM records WHERE "
                + selection
                + " AND json_extract(payload,'$.started_at') IS NOT NULL GROUP BY run_id, cycle, json_extract(payload,'$.stage_id'))",
                key,
            ).fetchone()[0]
            errors = connection.execute(
                "SELECT COUNT(*) FROM records AS error WHERE error.kind='errors' AND EXISTS (SELECT 1 FROM records WHERE "
                + selection
                + " AND json_extract(payload,'$.attempt_id')=json_extract(error.payload,'$.attempt_id'))",
                key,
            ).fetchone()[0]
            recent = connection.execute(
                "SELECT payload FROM records WHERE "
                + selection
                + " ORDER BY json_extract(payload,'$.recorded_at') DESC LIMIT 100",
                key,
            )
            group["runs"] += counts
            group["restarts"] += restarts
            group["error_count"] += errors
            group["complete"] = group["complete"] and dataset["complete"]
            group["experiments"].add(dataset["name"])
            group["caches"].append(connection)
            recent_attempts = []
            for row in recent:
                attempt = json.loads(row[0])
                if attempt.get("detail_ref"):
                    attempt["detail_ref"] = {
                        **attempt["detail_ref"],
                        **dataset["identity"],
                    }
                recent_attempts.append(attempt)
            group["recent_attempts"] = sorted(
                [
                    *group["recent_attempts"],
                    *recent_attempts,
                ],
                key=lambda row: row.get("recorded_at", ""),
                reverse=True,
            )[:100]
    for key, group in groups.items():
        selection = "kind='parameters' AND json_extract(payload,'$.module_name') IS ? AND json_extract(payload,'$.module_version') IS ? AND json_extract(payload,'$.module_hash') IS ? AND json_extract(payload,'$.duration_seconds') IS NOT NULL"
        caches = group.pop("caches")
        count = sum(
            connection.execute(
                "SELECT COUNT(*) FROM records WHERE " + selection, key
            ).fetchone()[0]
            for connection in caches
        )
        ranks = {
            "p50_seconds": max(0, math.ceil(count * 0.5) - 1),
            "p95_seconds": max(0, math.ceil(count * 0.95) - 1),
        }
        group.update(p50_seconds=None, p95_seconds=None)
        streams = [
            connection.execute(
                "SELECT json_extract(payload,'$.duration_seconds') FROM records WHERE "
                + selection
                + " ORDER BY json_extract(payload,'$.duration_seconds')",
                key,
            )
            for connection in caches
        ]
        try:
            for index, (duration,) in enumerate(heapq.merge(*streams)):
                for field, rank in ranks.items():
                    if index == rank:
                        group[field] = duration
                if index >= ranks["p95_seconds"]:
                    break
        finally:
            for stream in streams:
                stream.close()
        group["experiment_name"] = ", ".join(sorted(group.pop("experiments")))
    return list(groups.values())
