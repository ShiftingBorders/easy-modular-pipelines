"""Combine local read-only journal views with the system's existing live HTTP API."""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import defaultdict, deque
from datetime import UTC, datetime
from uuid import UUID, uuid4

from dashboard.api_client import SystemAPIClient, SystemAPIError
from dashboard.journals import LocalJournals, read_object
from dashboard.projections import experiment_views, instant, percentile


def validate_samples(samples: object) -> list[dict]:
    if not isinstance(samples, list):
        raise SystemAPIError(
            "invalid_response", "Collector measurements must be a list."
        )
    for sample in samples:
        if (
            not isinstance(sample, dict)
            or not isinstance(sample.get("series_id"), str)
            or instant(sample.get("observed_at")) is None
            or not isinstance(sample.get("resources"), dict)
        ):
            raise SystemAPIError(
                "invalid_response", "Invalid collector measurement envelope."
            )
        for metric in sample["resources"].values():
            if (
                not isinstance(metric, dict)
                or not isinstance(metric.get("attributes", {}), dict)
                or (
                    metric.get("value") is not None
                    and type(metric["value"]) not in (int, float)
                )
            ):
                raise SystemAPIError("invalid_response", "Invalid collector metric.")
        freshness = sample.get("freshness", {})
        if not isinstance(freshness, dict) or any(
            not isinstance(item, dict) or type(item.get("fresh")) is not bool
            for item in freshness.values()
        ):
            raise SystemAPIError(
                "invalid_response", "Invalid collector freshness metadata."
            )
    return samples


class DashboardViews:
    def __init__(self, settings: dict, system: SystemAPIClient) -> None:
        self.settings = settings
        self.system = system
        self.journals = LocalJournals(settings)
        self._live: dict = {}
        self._live_at = 0.0
        self._live_lock = asyncio.Lock()
        self._resource_lock = asyncio.Lock()
        self._resource_status: dict = {}
        self._resource_at = 0.0
        self._history: deque[dict] = deque(maxlen=50000)
        self._history_id = None
        self._history_cursor = 0
        self._history_gap = False
        self._publications: dict[str, dict] = {}
        self._commands: list[dict] = []
        self._command_lock = asyncio.Lock()
        self._command_task: asyncio.Task | None = None
        self._source_tasks: list[asyncio.Task] = []
        self._model_lock = asyncio.Lock()
        self._models: dict[tuple[str, str | None], tuple[tuple, dict, dict]] = {}
        self._resource_observed_at = 0.0

    async def open(self) -> None:
        path = self.settings["state_directory"] / "commands.json"
        if path.exists():
            document = await asyncio.to_thread(read_object, path, 8388608)
            if not isinstance(document.get("items"), list):
                raise ValueError("Invalid dashboard command history.")
            for record in document["items"]:
                if (
                    not isinstance(record, dict)
                    or not isinstance(record.get("status"), str)
                    or type(record.get("polling")) is not bool
                ):
                    raise ValueError("Invalid saved command record.")
                UUID(record["command_id"])
                if record["status"] == "submitting":
                    record.update(status="unknown", polling=True)
            self._commands = document["items"][-1000:]
        self._command_task = asyncio.create_task(self._poll_commands())
        self._source_tasks = [
            asyncio.create_task(self._poll_source("state")),
            asyncio.create_task(self._poll_source("resources")),
        ]

    def _write_commands(self) -> None:
        path = self.settings["state_directory"] / "commands.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(
            {"items": self._commands}, ensure_ascii=False, allow_nan=False
        )
        if len(encoded.encode("utf-8")) > 8388608:
            raise OSError("Dashboard command history exceeds its 8 MiB limit.")
        temporary = path.with_name(f".commands-{uuid4()}.json")
        try:
            with temporary.open("w", encoding="utf-8") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    async def _save_commands(self) -> None:
        writing = asyncio.create_task(asyncio.to_thread(self._write_commands))
        try:
            await asyncio.shield(writing)
        except asyncio.CancelledError:
            await writing
            raise

    async def state(self, *, refresh: bool = False) -> dict:
        if not refresh:
            snapshot = dict(self._live)
            if not self._live_at or time.monotonic() - self._live_at > 3:
                snapshot["fresh"] = False
            return snapshot
        async with self._live_lock:
            if time.monotonic() - self._live_at < 0.5:
                return dict(self._live)
            try:
                self._live = await self.system.read("state")
                self._live["available"] = True
            except SystemAPIError as error:
                self._live = {
                    **self._live,
                    "fresh": False,
                    "available": False,
                    "connection_error": str(error),
                }
            self._live_at = time.monotonic()
            return dict(self._live)

    async def _poll_source(self, source: str) -> None:
        while True:
            try:
                if source == "state":
                    await self.state(refresh=True)
                else:
                    await self._refresh_resources()
            except Exception as error:  # noqa: BLE001 - A broken source must not stop independent cached reads.
                if source == "state":
                    self._live = {
                        **self._live,
                        "available": False,
                        "fresh": False,
                        "connection_error": str(error),
                    }
                else:
                    self._resource_status = {
                        **self._resource_status,
                        "state": "unavailable",
                        "error": str(error),
                    }
            await asyncio.sleep(1)

    async def _model(
        self, identifier: str, run_id: str | None = None
    ) -> tuple[dict, dict]:
        async with self._model_lock:
            dataset = await asyncio.to_thread(self.journals.load, identifier)
            live = await self.state()
            key = (identifier, run_id)
            live_version = (
                (self._live_at, live.get("fresh"))
                if live.get("experiment_id") == identifier
                else None
            )
            version = (dataset.get("identity"), dataset.get("refreshed"), live_version)
            previous = self._models.get(key)
            if dataset.get("refreshed") and previous and previous[0] == version:
                return previous[1], previous[2]
            model = await asyncio.to_thread(experiment_views, dataset, live, run_id)
            self._models.pop(key, None)
            while len(self._models) >= 8:
                self._models.pop(next(iter(self._models)))
            self._models[key] = (version, dataset, model)
            return dataset, model

    async def models(self) -> list[tuple[dict, dict]]:
        try:
            registry = await asyncio.to_thread(self.journals.registry)
        except (ValueError, OSError) as error:
            raise SystemAPIError("registry_unavailable", str(error)) from error
        result = []
        for identifier in registry:
            try:
                dataset, model = await self._model(identifier)
            except SystemAPIError as error:
                dataset = {
                    "experiment_id": identifier,
                    "complete": False,
                    "identity": None,
                    "entries": [],
                    "error": str(error),
                }
                model = {
                    "summary": {
                        "experiment_id": identifier,
                        "name": identifier,
                        "status": "unavailable",
                        "fresh": False,
                        "error": str(error),
                        "error_count": None,
                    },
                    "errors": [],
                    "parameters": [],
                    "operations": [],
                }
            result.append((dataset, model))
        if not result and self.settings["project_root"] is None:
            raise SystemAPIError(
                "not_configured",
                "Configure project_root to read the experiment journals.",
            )
        return result

    async def read(self, resource: str, params: dict) -> dict:
        if resource == "compute":
            return await self.compute(params)
        source_error = None
        try:
            models = await self.models() if resource != "services" else []
        except SystemAPIError as error:
            if resource != "overview":
                raise
            models = []
            source_error = str(error)
        summaries = [model["summary"] for _, model in models]
        if resource == "experiments":
            return {"items": summaries}
        if resource == "overview":
            complete = (
                source_error is None
                and self.settings["project_root"] is not None
                and all(dataset["complete"] for dataset, _ in models)
            )
            metrics = {
                "active_experiments": sum(
                    summary.get("fresh", False)
                    and summary["status"]
                    not in {"completed", "failed", "stopped", "idle"}
                    for summary in summaries
                ),
                "completed_experiments": sum(
                    summary["status"] == "completed" for summary in summaries
                ),
                "failed_experiments": sum(
                    summary["status"] == "failed" for summary in summaries
                ),
                "error_events": sum(
                    summary.get("error_count") or 0 for summary in summaries
                )
                if complete
                else None,
            }
            compute = await self.compute({})
            if not complete:
                metrics = dict.fromkeys(metrics)
            if not (await self.state()).get("fresh"):
                metrics["active_experiments"] = None
            return {
                "metrics": metrics,
                "attention": [
                    summary
                    for summary in summaries
                    if summary["status"]
                    in {"failed", "unknown", "unavailable", "paused"}
                    or summary.get("error_count")
                ],
                "compute": compute["metrics"],
                "complete": complete,
                "error": source_error or compute.get("error"),
                "observed_at": datetime.now(UTC).isoformat(),
            }
        if resource == "modules":
            modules = {}
            for dataset, model in models:
                for attempt in model["parameters"]:
                    name, version = (
                        attempt.get("module_name"),
                        attempt.get("module_version"),
                    )
                    if not name:
                        continue
                    key = (name, version, attempt.get("module_hash"))
                    item = modules.setdefault(
                        key,
                        {
                            "module_id": "/".join(str(part or "") for part in key),
                            "name": name,
                            "version": version,
                            "module_hash": key[2],
                            "runs": 0,
                            "error_count": 0,
                            "restarts": 0,
                            "recent_attempts": [],
                            "durations": [],
                            "experiments": set(),
                            "complete": True,
                        },
                    )
                    item["runs"] += int(attempt.get("started_at") is not None)
                    item["recent_attempts"].append(attempt)
                    item["experiments"].add(model["summary"]["name"])
                    item["complete"] = item["complete"] and dataset["complete"]
                    if attempt.get("duration_seconds") is not None:
                        item["durations"].append(attempt["duration_seconds"])
                    item["error_count"] += sum(
                        error.get("attempt_id") == attempt.get("attempt_id")
                        for error in model["errors"]
                    )
                execution_groups = defaultdict(list)
                for attempt in model["parameters"]:
                    execution_groups[
                        (
                            attempt.get("module_name"),
                            attempt.get("module_version"),
                            attempt.get("module_hash"),
                            attempt.get("stage_id"),
                            attempt.get("cycle_number"),
                        )
                    ].append(attempt)
                for key, attempts in execution_groups.items():
                    if key[:3] in modules:
                        modules[key[:3]]["restarts"] += max(
                            0,
                            sum(item.get("started_at") is not None for item in attempts)
                            - 1,
                        )
            for module in modules.values():
                durations = module.pop("durations")
                module["p50_seconds"] = percentile(durations, 0.5)
                module["p95_seconds"] = percentile(durations, 0.95)
                module["experiment_name"] = ", ".join(sorted(module.pop("experiments")))
                module["recent_attempts"] = sorted(
                    module["recent_attempts"],
                    key=lambda item: item.get("recorded_at", ""),
                    reverse=True,
                )[:100]
            return {"items": list(modules.values())}
        if resource == "services":
            live = await self.state()
            if not live.get("available"):
                raise SystemAPIError(
                    "state_unavailable",
                    live.get(
                        "connection_error", "Current service state is unavailable."
                    ),
                )
            compute = await self.compute({})
            services = []
            for service in live.get("services", []):
                module = service.get("module", {})
                instance = service.get("service_instance_id")
                metrics = compute["processes"].get(instance, {})
                state = (
                    "stopped"
                    if service.get("stopped")
                    else "stopping"
                    if service.get("stopping")
                    else "ready"
                    if service.get("ready")
                    else "starting"
                )
                services.append(
                    {
                        **service,
                        "instance_id": instance or service["service_id"],
                        "name": module.get("name", service["service_id"]),
                        "version": module.get("version"),
                        "state": state if live.get("fresh") else "unknown",
                        "observed_at": live.get("observed_at"),
                        "queue_length": service.get("pending_requests"),
                        "process_metrics": metrics,
                        "experiment_id": live.get("experiment_id"),
                    }
                )
            return {"items": services, "fresh": live.get("fresh", False)}
        raise SystemAPIError("not_found", "Unknown dashboard view.", 404)

    async def experiment(self, identifier: str, view: str, params: dict) -> dict:
        dataset, model = await self._model(identifier, params.get("run_id"))
        live = await self.state()
        metadata = {
            "experiment_id": identifier,
            "journal": dataset["identity"],
            "complete": dataset["complete"],
            "observed_at": live.get("observed_at")
            if live.get("experiment_id") == identifier and live.get("fresh")
            else (
                dataset["entries"][-1]["occurred_at"] if dataset["entries"] else None
            ),
            "source": "local_journal",
            "error": dataset.get("error"),
        }
        if view == "summary":
            return {**metadata, **model["summary"]}
        if view == "template":
            template = model["template"]
            revision = params.get("revision")
            if revision:
                selected = next(
                    (
                        row
                        for row in template["revisions"]
                        if row["template_revision_id"] == revision
                    ),
                    None,
                )
                if selected is None:
                    raise SystemAPIError(
                        "not_found",
                        "The requested template revision is unavailable.",
                        404,
                    )
                template = {
                    **template,
                    "template": selected["template"],
                    "template_yaml": selected["template_yaml"],
                }
            return {**metadata, **template}
        if view == "forecast":
            return {**metadata, **model["forecast"]}
        if view == "snapshots":
            snapshots = await asyncio.to_thread(self.journals.snapshots, identifier)
            return self.page(snapshots, metadata, view, params)
        key = (
            "effective"
            if view == "events" and params.get("view", "effective") == "effective"
            else view
        )
        if key not in model or not isinstance(model[key], list):
            raise SystemAPIError("not_found", "Unknown experiment view.", 404)
        items = model[key]
        if view == "commands":
            items = [
                *items,
                *(
                    row
                    for row in self._commands
                    if row.get("experiment_id") == identifier
                ),
            ]
        return self.page(items, metadata, view, params)

    def page(self, items: list[dict], metadata: dict, view: str, params: dict) -> dict:
        limit = int(params.get("limit", 200))
        now = time.monotonic()
        self._publications = {
            key: publication
            for key, publication in self._publications.items()
            if now - publication["at"] < 300
        }
        scope = (
            metadata["experiment_id"],
            params.get("run_id"),
            view,
            params.get("view", "effective"),
        )
        if params.get("cursor"):
            try:
                cursor = json.loads(params["cursor"])
                publication = self._publications[cursor["publication"]]
                offset = cursor["offset"]
                if (
                    publication["scope"] != scope
                    or publication["metadata"]["journal"] != metadata["journal"]
                    or type(offset) is not int
                    or offset < 0
                ):
                    raise ValueError("Invalid publication cursor.")
            except (ValueError, KeyError, TypeError) as error:
                raise SystemAPIError(
                    "history_changed",
                    "The selected publication is unavailable; refresh the history.",
                    409,
                ) from error
            token = cursor["publication"]
            items, metadata = publication["items"], publication["metadata"]
        else:
            offset, token = 0, uuid4().hex
            encoded_size = len(json.dumps(items, ensure_ascii=False).encode())
            if encoded_size > self.settings["history_max_bytes"]:
                raise SystemAPIError(
                    "history_limit",
                    "This view exceeds the configured history memory limit.",
                    413,
                )
            while self._publications and (
                len(self._publications) >= 16
                or sum(item["bytes"] for item in self._publications.values())
                + encoded_size
                > self.settings["history_max_bytes"]
            ):
                self._publications.pop(next(iter(self._publications)))
            self._publications[token] = {
                "at": now,
                "scope": scope,
                "items": items,
                "metadata": metadata,
                "bytes": encoded_size,
            }
        selected, size = [], 1024
        for item in items[offset : offset + limit]:
            length = len(json.dumps(item, ensure_ascii=False).encode())
            if size + length > self.settings["max_response_bytes"]:
                if not selected:
                    raise SystemAPIError(
                        "response_too_large",
                        "This record exceeds the configured response size limit.",
                        413,
                    )
                break
            selected.append(item)
            size += length
        next_offset = offset + len(selected)
        return {
            **metadata,
            "items": selected,
            "total": len(items),
            "next_cursor": {"publication": token, "offset": next_offset}
            if next_offset < len(items)
            else None,
        }

    async def _refresh_resources(self) -> None:
        async with self._resource_lock:
            error = None
            if time.monotonic() - self._resource_at >= 0.5:
                try:
                    status = await self.system.read("resources")
                    if status.get("history_id") != self._history_id:
                        self._history.clear()
                        self._history_cursor = 0
                        self._history_id = status.get("history_id")
                        self._history_gap = False
                    if not isinstance(status.get("latest"), list):
                        raise SystemAPIError(
                            "invalid_response",
                            "Collector status has no measurements list.",
                        )
                    validate_samples(status["latest"])
                    self._resource_status = status
                    self._resource_observed_at = time.monotonic()
                except SystemAPIError as failure:
                    error = str(failure)
                    self._resource_status = {
                        **self._resource_status,
                        "state": "unavailable",
                        "error": error,
                    }
                if error is None:
                    try:
                        for _ in range(5):
                            page = await self.system.read(
                                "resources/history",
                                {"after": self._history_cursor, "limit": 1000},
                            )
                            if (
                                not isinstance(page.get("samples"), list)
                                or type(page.get("cursor")) is not int
                                or "history_id" not in page
                                or "gap" not in page
                            ):
                                raise SystemAPIError(
                                    "invalid_response", "Invalid resource history page."
                                )
                            if page["history_id"] != self._history_id:
                                self._history.clear()
                                self._history_id, self._history_cursor = (
                                    page["history_id"],
                                    0,
                                )
                                self._history_gap = False
                                continue
                            validate_samples(page["samples"])
                            self._history_gap = (
                                self._history_gap
                                or page["gap"]
                                or len(self._history) + len(page["samples"])
                                > self._history.maxlen
                            )
                            self._history.extend(page["samples"])
                            previous_cursor = self._history_cursor
                            self._history_cursor = page["cursor"]
                            if (
                                not page["samples"]
                                or self._history_cursor == previous_cursor
                            ):
                                break
                        self._resource_status["history_error"] = None
                    except SystemAPIError as failure:
                        # History failure does not invalidate fresh instantaneous readings.
                        self._resource_status["history_error"] = str(failure)
                self._resource_at = time.monotonic()

    async def compute(self, params: dict) -> dict:
        status = dict(self._resource_status)
        if (
            not self._resource_observed_at
            or time.monotonic() - self._resource_observed_at > 3
        ):
            status["state"] = "unavailable"
            status["error"] = (
                status.get("error") or "Awaiting a current resource observation."
            )
        host = next(
            (
                sample
                for sample in status.get("latest", [])
                if sample["series_id"].startswith("host:")
            ),
            {},
        )
        metrics = {}
        names = {
            "cpu": "host_cpu_percent",
            "ram": "host_memory_percent",
            "disk": "host_disk_percent",
        }
        for key, name in names.items():
            measured = host.get("resources", {}).get(name, {})
            fresh = status.get("state") == "running" and host.get("freshness", {}).get(
                name, {}
            ).get("fresh", False)
            metrics[key] = {
                "value": measured.get("value"),
                "fresh": fresh,
                "exceeded": False,
                "reason": measured.get("attributes", {}).get("reason"),
                "attributes": measured.get("attributes", {}),
            }
        metrics["disk"]["free_bytes"] = (
            host.get("resources", {}).get("host_disk_free_bytes", {}).get("value")
        )
        metrics["internet"] = {
            direction + "_mbps": host.get("resources", {})
            .get("internet_" + direction + "_mbps", {})
            .get("value")
            for direction in ("receive", "transmit")
        }
        metrics["internet"]["interface"] = (
            host.get("resources", {})
            .get("internet_receive_mbps", {})
            .get("attributes", {})
            .get("interface")
        )
        metrics["internet"]["fresh"] = status.get("state") == "running" and all(
            host.get("freshness", {})
            .get("internet_" + direction + "_mbps", {})
            .get("fresh", False)
            for direction in ("receive", "transmit")
        )
        since = instant(params.get("since"))
        until = instant(params.get("until"))
        if (
            params.get("since")
            and since is None
            or params.get("until")
            and until is None
            or since is not None
            and until is not None
            and since >= until
        ):
            raise SystemAPIError("invalid_range", "Choose a valid time range.", 400)
        samples = [
            sample
            for sample in self._history
            if (since is None or (instant(sample["observed_at"]) or 0) >= since)
            and (until is None or (instant(sample["observed_at"]) or 0) <= until)
        ]
        history = {
            key: [
                {
                    "observed_at": sample["observed_at"],
                    "value": sample["resources"].get(name, {}).get("value"),
                }
                for sample in samples
                if sample["series_id"].startswith("host:")
            ]
            for key, name in names.items()
        }
        processes = {}
        for sample in status.get("latest", []):
            if sample["series_id"].startswith("host:"):
                continue
            values = sample["resources"]
            processes[sample["series_id"]] = {
                "rss_bytes": values.get("process_memory_rss_bytes", {}).get("value"),
                "cpu_percent": values.get("process_cpu_percent", {}).get("value"),
                "fresh": status.get("state") == "running"
                and sample.get("fresh", False),
                "observed_process": values.get("process_cpu_percent", {})
                .get("attributes", {})
                .get("observed_process"),
                "history": [
                    {
                        "observed_at": point["observed_at"],
                        "rss_bytes": point["resources"]
                        .get("process_memory_rss_bytes", {})
                        .get("value"),
                        "cpu_percent": point["resources"]
                        .get("process_cpu_percent", {})
                        .get("value"),
                    }
                    for point in samples
                    if point["series_id"] == sample["series_id"]
                ],
            }
        return {
            "metrics": metrics,
            "history": history,
            "processes": processes,
            "collector": status,
            "observed_at": host.get("observed_at"),
            "history_id": self._history_id,
            "gap": self._history_gap,
            "error": status.get("error"),
            "journal_error": status.get("journal_error"),
            "history_error": status.get("history_error"),
        }

    async def command(self, command: dict) -> dict:
        allowed = {
            "run",
            "pause",
            "resume",
            "step",
            "stop",
            "rerun",
            "retry",
            "move",
            "reset_retries",
            "replace",
            "reload_template",
            "snapshot",
            "rollback",
            "recover",
        }
        if command.get("command") not in allowed or command.keys() - {
            "command",
            "args",
            "target",
            "command_id",
            "expected_experiment_id",
        }:
            raise ValueError("Unsupported dashboard command.")
        command = dict(command)
        expected = command.pop("expected_experiment_id", None)
        if expected is not None:
            self._live_at = 0
            live = await self.state(refresh=True)
            if (
                not isinstance(expected, str)
                or not live.get("fresh")
                or live.get("experiment_id") != expected
            ):
                raise SystemAPIError(
                    "selection_changed",
                    "The runtime selection changed or is unavailable. Refresh before sending a command.",
                    409,
                )
        command["command_id"] = (
            str(UUID(command["command_id"]))
            if command.get("command_id")
            else str(uuid4())
        )
        if not isinstance(command.get("args", {}), dict):
            raise TypeError("Command args must be an object.")
        async with self._command_lock:
            if any(
                item["command_id"] == command["command_id"] for item in self._commands
            ):
                raise SystemAPIError(
                    "command_exists",
                    "This command ID is already recorded; read its result instead.",
                    409,
                )
            record = {
                "command_id": command["command_id"],
                "command": command["command"],
                "experiment_id": command.get("args", {}).get("experiment_id")
                or self._live.get("experiment_id"),
                "target": "Runner",
                "kind": "control",
                "status": "submitting",
                "sent_at": datetime.now(UTC).isoformat(),
                "server_instance_id": self._live.get("server_instance_id"),
                "args": command.get("args", {}),
                "polling": False,
            }
            previous = list(self._commands)
            self._commands.append(record)
            self._commands = self._commands[-1000:]
            try:
                await self._save_commands()
            except OSError as error:
                self._commands = previous
                raise SystemAPIError(
                    "command_storage_unavailable",
                    "Command was not sent because its receipt could not be saved.",
                ) from error
        try:
            receipt = await self.system.submit(command)
        except SystemAPIError as error:
            async with self._command_lock:
                record.update(
                    status="unknown"
                    if error.code in {"connection_error", "timeout"}
                    else "failed",
                    error=str(error),
                )
                record["polling"] = record["status"] == "unknown"
                await self._save_commands()
            raise
        async with self._command_lock:
            record.update(
                status=receipt.get("state", "pending"),
                server_instance_id=receipt.get("server_instance_id"),
                polling=receipt.get("state", "pending") == "pending",
                result=receipt,
            )
            await self._save_commands()
        return receipt

    async def command_result(self, identifier: str) -> dict:
        result = await self.system.read(f"commands/{identifier}")
        async with self._command_lock:
            record = next(
                (item for item in self._commands if item["command_id"] == identifier),
                None,
            )
            if record:
                if record.get("server_instance_id") not in (
                    None,
                    result.get("server_instance_id"),
                ):
                    record.update(
                        status="unknown",
                        polling=False,
                        error="System server instance changed.",
                    )
                    await self._save_commands()
                    return {**result, "state": "unknown", "result": None}
                record.update(status=result.get("state", "unknown"), result=result)
                record["polling"] = result.get("state") == "pending"
                if result.get("experiment_id"):
                    record["experiment_id"] = result["experiment_id"]
                await self._save_commands()
        self._live_at = 0
        return result

    async def _poll_commands(self) -> None:
        while True:
            for record in list(self._commands):
                if not record.get("polling") or self.system.base_url is None:
                    continue
                try:
                    await self.command_result(record["command_id"])
                except SystemAPIError as error:
                    async with self._command_lock:
                        record.update(status="unknown", error=str(error))
                        if error.status_code == 404:
                            record["polling"] = False
                        try:
                            await self._save_commands()
                        except OSError as failure:
                            record["storage_error"] = str(failure)
                except OSError as error:
                    record["storage_error"] = str(error)
            await asyncio.sleep(1)

    async def close(self) -> None:
        for task in self._source_tasks:
            task.cancel()
        await asyncio.gather(*self._source_tasks, return_exceptions=True)
        self._source_tasks.clear()
        if self._command_task is not None:
            self._command_task.cancel()
            await asyncio.gather(self._command_task, return_exceptions=True)
        async with self._command_lock:
            pass
