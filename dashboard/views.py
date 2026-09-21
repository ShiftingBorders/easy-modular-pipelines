"""Combine local read-only journal views with the system's existing live HTTP API."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import UTC, datetime
from uuid import UUID, uuid4

from dashboard.api_client import SystemAPIClient, SystemAPIError
from dashboard.journals import (
    LocalJournals,
    cache_experiment,
    publish_module_statistics,
    read_object,
)
from dashboard.projections import (
    cached_experiment_views,
    cached_metrics,
    experiment_views,
    instant,
)


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
        self._cache_pool: ProcessPoolExecutor | None = None
        self._cache_jobs: dict = {}
        self._cache_targets: dict[str, dict] = {}
        self._cache_errors: dict[str, dict] = {}
        self._cache_checked: dict[str, float] = {}
        self._module_error: str | None = None
        self._module_job = None
        self._module_registry: tuple | None = None
        self._module_retry_at = 0.0

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
        self._replace_cache_pool()
        self._command_task = asyncio.create_task(self._poll_commands())
        self._source_tasks = [
            asyncio.create_task(self._poll_source("state")),
            asyncio.create_task(self._poll_source("resources")),
            asyncio.create_task(self._poll_journals()),
        ]

    async def _poll_journals(self) -> None:
        """Schedule independent process writers; HTTP reads never build projections."""
        while True:
            try:
                registry = await asyncio.to_thread(self.journals.registry)
            except (OSError, TypeError, ValueError):
                registry = {}
            self._collect_cache_jobs()
            self._collect_module_job()
            now = time.monotonic()
            signature = tuple(
                (identifier, str(path)) for identifier, path in registry.items()
            )
            if (
                self._module_job is None
                and self.settings["project_root"] is not None
                and signature != self._module_registry
                and now >= self._module_retry_at
            ):
                try:
                    self._module_job = self._cache_pool.submit(
                        publish_module_statistics, self.settings
                    )
                    self._module_registry = signature
                except BrokenProcessPool:
                    self._replace_cache_pool()
                    await asyncio.sleep(0.25)
                    continue
            for identifier in registry:
                if len(self._cache_jobs) >= self.settings.get("cache_workers", 2):
                    break
                if (
                    identifier in self._cache_jobs
                    or now - self._cache_checked.get(identifier, 0)
                    < self.journals.interval
                ):
                    continue
                try:
                    self._cache_jobs[identifier] = self._cache_pool.submit(
                        cache_experiment,
                        self.settings,
                        identifier,
                        self._cache_targets.get(identifier),
                    )
                except BrokenProcessPool:
                    self._replace_cache_pool()
                    break
            await asyncio.sleep(0.25)

    def _replace_cache_pool(self) -> None:
        if self._cache_pool is not None:
            self._cache_pool.shutdown(wait=False, cancel_futures=True)
        self._cache_pool = ProcessPoolExecutor(
            max_workers=self.settings.get("cache_workers", 2),
            mp_context=multiprocessing.get_context("spawn"),
        )

    def _collect_module_job(self) -> None:
        if self._module_job is None or not self._module_job.done():
            return
        try:
            if not self._module_job.result():
                self._module_registry = None
            else:
                self._module_error = None
        except Exception as error:  # noqa: BLE001 - Keep the last snapshot visibly incomplete until publication recovers.
            self._module_error = str(error)
            self._module_registry = None
            self._module_retry_at = time.monotonic() + self.journals.interval
        finally:
            self._module_job = None

    def _collect_cache_jobs(self) -> None:
        for identifier, future in list(self._cache_jobs.items()):
            if not future.done():
                continue
            del self._cache_jobs[identifier]
            try:
                result = future.result()
            except Exception as error:  # noqa: BLE001 - Surface failed worker processes through history reads.
                result = {
                    "error": {"code": "cache_worker_failed", "message": str(error)}
                }
            error = result.get("error")
            if result.get("modules_error"):
                self._module_error = result["modules_error"]
            elif result.get("modules_published"):
                self._module_error = None
            if error:
                self._cache_errors[identifier] = error
                self._cache_checked[identifier] = time.monotonic()
                self._cache_targets.pop(identifier, None)
                continue
            self._cache_errors.pop(identifier, None)
            self._cache_targets[identifier] = result["target_boundary"]
            if result["complete"]:
                self._cache_targets.pop(identifier, None)
                self._cache_checked[identifier] = time.monotonic()

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

    async def _cache_read_boundary(self, identifier: str, dataset: dict) -> dict:
        target = dataset.get("boundary")
        if target is None:
            return dataset
        # Two bounded batches allow an older background job to finish first.
        # Large rebuilds remain visibly incomplete instead of blocking on all history.
        for _ in range(2):
            future = self._cache_jobs.get(identifier)
            if future is None:
                future = self._cache_pool.submit(
                    cache_experiment, self.settings, identifier, target
                )
                self._cache_jobs[identifier] = future
            result = await asyncio.shield(asyncio.wrap_future(future))
            self._collect_cache_jobs()
            error = result.get("error")
            if error and error["code"] != "cache_busy":
                raise SystemAPIError(error["code"], error["message"])
            dataset = await asyncio.to_thread(
                self.journals.load, identifier, force=True, build=False, target=target
            )
            if dataset["complete"] or error:
                return dataset
        return dataset

    async def _model(
        self, identifier: str, run_id: str | None = None
    ) -> tuple[dict, dict]:
        async with self._model_lock:
            error = self._cache_errors.get(identifier)
            if error and error["code"] != "cache_busy":
                raise SystemAPIError(error["code"], error["message"])
            dataset = await asyncio.to_thread(
                self.journals.load, identifier, build=self._cache_pool is None
            )
            if dataset.get("gap"):
                self._cache_checked.pop(identifier, None)
            if self._cache_pool is not None and not dataset["complete"]:
                dataset = await self._cache_read_boundary(identifier, dataset)
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
            project = (
                cached_experiment_views if dataset.get("cache") else experiment_views
            )
            if dataset.get("cache"):
                model = await asyncio.to_thread(
                    self.journals.read_view, dataset, project, live, run_id
                )
            else:
                model = await asyncio.to_thread(project, dataset, live, run_id)
            self._models = {
                cached_key: value
                for cached_key, value in self._models.items()
                if cached_key[0] != identifier or value[1] is dataset
            }
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
        if resource == "modules":
            result = await asyncio.to_thread(self.journals.modules)
            unavailable = any(
                identifier in result.get("sources", {})
                for identifier in self._cache_errors
            )
            if unavailable or self._module_error:
                result = {
                    **result,
                    "complete": False,
                    "error": "Some experiment caches are unavailable; showing the last published statistics.",
                    "items": [{**item, "complete": False} for item in result["items"]],
                }
            return result
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

    def error_count(self, dataset: dict, seconds: float, now: float) -> int:
        cache = dataset.get("cache")
        if cache is None:
            return 0
        since = datetime.fromtimestamp(now - seconds, UTC).isoformat()
        until = datetime.fromtimestamp(now, UTC).isoformat()
        return cache.query(
            "SELECT COUNT(*) FROM facts WHERE kind='error.recorded' AND effective=1 AND occurred_at>=? AND occurred_at<=?",
            (since, until),
        )[0][0]

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
            "cached_through": dataset.get("cached_through"),
            "window_start_cursor": dataset.get("window_start_cursor"),
            "cache_gap": dataset.get("gap"),
            "target_boundary": dataset.get("target_boundary"),
        }
        if view == "summary":
            return {**metadata, **model["summary"]}
        if dataset.get("cache"):
            return await asyncio.to_thread(
                self.journals.read_view,
                dataset,
                self._cached_view,
                model,
                metadata,
                view,
                params,
            )
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

    def _cached_view(
        self, dataset: dict, model: dict, metadata: dict, view: str, params: dict
    ) -> dict:
        if view == "detail":
            return {
                **metadata,
                "record": self.journals.detail(
                    dataset, json.loads(params.get("ref", "{}"))
                ),
            }
        if view == "template":
            return {**metadata, **self._cached_template(dataset, model, params)}
        if view == "forecast":
            forecast = dict(model["forecast"])
            forecast.update(cached_metrics(dataset, model))
            return {**metadata, **forecast}
        if view == "snapshots":
            return self.page(
                self.journals.snapshots(dataset["experiment_id"]),
                metadata,
                view,
                params,
            )
        if view == "runs":
            page = self._cached_page(dataset, metadata, view, params)
            for row in page["items"]:
                if row["run_id"] == model["summary"]["run_id"]:
                    row["status"] = model["summary"]["status"]
            return {**metadata, **page}
        if view not in {
            "events",
            "operations",
            "errors",
            "parameters",
            "measurements",
            "commands",
            "artifacts",
        }:
            raise SystemAPIError("not_found", "Unknown experiment view.", 404)
        if view == "measurements":
            params = {
                **params,
                "run_id": model["summary"]["run_id"],
                "revision": model["summary"]["template_revision_id"],
            }
        page = self._cached_page(dataset, metadata, view, params)
        if view == "measurements":
            page.update(cached_metrics(dataset, model))
        if view == "operations":
            run_ids = list(
                dict.fromkeys(
                    row.get("run_id") for row in page["items"] if row.get("run_id")
                )
            )
            runs = self._cached_runs(dataset, model, run_ids)
            parents = [
                {
                    **run,
                    "operation_id": "run:" + run["run_id"],
                    "parent_operation_id": None,
                    "name": "Logical run " + run["run_id"],
                }
                for run in runs
            ]
            page["items"] = parents + page["items"]
            page["total"] += len(parents)
        if view == "commands":
            page["items"] += [
                row
                for row in self._commands
                if row.get("experiment_id") == dataset["experiment_id"]
            ]
        return {**metadata, **page}

    def _cached_page(
        self, dataset: dict, metadata: dict, view: str, params: dict
    ) -> dict:
        now = time.monotonic()
        self._publications = {
            key: value
            for key, value in self._publications.items()
            if now - value["at"] < 300
        }
        scope = (
            dataset["experiment_id"],
            params.get("run_id"),
            view,
            params.get("view", "effective"),
            params.get("revision"),
        )
        cursor = params.get("cursor")
        if cursor:
            try:
                cursor = json.loads(cursor)
                token = cursor["publication"]
                publication = self._publications[token]
                if (
                    publication["scope"] != scope
                    or publication["version"] != dataset["version"]
                    or publication["metadata"]["journal"] != dataset["identity"]
                ):
                    raise ValueError("History publication changed.")
                internal = {**publication["cursor"], "position": cursor["position"]}
                params = {**params, "cursor": json.dumps(internal)}
            except (ValueError, TypeError, KeyError) as error:
                raise SystemAPIError(
                    "history_changed", "Refresh this history publication.", 409
                ) from error
        else:
            token = uuid4().hex
            while len(self._publications) >= 16:
                self._publications.pop(next(iter(self._publications)))
            publication = {
                "at": now,
                "scope": scope,
                "metadata": metadata,
                "version": dataset["version"],
                "bytes": 0,
            }
        page = self.journals.page(dataset, view, params)
        if page["next_cursor"]:
            publication["cursor"] = page["next_cursor"]
            self._publications[token] = publication
            page["next_cursor"] = {
                "publication": token,
                "position": page["next_cursor"]["position"],
            }
        return page

    def _cached_runs(
        self, dataset: dict, model: dict, run_ids: list[str]
    ) -> list[dict]:
        if not run_ids:
            return []
        placeholders = ",".join("?" for _ in run_ids)
        rows = dataset["cache"].query(
            f"SELECT run_id, started_at, revision FROM runs WHERE run_id IN ({placeholders}) ORDER BY first_cursor",
            tuple(run_ids),
        )
        summary = model["summary"]
        runs = []
        for run, started, revision in rows:
            finishes = dataset["cache"].query(
                "SELECT MAX(json_extract(payload,'$.finished_at')), MIN(json_extract(payload,'$.finished_at') IS NOT NULL) FROM records WHERE kind='operations' AND run_id=?",
                (run,),
            )[0]
            finished = (
                finishes[0]
                if finishes[1]
                and summary["status"] in {"completed", "stopped", "failed"}
                else None
            )
            runs.append(
                {
                    "run_id": run,
                    "started_at": started,
                    "finished_at": finished,
                    "template_revision_id": revision,
                    "status": summary["status"]
                    if run == summary["run_id"]
                    else "recorded",
                }
            )
        return runs

    def _cached_template(self, dataset: dict, model: dict, params: dict) -> dict:
        selection = " AND run_id=?" if params.get("run_id") else ""
        args = (params["run_id"],) if params.get("run_id") else ()
        rows = dataset["cache"].query(
            "SELECT event_id, compact FROM facts WHERE kind='template.applied' AND effective=1"
            + selection
            + " ORDER BY cursor",
            args,
        )
        revisions = [
            json.loads(encoded)["data"]
            | {"event_id": event_id, "occurred_at": json.loads(encoded)["occurred_at"]}
            for event_id, encoded in rows
        ]
        selected = next(
            (
                row
                for row in reversed(revisions)
                if not params.get("revision")
                or row["template_revision_id"] == params["revision"]
            ),
            None,
        )
        if params.get("revision") and selected is None:
            raise SystemAPIError(
                "not_found", "The requested template revision is unavailable.", 404
            )
        template = dict(model["template"])
        if selected:
            source = dataset["cache"].events([selected["event_id"]])[0]
            template.update(
                template=source["data"]["template"],
                template_yaml=source["data"]["template_yaml"],
            )
        else:
            template.update(
                template=dataset["state"].get("template", {}),
                template_yaml=dataset["state"].get("template_yaml", ""),
            )
        definitions = {
            stage["stage_id"]: stage for stage in template["template"].get("stages", [])
        }
        template["nodes"] = [
            {**node, **definitions.get(node["stage_id"], {})}
            for node in template["nodes"]
        ]
        template["revisions"] = [
            {key: row[key] for key in ("template_revision_id", "occurred_at")}
            for row in revisions
        ]
        return template

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
        if self._cache_pool is not None:
            await asyncio.to_thread(
                self._cache_pool.shutdown, wait=True, cancel_futures=True
            )
            self._cache_pool = None
            self._cache_jobs.clear()
            self._module_job = None
        if self._command_task is not None:
            self._command_task.cancel()
            await asyncio.gather(self._command_task, return_exceptions=True)
        async with self._command_lock:
            pass
        await asyncio.to_thread(self.journals.close)
