"""Measure resources and write optional telemetry inside an isolated process."""

from __future__ import annotations

import multiprocessing
import os
import time
from datetime import UTC, datetime
from multiprocessing.connection import Connection
from pathlib import Path
from uuid import uuid4

import psutil

from core.logger import OperationLogger
from core.logger_utils.events import JsonObject, load_logging_settings
from core.resource_utils.state import CollectorSettings, ResourceTarget
from core.runner_utils.runtimeio import process_identity, write_json


class ResourceSampler:
    """Keep CPU baselines per process instance, independently of journal writes."""

    def __init__(self, collector_id: str) -> None:
        self.collector_id = collector_id
        self.host = process_identity(os.getpid())
        self._host_previous: float | None = None
        self._process_previous: dict[str, tuple[JsonObject, float, float]] = {}

    def reset(self) -> None:
        self._host_previous = None
        self._process_previous.clear()

    def sample(
        self, context: JsonObject, targets: list[ResourceTarget]
    ) -> list[JsonObject]:
        active = {target.series_id for target in targets}
        self._process_previous = {
            key: value for key, value in self._process_previous.items() if key in active
        }
        samples = [self._host_sample(context)]
        samples.extend(self._process_sample(target) for target in targets)
        return samples

    def _measurement(
        self,
        value: float | None,
        unit: str,
        scope: str,
        observed_at: str,
        *,
        reason: str | None = None,
        interval: float | None = None,
        identity: JsonObject | None = None,
    ) -> JsonObject:
        return {
            "value": value,
            "unit": unit,
            "kind": "gauge",
            "scope": scope,
            "estimated": False,
            "attributes": {
                "observed_at": observed_at,
                "interval_seconds": interval,
                "available": value is not None,
                "reason": reason,
                "source": "psutil",
                "collector_id": self.collector_id,
                "host_id": self.host["host_id"],
                "boot_id": self.host["boot_id"],
                "observed_process": identity,
            },
        }

    def _host_sample(self, context: JsonObject) -> JsonObject:
        observed_at = datetime.now(UTC).isoformat()
        now = time.monotonic()
        interval = None if self._host_previous is None else now - self._host_previous
        cpu = None
        cpu_reason = None
        try:
            measured = psutil.cpu_percent(interval=None)
            cpu = measured if interval is not None else None
            cpu_reason = "first_interval" if interval is None else None
            self._host_previous = now
        except (psutil.Error, OSError) as error:
            self._host_previous = None
            cpu_reason = type(error).__name__
        resources = {
            "host_cpu_percent": self._measurement(
                cpu,
                "percent",
                "host",
                observed_at,
                reason=cpu_reason,
                interval=interval,
            )
        }
        memory = {"total": None, "available": None, "used": None, "percent": None}
        memory_reason = None
        try:
            actual = psutil.virtual_memory()
            memory = {
                "total": actual.total,
                "available": actual.available,
                "used": actual.total - actual.available,
                "percent": actual.percent,
            }
        except (psutil.Error, OSError) as error:
            memory_reason = type(error).__name__
        for name, value in memory.items():
            suffix = "percent" if name == "percent" else f"{name}_bytes"
            resources[f"host_memory_{suffix}"] = self._measurement(
                value,
                "percent" if name == "percent" else "byte",
                "host",
                observed_at,
                reason=memory_reason,
            )
        return {
            "series_id": f"host:{self.host['host_id']}:{self.host['boot_id']}",
            "context": context,
            "observed_at": observed_at,
            "observed_monotonic": now,
            "resources": resources,
        }

    def _process_sample(self, target: ResourceTarget) -> JsonObject:
        observed_at = datetime.now(UTC).isoformat()
        now = time.monotonic()
        cpu = memory = None
        reason = cpu_reason = None
        interval = None
        try:
            identity = target.identity
            if any(identity[key] != self.host[key] for key in ("host_id", "boot_id")):
                reason = "different_host_or_boot"
            elif process_identity(identity["pid"]) != identity:
                reason = "identity_changed"
            else:
                process = psutil.Process(identity["pid"])
                times = process.cpu_times()
                measured_memory = process.memory_info().rss
                # Check both sides of the read so reused PIDs cannot mix two processes.
                if process_identity(identity["pid"]) != identity:
                    reason = "identity_changed"
                else:
                    memory = measured_memory
                    cpu_seconds = times.user + times.system
                    previous = self._process_previous.get(target.series_id)
                    if previous is not None and previous[0] == identity:
                        interval = now - previous[1]
                        elapsed_cpu = cpu_seconds - previous[2]
                        if interval > 0 and elapsed_cpu >= 0:
                            cpu = 100 * elapsed_cpu / interval
                        else:
                            cpu_reason = "counter_reset"
                    else:
                        cpu_reason = "first_interval"
                    self._process_previous[target.series_id] = (
                        identity,
                        now,
                        cpu_seconds,
                    )
        except (psutil.NoSuchProcess, ProcessLookupError, FileNotFoundError):
            reason = "process_gone"
        except (psutil.AccessDenied, PermissionError):
            reason = "access_denied"
        except (psutil.Error, OSError) as error:
            reason = type(error).__name__
        if reason is not None:
            self._process_previous.pop(target.series_id, None)
        return {
            "series_id": target.series_id,
            "context": target.context,
            "observed_at": observed_at,
            "observed_monotonic": now,
            "resources": {
                "process_cpu_percent": self._measurement(
                    cpu,
                    "percent",
                    "process",
                    observed_at,
                    reason=reason or cpu_reason,
                    interval=interval,
                    identity=target.identity,
                ),
                "process_memory_rss_bytes": self._measurement(
                    memory,
                    "byte",
                    "process",
                    observed_at,
                    reason=reason,
                    identity=target.identity,
                ),
            },
        }


class ResourceWriter:
    """Optional journal client; uncertain writes are counted and never replayed."""

    def __init__(self, settings: CollectorSettings, collector_id: str) -> None:
        self._settings = settings
        self._collector_id = collector_id
        self._client: OperationLogger | None = None
        self._source: Path | None = None
        self._retry_at = 0.0
        self.error: str | None = None
        self.unconfirmed_samples = 0
        self._reported_losses = 0
        self.restart_notice: JsonObject | None = None

    def select(self, source: str | None) -> None:
        path = None if source is None else Path(source)
        if path != self._source:
            self.close()
            self._source = path
            self._retry_at = 0
            self.error = None
            self.unconfirmed_samples = 0
            self._reported_losses = 0

    def record(self, sample: JsonObject) -> None:
        if self._source is None:
            return
        if time.monotonic() < self._retry_at:
            self.unconfirmed_samples += 1
            return
        try:
            if self._client is None:
                settings, _ = load_logging_settings(self._source)
                settings["db_path"] = str(settings["db_path"])
                settings["busy_timeout_seconds"] = (
                    self._settings.logging_busy_timeout_seconds
                )
                settings["open_mode"] = "existing"
                path = self._source.parent / f"resource-{self._collector_id}.json"
                write_json(
                    path,
                    {
                        "logging": settings,
                        "operation_context": {"source": "resource_collector"},
                    },
                )
                self._client = OperationLogger(path)
                self._client.open()
            context = {**sample["context"], "source": "resource_collector"}
            if self.restart_notice is not None:
                notice, self.restart_notice = self.restart_notice, None
                if context.get("experiment_id") is not None and all(
                    notice["context"].get(key) == context.get(key)
                    for key in ("experiment_id", "run_id")
                ):
                    self._client.record_event("resources.gap", notice, context=context)
            if self.unconfirmed_samples > self._reported_losses:
                self._client.record_event(
                    "resources.gap",
                    {
                        "collector_id": self._collector_id,
                        "unconfirmed_samples": self.unconfirmed_samples,
                    },
                    context=context,
                )
                self._reported_losses = self.unconfirmed_samples
            self._client.record_resources(sample["resources"], context=context)
            self.error = None
        except Exception as error:  # noqa: BLE001 - Telemetry failure must not escape into DAG policy.
            self.unconfirmed_samples += 1
            self.error = f"{type(error).__name__}: {error}"
            self.close()
            self._retry_at = time.monotonic() + self._settings.logging_retry_seconds

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None


def collect_resources(connection: Connection, settings: CollectorSettings) -> None:
    """Child entry point: only serializable settings and an owned pipe cross spawn."""
    collector_id = str(uuid4())
    writer = ResourceWriter(settings, collector_id)
    sampler = ResourceSampler(collector_id)
    revision = -1
    received_notice = False
    snapshot: JsonObject = {"context": {}, "logging_config_path": None, "targets": []}
    targets: list[ResourceTarget] = []
    last_owner = next_sample = next_status = time.monotonic()
    parent = multiprocessing.parent_process()
    try:
        while True:
            now = time.monotonic()
            if (
                parent is None
                or not parent.is_alive()
                or now - last_owner >= settings.heartbeat_timeout_seconds
            ):
                return
            timeout = max(0, min(next_sample, next_status) - now)
            changed = False
            if connection.poll(timeout):
                message = connection.recv()
                if message["command"] == "stop":
                    return
                last_owner = time.monotonic()
                if not received_notice and message.get("restart_notice") is not None:
                    writer.restart_notice = message["restart_notice"]
                    received_notice = True
                if message["revision"] != revision:
                    incoming = message["snapshot"]
                    if (
                        incoming["context"] != snapshot["context"]
                        or incoming["logging_config_path"]
                        != snapshot["logging_config_path"]
                    ):
                        sampler.reset()
                    writer.select(incoming["logging_config_path"])
                    targets = [
                        ResourceTarget.from_document(item)
                        for item in incoming["targets"]
                    ]
                    snapshot = incoming
                    revision = message["revision"]
                    changed = True
            now = time.monotonic()
            samples = []
            if now >= next_sample:
                samples = sampler.sample(snapshot["context"], targets)
                # A pending lifecycle change takes precedence over optional writes.
                for index, sample in enumerate(samples):
                    if connection.poll():
                        if snapshot["logging_config_path"] is not None:
                            writer.unconfirmed_samples += len(samples) - index
                        break
                    writer.record(sample)
                next_sample = time.monotonic() + settings.sample_interval_seconds
            if samples or changed or now >= next_status:
                connection.send(
                    {
                        "collector_id": collector_id,
                        "pid": os.getpid(),
                        "revision": revision,
                        "journal_closed": writer._client is None,
                        "journal_error": writer.error,
                        "unconfirmed_samples": writer.unconfirmed_samples,
                        "samples": samples,
                    }
                )
                next_status = time.monotonic() + settings.status_interval_seconds
    except (EOFError, BrokenPipeError, ConnectionResetError):
        return
    finally:
        try:
            writer.close()
        finally:
            connection.close()
