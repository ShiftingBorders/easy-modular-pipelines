"""Measure resources and write optional telemetry inside an isolated process."""

from __future__ import annotations

import multiprocessing
import os
import time
from dataclasses import replace
from datetime import UTC, datetime
from multiprocessing.connection import Connection
from pathlib import Path
from uuid import uuid4

import psutil

from core.journal.logger import OperationLogger
from core.journal.settings import _load_logging_settings
from core.models.journal_records import JournalContext
from core.models.process_identity import ProcessIdentity
from core.models.resource_messages import (
    CollectorCommand,
    CollectorRestart,
    CollectorSnapshot,
    CollectorStop,
    CollectorUpdate,
    ResourceSample,
)
from core.models.resource_target import ResourceTargetDocument
from core.models.updates import _update_model
from core.primitives.json_files import write_json
from core.primitives.json_values import JsonObject
from core.primitives.processes import process_identity
from core.resources.observations import MeasuredResource, ResourceObservation
from core.resources.state import CollectorSettings, ResourceTarget


class ResourceSampler:
    """Keep CPU baselines per process instance, independently of journal writes."""

    def __init__(
        self, collector_id: str, settings: CollectorSettings | None = None
    ) -> None:
        """Capture collector/host identity and initialize optional hardware sampling.

        Args:
            collector_id: Identity attached to produced measurements.
            settings: Collector settings enabling disk/network sampling, or None.
        """
        from core.resources.hardware import HardwareSampler

        self._hardware = None if settings is None else HardwareSampler(settings)
        self.collector_id = collector_id
        self.host = process_identity(os.getpid())
        self._host_previous: float | None = None
        self._process_previous: dict[
            str, tuple[ProcessIdentity | JsonObject, float, float]
        ] = {}

    def reset(self) -> None:
        """Clear host and process CPU baselines before collecting a new interval."""
        self._host_previous = None
        self._process_previous.clear()

    def sample(
        self,
        context: JournalContext | JsonObject,
        targets: list[ResourceTarget | ResourceTargetDocument],
    ) -> list[JsonObject]:
        """Collect host and target-process measurements as JSON-ready sample documents.

        Args:
            context: Journal context for the host sample.
            targets: Processes with expected identities and their own contexts.

        Returns:
            Host sample followed by one sample per target; unavailable values carry
            reasons rather than fabricated zeros.
        """
        return [
            sample.document() for sample in self._sample_observations(context, targets)
        ]

    def _sample_observations(
        self,
        context: JournalContext | JsonObject,
        targets: list[ResourceTarget | ResourceTargetDocument],
    ) -> list[ResourceObservation]:
        """Drop obsolete process baselines and return host and target observations.

        Args:
            context: Validated or JSON journal context attached to the host
                observation.
            targets: Process series and expected OS identities to observe after the
                host sample.

        Returns:
            Host observation followed by each target's process observation.
            Baselines for no-longer-selected series are removed first.
        """
        active = {target.series_id for target in targets}
        self._process_previous = {
            key: value for key, value in self._process_previous.items() if key in active
        }
        samples = [self._host_observation(context)]
        samples.extend(self._process_observation(target) for target in targets)
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
    ) -> MeasuredResource:
        """Build a gauge with availability, interval, collector, and process metadata.

        Args:
            value: Measured value, or None when unavailable.
            unit: Measurement unit.
            scope: Host or process scope.
            observed_at: Wall-clock observation timestamp.
            reason: Optional explanation for unavailability.
            interval: Sampling interval in seconds, when applicable.
            identity: Observed process identity, when applicable.

        Returns:
            Resource record with provenance attributes.
        """
        return MeasuredResource(
            value=value,
            unit=unit,
            scope=scope,
            attributes={
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
        )

    def _host_sample(self, context: JournalContext | JsonObject) -> JsonObject:
        """Return a serialized host observation, advancing its CPU/network baselines."""
        return self._host_observation(context).document()

    def _host_observation(
        self, context: JournalContext | JsonObject
    ) -> ResourceObservation:
        """Read host CPU, memory, and optional hardware gauges with availability metadata.

        Args:
            context: Validated or JSON journal context attached to the host
                observation.

        Returns:
            Timestamped host CPU/memory and optional hardware gauges. The first CPU
            interval and unavailable OS observations use None with explicit reasons.
        """
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
        if self._hardware is not None:
            for name, sample in self._hardware.sample().items():
                measured = self._measurement(
                    sample["value"],
                    sample["unit"],
                    "host",
                    observed_at,
                    reason=sample.get("reason"),
                )
                attributes = {**measured.attributes, **sample.get("attributes", {})}
                attributes["source"] = sample.get("attributes", {}).get(
                    "provider", "psutil"
                )
                resources[name] = replace(measured, attributes=attributes)
        return ResourceObservation(
            series_id=f"host:{self.host['host_id']}:{self.host['boot_id']}",
            context=context,
            observed_at=observed_at,
            observed_monotonic=now,
            resources=resources,
        )

    def _process_sample(
        self, target: ResourceTarget | ResourceTargetDocument
    ) -> JsonObject:
        """Return a serialized observation for the expected process identity."""
        return self._process_observation(target).document()

    def _process_observation(
        self, target: ResourceTarget | ResourceTargetDocument
    ) -> ResourceObservation:
        """Collect identity-checked process CPU and RSS gauges with observation times.

        Args:
            target: Monitored process series, expected OS identity, and per-process
                journal context.

        Returns:
            Timestamped identity-checked CPU-percent and RSS-byte measurements for
            the target series.
        """
        observed_at = datetime.now(UTC).isoformat()
        now = time.monotonic()
        cpu, memory, reason, cpu_reason, interval = self._observe_process(target, now)
        return ResourceObservation(
            series_id=target.series_id,
            context=target.context,
            observed_at=observed_at,
            observed_monotonic=now,
            resources={
                "process_cpu_percent": self._measurement(
                    cpu,
                    "percent",
                    "process",
                    observed_at,
                    reason=reason or cpu_reason,
                    interval=interval,
                    identity=target.identity.model_dump()
                    if isinstance(target, ResourceTargetDocument)
                    else target.identity,
                ),
                "process_memory_rss_bytes": self._measurement(
                    memory,
                    "byte",
                    "process",
                    observed_at,
                    reason=reason,
                    identity=target.identity.model_dump()
                    if isinstance(target, ResourceTargetDocument)
                    else target.identity,
                ),
            },
        )

    def _observe_process(
        self, target: ResourceTarget | ResourceTargetDocument, now: float
    ) -> tuple[float | None, int | None, str | None, str | None, float | None]:
        """Read process counters only when ownership matches before and after sampling.

        Args:
            target: Expected process identity and series ID.
            now: Monotonic sampling instant in seconds.

        Returns:
            CPU percent, RSS bytes, general unavailability reason, CPU-specific
            reason, and interval seconds. Invalid observations discard the baseline.
        """
        cpu = memory = None
        reason = cpu_reason = None
        interval = None
        try:
            identity = target.identity
            if isinstance(identity, ProcessIdentity):
                pid, host_id, boot_id = identity.pid, identity.host_id, identity.boot_id
                # Compare the actual OS record with the existing JSON identity contract.
                observed_identity = identity.model_dump()
            else:
                pid, host_id, boot_id = (
                    identity["pid"],
                    identity["host_id"],
                    identity["boot_id"],
                )
                observed_identity = identity
            if host_id != self.host["host_id"] or boot_id != self.host["boot_id"]:
                reason = "different_host_or_boot"
            elif process_identity(pid) != observed_identity:
                reason = "identity_changed"
            else:
                process = psutil.Process(pid)
                times = process.cpu_times()
                measured_memory = process.memory_info().rss
                # Check both sides of the read so reused PIDs cannot mix two processes.
                if process_identity(pid) != observed_identity:
                    reason = "identity_changed"
                else:
                    memory = measured_memory
                    cpu_seconds = times.user + times.system
                    cpu, cpu_reason, interval = self._process_cpu_usage(
                        target.series_id, identity, observed_identity, now, cpu_seconds
                    )
        except (psutil.NoSuchProcess, ProcessLookupError, FileNotFoundError):
            reason = "process_gone"
        except (psutil.AccessDenied, PermissionError):
            reason = "access_denied"
        except (psutil.Error, OSError) as error:
            reason = type(error).__name__
        if reason is not None:
            self._process_previous.pop(target.series_id, None)
        return cpu, memory, reason, cpu_reason, interval

    def _process_cpu_usage(
        self,
        series_id: str,
        identity: ProcessIdentity | JsonObject,
        observed_identity: JsonObject,
        now: float,
        cpu_seconds: float,
    ) -> tuple[float | None, str | None, float | None]:
        """Advance one identity's CPU baseline; interval and counters are seconds.

        Args:
            series_id: Stable resource series whose CPU baseline is updated.
            identity: Expected complete process identity used to detect PID reuse.
            observed_identity: JSON form of the expected process identity for
                comparisons with OS observations.
            now: Current monotonic observation time in seconds.
            cpu_seconds: Cumulative user plus system CPU seconds observed for the
                process.

        Returns:
            CPU percentage, optional unavailable/reset reason, and elapsed seconds.
            A new identity establishes a baseline instead of reporting fabricated
            usage.
        """
        cpu = interval = None
        reason = "first_interval"
        previous = self._process_previous.get(series_id)
        previous_identity = None if previous is None else previous[0]
        if isinstance(previous_identity, ProcessIdentity):
            previous_identity = previous_identity.model_dump()
        if previous is not None and previous_identity == observed_identity:
            interval = now - previous[1]
            elapsed_cpu = cpu_seconds - previous[2]
            if interval > 0 and elapsed_cpu >= 0:
                cpu = 100 * elapsed_cpu / interval
                reason = None
            else:
                reason = "counter_reset"
        self._process_previous[series_id] = (identity, now, cpu_seconds)
        return cpu, reason, interval


class ResourceWriter:
    """Optional journal client; uncertain writes are counted and never replayed."""

    def __init__(self, settings: CollectorSettings, collector_id: str) -> None:
        """Initialize optional journal delivery and loss accounting without opening a client.

        Args:
            settings: Validated settings used to configure this component.
            collector_id: UUID identifying this collector instance in telemetry
                records.
        """
        self._settings = settings
        self._collector_id = collector_id
        self._client: OperationLogger | None = None
        self._source: Path | None = None
        self._retry_at = 0.0
        self.error: str | None = None
        self.unconfirmed_samples = 0
        self._reported_losses = 0
        self.restart_notice: CollectorRestart | JsonObject | None = None

    def select(self, source: Path | str | None) -> None:
        """Switch the logging config path, closing the old client and resetting losses.

        Args:
            source: Absolute existing logger-config path to select, or None to
                disable journal delivery.
        """
        path = None if source is None else Path(source)
        if path != self._source:
            self.close()
            self._source = path
            self._retry_at = 0
            self.error = None
            self.unconfirmed_samples = 0
            self._reported_losses = 0

    def record(self, sample: ResourceObservation | ResourceSample | JsonObject) -> None:
        """Attempt one journal write, counting failures without replaying uncertain samples.

        Args:
            sample: Native, validated, or JSON sample containing context and resources.

        Delivery failures update error and unconfirmed_samples and defer reopening
        according to the configured retry interval.
        """
        if self._source is None:
            return
        if time.monotonic() < self._retry_at:
            self.unconfirmed_samples += 1
            return
        try:
            if self._client is None:
                self._open_selected_journal()
            sample_context = (
                sample.context
                if isinstance(sample, (ResourceObservation, ResourceSample))
                else sample["context"]
            )
            context = {
                **(
                    sample_context.root
                    if isinstance(sample_context, JournalContext)
                    else sample_context
                ),
                "source": "resource_collector",
            }
            self._record_gaps(context)
            resources = (
                {name: item.document() for name, item in sample.resources.items()}
                if isinstance(sample, ResourceObservation)
                else {
                    name: item.model_dump(exclude_unset=True)
                    for name, item in sample.resources.items()
                }
                if isinstance(sample, ResourceSample)
                else sample["resources"]
            )
            self._client.record_resources(resources, context=context)
            self.error = None
        except Exception as error:  # noqa: BLE001 - Telemetry failure must not escape into DAG policy.
            self.unconfirmed_samples += 1
            self.error = f"{type(error).__name__}: {error}"
            self.close()
            self._retry_at = time.monotonic() + self._settings.logging_retry_seconds

    def _record_gaps(self, context: JsonObject) -> None:
        """Publish pending losses once; uncertain writes keep the previous policy.

        Args:
            context: Journal/participant coordinates associated with this operation.
        """
        if self.restart_notice is not None:
            notice, self.restart_notice = self.restart_notice, None
            notice_context = (
                notice.context.root
                if isinstance(notice, CollectorRestart)
                else notice["context"]
            )
            if context.get("experiment_id") is not None and all(
                notice_context.get(key) == context.get(key)
                for key in ("experiment_id", "run_id")
            ):
                self._client.record_event(
                    "resources.gap",
                    notice.model_dump(exclude_unset=True)
                    if isinstance(notice, CollectorRestart)
                    else notice,
                    context=context,
                )
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

    def _open_selected_journal(self) -> None:
        """Write a collector-specific logger config and open the selected existing journal.

        Derives an existing-mode collector-specific configuration with the
        configured lock timeout and writes it beside the selected source config.
        Opens a new process-local OperationLogger; it does not create a missing
        experiment journal.
        """
        settings, _ = _load_logging_settings(self._source)
        settings = _update_model(
            settings,
            busy_timeout_seconds=self._settings.logging_busy_timeout_seconds,
            open_mode="existing",
        )
        path = self._source.parent / f"resource-{self._collector_id}.json"
        write_json(
            path,
            {
                "logging": settings.model_dump(mode="json"),
                "operation_context": {"source": "resource_collector"},
            },
        )
        self._client = OperationLogger(path)
        self._client.open()

    def close(self) -> None:
        """Close the owned logger client and clear its reference even if closing fails."""
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None


def collect_resources(connection: Connection, settings: CollectorSettings) -> None:
    """Child entry point: only serializable settings and an owned pipe cross spawn."""
    collector_id = str(uuid4())
    writer = ResourceWriter(settings, collector_id)
    sampler = ResourceSampler(collector_id, settings)
    revision = -1
    received_notice = False
    snapshot = CollectorSnapshot.model_validate(
        {"context": {}, "logging_config_path": None, "targets": []}
    )
    targets: list[ResourceTargetDocument] = []
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
                message = CollectorCommand.validate_python(connection.recv())
                if isinstance(message, CollectorStop):
                    return
                last_owner = time.monotonic()
                snapshot, targets, revision, received_notice, changed = (
                    _apply_collector_update(
                        message,
                        writer,
                        sampler,
                        snapshot,
                        targets,
                        revision,
                        received_notice,
                        changed,
                    )
                )
            now = time.monotonic()
            samples = []
            if now >= next_sample:
                samples = sampler._sample_observations(snapshot.context, targets)
                # A pending lifecycle change takes precedence over optional writes.
                for index, sample in enumerate(samples):
                    if connection.poll():
                        if snapshot.logging_config_path is not None:
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
                        "samples": [sample.document() for sample in samples],
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


def _apply_collector_update(
    message: CollectorUpdate,
    writer: ResourceWriter,
    sampler: ResourceSampler,
    snapshot: CollectorSnapshot,
    targets: list[ResourceTargetDocument],
    revision: int,
    received_notice: bool,
    changed: bool,
) -> tuple[CollectorSnapshot, list[ResourceTargetDocument], int, bool, bool]:
    if not received_notice and message.restart_notice is not None:
        writer.restart_notice = message.restart_notice
        received_notice = True
    if message.revision != revision:
        incoming = message.snapshot
        if incoming.context != snapshot.context or (
            None
            if incoming.logging_config_path is None
            else str(incoming.logging_config_path)
        ) != (
            None
            if snapshot.logging_config_path is None
            else str(snapshot.logging_config_path)
        ):
            sampler.reset()
        writer.select(incoming.logging_config_path)
        targets = list(incoming.targets)
        snapshot = incoming
        revision = message.revision
        changed = True
    return snapshot, targets, revision, received_notice, changed
