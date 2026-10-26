"""Validated collector settings, monitoring targets, and bounded RAM history."""

from __future__ import annotations

import json
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from core.models.resource_messages import ResourceSample
from core.models.resource_settings import CollectorConfiguration
from core.models.resource_target import ResourceTargetDocument
from core.primitives.json_files import read_json
from core.primitives.json_values import (
    JsonObject,
)


@dataclass(frozen=True)
class CollectorSettings:
    """Runtime collector options with a configuration-relative disk path resolved."""
    sample_interval_seconds: float
    history_seconds: float
    max_buffer_bytes: int
    stale_after_intervals: int
    status_interval_seconds: float
    startup_timeout_seconds: float
    heartbeat_timeout_seconds: float
    shutdown_timeout_seconds: float
    restart_delays_seconds: list[float]
    stable_reset_seconds: float
    logging_busy_timeout_seconds: float
    logging_retry_seconds: float
    disk_path: str | None = None
    network_interface: str | None = None
    network_reference_address: str = "1.1.1.1"
    gpu_interval_seconds: float = 5  # Legacy no-op field for settings compatibility.

    @classmethod
    def load(cls, path: Path) -> CollectorSettings:
        """Read and validate collector settings and resolve the monitored disk path.

        Args:
            path: Absolute configuration file path.

        Returns:
            Runtime settings; relative disk paths are anchored to the config file.

        Raises:
            ValueError: The configuration path or settings are invalid.
            OSError: The configuration cannot be read.
        """
        if not path.is_absolute():
            raise ValueError("Collector settings path must be absolute.")
        configuration = CollectorConfiguration.model_validate(read_json(path))
        configured = Path(configuration.disk_path)
        disk_path = str(
            configured
            if configured.is_absolute()
            else (path.parent / configured).resolve()
        )
        # Preserve the public dataclass/asdict contract used by collector IPC clients.
        return cls(
            sample_interval_seconds=configuration.sample_interval_seconds,
            history_seconds=configuration.history_seconds,
            max_buffer_bytes=configuration.max_buffer_bytes,
            stale_after_intervals=configuration.stale_after_intervals,
            status_interval_seconds=configuration.status_interval_seconds,
            startup_timeout_seconds=configuration.startup_timeout_seconds,
            heartbeat_timeout_seconds=configuration.heartbeat_timeout_seconds,
            shutdown_timeout_seconds=configuration.shutdown_timeout_seconds,
            restart_delays_seconds=list(configuration.restart_delays_seconds),
            stable_reset_seconds=configuration.stable_reset_seconds,
            logging_busy_timeout_seconds=configuration.logging_busy_timeout_seconds,
            logging_retry_seconds=configuration.logging_retry_seconds,
            disk_path=disk_path,
            network_interface=configuration.network_interface,
            network_reference_address=configuration.network_reference_address,
            gpu_interval_seconds=configuration.gpu_interval_seconds,
        )


@dataclass(frozen=True)
class ResourceTarget:
    """Internal monitored process identity, series ID, and journal context."""
    series_id: str
    identity: JsonObject
    context: JsonObject

    @classmethod
    def from_document(cls, document: JsonObject) -> ResourceTarget:
        """Validate target JSON and return an internal record with detached dictionaries.

        Args:
            document: Input JSON document to validate or publish.

        Returns:
            Internal target record containing validated series identity and detached
            process/context dictionaries.
        """
        target = ResourceTargetDocument.model_validate(document)
        return cls(
            target.series_id, target.identity.model_dump(), target.context.model_dump()
        )


class ResourceHistory:
    """Store encoded samples to keep Python object overhead out of the byte budget."""

    def __init__(self, settings: CollectorSettings) -> None:
        """Initialize an empty history with configured time/byte limits and a fresh ID."""
        self._settings = settings
        self._entries: deque[tuple[float, int, bytes, int]] = deque()
        self._bytes = 0
        self._sequence = 0
        self.evicted = 0
        self.history_id = str(uuid4())

    def append(self, sample: JsonObject) -> None:
        """Encode a sample with a new cursor and enforce time and memory retention limits.

        Args:
            sample: One resource sample with context, timestamps, and metric values.
        """
        self._sequence += 1
        encoded = json.dumps(
            {**sample, "cursor": self._sequence}, allow_nan=False, ensure_ascii=False
        ).encode("utf-8")
        now = time.monotonic()
        # A record that cannot fit a page must not defeat the read API's byte cap.
        if len(encoded) > min(self._settings.max_buffer_bytes, 1048576):
            self.evicted += 1
            self._prune(now)
            return
        # Include entry/scalar/deque overhead conservatively, not just JSON length.
        size = sys.getsizeof(encoded) + 256
        self._entries.append((now, self._sequence, encoded, size))
        self._bytes += size
        self._prune(now)

    def _append_sample(self, sample: ResourceSample) -> None:
        """Encode a validated current sample using the existing byte-budget layout."""
        self.append(sample.model_dump(exclude_unset=True))

    def _prune(self, now: float) -> None:
        """Evict oldest samples exceeding retention seconds or the configured byte budget."""
        while self._entries and (
            self._entries[0][0] < now - self._settings.history_seconds
            or self._bytes > self._settings.max_buffer_bytes
        ):
            self._bytes -= self._entries.popleft()[3]
            self.evicted += 1

    def read(self, *, after: int = 0, limit: int = 100) -> JsonObject:
        """Read retained samples after a cursor with bounded count and encoded size.

        Args:
            after: Nonnegative exclusive sample cursor.
            limit: Maximum sample count, from 1 to 1000.

        Returns:
            Samples, continuation cursor, history ID, and explicit gap/eviction metadata.

        Raises:
            ValueError: Cursor or page limit violates its integer bounds.
        """
        if type(after) is not int or after < 0:
            raise ValueError("after must be a nonnegative integer.")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000.")
        self._prune(time.monotonic())
        samples = []
        page_bytes = 0
        for _, cursor, encoded, _ in self._entries:
            if cursor <= after:
                continue
            if samples and page_bytes + len(encoded) > min(
                self._settings.max_buffer_bytes, 1048576
            ):
                break
            samples.append(json.loads(encoded))
            page_bytes += len(encoded)
            if len(samples) == limit:
                break
        oldest = self._entries[0][1] if self._entries else self._sequence + 1
        gap = after < oldest - 1 or after > self._sequence
        previous = after
        for sample in samples:
            gap = gap or sample["cursor"] != previous + 1
            previous = sample["cursor"]
        if not samples and self._sequence > after:
            gap = True
        return {
            "samples": samples,
            "cursor": samples[-1]["cursor"] if samples else self._sequence,
            "history_id": self.history_id,
            "gap": gap,
            "oldest_cursor": oldest,
            "evicted_samples": self.evicted,
            "buffer_bytes": self._bytes,
        }
