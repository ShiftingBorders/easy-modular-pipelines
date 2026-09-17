"""Validated collector settings, monitoring targets, and bounded RAM history."""

from __future__ import annotations

import ipaddress
import json
import sys
import time
from collections import deque
from dataclasses import dataclass, fields
from pathlib import Path
from uuid import UUID, uuid4

from core.logger_utils.events import (
    JsonObject,
    copy_json_object,
    require_number,
    require_text,
    validate_context,
)
from core.runner_utils.runtimeio import read_json


@dataclass(frozen=True)
class CollectorSettings:
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
    gpu_interval_seconds: float = 5  # Reserved; GPU/VRAM collection is unfinished.

    @classmethod
    def load(cls, path: Path) -> CollectorSettings:
        if not path.is_absolute():
            raise ValueError("Collector settings path must be absolute.")
        document = read_json(path)
        if document.keys() != {field.name for field in fields(cls)}:
            raise ValueError(
                "Collector settings require exactly the documented fields."
            )
        for name, value in document.items():
            if name == "disk_path":
                configured = Path(require_text(value, name))
                document[name] = str(
                    configured
                    if configured.is_absolute()
                    else (path.parent / configured).resolve()
                )
                continue
            if name == "network_interface":
                if value is not None:
                    require_text(value, name)
                continue
            if name == "network_reference_address":
                ipaddress.IPv4Address(require_text(value, name))
                continue
            if name == "restart_delays_seconds":
                if type(value) is not list or not value:
                    raise ValueError("restart_delays_seconds must be a nonempty array.")
                for delay in value:
                    if require_number(delay, name) <= 0:
                        raise ValueError("Restart delays must be positive.")
                if value != sorted(value):
                    raise ValueError("Restart delays must be nondecreasing.")
                continue
            if require_number(value, name) <= 0:
                raise ValueError(f"{name} must be positive.")
        for name in ("max_buffer_bytes", "stale_after_intervals"):
            if type(document[name]) is not int:
                raise TypeError(f"{name} must be an integer.")
        if document["max_buffer_bytes"] < 4096:
            raise ValueError("max_buffer_bytes must allow at least 4096 bytes.")
        if document["sample_interval_seconds"] < 0.1:
            raise ValueError("sample_interval_seconds must be at least 0.1.")
        if document["heartbeat_timeout_seconds"] <= document["status_interval_seconds"]:
            raise ValueError("Heartbeat timeout must exceed the status interval.")
        if document["logging_busy_timeout_seconds"] > 60:
            raise ValueError("logging_busy_timeout_seconds must not exceed 60.")
        return cls(**document)


@dataclass(frozen=True)
class ResourceTarget:
    series_id: str
    identity: JsonObject
    context: JsonObject

    @classmethod
    def from_document(cls, document: JsonObject) -> ResourceTarget:
        document = copy_json_object(document, "resource target")
        if document.keys() != {"series_id", "identity", "context"}:
            raise ValueError("A target requires series_id, identity, and context.")
        series_id = require_text(document["series_id"], "series_id")
        UUID(series_id)
        identity = copy_json_object(document["identity"], "process identity")
        if identity.keys() != {"pid", "created_at_os", "host_id", "boot_id"}:
            raise ValueError("A resource target requires the complete OS identity.")
        for name in ("pid", "created_at_os"):
            if type(identity[name]) is not int or identity[name] <= 0:
                raise ValueError(f"identity.{name} must be a positive integer.")
        for name in ("host_id", "boot_id"):
            require_text(identity[name], name)
        return cls(series_id, identity, validate_context(document["context"]))


class ResourceHistory:
    """Store encoded samples to keep Python object overhead out of the byte budget."""

    def __init__(self, settings: CollectorSettings) -> None:
        self._settings = settings
        self._entries: deque[tuple[float, int, bytes, int]] = deque()
        self._bytes = 0
        self._sequence = 0
        self.evicted = 0
        self.history_id = str(uuid4())

    def append(self, sample: JsonObject) -> None:
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

    def _prune(self, now: float) -> None:
        while self._entries and (
            self._entries[0][0] < now - self._settings.history_seconds
            or self._bytes > self._settings.max_buffer_bytes
        ):
            self._bytes -= self._entries.popleft()[3]
            self.evicted += 1

    def read(self, *, after: int = 0, limit: int = 100) -> JsonObject:
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
