"""Independent bounded startup evidence; writing it never calls the journal."""

from __future__ import annotations

import os
import threading
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

from core.models.process_identity import ProcessIdentity
from core.models.startup_diagnostics import (
    StartupContext,
    StartupDestination,
    StartupDetails,
    StartupError,
    StartupRecord,
)
from core.primitives.processes import process_identity


def describe_startup_error(error: BaseException) -> StartupError:
    """Retain the exception chain, without inspecting stack locals or settings."""
    try:
        message = str(error)
    except Exception:  # noqa: BLE001 - User exceptions cannot break diagnostics.
        message = "[exception message unavailable]"
    try:
        trace = "".join(traceback.format_exception(error))
    except Exception:  # noqa: BLE001 - Retain the primary failure even if formatting fails.
        trace = f"{type(error).__name__}: {message}"
    return StartupError(
        error_type=type(error).__name__,
        message=message,
        traceback=trace,
    )


class StartupDiagnostics:
    """Write JSON lines, retaining at most two bounded files per destination.

    Construction performs no I/O. A full file becomes .previous; the new file
    starts with an explicit loss/rotation record. No database connection is used.
    Owners sharing a destination share this instance and its thread lock.
    """

    def __init__(self, path: Path, context: dict, *, max_bytes: int = 262144) -> None:
        self._destination = StartupDestination(path=path, max_bytes=max_bytes)
        self._context = StartupContext.model_validate(context)
        self._lock = threading.Lock()
        self._identity: ProcessIdentity | None = None
        self._identity_read = False
        self.last_phase: str | None = None

    @property
    def path(self) -> Path:
        return self._destination.path

    def _encode(
        self, phase: str, details: StartupDetails, *, minimal: bool = False
    ) -> bytes:
        context = self._context
        if minimal:
            context = StartupContext.model_validate(
                {
                    name: value
                    for name in ("experiment_id", "run_id", "attempt_id", "request_id")
                    if (value := getattr(self._context, name)) is not None
                    and len(value) <= 128
                }
            )
        record = StartupRecord(
            phase=phase,
            occurred_at=datetime.now(UTC).isoformat(),
            monotonic=time.monotonic(),
            process_id=os.getpid(),
            process=None if minimal else self._identity,
            context=context,
            details=details,
        )
        return (record.model_dump_json(exclude_none=True) + "\n").encode("utf-8")

    def record(self, phase: str, **details: object) -> bool:
        """Best-effort append; diagnostic failures never replace execution errors."""
        self.last_phase = phase
        try:
            if not self._identity_read:
                self._identity_read = True
                try:
                    self._identity = ProcessIdentity.model_validate(
                        process_identity(os.getpid())
                    )
                except (OSError, ValueError):
                    pass
            encoded = self._encode(phase, StartupDetails.model_validate(details))
            with self._lock:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                size = self.path.stat().st_size if self.path.exists() else 0
                limit = self._destination.max_bytes
                if size + len(encoded) > limit:
                    previous = self.path.with_suffix(self.path.suffix + ".previous")
                    lost = previous.stat().st_size if previous.exists() else 0
                    if self.path.exists():
                        os.replace(self.path, previous)
                    marker = self._encode(
                        "diagnostics.truncated",
                        StartupDetails(
                            dropped_bytes=lost
                            + (len(encoded) if len(encoded) > limit // 2 else 0),
                            limit_bytes=limit,
                        ),
                        minimal=True,
                    )
                    with self.path.open("ab") as output:
                        output.write(marker)
                    size = len(marker)
                if size + len(encoded) > limit:
                    return False
                with self.path.open("ab") as output:
                    output.write(encoded)
            return True
        except (OSError, ValueError, TypeError):
            return False


def capture_bootstrap_stream(
    stream: BinaryIO,
    destination: Path,
    name: str,
    diagnostics: StartupDiagnostics,
    *,
    max_bytes: int = 1048576,
) -> None:
    """Drain a pipe even after output truncation or diagnostic filesystem failure."""
    output = None
    retained = dropped = 0
    try:
        try:
            output = destination.open("wb")
        except OSError:
            diagnostics.record("output.unavailable", stream=name)
        while chunk := stream.read1(65536):
            room = max(0, max_bytes - retained)
            if output is not None and room:
                try:
                    output.write(chunk[:room])
                    output.flush()
                    retained += min(room, len(chunk))
                except OSError:
                    try:
                        output.close()
                    except OSError:
                        pass
                    output = None
                    diagnostics.record("output.unavailable", stream=name)
            lost = (
                len(chunk) - min(room, len(chunk)) if output is not None else len(chunk)
            )
            if lost and not dropped:
                diagnostics.record(
                    "output.truncated",
                    stream=name,
                    dropped_bytes=lost,
                    limit_bytes=max_bytes,
                )
            dropped += lost
    except OSError:
        diagnostics.record("output.unavailable", stream=name)
    finally:
        try:
            stream.close()
        except OSError:
            pass
        if output is not None:
            try:
                output.close()
            except OSError:
                pass
        if dropped:
            diagnostics.record(
                "output.finished",
                stream=name,
                dropped_bytes=dropped,
                limit_bytes=max_bytes,
            )
