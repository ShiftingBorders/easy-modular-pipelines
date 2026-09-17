"""Small fixtures for HTTP responses, probe output and isolated runtime files."""

import asyncio
import json
import os
import tempfile
import time
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = PROJECT_ROOT / "dashboard"
FIXTURES = Path(__file__).with_name("fixtures")


def temporary_directory() -> tempfile.TemporaryDirectory:
    parent = PROJECT_ROOT / ".artifacts" / "tmp" / "dashboard" / "test-runs"
    parent.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(dir=parent)


def cleanup_directory(directory: tempfile.TemporaryDirectory) -> None:
    """Allow Windows to finish pending directory deletion, never hide a locked file."""
    root = Path(directory.name).resolve()
    parent = (PROJECT_ROOT / ".artifacts/tmp/dashboard/test-runs").resolve()
    if root.parent != parent:
        raise ValueError("Dashboard test cleanup escaped its scratch directory.")
    for attempt in range(5):
        try:
            directory.cleanup()
            return
        except OSError as error:
            if (
                os.name != "nt"
                or getattr(error, "winerror", None) != 145
                or attempt == 4
            ):
                raise
            failed = Path(error.filename).resolve()
            if not failed.is_relative_to(root):
                raise
            time.sleep(0.05)
            try:
                has_entries = any(failed.iterdir())
            except FileNotFoundError:
                has_entries = False
            if has_entries:
                raise


def settings_document(**changes) -> dict:
    document = json.loads((DASHBOARD / "settings.json").read_text(encoding="utf-8"))
    document.update(changes)
    return document


def write_settings(directory: Path, **changes) -> Path:
    path = directory / "settings.json"
    path.write_text(json.dumps(settings_document(**changes)), encoding="utf-8")
    return path


def icmp_settings(**changes) -> dict:
    document = {
        "enabled": True,
        "host": "www.google.com",
        "timeout_seconds": 0.1,
        "interval_seconds": 1,
    }
    document.update(changes)
    return document


def probe_result(status: str = "reply") -> dict:
    return {
        "status": status,
        "reason": None if status == "reply" else "timeout",
        "rtt_ms": 12.5 if status == "reply" else None,
        "address": "192.0.2.20",
    }


class ProbeProcess:
    """A controllable process boundary; it never sends network packets."""

    def __init__(
        self, result: object = None, *, exit_code: int = 0, blocked: bool = False
    ) -> None:
        self.output = (
            result
            if isinstance(result, bytes)
            else json.dumps(result if result is not None else probe_result()).encode()
        )
        self.exit_code = exit_code
        self.returncode = None
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        if not blocked:
            self.release.set()
        self.killed = False
        self.waited = False

    async def communicate(self) -> tuple[bytes, bytes]:
        self.started.set()
        await self.release.wait()
        if self.returncode is None:
            self.returncode = self.exit_code
        return self.output, b""

    def kill(self) -> None:
        self.killed = True
        self.returncode = -1
        self.release.set()

    async def wait(self) -> int:
        self.waited = True
        await self.release.wait()
        return self.returncode


class ResponseStream(httpx.AsyncByteStream):
    def __init__(
        self, chunks: list[bytes], *, delay: float = 0, failure: Exception | None = None
    ) -> None:
        self.chunks = chunks
        self.delay = delay
        self.failure = failure
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            if self.delay:
                await asyncio.sleep(self.delay)
            yield chunk
        if self.failure:
            raise self.failure

    async def aclose(self) -> None:
        self.closed = True
