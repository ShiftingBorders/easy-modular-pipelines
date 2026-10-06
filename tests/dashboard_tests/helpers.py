"""Small fixtures for HTTP responses, probe output and isolated runtime files."""

import asyncio
import errno
import json
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import httpx
import psutil

from core.primitives.processes import process_identity, process_running
from tests.helpers.dag import terminate_owned

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = PROJECT_ROOT / "dashboard"
FIXTURES = Path(__file__).with_name("fixtures")


def close_browser(identity: dict, profile: Path) -> None:
    """Close the isolated browser, then wait for its verified process tree."""
    owned = [identity]
    try:
        if (
            process_running(identity["pid"])
            and process_identity(identity["pid"]) == identity
        ):
            parent = psutil.Process(identity["pid"])
            children = parent.children(recursive=True)
            # Enumeration must not adopt a replacement process with a reused PID.
            if process_identity(parent.pid) != identity:
                return
            for child in children:
                try:
                    child_identity = process_identity(child.pid)
                    if child.is_running() and parent in child.parents():
                        owned.append(child_identity)
                except psutil.NoSuchProcess:
                    continue
                except OSError as error:
                    if error.errno not in (errno.ENOENT, errno.ESRCH) and getattr(
                        error, "winerror", None
                    ) not in (87, 1168):
                        raise
            try:
                port, browser_path = (
                    (profile / "DevToolsActivePort")
                    .read_text(encoding="utf-8")
                    .splitlines()
                )
                if (
                    port.isdigit()
                    and 0 < int(port) < 65536
                    and browser_path.startswith("/devtools/browser/")
                ):
                    subprocess.run(
                        [
                            shutil.which("node"),
                            str(FIXTURES / "browser_check.mjs"),
                            port,
                            "--close",
                            browser_path,
                        ],
                        capture_output=True,
                        timeout=5,
                        check=False,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
            except (OSError, ValueError, subprocess.TimeoutExpired):
                pass  # Incomplete startup still requires verified process cleanup.
    except (psutil.NoSuchProcess, FileNotFoundError):
        pass
    except OSError as error:
        if getattr(error, "winerror", None) not in (87, 1168):
            raise

    for force in (False, True):
        deadline = time.monotonic() + 5
        while owned:
            remaining = []
            for item in owned:
                try:
                    if (
                        process_running(item["pid"])
                        and process_identity(item["pid"]) == item
                    ):
                        if force:
                            terminate_owned(item)
                        remaining.append(item)
                except OSError as error:
                    if error.errno not in (errno.ENOENT, errno.ESRCH) and getattr(
                        error, "winerror", None
                    ) not in (87, 1168):
                        raise
            owned = remaining
            if not owned or time.monotonic() >= deadline:
                break
            time.sleep(0.05)
        if not owned:
            return
    raise RuntimeError(f"Test browser processes did not exit: {owned}")


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
            failed = Path(error.filename).resolve()
            if not failed.is_relative_to(root):
                raise
            # SQLite can briefly rename a released Windows SHM file during
            # deletion. Retry only this auxiliary file, not locked DBs/locks.
            transient_shm = (
                os.name == "nt"
                and getattr(error, "winerror", None) in (5, 32)
                and failed.name.lower().endswith(".sqlite-shm.tmp")
            )
            if transient_shm and attempt < 4:
                time.sleep(0.05)
                if not failed.exists():
                    continue
            if (
                os.name != "nt"
                or getattr(error, "winerror", None) != 145
                or attempt == 4
            ):
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
        self.pid = 1_000_000_000  # Synthetic process group; POSIX signalling is mocked.
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
        if self.returncode is None:
            # Complete termination after the caller's mocked POSIX killpg.
            self.kill()
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
