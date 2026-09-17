"""Server-owned ICMP scheduling, observations, and incident transitions."""

import asyncio
import ipaddress
import json
import os
import re
import signal
import socket
import subprocess
import sys
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from dashboard.config import number


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


def validate_icmp_settings(document: object) -> dict:
    if not isinstance(document, dict) or document.keys() != {
        "enabled",
        "host",
        "timeout_seconds",
        "interval_seconds",
    }:
        raise ValueError(
            "ICMP settings require enabled, host, timeout_seconds and interval_seconds."
        )
    if type(document["enabled"]) is not bool:
        raise ValueError("enabled must be true or false.")
    host = document["host"]
    if not isinstance(host, str):
        raise TypeError("host must be an IPv4 address or hostname.")
    host = host.strip()
    if host:
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            try:
                host = host.encode("idna").decode("ascii").lower().rstrip(".")
            except UnicodeError as error:
                raise ValueError("Invalid hostname.") from error
            if len(host) > 253 or any(
                not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", part)
                for part in host.split(".")
            ):
                raise ValueError(
                    "Use a hostname without a URL, port, spaces or options."
                )
        else:
            if (
                address.version != 4
                or address.is_multicast
                or address.is_unspecified
                or str(address) == "255.255.255.255"
            ):
                raise ValueError("Use a unicast IPv4 address or hostname.")
            host = str(address)
    if document["enabled"] and not host:
        raise ValueError("A target host is required to enable ICMP monitoring.")
    timeout = number(document["timeout_seconds"], "timeout_seconds", 0.1, 60)
    interval = number(document["interval_seconds"], "interval_seconds", 1, 600)
    return {
        "enabled": document["enabled"],
        "host": host,
        "timeout_seconds": timeout,
        "interval_seconds": interval,
    }


class ICMPMonitor:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.settings = {
            "enabled": False,
            "host": "www.google.com",
            "timeout_seconds": 5,
            "interval_seconds": 30,
        }
        self.history: list[dict] = []
        self.incidents: list[dict] = []
        self.host_name = socket.gethostname()
        self.session_id = str(uuid4())
        self.storage_error: str | None = None
        self._task: asyncio.Task | None = None
        self._probe_task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._lock = asyncio.Lock()
        self._file = None
        self._revision = 0
        self._closing = False

    async def open(self) -> None:
        if self._file is not None:
            raise RuntimeError("ICMP monitor is already open.")
        self._closing = False
        self.directory.mkdir(parents=True, exist_ok=True)
        self._file = (self.directory / "monitor.lock").open("a+b")
        self._file.seek(0, 2)
        if self._file.tell() == 0:
            self._file.write(b"0")
            self._file.flush()
        self._file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            path = self.directory / "icmp.json"
            if path.exists():
                if path.stat().st_size > 4 * 1024 * 1024:
                    raise ValueError("ICMP state file is too large.")
                document = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(document, dict) or document.keys() != {
                    "settings",
                    "history",
                    "incidents",
                }:
                    raise ValueError("Invalid ICMP state file.")
                self.settings = validate_icmp_settings(document["settings"])
                for field in ("history", "incidents"):
                    if not isinstance(document[field], list) or any(
                        not isinstance(row, dict) for row in document[field]
                    ):
                        raise ValueError(f"Invalid ICMP {field}.")
                for observation in document["history"]:
                    if observation.get("status") not in {"reply", "no_reply", "error"}:
                        raise ValueError("Invalid saved ICMP observation status.")
                    for field in ("host", "probe_host", "observed_at", "session_id"):
                        if not isinstance(observation.get(field), str):
                            raise TypeError(
                                f"Missing saved ICMP observation field: {field}."
                            )
                    observed_at = datetime.fromisoformat(observation["observed_at"])
                    if observed_at.utcoffset() is None:
                        raise ValueError(
                            "Saved ICMP timestamps must include a timezone."
                        )
                    if observation.get("rtt_ms") is not None:
                        number(observation["rtt_ms"], "saved RTT", 0, 3600000)
                for incident in document["incidents"]:
                    if incident.get("status") not in {"active", "resolved", "closed"}:
                        raise ValueError("Invalid saved ICMP incident status.")
                    for field in ("id", "host", "probe_host", "started_at"):
                        if not isinstance(incident.get(field), str):
                            raise TypeError(
                                f"Missing saved ICMP incident field: {field}."
                            )
                    for field in ("started_at", "ended_at"):
                        value = incident.get(field)
                        if value is None and field == "ended_at":
                            continue
                        instant = datetime.fromisoformat(value)
                        if instant.utcoffset() is None:
                            raise ValueError(
                                "Saved ICMP incident timestamps must include a timezone."
                            )
                if (
                    sum(item["status"] == "active" for item in document["incidents"])
                    > 1
                ):
                    raise ValueError("Multiple active incidents for one ICMP rule.")
                self.history = document["history"][-720:]
                self.incidents = document["incidents"][-200:]
        except BaseException:
            self._file.close()
            self._file = None
            raise
        self._task = asyncio.create_task(self._run(), name="dashboard-icmp")

    def _write(self, document: dict) -> None:
        temporary = self.directory / f".icmp-{uuid4()}.json"
        try:
            with temporary.open("w", encoding="utf-8") as stream:
                json.dump(document, stream, ensure_ascii=False, allow_nan=False)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.directory / "icmp.json")
        finally:
            temporary.unlink(missing_ok=True)

    async def configure(self, document: object) -> dict:
        settings = validate_icmp_settings(document)
        async with self._lock:
            if self._closing:
                raise ValueError("ICMP monitor is closing.")
            incidents = [dict(item) for item in self.incidents]
            if settings != self.settings:
                for incident in incidents:
                    if incident.get("status") == "active":
                        incident.update(
                            status="closed",
                            ended_at=timestamp(),
                            resolution="configuration_changed",
                        )
            writing = asyncio.create_task(
                asyncio.to_thread(
                    self._write,
                    {
                        "settings": settings,
                        "history": self.history,
                        "incidents": incidents,
                    },
                )
            )
            cancelled = False
            try:
                await asyncio.shield(writing)
            except asyncio.CancelledError:
                # A filesystem thread continues after its waiter is cancelled.
                # Complete publication and the matching in-memory state before releasing ownership.
                await writing
                cancelled = True
            self._revision += 1
            self.settings = settings
            self.incidents = incidents
            self.storage_error = None
            self._wake.set()
            if cancelled:
                raise asyncio.CancelledError
        return self.snapshot()

    def snapshot(self) -> dict:
        latest = next(
            (
                item
                for item in reversed(self.history)
                if item.get("host") == self.settings["host"]
            ),
            None,
        )
        fresh = bool(
            latest
            and latest.get("session_id") == self.session_id
            and latest.get("revision") == self._revision
        )
        if fresh:
            age = (
                datetime.now(UTC) - datetime.fromisoformat(latest["observed_at"])
            ).total_seconds()
            fresh = (
                0
                <= age
                <= self.settings["interval_seconds"]
                + self.settings["timeout_seconds"]
                + 10
            )
        state = "not_configured" if not self.settings["host"] else "disabled"
        if self.settings["enabled"]:
            state = latest["status"] if fresh else "waiting"
        return {
            "settings": dict(self.settings),
            "status": state,
            "probe_host": self.host_name,
            "source": "dashboard_host",
            "fresh": fresh,
            "probing": bool(self._probe_task and not self._probe_task.done()),
            "latest": latest,
            "history": self.history[-200:],
            "incidents": self.incidents,
            "storage_error": self.storage_error,
        }

    async def probe(self) -> dict:
        if self._closing:
            raise ValueError("ICMP monitor is closing.")
        if not self.settings["enabled"]:
            raise ValueError("Enable ICMP monitoring and configure a host first.")
        if self._probe_task is None or self._probe_task.done():
            self._probe_task = asyncio.create_task(
                self._measure(dict(self.settings), self._revision)
            )
        await asyncio.shield(self._probe_task)
        return self.snapshot()

    async def _measure(self, settings: dict, revision: int) -> None:
        worker = Path(__file__).with_name("icmp_worker.py")
        options = (
            {"creationflags": subprocess.CREATE_NO_WINDOW}
            if os.name == "nt"
            else {"start_new_session": True}
        )
        process = None
        started = timestamp()
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-B",
                str(worker),
                settings["host"],
                str(settings["timeout_seconds"]),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **options,
            )
            output, _ = await asyncio.wait_for(
                process.communicate(), settings["timeout_seconds"] + 4
            )
            if process.returncode != 0 or len(output) > 16384:
                raise ValueError("ICMP worker did not return a valid result.")
            result = json.loads(output)
            if not isinstance(result, dict) or result.get("status") not in {
                "reply",
                "no_reply",
                "error",
            }:
                raise ValueError("Unexpected ICMP worker status.")
            if result.get("rtt_ms") is not None:
                number(result["rtt_ms"], "ICMP RTT", 0, 3600000)
        except (TimeoutError, OSError, TypeError, ValueError) as error:
            result = {
                "status": "error",
                "reason": str(error) or "Probe deadline exceeded (including DNS).",
                "rtt_ms": None,
                "address": None,
            }
        finally:
            if process is not None and process.returncode is None:
                with suppress(ProcessLookupError):
                    if os.name == "nt":
                        process.kill()
                    else:
                        os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
        async with self._lock:
            if revision != self._revision:
                return
            result.update(
                host=settings["host"],
                probe_host=self.host_name,
                source="dashboard_host",
                started_at=started,
                observed_at=timestamp(),
                session_id=self.session_id,
                revision=revision,
                timeout_seconds=settings["timeout_seconds"],
            )
            self.history.append(result)
            self.history = self.history[-720:]
            active = next(
                (item for item in self.incidents if item.get("status") == "active"),
                None,
            )
            if result["status"] == "no_reply" and active is None:
                self.incidents.append(
                    {
                        "id": str(uuid4()),
                        "type": "icmp_no_reply",
                        "source": "dashboard_host",
                        "host": settings["host"],
                        "probe_host": self.host_name,
                        "status": "active",
                        "started_at": result["observed_at"],
                        "ended_at": None,
                    }
                )
                self.incidents = self.incidents[-200:]
            elif result["status"] == "reply" and active:
                active.update(
                    status="resolved",
                    ended_at=result["observed_at"],
                    resolution="echo_reply",
                )
            try:
                writing = asyncio.create_task(
                    asyncio.to_thread(
                        self._write,
                        {
                            "settings": self.settings,
                            "history": self.history,
                            "incidents": self.incidents,
                        },
                    )
                )
                try:
                    await asyncio.shield(writing)
                except asyncio.CancelledError:
                    await writing
                    raise
                self.storage_error = None
            except OSError as error:
                self.storage_error = f"Could not persist ICMP observations: {error}"

    async def _run(self) -> None:
        while not self._closing:
            self._wake.clear()
            if self.settings["enabled"]:
                await self.probe()
            try:
                await asyncio.wait_for(
                    self._wake.wait(), self.settings["interval_seconds"]
                )
            except TimeoutError:
                pass

    async def close(self) -> None:
        self._closing = True
        tasks = [task for task in (self._task, self._probe_task) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        async with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None
