"""Server-owned ICMP scheduling, observations, and incident transitions."""

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from core.models.dashboard_icmp import (
    ICMPIncident,
    ICMPObservation,
    ICMPSettings,
    ICMPWorkerResult,
    SavedICMPState,
)
from core.models.updates import _update_model


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


def validate_icmp_settings(document: object) -> dict:
    return ICMPSettings.model_validate(document).model_dump()


class ICMPMonitor:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.settings = ICMPSettings.model_validate({
            "enabled": False,
            "host": "www.google.com",
            "timeout_seconds": 5,
            "interval_seconds": 30,
        })
        self.history: list[ICMPObservation] = []
        self.incidents: list[ICMPIncident] = []
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
                document = SavedICMPState.model_validate(
                    json.loads(path.read_text(encoding="utf-8"))
                )
                self._restore_state(document)
        except BaseException:
            self._file.close()
            self._file = None
            raise
        self._task = asyncio.create_task(self._run(), name="dashboard-icmp")

    def _restore_state(self, document: SavedICMPState) -> None:
        self.settings = document.settings
        self.history = document.history[-720:]
        self.incidents = document.incidents[-200:]

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
        settings = ICMPSettings.model_validate(document)
        return await self._configure(settings)

    async def _configure(self, validated: ICMPSettings) -> dict:
        settings = validated
        async with self._lock:
            if self._closing:
                raise ValueError("ICMP monitor is closing.")
            incidents = list(self.incidents)
            if settings != self.settings:
                for index, incident in enumerate(incidents):
                    if incident.status == "active":
                        incidents[index] = _update_model(
                            incident,
                            status="closed",
                            ended_at=timestamp(),
                            resolution="configuration_changed",
                        )
            writing = asyncio.create_task(
                asyncio.to_thread(
                    self._write,
                    {
                        "settings": settings.model_dump(),
                        "history": [
                            item.model_dump(exclude_unset=True) for item in self.history
                        ],
                        "incidents": [
                            item.model_dump(exclude_unset=True) for item in incidents
                        ],
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
                if item.host == self.settings.host
            ),
            None,
        )
        fresh = bool(
            latest
            and latest.session_id == self.session_id
            and latest.revision == self._revision
        )
        if fresh:
            age = (
                datetime.now(UTC) - datetime.fromisoformat(latest.observed_at)
            ).total_seconds()
            fresh = (
                0
                <= age
                <= self.settings.interval_seconds
                + self.settings.timeout_seconds
                + 10
            )
        state = "not_configured" if not self.settings.host else "disabled"
        if self.settings.enabled:
            state = latest.status if fresh else "waiting"
        return {
            "settings": self.settings.model_dump(),
            "status": state,
            "probe_host": self.host_name,
            "source": "dashboard_host",
            "fresh": fresh,
            "probing": bool(self._probe_task and not self._probe_task.done()),
            "latest": None if latest is None else latest.model_dump(exclude_unset=True),
            "history": [item.model_dump(exclude_unset=True) for item in self.history[-200:]],
            "incidents": [item.model_dump(exclude_unset=True) for item in self.incidents],
            "storage_error": self.storage_error,
        }

    async def probe(self) -> dict:
        if self._closing:
            raise ValueError("ICMP monitor is closing.")
        if not self.settings.enabled:
            raise ValueError("Enable ICMP monitoring and configure a host first.")
        if self._probe_task is None or self._probe_task.done():
            self._probe_task = asyncio.create_task(
                self._measure(self.settings, self._revision)
            )
        await asyncio.shield(self._probe_task)
        return self.snapshot()

    async def _measure(self, settings: ICMPSettings, revision: int) -> None:
        started = timestamp()
        result = await self._probe_worker(settings)
        async with self._lock:
            if revision != self._revision:
                return
            self._record_probe_result(result, settings, revision, started)
            await self._persist_observations()

    async def _probe_worker(self, settings: ICMPSettings) -> ICMPWorkerResult:
        worker = Path(__file__).with_name("icmp_worker.py")
        options = (
            {"creationflags": subprocess.CREATE_NO_WINDOW}
            if os.name == "nt"
            else {"start_new_session": True}
        )
        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-B",
                str(worker),
                settings.host,
                str(settings.timeout_seconds),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **options,
            )
            output, _ = await asyncio.wait_for(
                process.communicate(), settings.timeout_seconds + 4
            )
            if process.returncode != 0 or len(output) > 16384:
                raise ValueError("ICMP worker did not return a valid result.")
            return ICMPWorkerResult.model_validate(json.loads(output))
        except (TimeoutError, OSError, TypeError, ValueError) as error:
            return ICMPWorkerResult.model_validate(
                {
                    "status": "error",
                    "reason": str(error) or "Probe deadline exceeded (including DNS).",
                    "rtt_ms": None,
                    "address": None,
                }
            )
        finally:
            if process is not None and process.returncode is None:
                with suppress(ProcessLookupError):
                    if os.name == "nt":
                        process.kill()
                    else:
                        os.killpg(process.pid, signal.SIGKILL)
                await process.wait()

    def _record_probe_result(
        self,
        observed: ICMPWorkerResult,
        settings: ICMPSettings,
        revision: int,
        started: str,
    ) -> None:
        result = self._probe_observation(observed, settings, revision, started)
        self.history.append(result)
        self.history = self.history[-720:]
        active = next(
            (item for item in self.incidents if item.status == "active"),
            None,
        )
        if result.status == "no_reply" and active is None:
            self.incidents.append(
                ICMPIncident.model_validate(
                    {
                        "id": str(uuid4()),
                        "type": "icmp_no_reply",
                        "source": "dashboard_host",
                        "host": settings.host,
                        "probe_host": self.host_name,
                        "status": "active",
                        "started_at": result.observed_at,
                        "ended_at": None,
                    }
                )
            )
            self.incidents = self.incidents[-200:]
        elif result.status == "reply" and active:
            self.incidents[self.incidents.index(active)] = _update_model(
                active,
                status="resolved",
                ended_at=result.observed_at,
                resolution="echo_reply",
            )

    def _probe_observation(
        self,
        observed: ICMPWorkerResult,
        settings: ICMPSettings,
        revision: int,
        started: str,
    ) -> ICMPObservation:
        values: dict[str, object] = {}
        for name in ICMPWorkerResult.model_fields:
            if name in observed.model_fields_set:
                values[name] = getattr(observed, name)
        values.update(observed.model_extra or {})
        values.update(
            host=settings.host,
            probe_host=self.host_name,
            source="dashboard_host",
            started_at=started,
            observed_at=timestamp(),
            session_id=self.session_id,
            revision=revision,
            timeout_seconds=settings.timeout_seconds,
        )
        return ICMPObservation.model_validate(values)

    async def _persist_observations(self) -> None:
        try:
            writing = asyncio.create_task(
                asyncio.to_thread(
                    self._write,
                    {
                        "settings": self.settings.model_dump(),
                        "history": [
                            item.model_dump(exclude_unset=True) for item in self.history
                        ],
                        "incidents": [
                            item.model_dump(exclude_unset=True)
                            for item in self.incidents
                        ],
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
            if self.settings.enabled:
                await self.probe()
            try:
                await asyncio.wait_for(
                    self._wake.wait(), self.settings.interval_seconds
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
