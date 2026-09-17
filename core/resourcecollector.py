"""Controller-owned supervision and RAM history for optional resource monitoring."""

from __future__ import annotations

import asyncio
import multiprocessing
import time
from multiprocessing.connection import Connection
from pathlib import Path

from core.logger_utils.events import JsonObject, copy_json_object
from core.resource_utils.sampling import collect_resources
from core.resource_utils.state import CollectorSettings, ResourceHistory


class ResourceCollector:
    def __init__(self, config_path: Path) -> None:
        self._config_path = Path(config_path)
        if not self._config_path.is_absolute():
            raise ValueError("Collector config_path must be absolute.")
        self._settings: CollectorSettings | None = None
        self._history: ResourceHistory | None = None
        self._snapshot: JsonObject = {
            "context": {},
            "logging_config_path": None,
            "targets": [],
        }
        self._revision = 0
        self._suspended = False
        self._closing = False
        self._serving = False
        self._serve_task: asyncio.Task | None = None
        self._stop_worker = False
        self._process: multiprocessing.Process | None = None
        self._connection: Connection | None = None
        self._tasks: list[asyncio.Task] = []
        self._wake = asyncio.Event()
        self._observed = asyncio.Event()
        self._state = "not_started"
        self._error: str | None = None
        self._packet: JsonObject = {}
        self._latest: dict[str, JsonObject] = {}
        self._last_received = 0.0
        self._failures = 0
        self._restarts = 0
        self._next_restart: float | None = None
        self._restart_notice: JsonObject | None = None
        self._last_success: dict[str, dict[str, tuple[float, str]]] = {}

    def update(self, snapshot: JsonObject) -> None:
        """Replace the desired target set without waiting for the collector process."""
        snapshot = copy_json_object(snapshot, "resource snapshot")
        if snapshot == self._snapshot:
            return
        self._snapshot = snapshot
        self._revision += 1
        self._latest.clear()
        self._last_success.clear()
        self._wake.set()

    async def serve(self) -> None:
        if self._serving or self._closing:
            raise RuntimeError("Resource collector is already serving or closed.")
        self._serving = True
        self._serve_task = asyncio.current_task()
        try:
            try:
                self._settings = await asyncio.to_thread(
                    CollectorSettings.load, self._config_path
                )
                self._history = ResourceHistory(self._settings)
            except (OSError, TypeError, ValueError) as error:
                self._state = "configuration_error"
                self._error = str(error)
                return
            while not self._closing:
                try:
                    await self._start_worker()
                    await self._watch_worker()
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001 - Optional monitoring never fails the command loop.
                    self._error = f"{type(error).__name__}: {error}"
                    self._state = "restarting"
                    self._restart_notice = {
                        "reason": "collector_restarted",
                        "error": self._error,
                        "previous_collector_id": self._packet.get("collector_id"),
                        "context": dict(self._snapshot["context"]),
                    }
                finally:
                    await self._shutdown_worker()
                if self._closing:
                    break
                self._state = "restarting"
                delays = self._settings.restart_delays_seconds
                delay = delays[min(self._failures, len(delays) - 1)]
                self._failures += 1
                self._restarts += 1
                self._next_restart = time.monotonic() + delay
                # Windows timers may wake before the requested delay has elapsed.
                while time.monotonic() < self._next_restart:
                    await asyncio.sleep(
                        max(0.001, self._next_restart - time.monotonic())
                    )
        except Exception as error:  # noqa: BLE001 - Even supervision failures are isolated from the controller.
            self._state = "unavailable"
            self._error = f"{type(error).__name__}: {error}"
        finally:
            self._serving = False
            try:
                await self._shutdown_worker()
            except Exception as error:  # noqa: BLE001 - Keep failed cleanup visible without failing the DAG.
                self._state = "unavailable"
                self._error = f"Collector cleanup failed: {error}"

    async def _start_worker(self) -> None:
        if self._process is not None:
            raise RuntimeError("Previous collector termination is unconfirmed.")
        self._state = "starting"
        self._next_restart = None
        self._packet = {}
        self._stop_worker = False
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        process = context.Process(
            target=collect_resources,
            args=(child, self._settings),
            name="resource-collector",
            daemon=True,
        )
        self._connection = parent
        self._process = process
        start = asyncio.create_task(asyncio.to_thread(process.start))
        try:
            await asyncio.shield(start)
        except asyncio.CancelledError:
            await start
            raise
        finally:
            child.close()
        self._last_received = time.monotonic()
        self._tasks = [
            asyncio.create_task(self._send_updates()),
            asyncio.create_task(self._receive_samples()),
        ]

    async def _send_updates(self) -> None:
        while not self._closing and not self._stop_worker:
            self._wake.clear()
            snapshot = self._snapshot
            if self._suspended:
                snapshot = {"context": {}, "logging_config_path": None, "targets": []}
            await asyncio.to_thread(
                self._connection.send,
                {
                    "command": "update",
                    "revision": self._revision,
                    "snapshot": snapshot,
                    "restart_notice": self._restart_notice,
                },
            )
            try:
                await asyncio.wait_for(
                    self._wake.wait(), self._settings.status_interval_seconds
                )
            except TimeoutError:
                pass
        await asyncio.to_thread(self._connection.send, {"command": "stop"})

    async def _receive_samples(self) -> None:
        while True:
            packet = await asyncio.to_thread(self._connection.recv)
            self._last_received = time.monotonic()
            self._packet = packet
            self._state = "running"
            self._error = None
            if packet["revision"] == self._revision:
                for sample in packet["samples"]:
                    self._history.append(sample)
                    self._latest[sample["series_id"]] = sample
                    successes = self._last_success.setdefault(sample["series_id"], {})
                    for name, measurement in sample["resources"].items():
                        if measurement["value"] is not None:
                            successes[name] = (
                                sample["observed_monotonic"],
                                sample["observed_at"],
                            )
            self._observed.set()

    async def _watch_worker(self) -> None:
        ready_since = None
        while True:
            if not self._process.is_alive():
                raise RuntimeError(
                    f"Collector exited with code {self._process.exitcode}."
                )
            for task in self._tasks:
                if task.done():
                    task.result()
                    raise RuntimeError("Collector communication stopped.")
            now = time.monotonic()
            if self._packet:
                if ready_since is None:
                    ready_since = now
                if now - ready_since >= self._settings.stable_reset_seconds:
                    self._failures = 0
                timeout = self._settings.heartbeat_timeout_seconds
            else:
                timeout = self._settings.startup_timeout_seconds
            if now - self._last_received >= timeout:
                raise TimeoutError("Collector stopped reporting its status.")
            await asyncio.sleep(self._settings.status_interval_seconds)

    async def _shutdown_worker(self) -> None:
        process = self._process
        if process is None:
            return
        self._stop_worker = True
        self._wake.set()
        timeout = self._settings.shutdown_timeout_seconds
        if process.pid is not None:
            await asyncio.to_thread(process.join, timeout)
            if process.is_alive():
                process.terminate()
                await asyncio.to_thread(process.join, timeout)
            if process.is_alive():
                process.kill()
                await asyncio.to_thread(process.join, timeout)
            if process.is_alive():
                self._error = (
                    "Collector termination is unconfirmed; restart is withheld."
                )
                return
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._connection.close()
        self._connection = None
        process.close()
        self._process = None
        self._observed.set()

    async def suspend_experiment(self) -> None:
        """Confirm writer closure before a caller replaces an experiment journal."""
        self._suspended = True
        self._revision += 1
        revision = self._revision
        self._latest.clear()
        self._last_success.clear()
        self._wake.set()
        if self._settings is None:
            return
        try:
            async with asyncio.timeout(self._settings.shutdown_timeout_seconds):
                while self._process is not None:
                    self._observed.clear()
                    if self._packet.get(
                        "revision", -1
                    ) >= revision and self._packet.get("journal_closed"):
                        return
                    await self._observed.wait()
        except TimeoutError:
            raise TimeoutError(
                "Collector has not confirmed journal closure; replacement must wait."
            ) from None

    def resume_experiment(self) -> None:
        self._suspended = False
        self._revision += 1
        self._wake.set()

    def get_status(self) -> JsonObject:
        now = time.monotonic()
        stale_after = (
            None
            if self._settings is None
            else (
                self._settings.sample_interval_seconds
                * self._settings.stale_after_intervals
            )
        )
        latest = []
        for sample in self._latest.values():
            age = max(0, now - sample["observed_monotonic"])
            successes = self._last_success.get(sample["series_id"], {})
            freshness = {}
            for name in sample["resources"]:
                previous = successes.get(name)
                freshness[name] = {
                    "last_success_at": None if previous is None else previous[1],
                    "fresh": self._state == "running"
                    and previous is not None
                    and stale_after is not None
                    and now - previous[0] <= stale_after,
                }
            latest.append(
                {
                    **sample,
                    "age_seconds": age,
                    "fresh": all(item["fresh"] for item in freshness.values()),
                    "freshness": freshness,
                }
            )
        return copy_json_object(
            {
                "state": self._state,
                "error": self._error,
                "config_path": str(self._config_path),
                "collector_id": self._packet.get("collector_id"),
                "history_id": None
                if self._history is None
                else self._history.history_id,
                "pid": None if self._process is None else self._process.pid,
                "restarts": self._restarts,
                "restart_in_seconds": None
                if self._next_restart is None
                else max(0, self._next_restart - now),
                "journal_error": self._packet.get("journal_error"),
                "journal_closed": self._process is None
                or self._packet.get("journal_closed", False),
                "unconfirmed_samples": self._packet.get("unconfirmed_samples", 0),
                "suspended": self._suspended,
                "latest": latest,
            },
            "collector status",
        )

    def read_history(self, *, after: int = 0, limit: int = 100) -> JsonObject:
        if self._history is None:
            raise RuntimeError("Resource history is not initialized.")
        return self._history.read(after=after, limit=limit)

    async def close(self) -> None:
        self._closing = True
        self._wake.set()
        if (
            self._serve_task is not None
            and self._serve_task is not asyncio.current_task()
        ):
            self._serve_task.cancel()
            await asyncio.gather(self._serve_task, return_exceptions=True)
        try:
            await self._shutdown_worker()
        except Exception as error:  # noqa: BLE001 - Report shutdown failure through independent status.
            self._error = f"Collector cleanup failed: {error}"
        self._state = "stopped" if self._process is None else "termination_unconfirmed"
