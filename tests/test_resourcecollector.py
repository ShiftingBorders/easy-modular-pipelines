"""Approved resource_collector.md A/C/D: process supervision and independent status."""

import asyncio
import functools
import multiprocessing
import os
import subprocess
import tempfile
import time
import unittest
from itertools import pairwise
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from core.resource_utils.state import ResourceHistory
from core.resourcecollector import ResourceCollector
from core.runner_utils.runtimeio import process_identity
from tests.helpers.dag import REPOSITORY, process_running, terminate_owned, wait_until
from tests.helpers.resources import (
    TEMP_ROOT,
    controlled_collector,
    events,
    journal,
    memory_and_cpu_process,
    run_owner,
    settings,
    spawned,
    stop_process,
    target,
    write_settings,
)


class CollectorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=TEMP_ROOT)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.collector = None
        self.task = None
        self.addAsyncCleanup(self.close_collector)

    async def close_collector(self):
        if self.collector is not None:
            await self.collector.close()
        if self.task is not None:
            await asyncio.gather(self.task, return_exceptions=True)

    async def start(self, path=None):
        self.collector = ResourceCollector(path or write_settings(self.root))
        self.task = asyncio.create_task(self.collector.serve())
        await wait_until(lambda: self.collector.get_status()["collector_id"])
        return self.collector.get_status()

    async def measured(self):
        return await wait_until(lambda: self.collector.get_status()["latest"])

    async def test_constructor_is_passive_and_idle_history_never_creates_a_journal(
        self,
    ):
        """A/C: importing/constructing is passive; idle sampling creates no files."""
        async with asyncio.timeout(30):
            probe = await asyncio.to_thread(
                subprocess.run,
                [
                    "uv",
                    "run",
                    "python",
                    "-B",
                    "-c",
                    (
                        "from unittest.mock import patch\n"
                        "with patch('multiprocessing.process.BaseProcess.start', side_effect=AssertionError('spawn at import')), "
                        "patch('core.logger.OperationLogger.open', side_effect=AssertionError('journal at import')):\n"
                        "    import core.resourcecollector, core.resource_utils.sampling\n"
                    ),
                ],
                cwd=REPOSITORY,
                capture_output=True,
                timeout=10,
            )
            self.assertEqual(probe.returncode, 0, probe.stderr.decode(errors="replace"))
            path = write_settings(self.root)
            before = set(self.root.iterdir())
            with patch("multiprocessing.process.BaseProcess.start") as start:
                collector = ResourceCollector(path)
                self.assertEqual(collector.get_status()["state"], "not_started")
                start.assert_not_called()
            status = await self.start(path)
            self.assertNotEqual(status["pid"], os.getpid())
            await self.measured()
            self.assertTrue(self.collector.read_history()["samples"])
            self.assertEqual(set(self.root.iterdir()), before)

    async def test_missing_and_corrupt_configuration_are_reported_without_raising(self):
        """A: configuration failure remains an observable monitoring state."""
        async with asyncio.timeout(30):
            for name, contents in (("missing", None), ("corrupt", "{")):
                path = self.root / name
                if contents is not None:
                    path.write_text(contents, encoding="utf-8")
                collector = ResourceCollector(path)
                await collector.serve()
                self.assertEqual(collector.get_status()["state"], "configuration_error")
                self.assertTrue(collector.get_status()["error"])
                self.assertIsNone(collector.get_status()["pid"])
                await collector.close()

    async def test_crash_restarts_the_worker_with_current_targets(self):
        """C: real worker death changes its identity and restores the latest target set."""
        async with asyncio.timeout(30):
            status = await self.start()
            identity = process_identity(status["pid"])
            observed = target(process_identity(os.getpid()))
            self.collector.update(
                {"context": {}, "logging_config_path": None, "targets": [observed]}
            )
            await wait_until(
                lambda: any(
                    item["series_id"] == observed["series_id"]
                    for item in self.collector.get_status()["latest"]
                )
            )
            terminate_owned(identity)
            await wait_until(
                lambda: (
                    self.collector.get_status()["collector_id"]
                    not in (None, status["collector_id"])
                )
            )
            await wait_until(
                lambda: any(
                    item["series_id"] == observed["series_id"]
                    for item in self.collector.get_status()["latest"]
                )
            )
            self.assertFalse(process_running(identity["pid"]))
            self.assertGreaterEqual(self.collector.get_status()["restarts"], 1)

    async def test_hang_is_detected_and_old_worker_stops_before_replacement(self):
        """C: an actual blocked sample loses heartbeat and is replaced after termination."""
        async with asyncio.timeout(30):
            worker = functools.partial(controlled_collector, controls=str(self.root))
            with patch("core.resourcecollector.collect_resources", worker):
                status = await self.start()
                identity = process_identity(status["pid"])
                (self.root / "hang").touch()
                await wait_until(lambda: (self.root / "hanging").exists())
                (self.root / "hang").unlink()
                await wait_until(
                    lambda: (
                        self.collector.get_status()["collector_id"]
                        not in (None, status["collector_id"])
                    )
                )
                self.assertFalse(process_running(identity["pid"]))
                await self.measured()

    async def test_startup_timeout_recovers_from_a_worker_that_never_reports_ready(
        self,
    ):
        """C: startup has its own deadline, followed by a real replacement."""
        async with asyncio.timeout(30):
            (self.root / "hang").touch()
            worker = functools.partial(controlled_collector, controls=str(self.root))
            path = write_settings(self.root, startup_timeout_seconds=0.5)
            with patch("core.resourcecollector.collect_resources", worker):
                self.collector = ResourceCollector(path)
                self.task = asyncio.create_task(self.collector.serve())
                await wait_until(lambda: (self.root / "hanging").exists())
                old_pid = self.collector.get_status()["pid"]
                (self.root / "hang").unlink()
                await wait_until(lambda: self.collector.get_status()["collector_id"])
                self.assertFalse(process_running(old_pid))
                self.assertGreaterEqual(self.collector.get_status()["restarts"], 1)

    async def test_failed_launches_obey_backoff_cap_and_stability_resets_it(self):
        """C: failed start operations exercise every scaled delay and the stable reset."""
        async with asyncio.timeout(60):
            delays = [0.01, 0.02, 0.04, 0.08, 0.16, 0.3]
            path = write_settings(
                self.root, restart_delays_seconds=delays, stable_reset_seconds=0.4
            )
            started = multiprocessing.process.BaseProcess.start
            attempts = []

            def fail_initial(process):
                attempts.append(time.monotonic())
                if len(attempts) <= 8:
                    raise OSError("injected process creation failure")
                return started(process)

            with patch("multiprocessing.process.BaseProcess.start", fail_initial):
                status = await self.start(path)
                for index, (previous, current) in enumerate(pairwise(attempts)):
                    self.assertGreaterEqual(
                        current - previous, delays[min(index, len(delays) - 1)] * 0.8
                    )
                self.assertEqual(len(attempts), 9)
                await wait_until(lambda: self.collector._failures == 0)
                terminate_owned(process_identity(status["pid"]))
                await wait_until(lambda: len(attempts) == 10)
                await wait_until(
                    lambda: (
                        self.collector.get_status()["collector_id"]
                        not in (None, status["collector_id"])
                    )
                )
                self.assertEqual(self.collector._failures, 1)

    async def test_broken_real_connection_is_recovered(self):
        """C: losing the actual parent endpoint does not terminate supervision."""
        async with asyncio.timeout(30):
            status = await self.start()
            self.collector._connection.close()
            await wait_until(
                lambda: (
                    self.collector.get_status()["collector_id"]
                    not in (None, status["collector_id"])
                )
            )
            await self.measured()
            self.assertFalse(self.task.done())

    async def test_full_channel_coalesces_updates_and_event_loop_remains_responsive(
        self,
    ):
        """C: a real blocked pipe retains the latest full snapshot without queued updates."""
        async with asyncio.timeout(30):
            worker = functools.partial(controlled_collector, controls=str(self.root))
            with patch("core.resourcecollector.collect_resources", worker):
                await self.start(write_settings(self.root, heartbeat_timeout_seconds=5))
                (self.root / "hang").touch()
                await wait_until(lambda: (self.root / "hanging").exists())
                self.collector.update(
                    {
                        "context": {"run_id": "x" * 1048576},
                        "logging_config_path": None,
                        "targets": [],
                    }
                )
                await asyncio.sleep(0.1)
                self.assertFalse(self.collector._tasks[0].done())
                for index in range(100):
                    self.collector.update(
                        {
                            "context": {"run_id": str(index)},
                            "logging_config_path": None,
                            "targets": [],
                        }
                    )
                async with asyncio.timeout(1):
                    await asyncio.sleep(0)
                (self.root / "hang").unlink()
                (self.root / "release").touch()
                await wait_until(
                    lambda: any(
                        item["context"].get("run_id") == "99"
                        for item in self.collector.get_status()["latest"]
                    )
                )

    async def test_close_reaps_its_worker_and_preserves_an_unrelated_process(self):
        """C: shutdown confirms owned-worker termination and leaves other processes alive."""
        async with asyncio.timeout(30):
            unrelated, pipe = spawned(memory_and_cpu_process)
            self.addCleanup(stop_process, unrelated, pipe)
            await asyncio.to_thread(pipe.recv)
            status = await self.start()
            await self.collector.close()
            self.assertFalse(process_running(status["pid"]))
            self.assertTrue(unrelated.is_alive())
            self.assertEqual(self.collector.get_status()["state"], "stopped")

    async def test_lost_controller_causes_the_actual_collector_to_exit(self):
        """C: an orphaned collector exits after its real owner is killed."""
        async with asyncio.timeout(30):
            owner, pipe = spawned(run_owner, str(write_settings(self.root)))
            self.addCleanup(stop_process, owner, pipe)
            identity = await asyncio.to_thread(pipe.recv)
            self.addCleanup(terminate_owned, identity)
            owner.kill()
            await asyncio.to_thread(owner.join, 5)
            await wait_until(lambda: not process_running(identity["pid"]), timeout=5)

    async def test_unconfirmed_termination_does_not_start_a_second_worker(self):
        """C: the launch boundary refuses replacement while an old process is retained."""
        async with asyncio.timeout(30):
            (self.root / "hang").touch()
            self.collector = ResourceCollector(write_settings(self.root))
            self.collector._settings = settings(self.root)
            self.collector._history = ResourceHistory(self.collector._settings)
            worker = functools.partial(controlled_collector, controls=str(self.root))
            with patch("core.resourcecollector.collect_resources", worker):
                await self.collector._start_worker()
            await wait_until(lambda: (self.root / "hanging").exists())
            process = self.collector._process
            try:
                with patch.object(process, "terminate"), patch.object(process, "kill"):
                    await self.collector._shutdown_worker()
                self.assertTrue(process.is_alive())
                with patch("multiprocessing.process.BaseProcess.start") as start:
                    with self.assertRaisesRegex(
                        RuntimeError, "termination is unconfirmed"
                    ):
                        await self.collector._start_worker()
                    start.assert_not_called()
                self.assertIs(self.collector._process, process)
            finally:
                (self.root / "release").touch()

    async def test_freshness_uses_last_success_not_latest_failed_observation(self):
        """B: repeated null readings cannot refresh the last successful metric."""
        collector = ResourceCollector(
            write_settings(self.root, sample_interval_seconds=1)
        )
        collector._settings = settings(self.root, sample_interval_seconds=1)
        collector._history = ResourceHistory(collector._settings)
        packets = []
        for observed, value in ((10, 25), (12, None)):
            packets.append(
                {
                    "revision": 0,
                    "samples": [
                        {
                            "series_id": "host",
                            "observed_monotonic": observed,
                            "observed_at": f"time-{observed}",
                            "resources": {"cpu": {"value": value}},
                        }
                    ],
                }
            )
        collector._connection = Mock(recv=Mock(side_effect=[*packets, EOFError()]))
        with self.assertRaises(EOFError):
            await collector._receive_samples()
        with patch(
            "core.resourcecollector.time", SimpleNamespace(monotonic=lambda: 14)
        ):
            status = collector.get_status()
        self.assertEqual(
            status["latest"][0]["freshness"]["cpu"]["last_success_at"], "time-10"
        )
        self.assertFalse(status["latest"][0]["fresh"])

    async def test_suspend_confirms_closure_before_new_journal_generation(self):
        """D: old writes stop before replacement; resume uses the new journal identity."""
        async with asyncio.timeout(30):
            path, reader, context = journal(self.root)
            self.addCleanup(reader.close)
            await self.start()
            self.collector.update(
                {"context": context, "logging_config_path": str(path), "targets": []}
            )
            await wait_until(lambda: events(reader, "resources.recorded"))
            await self.collector.suspend_experiment()
            self.assertTrue(self.collector.get_status()["journal_closed"])
            count = len(events(reader, "resources.recorded"))
            await asyncio.sleep(0.3)
            self.assertEqual(len(events(reader, "resources.recorded")), count)
            first_id = reader.get_journal_info()["generation"]
            reader.close()
            for name in ("events.sqlite", "events.sqlite-wal", "events.sqlite-shm"):
                (self.root / name).unlink(missing_ok=True)
            replacement_path, replacement, new_context = journal(self.root)
            self.addCleanup(replacement.close)
            self.collector.update(
                {
                    "context": new_context,
                    "logging_config_path": str(replacement_path),
                    "targets": [],
                }
            )
            self.collector.resume_experiment()
            await wait_until(lambda: events(replacement, "resources.recorded"))
            self.assertNotEqual(replacement.get_journal_info()["generation"], first_id)
            self.assertTrue(
                all(
                    item["context"]["experiment_id"] == new_context["experiment_id"]
                    for item in events(replacement, "resources.recorded")
                )
            )

    async def test_suspend_timeout_never_claims_the_writer_is_closed(self):
        """D: a hung writer cannot acknowledge a journal replacement barrier."""
        async with asyncio.timeout(30):
            path, reader, context = journal(self.root)
            self.addCleanup(reader.close)
            worker = functools.partial(controlled_collector, controls=str(self.root))
            with patch("core.resourcecollector.collect_resources", worker):
                await self.start(write_settings(self.root, heartbeat_timeout_seconds=5))
                self.collector.update(
                    {
                        "context": context,
                        "logging_config_path": str(path),
                        "targets": [],
                    }
                )
                await wait_until(lambda: events(reader, "resources.recorded"))
                (self.root / "hang").touch()
                await wait_until(lambda: (self.root / "hanging").exists())
                with self.assertRaisesRegex(TimeoutError, "not confirmed"):
                    await self.collector.suspend_experiment()
                self.assertFalse(self.collector.get_status()["journal_closed"])
                (self.root / "release").touch()
