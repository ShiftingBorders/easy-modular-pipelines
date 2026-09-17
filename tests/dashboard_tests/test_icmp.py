"""ICMP validation, concurrent requests, freshness and incident transitions."""

import asyncio
import json
import socket
import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

from dashboard.icmp import ICMPMonitor, validate_icmp_settings
from tests.dashboard_tests.helpers import (
    ProbeProcess,
    icmp_settings,
    probe_result,
    temporary_directory,
)


class ICMPSettingsTests(unittest.TestCase):
    def test_google_default_and_hostname_normalization(self) -> None:
        self.assertEqual(
            validate_icmp_settings(icmp_settings(host=" WWW.GOOGLE.COM. "))["host"],
            "www.google.com",
        )
        self.assertEqual(
            validate_icmp_settings(icmp_settings(host="127.0.0.1"))["host"], "127.0.0.1"
        )
        self.assertEqual(
            validate_icmp_settings(icmp_settings(host="", enabled=False))["host"], ""
        )

    def test_rejects_urls_options_ipv6_and_nonunicast_addresses(self) -> None:
        for host in [
            "",
            "https://www.google.com",
            "google.com:80",
            "-n",
            "google.com & calc",
            "a..b",
            "a" * 64 + ".com",
            "::1",
            "0.0.0.0",
            "224.0.0.1",
            "255.255.255.255",
            None,
            12,
        ]:
            with self.subTest(host=host), self.assertRaises((TypeError, ValueError)):
                validate_icmp_settings(icmp_settings(host=host))

    def test_requires_exact_settings_and_boolean_enabled(self) -> None:
        for document in [
            None,
            [],
            {},
            {**icmp_settings(), "other": 1},
            icmp_settings(enabled=1),
        ]:
            with (
                self.subTest(document=document),
                self.assertRaises((TypeError, ValueError)),
            ):
                validate_icmp_settings(document)

    def test_interval_and_timeout_boundaries(self) -> None:
        for field, low, high in [
            ("timeout_seconds", 0.1, 60),
            ("interval_seconds", 1, 600),
        ]:
            for value in [low, high]:
                self.assertEqual(
                    validate_icmp_settings(icmp_settings(**{field: value}))[field],
                    value,
                )
            for value in [0, high + 1, True, None, "1", float("nan"), float("inf")]:
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises((TypeError, ValueError)),
                ):
                    validate_icmp_settings(icmp_settings(**{field: value}))


@unittest.skipUnless(
    sys.platform == "win32", "This phase runs monitor/process checks on Windows only."
)
class ICMPMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        temporary = temporary_directory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.monitor = ICMPMonitor(self.directory)
        # Exercise explicit probe calls independently of the periodic scheduler.
        self.monitor.settings = icmp_settings()
        self.addAsyncCleanup(self.monitor.close)

    async def measure(self, status: str = "reply") -> dict:
        with patch(
            "dashboard.icmp.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=ProbeProcess(probe_result(status))),
        ):
            return await self.monitor.probe()

    async def test_probe_records_actual_dashboard_host_and_target(self) -> None:
        spawn = AsyncMock(return_value=ProbeProcess())
        with patch("dashboard.icmp.asyncio.create_subprocess_exec", new=spawn):
            state = await self.monitor.probe()
        self.assertEqual(state["source"], "dashboard_host")
        self.assertEqual(state["probe_host"], socket.gethostname())
        self.assertEqual(state["latest"]["host"], "www.google.com")
        self.assertEqual(state["latest"]["probe_host"], socket.gethostname())
        self.assertTrue(state["fresh"])
        self.assertIn("www.google.com", spawn.call_args.args)
        self.assertEqual(spawn.call_args.args[0], sys.executable)
        self.assertNotIn("shell", spawn.call_args.kwargs)

    async def test_parallel_check_now_calls_share_one_process(self) -> None:
        process = ProbeProcess(blocked=True)
        spawn = AsyncMock(return_value=process)
        with patch("dashboard.icmp.asyncio.create_subprocess_exec", new=spawn):
            first = asyncio.create_task(self.monitor.probe())
            await asyncio.wait_for(process.started.wait(), 1)
            second = asyncio.create_task(self.monitor.probe())
            await asyncio.sleep(0)
            self.assertTrue(self.monitor.snapshot()["probing"])
            process.release.set()
            results = await asyncio.gather(first, second)
        self.assertEqual(spawn.await_count, 1)
        self.assertEqual(len(self.monitor.history), 1)
        self.assertEqual([result["status"] for result in results], ["reply", "reply"])

    async def test_cancelled_browser_request_does_not_cancel_shared_probe(self) -> None:
        process = ProbeProcess(blocked=True)
        with patch(
            "dashboard.icmp.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ):
            browser = asyncio.create_task(self.monitor.probe())
            await process.started.wait()
            browser.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await browser
            self.assertFalse(process.killed)
            process.release.set()
            await self.monitor._probe_task
        self.assertEqual(self.monitor.snapshot()["status"], "reply")

    async def test_periodic_probes_continue_without_any_browser(self) -> None:
        second_probe = asyncio.Event()
        count = 0

        async def spawn(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                second_probe.set()
            return ProbeProcess()

        with patch("dashboard.icmp.asyncio.create_subprocess_exec", side_effect=spawn):
            await self.monitor.open()
            await asyncio.wait_for(second_probe.wait(), 3)
            await self.monitor.close()
        self.assertGreaterEqual(count, 2)
        self.assertTrue(self.monitor._task.done())

    async def test_incident_opens_once_and_only_reply_resolves_it(self) -> None:
        await self.measure("no_reply")
        incident_id = self.monitor.incidents[0]["id"]
        await self.measure("no_reply")
        await self.measure("error")
        self.assertEqual(len(self.monitor.incidents), 1)
        self.assertEqual(self.monitor.incidents[0]["status"], "active")
        self.assertIsNone(self.monitor.history[-1]["rtt_ms"])
        await self.measure("reply")
        incident = self.monitor.incidents[0]
        self.assertEqual(incident["id"], incident_id)
        self.assertEqual(incident["status"], "resolved")
        self.assertEqual(incident["resolution"], "echo_reply")
        self.assertIsNotNone(incident["ended_at"])

    async def test_configuration_change_closes_incident_without_fake_reply(
        self,
    ) -> None:
        await self.measure("no_reply")
        await self.monitor.configure(icmp_settings(timeout_seconds=2))
        self.assertEqual(self.monitor.incidents[0]["status"], "closed")
        self.assertEqual(
            self.monitor.incidents[0]["resolution"], "configuration_changed"
        )
        self.assertEqual(self.monitor.history[-1]["status"], "no_reply")
        self.assertFalse(self.monitor.snapshot()["fresh"])

    async def test_late_response_from_old_configuration_is_discarded(self) -> None:
        process = ProbeProcess(blocked=True)
        with patch(
            "dashboard.icmp.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ):
            pending = asyncio.create_task(self.monitor.probe())
            await process.started.wait()
            await self.monitor.configure(
                icmp_settings(enabled=False, timeout_seconds=2)
            )
            process.release.set()
            await pending
        self.assertEqual(self.monitor.history, [])
        self.assertEqual(self.monitor.incidents, [])
        self.assertEqual(self.monitor.snapshot()["status"], "disabled")

    async def test_old_session_and_old_timestamps_are_not_fresh(self) -> None:
        await self.measure()
        latest = self.monitor.history[-1]
        for changes in [
            {"session_id": "old-session"},
            {"revision": -1},
            {"observed_at": (datetime.now(UTC) - timedelta(hours=1)).isoformat()},
            {"observed_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat()},
        ]:
            original = dict(latest)
            latest.update(changes)
            self.assertFalse(self.monitor.snapshot()["fresh"])
            self.assertEqual(self.monitor.snapshot()["status"], "waiting")
            latest.clear()
            latest.update(original)

    async def test_disabled_monitor_does_not_spawn_process(self) -> None:
        self.monitor.settings["enabled"] = False
        with (
            patch("dashboard.icmp.asyncio.create_subprocess_exec") as spawn,
            self.assertRaises(ValueError),
        ):
            await self.monitor.probe()
        spawn.assert_not_called()

    async def test_abnormal_exit_and_malformed_worker_output_become_errors(
        self,
    ) -> None:
        for process in [
            ProbeProcess(exit_code=7),
            ProbeProcess(b"{"),
            ProbeProcess(b"[]"),
            ProbeProcess(b"null"),
            ProbeProcess(b"x" * 16385),
            ProbeProcess({"status": "unexpected"}),
        ]:
            with self.subTest(output=process.output[:30]):
                with patch(
                    "dashboard.icmp.asyncio.create_subprocess_exec",
                    new=AsyncMock(return_value=process),
                ):
                    result = await self.monitor.probe()
                self.assertEqual(result["status"], "error")
                self.assertIsNone(result["latest"]["rtt_ms"])
        self.assertEqual(self.monitor.incidents, [])
        self.assertEqual((await self.measure())["status"], "reply")

    async def test_os_launch_error_does_not_kill_monitor(self) -> None:
        with patch(
            "dashboard.icmp.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=OSError("cannot start")),
        ):
            result = await self.monitor.probe()
        self.assertEqual(result["status"], "error")
        self.assertEqual((await self.measure())["status"], "reply")

    async def test_shutdown_kills_pending_probe_and_releases_lock(self) -> None:
        process = ProbeProcess(blocked=True)
        with patch(
            "dashboard.icmp.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ):
            await self.monitor.open()
            await asyncio.wait_for(process.started.wait(), 1)
            await self.monitor.close()
        self.assertTrue(process.killed)
        self.assertTrue(process.waited)
        replacement = ICMPMonitor(self.directory)
        await replacement.open()
        await replacement.close()

    async def test_probe_write_failure_is_visible_and_next_probe_recovers(self) -> None:
        with patch.object(
            self.monitor, "_write", side_effect=OSError("disk unavailable")
        ):
            result = await self.measure()
        self.assertEqual(result["status"], "reply")
        self.assertIn("disk unavailable", result["storage_error"])
        recovered = await self.measure()
        self.assertIsNone(recovered["storage_error"])
        stored = json.loads((self.directory / "icmp.json").read_text())
        self.assertEqual(len(stored["history"]), 2)

    async def test_history_and_incidents_are_bounded(self) -> None:
        await self.measure()
        original = self.monitor.history[0]
        self.monitor.history = [{**original, "marker": index} for index in range(720)]
        now = datetime.now(UTC).isoformat()
        self.monitor.incidents = [
            {"id": str(index), "status": "closed", "started_at": now}
            for index in range(200)
        ]
        result = await self.measure("no_reply")
        self.assertEqual(len(self.monitor.history), 720)
        self.assertEqual(self.monitor.history[0]["marker"], 1)
        self.assertEqual(len(result["history"]), 200)
        self.assertEqual(len(self.monitor.incidents), 200)
        self.assertEqual(self.monitor.incidents[-1]["status"], "active")
