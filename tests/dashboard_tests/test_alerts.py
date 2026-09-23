"""Approved G: real persistence, sustained conditions and independent notification queue."""

import asyncio
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from dashboard.alerts import AlertMonitor
from dashboard.api_client import SystemAPIClient
from dashboard.config import load_settings
from dashboard.views import DashboardViews
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import JournalWorkspace, history
from tests.helpers.dag import wait_until


class AlertTests(unittest.IsolatedAsyncioTestCase):
    async def test_cached_error_window_includes_old_payloads_and_preserves_unknown(
        self,
    ):
        """T077/T078: indexed counts include the window boundary, not future/ignored rows."""
        data = history(((1, 2),))
        now = datetime.now(UTC)
        for index, offset in enumerate((-60, -61, 0, 1, -1)):
            data["entries"].append(
                {
                    **data["entries"][0],
                    "event_id": f"alert-{index}",
                    "sequence_number": 100 + index,
                    "event_type": "error.recorded",
                    "occurred_at": (now + timedelta(seconds=offset)).isoformat(),
                    "data": {
                        "error_id": f"error-{index}",
                        "error_type": "Example",
                        "message": "failure",
                        **({"ignored": "superseded"} if index == 4 else {}),
                    },
                }
            )
        workspace = JournalWorkspace(self.root / "project", data)
        self.addCleanup(workspace.close)
        settings = load_settings(
            write_settings(
                self.root, project_root=str(workspace.root), history_window_events=1
            )
        )
        actual = DashboardViews(settings, SystemAPIClient(settings))
        self.addAsyncCleanup(actual.close)
        dataset = actual.journals.load("exp-test", force=True)
        self.assertEqual(len(dataset["cache"].window), 1)
        self.assertEqual(actual.error_count(dataset, 60, now.timestamp()), 2)
        self.views.error_count = actual.error_count
        self.views.models.return_value = [(dataset, {"errors": []})]
        await self.rule(
            kind="errors", experiment_id="exp-test", threshold=2, window_seconds=60
        )
        with patch("dashboard.alerts.time.time", return_value=now.timestamp()):
            await self.evaluate()
        self.assertEqual(self.monitor.status()["active_count"], 1)
        dataset["complete"] = False
        with patch("dashboard.alerts.time.time", return_value=now.timestamp() + 120):
            await self.evaluate()
        self.assertEqual(self.monitor.status()["active_count"], 1)

    async def asyncSetUp(self):
        tmp = temporary_directory()
        self.addCleanup(cleanup_directory, tmp)
        self.root = Path(tmp.name)
        self.metrics = {
            name: {"value": 95, "fresh": True} for name in ("cpu", "ram", "disk")
        }
        self.metrics["disk"]["free_bytes"] = 2 * 1073741824
        self.metrics["internet"] = {
            "receive_mbps": 95,
            "transmit_mbps": 95,
            "fresh": True,
        }
        self.views = SimpleNamespace(
            compute=AsyncMock(return_value={"metrics": self.metrics}),
            models=AsyncMock(return_value=[]),
        )
        self.icmp = SimpleNamespace(
            host_name="dashboard-machine",
            incidents=[],
            snapshot=lambda: {"fresh": True},
        )
        self.monitor = AlertMonitor(self.root, self.views, self.icmp)
        self.addAsyncCleanup(self.monitor.close)

    async def evaluate(self):
        # A controlled loop boundary, not a mocked condition/persistence implementation.
        with (
            patch("dashboard.alerts.asyncio.sleep", side_effect=asyncio.CancelledError),
            self.assertRaises(asyncio.CancelledError),
        ):
            await self.monitor._run()
        self.assertIsNone(self.monitor.error)

    async def rule(self, **changes):
        document = {
            "id": "rule",
            "name": "Hot CPU",
            "enabled": True,
            "kind": "resource",
            "metric": "cpu",
            "operator": "above",
            "threshold": 90,
            "duration_seconds": 0,
            **changes,
        }
        await self.monitor.configure(document)
        return document

    async def test_background_engine_fires_without_a_browser_and_deduplicates(self):
        await self.rule()
        await self.monitor.open()
        await wait_until(lambda: self.monitor.status()["active_count"] == 1)
        await self.monitor.close()
        self.monitor._closed = False
        await self.evaluate()
        self.assertEqual(len(self.monitor.incidents), 1)
        self.metrics["cpu"]["value"] = 10
        await self.evaluate()
        self.assertEqual(self.monitor.incidents[0]["status"], "resolved")
        self.assertEqual(self.monitor.incidents[0]["resolution"], "condition_cleared")

    async def test_sustained_duration_resets_after_unknown_and_does_not_resolve_active(
        self,
    ):
        await self.rule(duration_seconds=60)
        await self.evaluate()
        self.assertEqual(self.monitor.incidents, [])
        self.metrics["cpu"]["fresh"] = False
        await self.evaluate()
        self.assertNotIn("rule", self.monitor._streaks)
        self.metrics["cpu"]["fresh"] = True
        self.monitor._streaks["rule"] = time.monotonic() - 61
        await self.evaluate()
        self.assertEqual(self.monitor.status()["active_count"], 1)
        self.metrics["cpu"].update(value=0, fresh=False)
        await self.evaluate()
        self.assertEqual(self.monitor.status()["active_count"], 1)
        self.assertFalse(self.monitor.incidents[0]["fresh"])

    async def test_disk_ram_and_network_thresholds_use_their_units(self):
        for metric, operator, threshold in (
            ("disk_free_gib", "below", 3),
            ("ram", "above", 90),
            ("disk", "above", 90),
            ("internet_receive", "above", 90),
            ("internet_transmit", "above", 90),
        ):
            with self.subTest(metric=metric):
                await self.rule(
                    id=metric, metric=metric, operator=operator, threshold=threshold
                )
        await self.evaluate()
        self.assertEqual(self.monitor.status()["active_count"], 5)
        self.assertEqual(
            next(
                row
                for row in self.monitor.incidents
                if row["rule_id"] == "disk_free_gib"
            )["value"],
            2,
        )

    async def test_error_frequency_filters_experiment_window_and_incomplete_history(
        self,
    ):
        now = datetime.now(UTC)
        self.views.models.return_value = [
            (
                {"experiment_id": "exp", "complete": True},
                {
                    "errors": [
                        {"occurred_at": now.isoformat()},
                        {"occurred_at": (now - timedelta(seconds=90)).isoformat()},
                    ]
                },
            ),
            (
                {"experiment_id": "other", "complete": True},
                {"errors": [{"occurred_at": now.isoformat()}]},
            ),
        ]
        await self.rule(
            kind="errors", experiment_id="exp", threshold=2, window_seconds=60
        )
        await self.evaluate()
        self.assertEqual(self.monitor.status()["active_count"], 0)
        self.views.models.return_value[0][1]["errors"].append(
            {"occurred_at": now.isoformat()}
        )
        await self.evaluate()
        self.assertEqual(self.monitor.status()["active_count"], 1)
        self.views.models.return_value[0][0]["complete"] = False
        self.views.models.return_value[0][1]["errors"].clear()
        await self.evaluate()
        self.assertEqual(self.monitor.status()["active_count"], 1)

    async def test_rule_edit_delete_and_write_failure_have_explicit_outcomes(self):
        rule = await self.rule()
        await self.evaluate()
        with (
            patch.object(self.monitor, "_write", side_effect=OSError("disk full")),
            self.assertRaises(OSError),
        ):
            await self.monitor.configure({**rule, "threshold": 100})
        self.assertEqual(self.monitor.rules[0]["threshold"], 90)
        self.assertEqual(self.monitor.incidents[0]["status"], "active")
        await self.monitor.configure(delete="rule")
        self.assertEqual(
            self.monitor.incidents[0]["resolution"], "configuration_changed"
        )
        self.assertEqual(self.monitor.status()["active_count"], 0)

    async def test_restart_preserves_incident_but_not_freshness_and_corruption_is_rejected(
        self,
    ):
        await self.rule()
        await self.evaluate()
        restored = AlertMonitor(self.root, self.views, self.icmp)
        await restored.open()
        self.assertEqual(restored.status()["active_count"], 1)
        self.assertFalse(restored.incidents[0]["fresh"])
        await restored.close()
        path = self.root / "alerts.json"
        path.write_text("{", encoding="utf-8")
        broken = AlertMonitor(self.root, self.views, self.icmp)
        with self.assertRaises(ValueError):
            await broken.open()
        self.assertEqual(path.read_text(), "{")

    async def test_icmp_is_mirrored_once_but_excluded_from_system_count(self):
        self.icmp.incidents = [
            {
                "id": "icmp-id",
                "host": "example.org",
                "status": "active",
                "started_at": datetime.now(UTC).isoformat(),
                "ended_at": None,
            }
        ]
        await self.evaluate()
        await self.evaluate()
        self.assertEqual(self.monitor.status()["active_count"], 1)
        self.assertEqual(self.monitor.status(system_only=True)["active_count"], 0)
        self.assertEqual(len(self.monitor.incidents), 1)

    async def test_notifications_are_opt_in_deduplicated_and_delivered_on_host(self):
        await self.rule()
        await self.evaluate()
        self.assertTrue(self.monitor._queue.empty())
        await self.monitor.configure(
            channels={
                "desktop": True,
                "sound": False,
                "on_recovery": True,
                "repeat_seconds": 60,
            }
        )
        await self.evaluate()
        await self.evaluate()
        self.assertEqual(self.monitor._queue.qsize(), 1)
        with patch(
            "dashboard.alerts.deliver",
            new=AsyncMock(
                return_value={"status": "submitted", "source": "dashboard_host"}
            ),
        ) as delivery:
            task = asyncio.create_task(self.monitor._deliver())
            try:
                await wait_until(lambda: self.monitor.incidents[0].get("delivery"))
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            delivery.assert_awaited_once()
        await self.evaluate()
        self.assertTrue(self.monitor._queue.empty())

    async def test_invalid_rules_gpu_and_channel_ranges_are_rejected(self):
        base = await self.rule()
        for changes in (
            {"metric": "gpu"},
            {"metric": "vram"},
            {"threshold": float("nan")},
            {"enabled": 1},
            {"duration_seconds": -1},
            {"operator": "unknown"},
            {"kind": "errors", "threshold": 1.5},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                await self.monitor.configure({**base, **changes})
        with self.assertRaises(ValueError):
            await self.monitor.configure(channels={"desktop": True})
