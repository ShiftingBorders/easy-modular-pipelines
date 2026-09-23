"""Approved C/F and navigation additions: cache liveness and durable command outcomes."""

import asyncio
import json
import os
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from dashboard.api_client import SystemAPIError
from dashboard.config import load_settings
from dashboard.views import DashboardViews
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import resource_status
from tests.helpers.dag import wait_until


class ViewTests(unittest.IsolatedAsyncioTestCase):
    def test_command_publication_handles_platform_reader_semantics(self):
        """T050/T074: command-state publication survives native reader semantics."""
        self.views._write_commands()
        path = self.config["state_directory"] / "commands.json"
        self.views._commands = [
            {"command_id": str(uuid4()), "status": "pending", "polling": True}
        ]
        if os.name == "nt":
            failure = PermissionError("MoveFileEx denied replacement")
            failure.winerror = 32
            with patch("core.runner_utils.runtimeio.os.replace", side_effect=failure):
                self.views._write_commands()
        else:
            with path.open("r", encoding="utf-8") as previous:
                self.views._write_commands()
                self.assertEqual(json.load(previous), {"items": []})
        self.assertEqual(
            json.loads(path.read_text(encoding="utf-8")),
            {"items": self.views._commands},
        )
        self.assertFalse(list(path.parent.glob(".publish-*")))

    async def test_concurrent_command_polling_shares_one_history_refresh(self):
        """T074/T075: successful command observers share the same history task."""
        self.views.settings["project_root"] = self.root
        self.views._cache_pool = Mock()
        receipt = await self.views.command({"command": "pause"})
        entered, release = asyncio.Event(), asyncio.Event()

        async def refresh(identifier):
            self.assertEqual(identifier, "exp")
            entered.set()
            await release.wait()

        with patch.object(
            self.views, "_refresh_command_history", side_effect=refresh
        ) as update:
            first = asyncio.create_task(
                self.views.command_result(receipt["command_id"])
            )
            await entered.wait()
            second = asyncio.create_task(
                self.views.command_result(receipt["command_id"])
            )
            await asyncio.sleep(0)
            release.set()
            results = await asyncio.gather(first, second)
        update.assert_awaited_once_with("exp")
        self.assertTrue(all(result["result"] == "success" for result in results))
        self.api.submit.assert_awaited_once()

    async def test_cache_failure_does_not_reverse_successful_command_outcome(self):
        """T076: runtime success remains success even when cache refresh fails."""
        self.views.settings["project_root"] = self.root
        self.views._cache_pool = Mock()
        self.views._cache_pool.submit.side_effect = OSError("worker unavailable")
        receipt = await self.views.command({"command": "pause"})
        result = await self.views.command_result(receipt["command_id"])
        self.assertEqual(result["result"], "success")
        self.assertEqual(
            self.views._cache_errors["exp"]["code"], "history_refresh_failed"
        )
        self.assertNotIn("exp", self.views._command_refreshing)

    async def test_failed_cancelled_and_expired_results_do_not_repeat_commands(self):
        for state in ("failed", "cancelled"):
            receipt = await self.views.command({"command": "pause"})
            self.result.update(state=state, result="fail")
            result = await self.views.command_result(receipt["command_id"])
            self.assertEqual(result["state"], state)
            self.assertFalse(self.views._commands[-1]["polling"])
        await self.views.command({"command": "pause"})
        self.api.read.side_effect = SystemAPIError("unavailable", "expired", 404)
        task = asyncio.create_task(self.views._poll_commands())
        try:
            await wait_until(lambda: not self.views._commands[-1]["polling"])
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(self.views._commands[-1]["status"], "unknown")
        self.assertEqual(self.api.submit.await_count, 3)

    async def test_receipt_persistence_failure_leaves_reconcilable_submission(self):
        write = self.views._write_commands
        writes = 0

        def fail_after_send():
            nonlocal writes
            writes += 1
            if writes == 2:
                raise OSError("disk became read only")
            write()

        with (
            patch.object(self.views, "_write_commands", side_effect=fail_after_send),
            self.assertRaises(OSError),
        ):
            await self.views.command({"command": "pause"})
        saved = json.loads(
            (self.config["state_directory"] / "commands.json").read_text()
        )
        self.assertEqual(saved["items"][0]["status"], "submitting")
        self.api.submit.assert_awaited_once()
        self.assertEqual(self.views._commands[0]["status"], "pending")

    async def asyncSetUp(self):
        tmp = temporary_directory()
        self.addCleanup(cleanup_directory, tmp)
        self.root = Path(tmp.name)
        self.config = load_settings(write_settings(self.root))
        self.api = SimpleNamespace(
            base_url="http://runtime/",
            read=AsyncMock(side_effect=self.read),
            submit=AsyncMock(side_effect=self.submit),
        )
        self.views = DashboardViews(self.config, self.api)
        self.addAsyncCleanup(self.views.close)
        self.status = resource_status()
        self.result = {
            "state": "succeeded",
            "result": "success",
            "server_instance_id": "server",
            "experiment_id": "exp",
        }

    async def read(self, path, params=None):
        if path == "state":
            return {
                "fresh": True,
                "experiment_id": "exp",
                "server_instance_id": "server",
                "phase": "waiting",
                "mode": "paused",
            }
        if path == "resources":
            return self.status
        if path == "resources/history":
            return {"samples": [], "cursor": 0, "gap": False, "history_id": "history"}
        return self.result

    async def submit(self, document):
        saved = json.loads(
            (self.config["state_directory"] / "commands.json").read_text()
        )
        self.assertEqual(saved["items"][-1]["status"], "submitting")
        self.assertEqual(saved["items"][-1]["command_id"], document["command_id"])
        return {
            "command_id": document["command_id"],
            "state": "pending",
            "server_instance_id": "server",
        }

    async def test_pages_never_wait_for_blocked_live_sources_and_age_becomes_stale(
        self,
    ):
        await self.views.state(refresh=True)
        await self.views._refresh_resources()
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocked(*args):
            started.set()
            await release.wait()

        self.api.read.side_effect = blocked
        self.views._resource_at = 0
        pending = asyncio.create_task(self.views._refresh_resources())
        try:
            await started.wait()
            result = await asyncio.wait_for(self.views.compute({}), 0.5)
            self.assertEqual(result["metrics"]["cpu"]["value"], 25)
            self.assertFalse(pending.done())
            self.views._resource_observed_at -= 4
            self.views._live_at -= 4
            self.assertFalse((await self.views.compute({}))["metrics"]["cpu"]["fresh"])
            self.assertFalse((await self.views.state())["fresh"])
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)

    async def test_failed_history_does_not_invalidate_fresh_status(self):
        async def read(path, params=None):
            if path.endswith("history"):
                raise SystemAPIError("timeout", "history timeout")
            return self.status

        self.api.read.side_effect = read
        await self.views._refresh_resources()
        result = await self.views.compute({})
        self.assertTrue(result["metrics"]["cpu"]["fresh"])
        self.assertEqual(result["history_error"], "history timeout")

    async def test_resource_history_generation_gap_ranges_and_malformed_sample(self):
        sample = self.status["latest"][0]
        calls = 0

        async def read(path, params=None):
            nonlocal calls
            if path == "resources":
                return self.status
            calls += 1
            return {
                "samples": [sample] if calls == 1 else [],
                "cursor": 10,
                "gap": True,
                "history_id": "history",
            }

        self.api.read.side_effect = read
        await self.views._refresh_resources()
        result = await self.views.compute({"since": "2000-01-01T00:00:00+00:00"})
        self.assertTrue(result["gap"])
        self.assertEqual(len(result["history"]["cpu"]), 1)
        self.assertEqual(
            (await self.views.compute({"until": "2000-01-01T00:00:00+00:00"}))[
                "history"
            ]["cpu"],
            [],
        )
        with self.assertRaises(SystemAPIError):
            await self.views.compute({"since": "bad"})
        self.status = {"state": "running", "latest": [{}], "history_id": "other"}
        self.views._resource_at = 0
        await self.views._refresh_resources()
        self.assertTrue((await self.views.compute({}))["error"])

    async def test_commands_persist_before_submission_and_pending_is_not_success(self):
        receipt = await self.views.command({"command": "pause"})
        self.assertEqual(receipt["state"], "pending")
        self.assertTrue(self.views._commands[0]["polling"])
        result = await self.views.command_result(receipt["command_id"])
        self.assertEqual(result["result"], "success")
        self.assertFalse(self.views._commands[0]["polling"])
        with self.assertRaises(SystemAPIError) as duplicate:
            await self.views.command(
                {"command": "pause", "command_id": receipt["command_id"]}
            )
        self.assertEqual(duplicate.exception.status_code, 409)
        self.api.submit.assert_awaited_once()

    async def test_failure_before_send_leaves_no_command_or_network_side_effect(self):
        with (
            patch.object(
                self.views, "_write_commands", side_effect=OSError("disk full")
            ),
            self.assertRaises(SystemAPIError) as failure,
        ):
            await self.views.command({"command": "pause"})
        self.assertEqual(failure.exception.code, "command_storage_unavailable")
        self.assertEqual(self.views._commands, [])
        self.api.submit.assert_not_awaited()

    async def test_unknown_send_outcome_is_polled_but_never_resent(self):
        self.api.submit.side_effect = SystemAPIError("timeout", "lost response")
        with self.assertRaises(SystemAPIError):
            await self.views.command({"command": "pause"})
        self.assertEqual(self.views._commands[0]["status"], "unknown")
        task = asyncio.create_task(self.views._poll_commands())
        try:
            await wait_until(lambda: self.views._commands[0]["status"] == "succeeded")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.api.submit.assert_awaited_once()

    async def test_wrong_runtime_selection_uses_fresh_preflight_and_rejects_send(self):
        self.views._live = {"experiment_id": "wrong", "fresh": True}
        self.views._live_at = time.monotonic()
        with self.assertRaises(SystemAPIError) as caught:
            await self.views.command(
                {"command": "pause", "expected_experiment_id": "wrong"}
            )
        self.assertEqual(caught.exception.code, "selection_changed")
        self.api.read.assert_awaited_with("state")
        self.api.submit.assert_not_awaited()

    async def test_server_instance_change_cannot_confirm_old_command(self):
        receipt = await self.views.command({"command": "stop"})
        self.result["server_instance_id"] = "replacement"
        result = await self.views.command_result(receipt["command_id"])
        self.assertEqual(result["state"], "unknown")
        self.assertIsNone(result["result"])
        self.assertFalse(self.views._commands[0]["polling"])

    async def test_restart_reconciles_submitting_record_and_closes_background_tasks(
        self,
    ):
        directory = self.config["state_directory"]
        directory.mkdir()
        (directory / "commands.json").write_text(
            json.dumps(
                {
                    "items": [
                        {
                            "command_id": str(uuid4()),
                            "status": "submitting",
                            "polling": False,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        await self.views.open()
        tasks = list(self.views._source_tasks) + [self.views._command_task]
        await wait_until(lambda: self.views._commands[0]["status"] == "succeeded")
        self.api.submit.assert_not_awaited()
        await self.views.close()
        self.assertTrue(all(task.done() for task in tasks))

    async def test_corrupt_command_history_is_not_overwritten(self):
        directory = self.config["state_directory"]
        directory.mkdir()
        path = directory / "commands.json"
        path.write_text("{", encoding="utf-8")
        with self.assertRaises(ValueError):
            await self.views.open()
        self.assertEqual(path.read_text(), "{")
