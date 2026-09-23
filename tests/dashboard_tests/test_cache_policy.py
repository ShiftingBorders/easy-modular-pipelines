"""Approved lifecycle policy: automatic active histories and explicit stopped reads."""

import asyncio
import json
import unittest
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from unittest.mock import Mock, patch

from dashboard.api_client import SystemAPIClient
from dashboard.config import load_settings
from dashboard.journals import cache_experiment
from dashboard.views import DashboardViews
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import JournalWorkspace, history


class CachePolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_broken_pool_is_replaced_without_losing_captured_target(self):
        """T049/T050: pool recovery retries the saved target in a replacement pool."""
        target = {"cursor": 50, "change_cursor": 50}
        self.views._cache_targets["stopped"] = target
        self.views._cache_requests.add("stopped")
        self.pool.submit.side_effect = BrokenProcessPool("worker died")
        replacement = Mock()
        replacement.submit.return_value = Future()
        with patch("dashboard.views.ProcessPoolExecutor", return_value=replacement):
            await self.views._submit_cache("stopped")
        self.pool.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
        self.assertIs(self.views._cache_pool, replacement)
        self.assertEqual(self.views._cache_targets["stopped"], target)
        await self.views._submit_cache("stopped")
        replacement.submit.assert_called_once_with(
            cache_experiment, self.settings, "stopped", target
        )

    async def asyncSetUp(self):
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        self.root = Path(temporary.name)
        self.workspaces = []
        for identifier, phase, mode in (
            ("running", "stage_running", "running"),
            ("paused", "waiting", "paused"),
            ("stopped", "stopped", "paused"),
            ("completed", "completed", "running"),
            ("failed", "failed", "running"),
            ("idle", "idle", "paused"),
        ):
            data = history(((1, 2),))
            data["state"].update(phase=phase, mode=mode)
            workspace = JournalWorkspace(
                self.root / "project", data, identifier=identifier, folder=identifier
            )
            self.workspaces.append(workspace)
            self.addCleanup(workspace.close)
            workspace.close()
        self.settings = load_settings(
            write_settings(self.root, project_root=str(self.root / "project"))
        )
        self.views = DashboardViews(self.settings, SystemAPIClient(self.settings))
        self.addCleanup(self.views.journals.close)
        self.views._registry = self.views.journals.registry()
        await self.views._refresh_cache_selection()
        self.pool = Mock()
        self.pool.submit.side_effect = lambda *args: Future()
        self.views._cache_pool = self.pool

    async def test_only_running_and_paused_are_automatic(self):
        """T052/T053: metadata classification never opens a stopped journal."""
        with patch(
            "core.logger.OperationLogger.open",
            side_effect=AssertionError("Unexpected source read"),
        ):
            await self.views._refresh_cache_selection()
            self.assertEqual(self.views._automatic_caches, {"running", "paused"})
            for page in ("experiments", "overview", "modules"):
                await self.views.read(page, {})
        self.pool.submit.assert_not_called()
        self.assertFalse(self.views._opened_caches)

    async def test_opening_one_stopped_experiment_requests_only_its_cache(self):
        """T045/T052: a stopped history starts on selection, not on listing."""
        response = await self.views.experiment("stopped", "errors", {"compact": "1"})
        self.assertTrue(response["complete"])
        self.assertEqual(response["source"], "ram_window")
        self.assertTrue(response["cache_pending"])
        self.assertEqual(self.views._opened_caches, {"stopped"})
        self.pool.submit.assert_called_once_with(
            cache_experiment, self.settings, "stopped", None
        )
        await self.views.experiment("stopped", "artifacts", {"compact": "1"})
        self.assertEqual(self.pool.submit.call_count, 1)
        self.assertEqual(self.views.cache_activity()["building"], ["stopped"])

    async def test_concurrent_open_does_not_submit_duplicate_writers(self):
        """T048/T075: concurrent tabs share one pending writer."""
        await asyncio.gather(
            *(
                self.views.experiment("stopped", view, {})
                for view in ("errors", "artifacts", "summary")
            )
        )
        self.assertEqual(self.pool.submit.call_count, 1)
        self.assertEqual(len(self.views.cache_activity()["active"]), 1)

    async def test_finished_stopped_cache_is_not_scheduled_again(self):
        """T024/T052: completion ends an explicit stopped build."""
        await self.views.experiment("stopped", "summary", {})
        future = self.views._cache_jobs["stopped"]
        future.set_result(
            {
                "complete": True,
                "target_boundary": {"cursor": 1, "change_cursor": 1},
                "cached_through": {"cursor": 1, "change_cursor": 1},
            }
        )
        self.views._collect_cache_jobs()
        self.assertNotIn("stopped", self.views._cache_requests)
        self.assertNotIn("stopped", self.views._cache_targets)
        self.assertEqual(self.views.cache_activity()["building"], [])
        await self.views.experiment("stopped", "summary", {})
        self.assertEqual(self.pool.submit.call_count, 1)

    async def test_state_transition_changes_background_selection(self):
        """T052/T055: transitions to and from stopped follow runner metadata."""
        path = self.workspaces[0].directory / "runner/state.json"
        document = json.loads(path.read_text(encoding="utf-8"))
        document.update(phase="stopped", mode="paused")
        path.write_text(json.dumps(document), encoding="utf-8")
        await self.views._refresh_cache_selection()
        self.assertNotIn("running", self.views._automatic_caches)
        document.update(phase="waiting", mode="paused")
        path.write_text(json.dumps(document), encoding="utf-8")
        await self.views._refresh_cache_selection()
        self.assertIn("running", self.views._automatic_caches)

    async def test_initial_build_status_spans_batches_and_clears_on_error(self):
        """T056/T087: slow initial construction remains explicitly visible."""
        await self.views.experiment("stopped", "summary", {})
        future = self.views._cache_jobs["stopped"]
        target = {"event_count": 10000, "cursor": 10000, "change_cursor": 10000}
        future.set_result(
            {
                "complete": False,
                "target_boundary": target,
                "cached_through": {"cursor": 100, "change_cursor": 100},
            }
        )
        self.views._collect_cache_jobs()
        self.assertEqual(self.views.cache_activity()["building"], ["stopped"])
        await self.views._submit_cache("stopped")
        active = self.views.cache_activity()["active"][0]
        self.assertTrue(active["initial"])
        self.assertEqual(active["event_count"], 10000)
        self.views._cache_jobs["stopped"].set_result(
            {"error": {"code": "journal_unavailable", "message": "Unavailable"}}
        )
        self.views._collect_cache_jobs()
        self.assertFalse(self.views.cache_activity()["building"])
