"""Approved D: local SQLite history, immutable snapshots, paths and publication limits."""

import json
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from dashboard.api_client import SystemAPIClient, SystemAPIError
from dashboard.config import load_settings
from dashboard.journals import LocalJournals
from dashboard.views import DashboardViews
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import JournalWorkspace


class JournalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        tmp = temporary_directory()
        self.addCleanup(cleanup_directory, tmp)
        self.root = Path(tmp.name)
        self.workspace = JournalWorkspace(self.root / "project")
        self.addCleanup(self.workspace.close)
        config = write_settings(self.root, project_root=str(self.workspace.root))
        self.settings = load_settings(config)
        self.reader = LocalJournals(self.settings)
        self.views = DashboardViews(self.settings, SystemAPIClient(self.settings))

    async def test_readonly_snapshot_is_reused_and_not_mutated_by_later_ingestion(self):
        before = self.reader.load("exp-test")
        self.assertIs(before, self.reader.load("exp-test"))
        size = len(before["entries"])
        self.workspace.logger.record_event("later.event")
        after = self.reader.load("exp-test", force=True)
        self.assertEqual(len(before["entries"]), size)
        self.assertEqual(len(after["entries"]), size + 1)
        self.assertIsNot(before, after)
        self.assertFalse(list(self.workspace.directory.rglob("*.emergency*")))

    async def test_late_runner_confirmation_reconciles_service_observation(self):
        context = {"participant_id": "service", "participant_instance_id": "instance"}
        first = self.workspace.logger.record_command_result(
            "request",
            {"ok": 1},
            author="participant",
            outcome="succeeded",
            context=context,
        )
        before = self.reader.load("exp-test")
        self.assertTrue(
            next(row for row in before["entries"] if row["event_id"] == first)[
                "provisional"
            ]
        )
        self.workspace.logger.record_command_result(
            "request", {"ok": 1}, author="runner", outcome="succeeded", context=context
        )
        after = self.reader.load("exp-test", force=True)
        effective = [
            row
            for row in after["entries"]
            if row["event_type"] == "command.result" and row.get("effective")
        ]
        self.assertEqual(len(effective), 1)
        self.assertFalse(effective[0]["provisional"])
        self.assertTrue(
            next(row for row in before["entries"] if row["event_id"] == first)[
                "provisional"
            ]
        )

    async def test_generation_change_invalidates_warm_cache_and_old_page(self):
        page = await self.views.experiment("exp-test", "events", {"limit": 1})
        self.workspace.close()
        generation = uuid4().hex
        path = self.workspace.directory / "journals/events.sqlite"
        with closing(sqlite3.connect(path)) as db:
            db.execute("UPDATE journal_info SET generation=?", (generation,))
            db.commit()
        identity = {**self.workspace.identity, "generation": generation}
        (self.workspace.directory / "runner/journal.json").write_text(
            json.dumps(identity), encoding="utf-8"
        )
        with self.assertRaises(SystemAPIError) as caught:
            await self.views.experiment(
                "exp-test", "events", {"cursor": json.dumps(page["next_cursor"])}
            )
        self.assertEqual(caught.exception.code, "history_changed")
        self.assertEqual(
            (await self.views.experiment("exp-test", "summary", {}))["journal"],
            identity,
        )

    async def test_corrupt_registry_and_one_bad_journal_do_not_invent_zero_counts(self):
        (self.workspace.root / "experiments.json").write_text("{", encoding="utf-8")
        overview = await self.views.read("overview", {})
        self.assertIsNone(overview["metrics"]["completed_experiments"])
        self.assertTrue(overview["error"])
        (self.workspace.root / "experiments.json").write_text(
            json.dumps({"exp-test": "recorded", "broken": "broken"}), encoding="utf-8"
        )
        broken = self.workspace.root / "experiments/broken/runner"
        broken.mkdir(parents=True)
        (broken / "state.json").write_text("{", encoding="utf-8")
        rows = (await self.views.read("experiments", {}))["items"]
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            next(row for row in rows if row["experiment_id"] == "broken")["status"],
            "unavailable",
        )

    async def test_registry_rejects_traversal_and_missing_experiment(self):
        with self.assertRaises(SystemAPIError):
            self.reader.load("missing")
        for path in ("..", "../outside", str(self.root)):
            (self.workspace.root / "experiments.json").write_text(
                json.dumps({"bad": path}), encoding="utf-8"
            )
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.reader.registry()

    async def test_history_limits_are_errors_and_publications_expire(self):
        limited = LocalJournals({**self.settings, "history_max_events": 1})
        with self.assertRaises(SystemAPIError) as caught:
            limited.load("exp-test")
        self.assertEqual(caught.exception.code, "history_limit")
        page = await self.views.experiment("exp-test", "events", {"limit": 1})
        token = page["next_cursor"]["publication"]
        self.views._publications[token]["at"] -= 301
        with self.assertRaises(SystemAPIError) as expired:
            await self.views.experiment(
                "exp-test", "events", {"cursor": json.dumps(page["next_cursor"])}
            )
        self.assertEqual(expired.exception.status_code, 409)
        self.views.settings["max_response_bytes"] = 1024
        with self.assertRaises(SystemAPIError) as large:
            await self.views.experiment("exp-test", "parameters", {})
        self.assertEqual(large.exception.status_code, 413)

    async def test_artifact_uses_attempt_directory_and_missing_file_returns_404(self):
        context = {
            "attempt_id": "1-0",
            "cycle_number": 1,
            "stage_id": "A",
            "attempt_number": 1,
            "module_name": "short",
        }
        identity = self.workspace.logger.record_artifact(
            "result.txt", "output", context=context
        )
        directory = (
            self.workspace.directory / "shared_artifacts/epoch_1/short/A/attempt_1"
        )
        directory.mkdir(parents=True)
        target = directory / "result.txt"
        target.write_text("recorded", encoding="utf-8")
        self.assertEqual(self.reader.artifact("exp-test", identity), target.resolve())
        target.unlink()
        with self.assertRaises(SystemAPIError) as caught:
            self.reader.artifact("exp-test", identity)
        self.assertEqual(caught.exception.status_code, 404)
        with self.assertRaises(ValueError):
            self.reader.safe_path(directory, "../../../../../../outside")

    async def test_bad_snapshot_manifest_is_visible(self):
        directory = self.workspace.root / "snapshots/recorded/snapshot"
        directory.mkdir(parents=True)
        (directory / "manifest.json").write_text("{", encoding="utf-8")
        self.assertFalse(self.reader.snapshots("exp-test")[0]["available"])

    async def test_model_cache_reuses_projection_then_refreshes_when_snapshot_changes(
        self,
    ):
        import dashboard.views as module

        original = module.experiment_views
        with patch.object(module, "experiment_views", wraps=original) as project:
            await self.views.experiment("exp-test", "summary", {})
            await self.views.experiment("exp-test", "operations", {})
            self.assertEqual(project.call_count, 1)
            self.views.journals.load("exp-test", force=True)
            await self.views.experiment("exp-test", "summary", {})
            self.assertEqual(project.call_count, 2)
