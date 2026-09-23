"""Approved D: local SQLite history, immutable snapshots, paths and publication limits."""

import json
import sqlite3
import subprocess
import sys
import textwrap
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.runner_utils.runtimeio import write_json
from dashboard.api_client import SystemAPIClient, SystemAPIError
from dashboard.config import load_settings
from dashboard.journals import LocalJournals, read_object
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

    def test_metadata_byte_limits_and_invalid_reads_release_handles(self):
        path = self.root / "metadata.json"
        with self.assertRaises(FileNotFoundError):
            read_object(path)
        encoded = json.dumps({"text": "погода"}, ensure_ascii=False).encode("utf-8")
        path.write_bytes(encoded)
        self.assertEqual(read_object(path, len(encoded)), {"text": "погода"})
        with self.assertRaisesRegex(ValueError, "size limit"):
            read_object(path, len(encoded) - 1)
        large = json.dumps({"text": "Ж" * 70000}, ensure_ascii=False).encode("utf-8")
        path.write_bytes(large)
        self.assertEqual(read_object(path, len(large))["text"], "Ж" * 70000)
        with self.assertRaisesRegex(ValueError, "size limit"):
            read_object(path, len(large) - 1)
        for payload, error in ((b'{"unfinished":', ValueError), (b"[]", TypeError)):
            path.write_bytes(payload)
            with self.assertRaises(error):
                read_object(path)
            write_json(path, {"readers_closed": True})
            self.assertEqual(read_object(path), {"readers_closed": True})

    def test_separate_dashboard_reader_observes_only_complete_publications(self):
        """Use real processes and file sharing on both Windows and Linux."""
        path = self.root / "metadata.json"
        write_json(path, {"sequence": 0, "payload": "0" * 1024})
        code = textwrap.dedent('''
            import json
            import sys
            import time
            from pathlib import Path
            from dashboard.journals import read_object
            path = Path(sys.argv[1])
            print("ready", flush=True)
            for _ in range(500):
                document = read_object(path)
                if document["payload"] != str(document["sequence"]) * 1024:
                    raise RuntimeError("Mixed or partial publication")
                time.sleep(0.001)
            sys.stdin.readline()
            print(json.dumps(read_object(path)), flush=True)
        ''')
        process = subprocess.Popen(
            [sys.executable, "-B", "-c", code, str(path)],
            cwd=Path(__file__).resolve().parents[2],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8",
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        try:
            self.assertEqual(process.stdout.readline().strip(), "ready")
            for sequence in range(1, 151):
                write_json(path, {"sequence": sequence, "payload": str(sequence) * 1024})
            output, errors = process.communicate("done\n", timeout=20)
            self.assertEqual(process.returncode, 0, errors)
            self.assertEqual(json.loads(output), {"sequence": 150, "payload": "150" * 1024})
            self.assertEqual(list(self.root.glob(".publish-*")), [])
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=10)

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
        """T073: historical artifact references enforce path and existence checks."""
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
        """T093: unchanged reads reuse history; appends update visible results."""
        self.addCleanup(self.views.journals.close)
        before = self.views.journals.load("exp-test", force=True)
        self.assertTrue(before["complete"])
        with patch(
            "dashboard.views.experiment_views",
            side_effect=AssertionError("Cached pages must not project full history"),
        ):
            summary = await self.views.experiment("exp-test", "summary", {})
            operations = await self.views.experiment(
                "exp-test", "operations", {"compact": "1"}
            )
            with patch.object(
                before["cache"],
                "_project_scope",
                side_effect=AssertionError("Unchanged history must not be rebuilt"),
            ):
                unchanged = self.views.journals.load("exp-test", force=True)
                repeated = await self.views.experiment("exp-test", "summary", {})
                repeated_operations = await self.views.experiment(
                    "exp-test", "operations", {"compact": "1"}
                )
            self.assertEqual(unchanged["version"], before["version"])
            self.assertEqual(unchanged["cached_through"], before["cached_through"])
            self.assertEqual(repeated["summary"], summary["summary"])
            self.assertEqual(repeated_operations["items"], operations["items"])

            error_id = self.workspace.logger.record_error(ValueError("New failure"))
            after = self.views.journals.load("exp-test", force=True)
            refreshed = await self.views.experiment("exp-test", "summary", {})
            errors = await self.views.experiment("exp-test", "errors", {"compact": "1"})
            self.assertTrue(after["complete"])
            self.assertGreater(after["version"], before["version"])
            self.assertGreater(
                after["cached_through"]["cursor"], before["cached_through"]["cursor"]
            )
            self.assertEqual(refreshed["error_count"], summary["error_count"] + 1)
            self.assertEqual(errors["total"], refreshed["error_count"])
            self.assertIn(error_id, [row["error_id"] for row in errors["items"]])
