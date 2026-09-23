"""Approved branch plan: persistent history, windows, references and recovery."""

import copy
import json
import os
import sqlite3
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from core.historycache import (
    HistoryCacheBusy,
    HistoryCacheChanged,
    HistoryCacheLimit,
    JournalHistoryCache,
    acquire_cache_writer,
)
from core.logger import OperationLogger
from dashboard.api_client import SystemAPIError
from dashboard.config import load_settings
from dashboard.journals import LocalJournals
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import JournalWorkspace


class HistoryCacheTests(unittest.TestCase):
    def test_replaced_reader_configuration_cannot_reuse_an_old_ram_event(self):
        """T005/T031/T071: detail identity is checked even when the payload is in RAM."""
        dataset = self.build()
        identifier = next(reversed(dataset["cache"].window))
        other = JournalWorkspace(self.root / "other-project", identifier="other")
        self.addCleanup(other.close)
        replacement = json.loads(other.config.read_text(encoding="utf-8"))
        replacement["logging"]["db_path"] = str(
            other.directory / "journals/events.sqlite"
        )
        dataset["cache"].config_path.write_text(
            json.dumps(replacement), encoding="utf-8"
        )
        with self.assertRaises(HistoryCacheChanged):
            dataset["cache"].events([identifier])

    def test_failed_publication_retains_completed_boundary_until_retry(self):
        """T030: publication failure cannot advance the advertised disk prefix."""
        first = self.build()
        self.workspace.logger.record_error(ValueError("new"))
        with (
            patch.object(
                first["cache"],
                "_publication",
                side_effect=OSError("publication failed"),
            ),
            self.assertRaises(SystemAPIError),
        ):
            self.reader.load("exp-test", force=True)
        saved = json.loads(
            first["cache"].query(
                "SELECT value FROM metadata WHERE key='cached_through'"
            )[0][0]
        )
        self.assertEqual(saved, first["cached_through"])
        self.assertFalse(self.reader.cached("exp-test")["complete"])
        self.assertGreater(
            self.reader.cached("exp-test")["target_boundary"]["cursor"], saved["cursor"]
        )
        recovered = self.build()
        self.assertTrue(self.reader.cached("exp-test")["complete"])
        self.assertGreater(recovered["cached_through"]["cursor"], saved["cursor"])
        self.assertEqual(
            self.reader.page(recovered, "errors", {"compact": "1"})["total"], 1
        )

    def test_latest_ram_window_marks_an_unpublished_suffix_incomplete(self):
        """T027/T028/T055: a newer observed tail cannot be labelled fully cached."""
        first = self.build()
        self.assertTrue(self.reader.cached("exp-test")["complete"])
        self.workspace.logger.record_error(ValueError("not yet projected"))
        observed = self.reader.load("exp-test", force=True, build=False)
        current = self.reader.cached("exp-test")
        self.assertFalse(current["complete"])
        self.assertEqual(current["cached_through"], first["cached_through"])
        self.assertEqual(current["target_boundary"], observed["boundary"])
        self.assertGreater(
            current["target_boundary"]["cursor"], current["cached_through"]["cursor"]
        )
        self.build()
        self.assertTrue(self.reader.cached("exp-test")["complete"])

    def test_window_size_boundaries_and_backdated_append(self):
        """T016/T017: fewer/exact/more events and backdated timestamps use cursors."""
        count = self.workspace.logger.read_event_batch([])["boundary"]["event_count"]
        for limit in (1, count, count + 1):
            with self.subTest(limit=limit):
                reader = LocalJournals(
                    {**self.settings, "history_window_events": limit}
                )
                self.addCleanup(reader.close)
                dataset = reader.load("exp-test", force=True)
                self.assertEqual(dataset["window_count"], min(count, limit))
        event = copy.deepcopy(
            self.workspace.logger.read_event_batch(limit=1)["events"][0]["event"]
        )
        event.update(
            event_id="backdated",
            sequence_number=9999,
            occurred_at="2000-01-01T00:00:00+00:00",
            event_type="backdated",
            data={},
        )
        self.workspace.logger._store.append(event)
        dataset = self.build()
        self.assertEqual(next(reversed(dataset["cache"].window)), "backdated")

    def test_empty_history_and_total_history_larger_than_scope_budget(self):
        """T016/T090: limits apply to a working scope, not to historical totals."""
        reader = LocalJournals({**self.settings, "history_max_events": 7})
        self.addCleanup(reader.close)
        dataset = reader.load("exp-test", force=True)
        self.assertTrue(dataset["complete"])
        self.assertGreater(
            dataset["cache"].query("SELECT COUNT(*) FROM facts")[0][0], 7
        )
        data = {"state": {"experiment_id": "empty", "template": {}}, "entries": []}
        workspace = JournalWorkspace(
            self.root / "empty-project", data, identifier="empty"
        )
        self.addCleanup(workspace.close)
        empty_reader = LocalJournals(
            {
                **self.settings,
                "project_root": workspace.root,
                "state_directory": self.root / "empty-state",
            }
        )
        self.addCleanup(empty_reader.close)
        empty = empty_reader.load("empty", force=True)
        self.assertTrue(empty["complete"])
        self.assertEqual(empty["window_count"], 0)
        self.assertEqual(empty["cached_through"]["cursor"], 0)

    def test_foreign_experiment_cache_is_not_accepted(self):
        """T005: another experiment's publication cannot masquerade as this one."""
        dataset = self.build()
        with closing(sqlite3.connect(dataset["cache"].path)) as db, db:
            encoded = db.execute(
                "SELECT value FROM metadata WHERE key='source'"
            ).fetchone()[0]
            source = json.loads(encoded)
            source["experiment_id"] = "other-experiment"
            db.execute(
                "UPDATE metadata SET value=? WHERE key='source'", (json.dumps(source),)
            )
        self.assertFalse(self.reader.cached("exp-test")["complete"])
        self.assertNotIn("cache", self.reader.cached("exp-test"))

    def test_confirmation_without_new_event_updates_historical_metadata(self):
        """T026/T021: a late confirmation changes history outside the RAM window."""
        context = {"participant_id": "service", "participant_instance_id": "instance"}
        identity = self.workspace.logger.record_command_result(
            "request",
            {"ok": True},
            author="participant",
            outcome="succeeded",
            context=context,
        )
        for _ in range(5):
            self.workspace.logger.record_event("padding")
        first = self.build()
        self.assertNotIn(identity, first["cache"].window)
        before = first["cached_through"]
        confirmed = self.workspace.logger.record_command_result(
            "request",
            {"ok": True},
            author="runner",
            outcome="succeeded",
            context=context,
        )
        self.assertEqual(confirmed, identity)
        after = self.build()
        self.assertEqual(after["cached_through"]["cursor"], before["cursor"])
        self.assertGreater(
            after["cached_through"]["change_cursor"], before["change_cursor"]
        )
        row = after["cache"].events([identity])[0]
        self.assertFalse(row["provisional"])
        self.assertEqual(row["effective_author"], "runner")

    def test_read_transaction_does_not_mix_concurrent_publications(self):
        """T029/T054: worker publication does not alter an existing read snapshot."""
        first = self.build()
        writer = LocalJournals(self.settings)
        self.addCleanup(writer.close)
        self.workspace.logger.record_error(ValueError("concurrent"))
        with ThreadPoolExecutor(max_workers=1) as pool:

            def observe(dataset):
                before = dataset["cache"].query("SELECT COUNT(*) FROM facts")[0][0]
                updated = pool.submit(writer.load, "exp-test", force=True).result(
                    timeout=10
                )
                self.assertGreater(updated["version"], dataset["version"])
                self.assertEqual(
                    dataset["cache"].query("SELECT COUNT(*) FROM facts")[0][0], before
                )
                return before

            first["cache"].read_view(first["version"], observe, first)
        with self.assertRaises(HistoryCacheChanged):
            first["cache"].read_view(first["version"], observe, first)

    def setUp(self):
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        self.root = Path(temporary.name)
        self.workspace = JournalWorkspace(self.root / "project")
        self.addCleanup(self.workspace.close)
        self.settings = load_settings(
            write_settings(
                self.root,
                project_root=str(self.workspace.root),
                history_window_events=3,
            )
        )
        self.reader = LocalJournals(self.settings)
        self.addCleanup(self.reader.close)

    def build(self):
        dataset = self.reader.load("exp-test", force=True)
        for _ in range(50):
            if dataset["complete"]:
                return dataset
            dataset = self.reader.load(
                "exp-test", force=True, target=dataset["target_boundary"]
            )
        self.fail("Cache did not complete its captured boundary")

    def test_cache_rebuild_preserves_source_and_all_screen_records(self):
        """T001/T002/T003/T033: disposable cache, authoritative unchanged source."""
        connection = self.workspace.logger._store._connection
        original = list(connection.iterdump())
        first = self.build()
        records = first["cache"].query(
            "SELECT kind,record_key,payload FROM records ORDER BY kind,record_key"
        )
        self.reader.close()
        first["cache"].path.unlink()
        rebuilt = self.build()
        self.assertEqual(
            rebuilt["cache"].query(
                "SELECT kind,record_key,payload FROM records ORDER BY kind,record_key"
            ),
            records,
        )
        self.assertEqual(list(connection.iterdump()), original)
        self.assertTrue(
            rebuilt["cache"].path.is_relative_to(self.settings["state_directory"])
        )

    def test_source_alias_is_rejected(self):
        """T003: a cache cannot overwrite its own journal, including a hard link."""
        first = self.build()
        source = self.workspace.directory / "journals/events.sqlite"
        alias = self.root / "alias.sqlite"
        os.link(source, alias)
        cache = JournalHistoryCache(
            alias,
            first["cache"].config_path,
            first["identity"],
            first["file_key"],
            "exp-test",
            3,
            1000000,
        )
        with self.assertRaisesRegex(ValueError, "alias"):
            cache.open()

    def test_missing_source_does_not_create_or_repair_a_journal(self):
        """T004: reads fail explicitly after a source disappears."""
        self.build()
        self.workspace.close()
        source = self.workspace.directory / "journals/events.sqlite"
        source.unlink()
        with self.assertRaises(SystemAPIError):
            self.reader.load("exp-test", force=True)
        self.assertFalse(source.exists())

    def test_window_tail_restart_append_and_old_details(self):
        """T016/T017/T019/T020/T021: bounded raw cursor tail and indexed hydration."""
        first = self.build()
        source = self.workspace.logger.read_events(limit=1000)["events"]
        expected = [row["event"]["event_id"] for row in source[-3:]]
        self.assertEqual(list(first["cache"].window), expected)
        previous_window = copy.deepcopy(first["cache"].window)
        old_id = source[0]["event"]["event_id"]
        self.assertEqual(first["cache"].events([old_id])[0]["event_id"], old_id)
        self.assertEqual(first["cache"].window, previous_window)
        added = self.workspace.logger.record_event("later", {"value": 3})
        observed = first["cache"].observe()
        self.assertEqual(observed["window_count"], 3)
        self.assertEqual(list(first["cache"].window), [*expected[1:], added])
        self.reader.close()
        restarted = self.build()
        self.assertEqual(list(restarted["cache"].window), [*expected[1:], added])

    def test_paused_window_is_not_aged_out(self):
        """T018/T024: old wall-clock timestamps do not invalidate an unchanged tail."""
        first = self.build()
        original = list(first["cache"].window)
        with patch("time.time", return_value=time.time() + 86400 * 365):
            repeated = self.build()
        self.assertEqual(list(repeated["cache"].window), original)
        self.assertEqual(first["version"], repeated["version"])

    def test_window_budget_error_does_not_delete_published_records(self):
        """T022/T090: oversized working sets are explicit, not truncated."""
        first = self.build()
        rows = first["cache"].query("SELECT COUNT(*) FROM records")[0][0]
        first["cache"].max_bytes = 1
        self.workspace.logger.record_event("large", {"text": "x" * 1024})
        with self.assertRaises(HistoryCacheLimit):
            first["cache"].observe()
        self.assertEqual(
            first["cache"].query("SELECT COUNT(*) FROM records")[0][0], rows
        )

    def test_incremental_append_projects_only_the_changed_scope(self):
        """T025/T088: old completed cycles are not rebuilt for an unrelated append."""
        first = self.build()
        before = dict(first["cache"].query("SELECT scope,summary FROM scopes"))
        self.workspace.logger.record_error(
            ValueError("new"),
            context={
                "template_revision_id": "rev-1",
                "cycle_number": 1,
                "attempt_id": "1-0",
            },
        )
        with patch.object(
            first["cache"], "_project_scope", wraps=first["cache"]._project_scope
        ) as project:
            updated = self.build()
        changed = [json.loads(call.args[1]) for call in project.call_args_list]
        self.assertTrue(changed)
        self.assertTrue(all(key[2] == 1 for key in changed))
        after = dict(updated["cache"].query("SELECT scope,summary FROM scopes"))
        for scope, value in before.items():
            if json.loads(scope)[2] != 1:
                self.assertEqual(after[scope], value)

    def test_failed_projection_resumes_from_committed_ingestion(self):
        """T023/T027/T030: dirty scopes survive failure and the completed boundary lags."""
        first = self.build()
        self.workspace.logger.record_error(ValueError("new"))
        with (
            patch.object(
                first["cache"], "_project_scope", side_effect=ValueError("interrupted")
            ),
            self.assertRaises(SystemAPIError),
        ):
            self.reader.load("exp-test", force=True)
        metadata = dict(first["cache"].query("SELECT key,value FROM metadata"))
        self.assertGreater(
            json.loads(metadata["ingested_through"])["cursor"],
            json.loads(metadata["cached_through"])["cursor"],
        )
        self.assertTrue(first["cache"].query("SELECT scope FROM dirty"))
        self.reader.close()
        recovered = self.build()
        self.assertFalse(recovered["cache"].query("SELECT scope FROM dirty"))
        self.assertEqual(
            self.reader.page(recovered, "errors", {"compact": "1"})["total"], 1
        )

    def test_interrupted_ingestion_does_not_commit_partial_batch(self):
        """T030: fact/checkpoint/dirty writes roll back together."""
        first = self.build()
        before = dict(first["cache"].query("SELECT key,value FROM metadata"))
        self.workspace.logger.record_event("next")
        original = first["cache"]._store_event

        def interrupted(*args):
            original(*args)
            raise ValueError("interrupted after fact insert")

        with (
            patch.object(first["cache"], "_store_event", side_effect=interrupted),
            self.assertRaises(SystemAPIError),
        ):
            self.reader.load("exp-test", force=True)
        after = dict(first["cache"].query("SELECT key,value FROM metadata"))
        self.assertEqual(after["checkpoint"], before["checkpoint"])
        self.assertEqual(
            first["cache"].query("SELECT MAX(cursor) FROM facts")[0][0],
            first["cached_through"]["cursor"],
        )
        self.assertTrue(self.build()["complete"])

    def test_gap_is_filled_from_checkpoint_and_sparse_cursors_do_not_invent_gap(self):
        """T028: a real uncached prefix is distinguished from absent cursor numbers."""
        first = self.build()
        for _ in range(8):
            self.workspace.logger.record_event("appended")
        observed = first["cache"].observe()
        self.assertIsNotNone(observed["gap"])
        self.assertFalse(observed["complete"])
        filled = self.build()
        self.assertIsNone(filled["cache"].observe()["gap"])
        connection = self.workspace.logger._store._connection
        connection.execute("UPDATE sqlite_sequence SET seq=seq+100 WHERE name='events'")
        self.workspace.logger.record_event("sparse")
        self.build()
        self.assertIsNone(first["cache"].observe()["gap"])

    def test_version_migration_and_impossible_checkpoint(self):
        """T031/T032: old caches rebuild, skipped source facts cannot appear complete."""
        first = self.build()
        with closing(sqlite3.connect(first["cache"].path)) as db, db:
            source = json.loads(
                db.execute("SELECT value FROM metadata WHERE key='source'").fetchone()[
                    0
                ]
            )
            source["version"] = 4
            db.execute(
                "UPDATE metadata SET value=? WHERE key='source'", (json.dumps(source),)
            )
        self.reader.close()
        migrated = self.build()
        source = json.loads(
            migrated["cache"].query("SELECT value FROM metadata WHERE key='source'")[0][
                0
            ]
        )
        self.assertEqual(source["version"], 5)
        with closing(sqlite3.connect(migrated["cache"].path)) as db, db:
            db.execute("DELETE FROM facts WHERE cursor=(SELECT MIN(cursor) FROM facts)")
        with self.assertRaisesRegex(SystemAPIError, "skips source"):
            self.reader.load("exp-test", force=True)

    def test_writer_lock_is_exclusive_and_released_on_close(self):
        """T048/T050: exercise the native Windows or POSIX lock implementation."""
        path = self.root / "cache.sqlite"
        with acquire_cache_writer(path), self.assertRaises(HistoryCacheBusy):
            acquire_cache_writer(path)
        with acquire_cache_writer(path):
            pass

    def test_pages_details_and_publication_replacement(self):
        """T067/T068/T069/T070/T071: stable keyset pages and source-backed details."""
        first = self.build()
        page = self.reader.page(first, "parameters", {"compact": "1", "limit": 1})
        items = list(page["items"])
        while page["next_cursor"]:
            page = self.reader.page(
                first,
                "parameters",
                {"compact": "1", "limit": 1, "cursor": json.dumps(page["next_cursor"])},
            )
            items.extend(page["items"])
        self.assertEqual(len(items), 4)
        self.assertEqual(len({row["attempt_id"] for row in items}), 4)
        detail = self.reader.detail(first, items[0]["detail_ref"])
        self.assertTrue(detail["source_events"])
        self.assertIn("effective_settings", detail)
        reference = {**items[0]["detail_ref"], "generation": "other"}
        with self.assertRaises(SystemAPIError) as caught:
            self.reader.detail(first, reference)
        self.assertEqual(caught.exception.status_code, 409)
        stale = self.reader.page(first, "events", {"compact": "1", "limit": 1})[
            "next_cursor"
        ]
        self.workspace.logger.record_event("new")
        current = self.build()
        with self.assertRaises(SystemAPIError):
            self.reader.page(current, "events", {"cursor": json.dumps(stale)})

    def test_prepared_compact_pages_never_open_source(self):
        """T053/T054/T059/T063: indexed screen reads remain cache-only."""
        first = self.build()
        self.reader.publish_modules()
        with patch.object(
            OperationLogger, "open", side_effect=AssertionError("Source opened")
        ):
            cached = self.reader.cached("exp-test")
            for kind in (
                "operations",
                "parameters",
                "events",
                "errors",
                "artifacts",
                "measurements",
                "commands",
                "runs",
            ):
                with self.subTest(kind=kind):
                    self.reader.page(cached, kind, {"compact": "1"})
            self.assertTrue(self.reader.modules()["complete"])
        self.assertEqual(first["version"], cached["version"])
