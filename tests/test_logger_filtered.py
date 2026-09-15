"""Approved G1-G8: real SQLite publications, fallback and explicit scheduling."""

import json
import os
import shutil
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.logger import OperationLogger
from core.logger_utils.events import (
    JournalGenerationChanged,
    LoggingStateError,
    LoggingStorageError,
)
from core.logger_utils.filtered import FilteredJournal
from tests.helpers.logging_fixtures import write_context_settings
from tests.helpers.logging_process import (
    SCRATCH_ROOT,
    LoggingProcess,
    cleanup_directory,
    make_store,
    read_database,
)


class FilteredJournalTests(unittest.TestCase):
    def test_run_preserves_primary_failure_when_closing_also_fails(self):
        """G7/E6: worker cleanup cannot replace the original failure."""
        self.view.close()
        primary = RuntimeError("refresh interrupted")
        secondary = RuntimeError("close failed")
        with (
            patch.object(self.view, "refresh", side_effect=primary),
            patch.object(self.view, "close", side_effect=secondary),
            self.assertRaises(RuntimeError) as caught,
        ):
            self.view.run(threading.Event())
        self.assertIs(caught.exception, primary)
        self.assertTrue(any("cleanup" in note for note in primary.__notes__))

    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.config = write_context_settings(self.folder)
        self.path = self.folder / "events.db"
        self.logger = OperationLogger(self.config)
        self.addCleanup(self.logger.close)
        self.logger.open()
        self.view_config = write_context_settings(
            self.folder / "reader", db_path=str(self.path), open_mode="existing"
        )
        self.view_path = self.folder / "view.db"
        self.view = FilteredJournal(self.view_config, self.view_path)
        self.addCleanup(self.view.close)
        self.view.open()

    def reader_process(self, mode):
        worker = LoggingProcess(
            mode,
            self.view_config,
            str(self.view_path),
            module_name="tests.helpers.logging_fixtures",
        )
        self.addCleanup(worker.close)
        worker.start()
        return worker

    def test_primary_paths_and_links_cannot_be_used_as_view(self):
        """G1: derived creation cannot overwrite primary data or its settings."""
        for path in (self.path, self.view_config):
            with self.subTest(path=path), self.assertRaises(ValueError):
                FilteredJournal(self.view_config, path).open()
        alias = self.folder / "alias.db"
        os.link(self.path, alias)
        with self.assertRaises(ValueError):
            FilteredJournal(self.view_config, alias).open()
        with self.assertRaises(ValueError):
            FilteredJournal(self.config, self.folder / "invalid-mode.db").open()
        self.logger.record_event("primary.healthy")

    def test_first_build_and_incremental_updates_preserve_effective_authority(self):
        """G2/G8: retraction and same-ID confirmation are both applied."""
        service = self.logger.record_command_result(
            "conflict", {}, author="service", outcome="succeeded"
        )
        matched = self.logger.record_command_result(
            "matched", {}, author="service", outcome="succeeded"
        )
        self.view.refresh()
        initial = self.view.read_events()
        self.assertEqual(
            [entry["event"]["event_id"] for entry in initial["events"]],
            [service, matched],
        )
        self.assertTrue(all(entry["provisional"] for entry in initial["events"]))
        checkpoint = self.logger.read_changes()["checkpoint"]
        runner = self.logger.record_command_result(
            "conflict", {"error": "timeout"}, author="runner", outcome="timed_out"
        )
        self.assertEqual(
            self.logger.record_command_result(
                "matched", {}, author="runner", outcome="succeeded"
            ),
            matched,
        )
        pending = self.logger.read_changes(checkpoint)["changes"]
        self.assertEqual(len(pending), 2)
        # The alert feed sees confirmed changes before the next publication.
        self.assertTrue(all(not item["provisional"] for item in pending))
        self.assertEqual(
            self.view.read_events()["publication_id"], initial["publication_id"]
        )
        self.view.refresh()
        updated = self.view.read_events()
        self.assertEqual(
            [entry["event"]["event_id"] for entry in updated["events"]],
            [matched, runner],
        )
        self.assertTrue(
            all(entry["effective_author"] == "runner" for entry in updated["events"])
        )
        self.assertEqual(updated["events"][0]["event"]["data"]["author"], "service")
        self.assertEqual(len(read_database(self.path)), 3)

    def test_publication_pages_reject_mixing_snapshots_and_keep_freshness(self):
        """G4: unchanged content keeps publication identity; changed content invalidates pagination."""
        for number in range(3):
            self.logger.record_event("page", {"number": number})
        first_publication = self.view.refresh()
        first = self.view.read_events(limit=1)
        second = self.view.read_events(first["checkpoint"], limit=1)
        self.assertEqual(second["events"][0]["event"]["data"], {"number": 1})
        unchanged = self.view.refresh()
        self.assertEqual(
            unchanged["publication_id"], first_publication["publication_id"]
        )
        self.assertGreaterEqual(
            unchanged["published_at"], first_publication["published_at"]
        )
        self.logger.record_event("new")
        self.view.refresh()
        with self.assertRaises(LoggingStateError):
            self.view.read_events(first["checkpoint"])
        self.assertEqual(self.view.read_events()["source"], "filtered")

    def test_unpublished_view_falls_back_and_detects_source_page_changes(self):
        """G5/G4: fallback still filters and does not mix effective results between pages."""
        self.logger.record_event("first")
        self.logger.record_event("second")
        page = self.view.read_events(limit=1)
        self.assertEqual(page["source"], "journal")
        self.assertIsNone(page["published_at"])
        self.logger.record_event("third")
        with self.assertRaises(LoggingStateError):
            self.view.read_events(page["checkpoint"])

    def test_another_process_sees_only_complete_publications(self):
        """G3: a reader on a separate connection/process sees the old committed view during refresh."""
        first = self.logger.record_event("first")
        initial = self.view.refresh()
        worker = self.reader_process("view_reader")
        self.assertTrue(worker.receive()["ready"])
        second = self.logger.record_event("second")
        original = self.view._write_entry
        observed = []

        def read_before_commit(entry):
            original(entry)
            worker.send("read")
            observed.append(worker.receive())

        with patch.object(self.view, "_write_entry", side_effect=read_before_commit):
            self.assertIsNotNone(self.view.refresh())
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0]["ids"], [first])
        self.assertEqual(observed[0]["publication_id"], initial["publication_id"])
        worker.send("read")
        self.assertEqual(worker.receive()["ids"], [first, second])
        worker.send("stop")
        self.assertEqual(worker.wait(), 0)

    def test_killed_publication_keeps_the_previous_view_and_primary_history(self):
        """G3/E2: killing a publisher inside its transaction cannot expose a partial view."""
        first = self.logger.record_event("first")
        initial = self.view.refresh()
        second = self.logger.record_event("second")
        before = read_database(self.path)
        worker = self.reader_process("view_crash")
        self.assertEqual(worker.receive(), {"phase": "view_uncommitted"})
        worker.kill_writer()
        self.assertNotEqual(worker.wait(), 0)
        page = self.view.read_events()
        self.assertEqual(page["publication_id"], initial["publication_id"])
        self.assertEqual(
            [entry["event"]["event_id"] for entry in page["events"]], [first]
        )
        self.assertEqual(read_database(self.path), before)
        self.view.refresh()
        self.assertEqual(
            [entry["event"]["event_id"] for entry in self.view.read_events()["events"]],
            [first, second],
        )

    def test_refresh_failure_is_noncritical_reported_once_and_recovers(self):
        """G5/G6: derived write failure rolls back, retains primary reads and avoids log storms."""
        self.logger.record_event("first")
        self.view.refresh()
        self.logger.record_event("second")
        error = sqlite3.OperationalError("controlled view failure")
        with patch.object(self.view, "_write_entry", side_effect=error):
            self.assertIsNone(self.view.refresh())
            self.assertIsNone(self.view.refresh())
            page = self.view.read_events()
            self.assertEqual(page["source"], "journal")
            self.assertIn("controlled view failure", page["refresh_error"]["message"])
            page["refresh_error"]["message"] = "mutated"
        failures = [
            event
            for event in read_database(self.path)
            if event["event_type"] == "logger.filtered_refresh_failed"
        ]
        self.assertEqual(len(failures), 1)
        self.assertIsNotNone(self.view.refresh())
        self.assertEqual(self.view.read_events()["source"], "filtered")
        self.assertEqual(
            len(
                [
                    event
                    for event in read_database(self.path)
                    if event["event_type"] == "logger.filtered_refresh_recovered"
                ]
            ),
            1,
        )

    def test_corrupt_metadata_and_payload_are_reported_then_rebuilt(self):
        """G5/G6: a recognized derived database can be repaired from primary facts."""
        event_id = self.logger.record_event("original")
        self.view.refresh()
        with closing(sqlite3.connect(self.view_path, isolation_level=None)) as db:
            db.execute("UPDATE filtered_info SET metadata_json='{'")
        self.assertIsNotNone(self.view.refresh())
        with closing(sqlite3.connect(self.view_path, isolation_level=None)) as db:
            db.execute(
                "UPDATE filtered_events SET event_json='{' WHERE event_id=?",
                (event_id,),
            )
        self.assertEqual(self.view.read_events()["source"], "journal")
        self.assertIsNotNone(self.view.refresh())
        self.assertIn(
            event_id,
            {entry["event"]["event_id"] for entry in self.view.read_events()["events"]},
        )
        failures = [
            event
            for event in read_database(self.path)
            if event["event_type"] == "logger.filtered_refresh_failed"
        ]
        self.assertEqual(len(failures), 2)

    def test_foreign_or_unwritable_view_preserves_primary_operation(self):
        """G5: an unrecognized file is not overwritten or treated as a broken primary journal."""
        for name, content in (("foreign.db", b"foreign contents"), ("directory", None)):
            with self.subTest(name=name):
                path = self.folder / name
                if content is None:
                    path.mkdir()
                else:
                    path.write_bytes(content)
                view = FilteredJournal(self.view_config, path)
                self.addCleanup(view.close)
                view.open()
                self.assertIsNone(view.refresh())
                self.assertEqual(view.read_events()["source"], "journal")
                if content is not None:
                    self.assertEqual(path.read_bytes(), content)
                self.logger.record_event("primary.healthy")

    def test_failure_to_record_view_error_remains_a_primary_failure(self):
        """G6: source error cannot be swallowed as a rebuildable cache error."""
        self.logger.record_event("first")
        error = LoggingStorageError("primary unavailable")
        error.journal_failed = True
        with (
            patch.object(
                self.view, "_write_entry", side_effect=OSError("cache unavailable")
            ),
            patch.object(self.view._logger, "record_event", side_effect=error),
            self.assertRaises(LoggingStorageError) as caught,
        ):
            self.view.refresh()
        self.assertIs(caught.exception, error)

    def test_generation_change_invalidates_the_previous_publication(self):
        """G2/F9: the old generation is never reused after restoring a journal."""
        self.logger.record_event("prefix")
        self.view.refresh()
        old = self.view.read_events()["checkpoint"]
        snapshot = self.folder / "snapshot"
        manifest = self.logger.export_snapshot(snapshot, min_free_bytes=0)
        self.view.close()
        self.logger.close()
        # Restore a closed snapshot into a new file, keeping the old view to exercise invalidation.
        restored = self.folder / "restored.db"
        shutil.copyfile(snapshot / "journal.sqlite", restored)
        store = make_store(restored, open_mode="existing")
        store.complete_restore(
            manifest, restoration_id=uuid4().hex, new_generation=uuid4().hex
        )
        config = write_context_settings(
            self.folder / "restored-reader", db_path=str(restored), open_mode="existing"
        )
        fresh = FilteredJournal(config, self.view_path)
        self.addCleanup(fresh.close)
        fresh.open()
        with self.assertRaises(JournalGenerationChanged):
            fresh.read_events(old)
        self.assertEqual(fresh.read_events()["source"], "journal")
        fresh.refresh()
        self.assertEqual(fresh.read_events()["source"], "filtered")
        self.assertNotEqual(
            fresh.read_events()["checkpoint"]["generation"], old["generation"]
        )

    def test_explicit_run_uses_interval_and_stops_without_overlapping_refreshes(self):
        """G1/G7: scheduling belongs to an explicitly started caller-owned thread."""
        self.view.close()
        document = json.loads(self.view_config.read_text(encoding="utf-8"))
        document["logging"]["filtered_refresh_interval_seconds"] = 0.01
        self.view_config.write_text(json.dumps(document), encoding="utf-8")
        stop, published = threading.Event(), threading.Event()
        failures, calls = [], []
        original = self.view.refresh

        def observed_refresh():
            calls.append(threading.get_ident())
            result = original()
            if len(calls) == 2:
                published.set()
                stop.set()
            return result

        def run():
            try:
                self.view.run(stop)
            except BaseException as error:  # noqa: BLE001 - Report thread failures to the test owner.
                failures.append(error)
                published.set()

        with patch.object(self.view, "refresh", side_effect=observed_refresh):
            worker = threading.Thread(target=run)
            worker.start()
            try:
                self.assertTrue(published.wait(10), "publisher did not finish")
            finally:
                stop.set()
                worker.join(10)
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(set(calls)), 1)
        self.assertEqual(self.view._interval, 0.01)
        with self.assertRaises(LoggingStateError):
            self.view.read_events()
