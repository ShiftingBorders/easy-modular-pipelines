"""Approved v2 snapshot boundaries, concurrent writers and destination failures."""

import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from pathlib import Path
from uuid import UUID

from core.journal.events import LoggingStateError, LoggingStorageError
from core.journal.logger import OperationLogger
from tests.helpers.journal_owner import WriterOwner
from tests.helpers.logging_fixtures import BASE_CONTEXT, write_context_settings
from tests.helpers.logging_process import (
    SCRATCH_ROOT,
    LoggingProcess,
    cleanup_directory,
    read_database,
)


class LoggerSnapshotTests(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.config = write_context_settings(
            self.folder, context={**BASE_CONTEXT, "source": "participant"}
        )
        self.path = self.folder / "events.db"
        self.owner = WriterOwner(self.config).open()
        self.addCleanup(self.owner.close)
        self.logger = OperationLogger(self.config)
        self.addCleanup(self.logger.close)
        self.logger.open()
        self.logger.record_event("snapshot.prefix", {"nested": [1, None, False]})

    def start_worker(self):
        config = write_context_settings(
            self.folder / "writer",
            db_path=str(self.path),
            open_mode="existing",
            context={**BASE_CONTEXT, "source": "runner"},
        )
        worker = LoggingProcess(
            "worker", config, module_name="tests.helpers.logging_fixtures"
        )
        self.addCleanup(worker.close)
        worker.start()
        self.assertTrue(worker.receive()["ready"])
        return worker

    def test_export_is_a_standalone_copy_with_identified_committed_boundary(self):
        self.logger.record_command_result(
            "request", {"value": 3}, author="participant", outcome="succeeded"
        )
        original = read_database(self.path)
        identity = self.logger.get_journal_info()
        target = self.folder / "snapshot"
        manifest = self.logger.export_snapshot(target, min_free_bytes=0)
        self.assertEqual(
            json.loads((target / "manifest.json").read_text(encoding="utf-8")), manifest
        )
        UUID(manifest["snapshot_id"])
        self.assertEqual(manifest["journal_id"], identity["journal_id"])
        self.assertEqual(UUID(manifest["generation"]).hex, manifest["generation"])
        self.assertEqual(manifest["cursor"], len(original))
        self.assertEqual(manifest["event_count"], len(original))
        self.assertEqual(manifest["storage_schema_version"], 3)
        self.assertEqual(manifest["database"], "journal.sqlite")
        # Windows may briefly expose SQLite's delete-pending temporary journal.
        deadline = time.monotonic() + 1
        while {p.name for p in target.iterdir()} != {
            "journal.sqlite",
            "manifest.json",
        } and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(
            {p.name for p in target.iterdir()}, {"journal.sqlite", "manifest.json"}
        )
        snapshot = target / "journal.sqlite"
        self.assertEqual(read_database(snapshot), original)
        self.assertEqual(read_database(self.path), original)
        with closing(sqlite3.connect(snapshot.as_uri() + "?mode=ro", uri=True)) as db:
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "delete")
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])
        second = self.logger.export_snapshot(
            self.folder / "second-snapshot", min_free_bytes=0
        )
        self.assertNotEqual(second["snapshot_id"], manifest["snapshot_id"])
        self.assertEqual(second["cursor"], manifest["cursor"])

    def test_snapshot_pins_events_and_result_index_while_another_process_commits(self):
        event_id = self.logger.record_command_result(
            "shared-request", {"value": 7}, author="participant", outcome="succeeded"
        )
        before = read_database(self.path)
        worker = self.start_worker()
        appended_ids = []

        def write_after_boundary(reader):
            worker.send(
                json.dumps(
                    {
                        "action": "result",
                        "request_id": "shared-request",
                        "author": "runner",
                        "outcome": "succeeded",
                        "response": {"value": 7},
                    }
                )
            )
            self.assertEqual(worker.receive(), {"attempting": "result"})
            self.assertEqual(worker.receive()["event_id"], event_id)
            worker.send(json.dumps({"action": "write", "count": 20}))
            self.assertEqual(worker.receive(), {"attempting": "write"})
            appended_ids.extend(worker.receive()["ids"])

        target = self.folder / "snapshot"
        with self.owner.observe("snapshot_boundary", write_after_boundary):
            manifest = self.logger.export_snapshot(target, min_free_bytes=0)
        worker.send(json.dumps({"action": "stop"}))
        self.assertEqual(worker.wait(), 0)
        self.assertEqual(len(appended_ids), 20)
        self.assertEqual(manifest["event_count"], len(before))
        self.assertEqual(read_database(target / "journal.sqlite"), before)
        current = read_database(self.path)
        self.assertEqual([e["event_id"] for e in current[len(before) :]], appended_ids)
        self.assertEqual(
            self.logger.read_command_result("shared-request")["author"], "runner"
        )
        with closing(
            sqlite3.connect((target / "journal.sqlite").as_uri() + "?mode=ro", uri=True)
        ) as db:
            indexed = db.execute(
                "SELECT effective_author, runner_event_id, participant_event_id "
                "FROM command_results WHERE request_id='shared-request'"
            ).fetchone()
            self.assertEqual(indexed, ("participant", None, event_id))
            self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_another_process_can_commit_between_backup_page_steps(self):
        for index in range(3):
            self.logger.record_event(
                "large.payload", {"index": index, "text": "x" * 300000}
            )
        before = read_database(self.path)
        worker = self.start_worker()
        write_completed = []
        page_counts = []

        def progress(data):
            _status, remaining, total = data
            page_counts.append((remaining, total))
            if remaining > 0 and not write_completed:
                worker.send(json.dumps({"action": "write", "count": 5}))
                self.assertEqual(worker.receive(), {"attempting": "write"})
                write_completed.extend(worker.receive()["ids"])

        target = self.folder / "snapshot"
        with self.owner.observe("backup_progress", progress):
            manifest = self.logger.export_snapshot(target, min_free_bytes=0)
        worker.send(json.dumps({"action": "stop"}))
        self.assertEqual(worker.wait(), 0)
        self.assertTrue(any(remaining > 0 for remaining, _ in page_counts))
        self.assertEqual(len(write_completed), 5)
        self.assertEqual(read_database(target / "journal.sqlite"), before)
        self.assertEqual(manifest["event_count"], len(before))
        self.assertEqual(
            [e["event_id"] for e in read_database(self.path)[len(before) :]],
            write_completed,
        )

    def test_existing_destinations_are_not_overwritten_and_source_stays_writable(self):
        for name, is_directory in (("directory", True), ("file", False)):
            with self.subTest(name=name):
                target = self.folder / name
                if is_directory:
                    target.mkdir()
                    sentinel = target / "keep.txt"
                else:
                    sentinel = target
                sentinel.write_text("keep", encoding="utf-8")
                with self.assertRaises(ValueError):
                    self.logger.export_snapshot(target, min_free_bytes=0)
                self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")
                self.logger.record_event("after.existing.destination")
        self.assertEqual(len(read_database(self.path)), 3)

    def test_invalid_snapshot_arguments_have_no_persistent_side_effects(self):
        for destination, threshold in (
            ("relative", 0),
            (None, 0),
            (self.folder / "new", -1),
            (self.folder / "new", True),
            (self.folder / "new", 0.5),
        ):
            with (
                self.subTest(destination=destination, threshold=threshold),
                self.assertRaises((ValueError, TypeError)),
            ):
                self.logger.export_snapshot(destination, min_free_bytes=threshold)
        self.assertFalse((self.folder / "new").exists())
        self.assertEqual(len(read_database(self.path)), 1)
        self.assertEqual(list(self.folder.glob("*.emergency-*.jsonl")), [])

    def test_destination_threshold_is_separate_from_journal_threshold(self):
        archives = self.folder / "archives"
        archives.mkdir()
        self.logger.close()
        self.config = write_context_settings(self.folder, min_free_bytes=100)
        with self.owner.disk_space({str(archives): 50, "default": 500}) as visited:
            self.logger.open()
            with self.assertRaises(LoggingStorageError):
                self.logger.export_snapshot(archives / "snapshot", min_free_bytes=200)
            self.logger.record_event("snapshot.failed")
        self.assertIn((archives,), visited)
        self.assertIn((self.path.parent,), visited)
        self.assertFalse((archives / "snapshot").exists())
        self.assertEqual(len(read_database(self.path)), 2)
        self.assertEqual(len(list(self.folder.glob("*.emergency-*.jsonl"))), 1)

    def test_publication_failure_keeps_partial_copy_and_source_can_record_error(self):
        target = self.folder / "snapshot"
        failure = OSError("injected manifest publication failure")

        before = read_database(self.path)
        with (
            self.owner.fault("publication_failure", error=failure),
            self.assertRaises(LoggingStorageError) as caught,
        ):
            self.logger.export_snapshot(target, min_free_bytes=0)
        self.assertIsInstance(caught.exception.__cause__, type(failure))
        self.assertEqual(str(caught.exception.__cause__), str(failure))
        self.assertFalse((target / "manifest.json").exists())
        self.assertTrue((target / "journal.sqlite").exists())
        self.assertEqual(read_database(target / "journal.sqlite"), before)
        self.logger.record_event("snapshot.failed", {"reason": "publication"})
        self.assertEqual(len(read_database(self.path)), len(before) + 1)

    def test_corrupt_source_payload_or_index_prevents_ready_snapshot_and_blocks_writes(
        self,
    ):
        for kind in ("payload", "index"):
            with self.subTest(kind=kind):
                folder = self.folder / kind
                config = write_context_settings(folder)
                with OperationLogger(config) as logger:
                    logger.record_command_result(
                        "request", {}, author="runner", outcome="succeeded"
                    )
                    with closing(
                        sqlite3.connect(folder / "events.db", isolation_level=None)
                    ) as db:
                        if kind == "payload":
                            db.execute("UPDATE events SET event_json='{'")
                        else:
                            db.execute(
                                "UPDATE command_results SET runner_event_id='missing'"
                            )
                    target = folder / "snapshot"
                    with self.assertRaises(LoggingStorageError):
                        logger.export_snapshot(target, min_free_bytes=0)
                    self.assertFalse((target / "manifest.json").exists())
                    with self.assertRaises(LoggingStateError):
                        logger.record_event("must-not-write")

    def test_export_of_confirmed_prefix_is_allowed_after_failed_write(self):
        self.owner.execute(
            "CREATE TEMP TRIGGER deny_write BEFORE INSERT ON events "
            "BEGIN SELECT RAISE(ABORT, 'blocked write'); END"
        )
        with self.assertRaises(LoggingStorageError):
            self.logger.record_event("unconfirmed")
        target = self.folder / "snapshot"
        manifest = self.logger.export_snapshot(target, min_free_bytes=0)
        self.assertEqual(manifest["event_count"], 1)
        self.assertEqual(
            read_database(target / "journal.sqlite"), read_database(self.path)
        )
        with self.assertRaises(LoggingStateError):
            self.logger.record_event("still-blocked")
