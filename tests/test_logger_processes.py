"""Approved cross-process races and crash checkpoints for v2 logging."""

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from core.logger import OperationLogger
from tests.helpers.logging_fixtures import BASE_CONTEXT, write_context_settings
from tests.helpers.logging_process import (
    SCRATCH_ROOT,
    LoggingProcess,
    cleanup_directory,
    existing_settings,
    read_database,
)


class LoggerProcessTests(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.config = write_context_settings(self.folder)
        self.path = self.folder / "events.db"
        with OperationLogger(self.config) as logger:
            self.prefix_id = logger.record_event("confirmed.prefix")
        existing_settings(self.config)

    def process(self, mode, name, *arguments, context=None, config=None):
        if config is None:
            config = write_context_settings(
                self.folder / name,
                db_path=str(self.path),
                open_mode="existing",
                context={**BASE_CONTEXT, "source": name}
                if context is None
                else context,
            )
        worker = LoggingProcess(
            mode, config, *arguments, module_name="tests.helpers.logging_fixtures"
        )
        self.addCleanup(worker.close)
        worker.start()
        return worker

    def test_import_and_construction_do_not_open_files_or_connections(self):
        config = self.folder / "unused.json"
        worker = self.process("import", "import", config=config)
        self.assertEqual(worker.receive(), {"imported": True})
        self.assertEqual(worker.wait(), 0)
        self.assertFalse(config.exists())
        self.assertFalse((config.parent / "unopened.db").exists())

    def test_independent_processes_share_one_journal_without_losing_source_sequences(
        self,
    ):
        workers = [self.process("worker", name) for name in ("runner", "service")]
        for worker in workers:
            self.assertTrue(worker.receive()["ready"])
        for worker in workers:
            worker.send(json.dumps({"action": "write", "count": 25}))
        ids_by_pid = {}
        for worker in workers:
            self.assertEqual(worker.receive(), {"attempting": "write"})
            ids_by_pid[worker.pid] = worker.receive()["ids"]
            worker.send(json.dumps({"action": "stop"}))
        for worker in workers:
            self.assertEqual(worker.wait(), 0)
        events = read_database(self.path)
        self.assertEqual(len(events), 51)
        self.assertEqual(events[0]["event_id"], self.prefix_id)
        producer_ids = set()
        for pid, expected_ids in ids_by_pid.items():
            observed = [e for e in events if e["context"]["process_id"] == pid]
            self.assertEqual([e["event_id"] for e in observed], expected_ids)
            self.assertEqual(
                [e["sequence_number"] for e in observed], list(range(1, 26))
            )
            self.assertEqual([e["data"]["index"] for e in observed], list(range(25)))
            producer_ids.update(e["producer_instance_id"] for e in observed)
        self.assertEqual(len(producer_ids), 2)
        self.assertEqual(len({e["event_id"] for e in events}), 51)

    def test_concurrent_matching_and_conflicting_responses_are_atomic(self):
        workers = {
            author: self.process("worker", author) for author in ("runner", "service")
        }
        for worker in workers.values():
            self.assertTrue(worker.receive()["ready"])
        for same in (True, False):
            request = "simultaneous-equal" if same else "simultaneous-conflict"
            before_count = len(read_database(self.path))
            with closing(sqlite3.connect(self.path, isolation_level=None)) as blocker:
                blocker.execute("BEGIN IMMEDIATE")
                try:
                    for author, worker in workers.items():
                        worker.send(
                            json.dumps(
                                {
                                    "action": "result",
                                    "request_id": request,
                                    "author": author,
                                    "outcome": "succeeded",
                                    "response": {"value": "same" if same else author},
                                }
                            )
                        )
                    for worker in workers.values():
                        self.assertEqual(worker.receive(), {"attempting": "result"})
                finally:
                    blocker.execute("ROLLBACK")
            results = {
                author: worker.receive()["event_id"]
                for author, worker in workers.items()
            }
            with OperationLogger(self.config) as reader:
                result = reader.read_command_result(request)
            self.assertEqual(result["author"], "runner")
            self.assertFalse(result["provisional"])
            self.assertEqual(
                result["response"], {"value": "same" if same else "runner"}
            )
            self.assertEqual(
                len(read_database(self.path)) - before_count, 1 if same else 2
            )
            if same:
                self.assertEqual(results["runner"], results["service"])
                self.assertTrue(
                    all(o["ignored"] is None for o in result["observations"])
                )
            else:
                self.assertNotEqual(results["runner"], results["service"])
                service = next(
                    o for o in result["observations"] if o["author"] == "service"
                )
                self.assertEqual(service["ignored"], "runner_result_precedence")
            with closing(sqlite3.connect(self.path)) as db:
                self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])
        for worker in workers.values():
            worker.send(json.dumps({"action": "stop"}))
            self.assertEqual(worker.wait(), 0)

    def test_kill_before_and_after_command_commit_keeps_index_and_events_together(self):
        for mode in ("result_before_commit", "result_after_commit"):
            with self.subTest(mode=mode):
                request = mode
                service_config = write_context_settings(
                    self.folder / (mode + "-service"),
                    db_path=str(self.path),
                    open_mode="existing",
                    context={**BASE_CONTEXT, "source": "service"},
                )
                with OperationLogger(service_config) as service:
                    service.record_command_result(
                        request,
                        {"origin": "service"},
                        author="service",
                        outcome="succeeded",
                    )
                before = read_database(self.path)
                worker = self.process(mode, mode, request)
                checkpoint = worker.receive()
                self.assertEqual(
                    checkpoint["phase"],
                    "result_uncommitted"
                    if mode == "result_before_commit"
                    else "result_committed",
                )
                worker.kill_writer()
                self.assertNotEqual(worker.wait(), 0)
                with OperationLogger(self.config) as reader:
                    result = reader.read_command_result(request)
                    after = read_database(self.path)
                    self.assertEqual(after[: len(before)], before)
                    if mode == "result_before_commit":
                        self.assertEqual(after, before)
                        self.assertEqual(result["author"], "service")
                        self.assertTrue(result["provisional"])
                    else:
                        self.assertEqual(len(after), len(before) + 1)
                        self.assertEqual(result["author"], "runner")
                        self.assertEqual(result["event_id"], checkpoint["event_id"])
                        self.assertEqual(
                            result["event"]["context"]["process_id"], worker.pid
                        )
                        duplicate = reader.record_command_result(
                            request,
                            {"origin": "runner"},
                            author="runner",
                            outcome="failed",
                        )
                        self.assertEqual(duplicate, checkpoint["event_id"])
                        self.assertEqual(read_database(self.path), after)
                with closing(sqlite3.connect(self.path)) as db:
                    self.assertEqual(
                        db.execute("PRAGMA foreign_key_check").fetchall(), []
                    )

    def test_killed_snapshot_export_does_not_publish_manifest_or_change_source(self):
        target = self.folder / "snapshot"
        before = read_database(self.path)
        worker = self.process("snapshot_crash", "snapshot-worker", str(target))
        self.assertEqual(worker.receive(), {"phase": "database_copied"})
        worker.kill_writer()
        self.assertNotEqual(worker.wait(), 0)
        self.assertTrue((target / "journal.sqlite").exists())
        self.assertFalse((target / "manifest.json").exists())
        self.assertEqual(read_database(self.path), before)
        self.assertEqual(read_database(target / "journal.sqlite"), before)
        with OperationLogger(self.config) as logger:
            logger.record_event("source.continues")
        self.assertEqual(len(read_database(self.path)), len(before) + 1)
