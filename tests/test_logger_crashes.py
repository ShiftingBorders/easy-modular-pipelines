"""CRASH-01..08: real process termination and controlled secondary I/O failures."""

import io
import json
import sqlite3
import tempfile
import traceback
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

from core.logger import OperationLogger
from core.logger_utils.events import (
    LoggingConfigurationError,
    LoggingStateError,
    LoggingStorageError,
)
from core.logger_utils.storage import SQLiteEventStore
from tests.helpers.logging_process import (
    SCRATCH_ROOT,
    LoggingProcess,
    cleanup_directory,
    existing_settings,
    read_database,
    write_settings,
)


class LoggingCrashTests(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.config = write_settings(self.folder)
        self.db_path = self.folder / "events.db"

    def start_process(self, mode, config=None):
        process = LoggingProcess(mode, self.config if config is None else config)
        self.addCleanup(process.close)
        process.start()
        return process

    def read_in_new_process(self, config=None):
        reader = self.start_process(
            "read", existing_settings(self.config if config is None else config)
        )
        result = reader.receive()
        self.assertEqual(reader.wait(), 0)
        self.assertEqual(result["integrity"], "ok")
        return [record["event"] for record in result["records"]]

    def test_kill_before_during_and_after_event_commit(self):
        """CRASH-01: confirmed prefix survives; only the unconfirmed insertion is uncertain."""
        for mode in ("before_insert", "during_insert", "after_commit"):
            with self.subTest(mode=mode):
                config = write_settings(self.folder / mode)
                writer = self.start_process(mode, config)
                confirmed = writer.receive()["confirmed_ids"]
                checkpoint = writer.receive()
                self.assertEqual(len(confirmed), 20)
                self.assertIn(
                    {
                        "before_insert": "before_insert",
                        "during_insert": "during_insert",
                        "after_commit": "confirmed_last",
                    }[mode],
                    checkpoint,
                )
                writer.kill_writer()
                self.assertNotEqual(writer.wait(), 0)
                events = self.read_in_new_process(config)
                self.assertEqual([e["event_id"] for e in events[:20]], confirmed)
                self.assertEqual(
                    [e["data"]["number"] for e in events[:20]], list(range(20))
                )
                self.assertEqual(len({e["event_id"] for e in events}), len(events))
                if mode == "before_insert":
                    self.assertEqual(len(events), 20)
                elif mode == "after_commit":
                    self.assertEqual(len(events), 21)
                    self.assertEqual(
                        events[-1]["event_id"], checkpoint["confirmed_last"]
                    )
                else:
                    self.assertIn(len(events), (20, 21))
                if len(events) == 21:
                    self.assertEqual(events[-1]["event_type"], "test.last")
                    self.assertEqual(events[-1]["data"], {"payload": "x" * 10000})

    def test_killed_operation_keeps_start_without_inventing_finish(self):
        """CRASH-02: reopen the original database plus WAL, twice, without fake completion."""
        writer = self.start_process("operation")
        operation_id = writer.receive()["operation_id"]
        writer.kill_writer()
        self.assertNotEqual(writer.wait(), 0)
        self.assertTrue(self.db_path.with_name("events.db-wal").exists())
        events = self.read_in_new_process()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "operation.started")
        self.assertEqual(events[0]["operation_id"], operation_id)
        self.assertEqual(self.read_in_new_process(), events)

    def test_kill_during_schema_creation_rolls_back_whole_schema(self):
        """STORE-02, CRASH-01: an uncommitted schema never becomes partially accepted."""
        writer = self.start_process("schema_crash")
        self.assertEqual(writer.receive(), {"schema_uncommitted": True})
        writer.kill_writer()
        self.assertNotEqual(writer.wait(), 0)
        with closing(sqlite3.connect(self.db_path)) as db:
            objects = db.execute(
                "SELECT name FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'"
            ).fetchall()
            self.assertEqual(objects, [])
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 0)
            self.assertEqual(db.execute("PRAGMA application_id").fetchone()[0], 0)
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        # A failed initializer leaves an unclaimed file; existing must reject it.
        with self.assertRaises(LoggingConfigurationError):
            OperationLogger(self.config).open()

    def test_failed_start_never_enters_body(self):
        """CRASH-03: write failure blocks execution and subsequent journal writes."""
        with OperationLogger(self.config) as logger:
            entered = []
            with (
                patch.object(
                    logger._store,
                    "append",
                    side_effect=LoggingStorageError("Controlled write failure"),
                ),
                self.assertRaises(LoggingStorageError),
                logger.operation("test", "never-entered"),
            ):
                entered.append(True)
            self.assertEqual(entered, [])
            self.assertEqual(read_database(self.db_path), [])
            with self.assertRaises(LoggingStateError):
                logger.record_event("test.rejected")

    def test_oversized_start_is_rejected_without_poisoning_client(self):
        """CRASH-03: prewrite validation also blocks the body but permits a corrected call."""
        write_settings(self.folder, max_event_bytes=1024)
        with OperationLogger(self.config) as logger:
            entered = []
            with (
                self.assertRaises(ValueError),
                logger.operation("test", "too-large", attributes={"text": "x" * 4096}),
            ):
                entered.append(True)
            self.assertEqual(entered, [])
            with logger.operation("test", "valid"):
                pass
        self.assertEqual(
            [e["event_type"] for e in read_database(self.db_path)],
            ["operation.started", "operation.finished"],
        )

    def test_committed_but_unconfirmed_transitions_require_reopen(self):
        """CRASH-04: interrupt after the real commit, before the caller receives confirmation."""
        for phase in ("start", "finish"):
            with self.subTest(phase=phase):
                config = write_settings(self.folder / phase)
                with OperationLogger(config) as logger:
                    operation = (
                        logger.operation("test", phase)
                        if phase == "start"
                        else logger.start_operation("test", phase)
                    )
                    original_append = logger._store.append
                    interruption = KeyboardInterrupt(
                        "Controlled post-commit interruption"
                    )

                    def commit_then_interrupt(
                        event, append=original_append, error=interruption
                    ):
                        append(event)
                        raise error

                    with (
                        patch.object(
                            logger._store, "append", side_effect=commit_then_interrupt
                        ),
                        self.assertRaises(KeyboardInterrupt) as caught,
                    ):
                        if phase == "start":
                            with operation:
                                self.fail("Unconfirmed start must not enter the body")
                        else:
                            logger.finish_operation(operation)
                    self.assertIs(caught.exception, interruption)
                    events = read_database(config.parent / "events.db")
                    self.assertEqual(len(events), 1 if phase == "start" else 2)
                    with self.assertRaises(LoggingStateError):
                        logger.record_event("test.rejected")
                    logger.close()
                    logger.open()
                    with self.assertRaises(LoggingStateError):
                        logger.finish_operation(operation)
                    self.assertEqual(read_database(config.parent / "events.db"), events)

    def test_secondary_error_or_finish_failure_preserves_body_exception(self):
        """CRASH-05: failed diagnostics cannot replace the original exception or traceback."""
        for failed_type in ("error.recorded", "operation.finished"):
            with self.subTest(failed_type=failed_type):
                config = write_settings(self.folder / failed_type)
                with OperationLogger(config) as logger:
                    original_append = logger._store.append
                    primary = ValueError("primary body exception")
                    stderr = io.StringIO()

                    def fail_selected(
                        event, event_type=failed_type, append=original_append
                    ):
                        if event["event_type"] == event_type:
                            raise LoggingStorageError(
                                "Controlled secondary write failure"
                            )
                        append(event)

                    with (
                        patch.object(
                            logger._store, "append", side_effect=fail_selected
                        ),
                        patch("sys.stderr", stderr),
                    ):
                        try:
                            with logger.operation("test", "primary"):
                                raise primary
                        except ValueError as error:
                            self.assertIs(error, primary)
                            self.assertIn(
                                "test_secondary_error_or_finish_failure_preserves_body_exception",
                                [
                                    frame.name
                                    for frame in traceback.extract_tb(
                                        error.__traceback__
                                    )
                                ],
                            )
                        else:
                            self.fail("The body exception must propagate")
                    self.assertTrue(
                        any("Journal failure" in note for note in primary.__notes__)
                    )
                    self.assertIn("Journal failure", stderr.getvalue())

    def test_secondary_close_failure_preserves_body_exception(self):
        """CRASH-05: context-manager cleanup preserves the error already in flight."""
        logger = OperationLogger(self.config)
        self.addCleanup(logger.close)
        primary = ValueError("primary body exception")
        stderr = io.StringIO()
        with (
            patch.object(
                SQLiteEventStore,
                "close",
                side_effect=LoggingStorageError("Controlled close failure"),
            ),
            patch("sys.stderr", stderr),
        ):
            try:
                with logger:
                    raise primary
            except ValueError as error:
                self.assertIs(error, primary)
                self.assertIn(
                    "test_secondary_close_failure_preserves_body_exception",
                    [frame.name for frame in traceback.extract_tb(error.__traceback__)],
                )
            else:
                self.fail("The body exception must propagate")
        self.assertIn("closing the journal", stderr.getvalue())
        self.assertTrue(primary.__notes__)

    def test_oversized_error_marks_incomplete_error_recording(self):
        """CRASH-05: rejected error payload does not prevent a smaller failed outcome."""
        write_settings(self.folder, max_event_bytes=1024)
        primary = ValueError("x" * 4000)
        with OperationLogger(self.config) as logger, patch("sys.stderr", io.StringIO()):
            with (
                self.assertRaises(ValueError) as caught,
                logger.operation("test", "large-error"),
            ):
                raise primary
            self.assertIs(caught.exception, primary)
            logger.record_event("test.still-usable")
        events = read_database(self.db_path)
        self.assertEqual(
            [e["event_type"] for e in events],
            ["operation.started", "operation.finished", "test.still-usable"],
        )
        self.assertEqual(events[1]["data"]["status"], "failed")
        self.assertEqual(
            events[1]["data"]["attributes"], {"error_recording_failed": True}
        )

    def test_finish_failure_without_body_error_propagates(self):
        """CRASH-05: success cannot be reported when the terminal write was not confirmed."""
        with OperationLogger(self.config) as logger:
            original_append = logger._store.append

            def fail_finish(event):
                if event["event_type"] == "operation.finished":
                    raise LoggingStorageError("Controlled outcome failure")
                original_append(event)

            with (
                patch.object(logger._store, "append", side_effect=fail_finish),
                self.assertRaises(LoggingStorageError),
                logger.operation("test", "success"),
            ):
                pass
        self.assertEqual(len(read_database(self.db_path)), 1)

    def test_missing_or_broken_stderr_never_replaces_original(self):
        """CRASH-06: no stderr/failed write/failed flush does not trigger recursive logging."""
        bad_write = Mock()
        bad_write.write.side_effect = OSError("Controlled stderr write failure")
        bad_flush = Mock()
        bad_flush.flush.side_effect = OSError("Controlled stderr flush failure")
        for index, stream in enumerate((None, bad_write, bad_flush)):
            config = write_settings(self.folder / str(index))
            with OperationLogger(config) as logger:
                primary = ValueError("primary")
                original_append = logger._store.append

                def fail_error(event, append=original_append):
                    if event["event_type"] == "operation.started":
                        return append(event)
                    raise LoggingStorageError("Controlled write failure")

                with (
                    patch.object(
                        logger._store, "append", side_effect=fail_error
                    ) as append,
                    patch("sys.stderr", stream),
                ):
                    with (
                        self.assertRaises(ValueError) as caught,
                        logger.operation("test", "stderr"),
                    ):
                        raise primary
                    self.assertIs(caught.exception, primary)
                    self.assertEqual(append.call_count, 2)
                self.assertTrue(primary.__notes__)

    def test_sqlite_full_preserves_confirmed_prefix(self):
        """CRASH-07: force real SQLITE_FULL by limiting pages, without filling the disk."""
        with OperationLogger(self.config) as logger:
            confirmed_id = logger.record_event("test.confirmed")
            connection = logger._store._connection
            page_count = connection.execute("PRAGMA page_count").fetchone()[0]
            connection.execute(f"PRAGMA max_page_count={page_count}")
            with self.assertRaises(LoggingStorageError) as caught:
                logger.record_event("test.exceeds-capacity", {"text": "x" * 100000})
            self.assertEqual(
                caught.exception.__cause__.sqlite_errorcode, sqlite3.SQLITE_FULL
            )
            self.assertEqual(
                [
                    record["event"]["event_id"]
                    for record in logger.read_events()["events"]
                ],
                [confirmed_id],
            )
            with self.assertRaises(LoggingStateError):
                logger.record_event("test.rejected")
        self.assertEqual(
            [e["event_id"] for e in read_database(self.db_path)], [confirmed_id]
        )

    def test_corrupt_database_is_not_recreated(self):
        """CRASH-07: an invalid header is reported without replacing the existing file."""
        contents = b"This is not a SQLite database." * 200
        self.db_path.write_bytes(contents)
        with self.assertRaises(LoggingConfigurationError):
            OperationLogger(self.config).open()
        self.assertEqual(self.db_path.read_bytes(), contents)

    def test_runner_crash_does_not_stop_independent_module_journal(self):
        """CRASH-08: independent module writer can finish after the runner writer dies."""
        runner_config = write_settings(self.folder / "runner")
        document = json.loads(runner_config.read_text(encoding="utf-8"))
        document["operation_context"]["source"] = "runner"
        runner_config.write_text(json.dumps(document), encoding="utf-8")
        runner = self.start_process("operation", runner_config)
        runner_id = runner.receive()["operation_id"]
        module_config = write_settings(self.folder / "module")
        document = json.loads(module_config.read_text(encoding="utf-8"))
        document["operation_context"]["parent_operation_id"] = runner_id
        module_config.write_text(json.dumps(document), encoding="utf-8")
        module = self.start_process("operation", module_config)
        module_id = module.receive()["operation_id"]
        runner.kill_writer()
        self.assertNotEqual(runner.wait(), 0)
        module.send()
        self.assertEqual(module.receive(), {"completed": True})
        self.assertEqual(module.wait(), 0)
        runner_events = self.read_in_new_process(runner_config)
        module_events = self.read_in_new_process(module_config)
        self.assertEqual(
            [e["event_type"] for e in runner_events], ["operation.started"]
        )
        self.assertEqual(
            [e["event_type"] for e in module_events],
            ["operation.started", "test.continued", "operation.finished"],
        )
        self.assertEqual({e["operation_id"] for e in module_events}, {module_id})
        self.assertEqual(module_events[0]["data"]["parent_operation_id"], runner_id)
