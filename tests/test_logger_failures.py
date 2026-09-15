"""Approved v2 storage faults and independent emergency-file behavior."""

import io
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from core.logger import OperationLogger
from core.logger_utils.events import (
    LoggingConfigurationError,
    LoggingStateError,
    LoggingStorageError,
)
from tests.helpers.logging_fixtures import write_context_settings
from tests.helpers.logging_process import SCRATCH_ROOT, cleanup_directory, read_database


class LoggerFailureTests(unittest.TestCase):
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
        self.prefix = self.logger.record_event("confirmed.prefix")

    def deny_insert(self, logger=None):
        client = self.logger if logger is None else logger
        client._store._connection.execute(
            "CREATE TEMP TRIGGER deny_insert BEFORE INSERT ON events "
            "BEGIN SELECT RAISE(ABORT, 'injected write denial'); END"
        )

    def test_failed_write_creates_one_diagnostic_with_context_and_unconfirmed_id(self):
        operation = self.logger.start_operation("attempt", "stage-A")
        self.deny_insert()
        with self.assertRaises(LoggingStorageError) as caught:
            self.logger.record_event(
                "attempt.output", {"payload": 1}, operation=operation
            )
        self.assertIsInstance(caught.exception.__cause__, sqlite3.IntegrityError)
        files = list(self.folder.glob("events.db.emergency-*.jsonl"))
        self.assertEqual(len(files), 1)
        lines = files[0].read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 1)
        diagnostic = json.loads(lines[0])
        self.assertEqual(diagnostic["diagnostic_type"], "journal.failure")
        self.assertEqual(diagnostic["db_path"], str(self.path))
        self.assertEqual(diagnostic["event_type"], "attempt.output")
        self.assertEqual(diagnostic["event_id"], caught.exception.event_id)
        self.assertEqual(diagnostic["operation_id"], operation.get_operation_id())
        self.assertEqual(diagnostic["process_id"], os.getpid())
        self.assertEqual(diagnostic["context"]["attempt_id"], "attempt-A-3-1")
        self.assertIn("injected write denial", diagnostic["cause_message"])
        self.assertTrue(
            any(str(files[0]) in note for note in caught.exception.__notes__)
        )
        self.assertEqual(len(self.logger.read_events()["events"]), 2)
        with self.assertRaises(LoggingStateError):
            self.logger.record_event("blocked")
        self.assertEqual(len(list(self.folder.glob("events.db.emergency-*.jsonl"))), 1)

    def test_low_journal_space_still_attempts_emergency_write_on_journal_filesystem(
        self,
    ):
        self.logger.close()
        write_context_settings(self.folder, min_free_bytes=100)
        with patch(
            "core.logger_utils.storage.shutil.disk_usage",
            return_value=SimpleNamespace(free=200),
        ):
            self.logger.open()
        with (
            patch(
                "core.logger_utils.storage.shutil.disk_usage",
                return_value=SimpleNamespace(free=50),
            ) as usage,
            self.assertRaises(LoggingStorageError),
        ):
            self.logger.record_event("space.rejected")
        self.assertTrue(usage.called)
        self.assertTrue(
            all(call.args == (self.path.parent,) for call in usage.call_args_list)
        )
        self.assertEqual(
            [e["event_id"] for e in read_database(self.path)], [self.prefix]
        )
        self.assertEqual(len(list(self.folder.glob("events.db.emergency-*.jsonl"))), 1)
        self.assertEqual(len(self.logger.read_events()["events"]), 1)

    def test_emergency_write_flush_and_fsync_failures_never_recurse_or_replace_cause(
        self,
    ):
        for phase in ("open", "write", "flush", "fsync"):
            with self.subTest(phase=phase):
                folder = self.folder / phase
                config = write_context_settings(folder)
                with OperationLogger(config) as logger:
                    logger.record_event("confirmed")
                    self.deny_insert(logger)
                    opened = []
                    real_open = Path.open
                    primary_io_failure = OSError("injected diagnostic failure")

                    def controlled_open(
                        path,
                        *args,
                        open_file=real_open,
                        opened_paths=opened,
                        selected_phase=phase,
                        io_failure=primary_io_failure,
                        **kwargs,
                    ):
                        if ".emergency-" not in path.name:
                            return open_file(path, *args, **kwargs)
                        opened_paths.append(path)
                        if selected_phase == "open":
                            raise io_failure
                        stream = open_file(path, *args, **kwargs)
                        if selected_phase not in ("write", "flush"):
                            return stream
                        wrapped = MagicMock(wraps=stream)
                        wrapped.__enter__.return_value = wrapped
                        wrapped.__exit__.side_effect = lambda *_: stream.close()
                        getattr(wrapped, selected_phase).side_effect = io_failure
                        return wrapped

                    stderr = io.StringIO()
                    with (
                        patch.object(Path, "open", controlled_open),
                        patch("sys.stderr", stderr),
                        patch(
                            "core.logger_utils.storage.os.fsync",
                            wraps=os.fsync,
                            side_effect=primary_io_failure
                            if phase == "fsync"
                            else None,
                        ),
                        self.assertRaises(LoggingStorageError) as caught,
                    ):
                        logger.record_event("rejected")
                    self.assertIsInstance(
                        caught.exception.__cause__, sqlite3.IntegrityError
                    )
                    self.assertEqual(len(opened), 1)
                    self.assertIn(
                        "Emergency journal diagnostic failed", stderr.getvalue()
                    )
                    self.assertTrue(
                        any(
                            "diagnostic failed" in note
                            for note in caught.exception.__notes__
                        )
                    )
                    self.assertEqual(len(read_database(folder / "events.db")), 1)

    def test_original_body_exception_survives_failed_error_recording(self):
        primary = ValueError("primary application failure")
        with (
            patch("sys.stderr", io.StringIO()),
            self.assertRaises(ValueError) as caught,
            self.logger.operation("attempt", "stage-A"),
        ):
            self.deny_insert()
            raise primary
        self.assertIs(caught.exception, primary)
        events = read_database(self.path)
        self.assertEqual(
            [e["event_type"] for e in events], ["confirmed.prefix", "operation.started"]
        )
        self.assertEqual(len(list(self.folder.glob("events.db.emergency-*.jsonl"))), 1)

    def test_unconfirmed_command_commit_can_be_reconciled_after_reopening(self):
        original = self.logger._store.append_command_result
        interruption = KeyboardInterrupt("after commit")

        def commit_then_interrupt(event):
            original(event)
            raise interruption

        with (
            patch.object(
                self.logger._store,
                "append_command_result",
                side_effect=commit_then_interrupt,
            ),
            self.assertRaises(KeyboardInterrupt) as caught,
        ):
            self.logger.record_command_result(
                "request", {"value": 1}, author="runner", outcome="succeeded"
            )
        self.assertIs(caught.exception, interruption)
        committed = read_database(self.path)
        self.assertEqual(len(committed), 2)
        with self.assertRaises(LoggingStateError):
            self.logger.record_event("blocked")
        self.logger.close()
        self.logger.open()
        result = self.logger.read_command_result("request")
        self.assertEqual(result["event_id"], committed[-1]["event_id"])
        self.assertEqual(
            self.logger.record_command_result(
                "request", {"value": 1}, author="runner", outcome="succeeded"
            ),
            result["event_id"],
        )
        self.assertEqual(read_database(self.path), committed)
        diagnostics = list(self.folder.glob("events.db.emergency-*.jsonl"))
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(
            json.loads(diagnostics[0].read_text())["event_id"], result["event_id"]
        )

    def test_broken_add_note_does_not_replace_unexpected_write_exception(self):
        class UnnotableError(Exception):
            def add_note(self, note):
                raise RuntimeError("note failed")

        failure = UnnotableError("writer failed")
        with (
            patch.object(self.logger._store, "append", side_effect=failure),
            self.assertRaises(UnnotableError) as caught,
        ):
            self.logger.record_event("not-confirmed")
        self.assertIs(caught.exception, failure)
        self.assertEqual(len(list(self.folder.glob("events.db.emergency-*.jsonl"))), 1)

    def test_open_read_and_close_failures_have_independent_diagnostics(self):
        with self.subTest(action="open"):
            path = self.folder / "corrupt" / "events.db"
            config = write_context_settings(path.parent)
            path.write_bytes(b"not sqlite" * 512)
            before = path.read_bytes()
            with self.assertRaises(LoggingConfigurationError):
                OperationLogger(config).open()
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(len(list(path.parent.glob("*.emergency-*.jsonl"))), 1)
        with self.subTest(action="read"):
            failure = sqlite3.OperationalError("read failed")
            real = self.logger._store._connection
            mocked = Mock(wraps=real)
            mocked.execute.side_effect = failure
            with (
                patch.object(self.logger._store, "_connection", mocked),
                self.assertRaises(LoggingStorageError) as caught,
            ):
                self.logger.read_events()["events"]
            self.assertIs(caught.exception.__cause__, failure)
        self.logger.close()
        self.logger.open()
        with self.subTest(action="close"):
            failure = sqlite3.OperationalError("close failed")
            real = self.logger._store._connection
            mocked = Mock(wraps=real)
            mocked.close.side_effect = failure
            with (
                patch.object(self.logger._store, "_connection", mocked),
                self.assertRaises(LoggingStorageError) as caught,
            ):
                self.logger.close()
            self.assertIs(caught.exception.__cause__, failure)
        self.assertEqual(len(list(self.folder.glob("events.db.emergency-*.jsonl"))), 2)

    def test_changed_disk_header_is_detected_even_with_open_sqlite_connection(self):
        with self.path.open("r+b") as file:
            original_header = file.read(16)
            file.seek(0)
            file.write(b"invalid header!!")
            file.flush()
        try:
            with self.assertRaises(LoggingStorageError):
                self.logger.record_event("must-not-write")
            with self.assertRaises(LoggingStateError):
                self.logger.record_event("blocked")
        finally:
            with self.path.open("r+b") as file:
                file.write(original_header)
        self.assertEqual(
            [e["event_id"] for e in read_database(self.path)], [self.prefix]
        )
