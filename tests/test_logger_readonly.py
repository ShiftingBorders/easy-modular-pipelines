"""Dashboard plan A: real SQLite reader isolation and failure boundaries."""

import json
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.logger import OperationLogger
from core.logger_utils.events import LoggingError, LoggingStateError
from tests.dashboard_tests.helpers import cleanup_directory, temporary_directory
from tests.helpers.logging_process import (
    event_fixture,
    existing_settings,
    make_store,
    write_settings,
)


class ReadOnlyLoggerTests(unittest.TestCase):
    def test_reader_opens_while_writer_holds_uncommitted_write_transaction(self):
        self.writer.record_event("committed")
        connection = self.writer._store._connection
        connection.execute("BEGIN IMMEDIATE")
        try:
            reader = self.reader()
            self.assertEqual(
                reader.read_events()["events"][0]["event"]["event_type"], "committed"
            )
        finally:
            connection.execute("ROLLBACK")

    def setUp(self):
        tmp = temporary_directory()
        self.addCleanup(cleanup_directory, tmp)
        self.root = Path(tmp.name)
        self.config = write_settings(self.root)
        self.writer = OperationLogger(self.config)
        self.writer.open()
        self.addCleanup(self.writer.close)
        existing_settings(self.config)

    def reader(self):
        reader = OperationLogger(self.config, read_only=True)
        self.addCleanup(reader.close)
        reader.open()
        return reader

    def test_live_reader_sees_new_events_and_rejected_write_does_not_poison_it(self):
        reader = self.reader()
        first = reader.read_events()
        identity = self.writer.record_event("fixture.live")
        with self.assertRaises(LoggingStateError):
            reader.record_event("forbidden")
        self.assertEqual(
            reader.read_events(first["checkpoint"])["events"][0]["event"]["event_id"],
            identity,
        )
        self.assertTrue(reader.read_changes()["changes"])
        self.assertEqual(
            reader._store._connection.execute("PRAGMA query_only").fetchone()[0], 1
        )
        with self.assertRaises(sqlite3.OperationalError):
            reader._store._connection.execute("CREATE TABLE forbidden (value TEXT)")
        self.assertEqual(len(reader.read_events()["events"]), 1)
        self.assertFalse(list(self.root.glob("*.emergency*")))

    def test_reader_ignores_writer_free_space_limit_and_can_export_to_explicit_destination(
        self,
    ):
        document = json.loads(self.config.read_text())
        document["logging"]["min_free_bytes"] = 10**18
        self.config.write_text(json.dumps(document), encoding="utf-8")
        self.writer.record_event("fixture.snapshot")
        reader = self.reader()
        manifest = reader.export_snapshot(self.root / "export", min_free_bytes=0)
        self.assertEqual(manifest["event_count"], 1)
        self.assertTrue((self.root / "export/journal.sqlite").is_file())

    def test_missing_corrupt_empty_and_wrong_identity_never_create_emergency_files(
        self,
    ):
        self.writer.close()
        original = (self.root / "events.db").read_bytes()
        for content in (b"", b"not SQLite", None):
            with self.subTest(content=content):
                path = self.root / "events.db"
                if content is None:
                    path.unlink()
                else:
                    path.write_bytes(content)
                with self.assertRaises(LoggingError):
                    self.reader()
                self.assertFalse(list(self.root.glob("*.emergency*")))
                path.write_bytes(original)
        document = json.loads(self.config.read_text())
        document["logging"]["expected_journal"]["generation"] = uuid4().hex
        self.config.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaises(LoggingError):
            self.reader()

    def test_readonly_requires_boolean_existing_and_refuses_restore(self):
        for value in (1, None, "yes"):
            with self.subTest(value=value), self.assertRaises(TypeError):
                OperationLogger(self.config, read_only=value)
        store = make_store(
            self.root / "events.db", open_mode="existing", read_only=True
        )
        with self.assertRaises(LoggingStateError):
            store.complete_restore(
                {}, restoration_id=uuid4().hex, new_generation=uuid4().hex
            )
        document = json.loads(self.config.read_text())
        document["logging"].update(open_mode="create", expected_journal=None)
        self.config.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaises(LoggingError):
            self.reader()

    def test_low_level_append_is_blocked_before_health_checks(self):
        reader = self.reader()
        with (
            patch.object(
                reader._store,
                "_check_health",
                side_effect=AssertionError("write path reached"),
            ),
            self.assertRaises(LoggingStateError),
        ):
            reader._store.append(event_fixture())
        self.assertEqual(reader.read_events()["events"], [])
