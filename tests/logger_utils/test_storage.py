"""STORE-01..07: independent SQLite checks for the approved local journal contract."""

import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

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
    event_fixture,
    read_database,
    write_settings,
)


class SQLiteEventStoreTests(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.db_path = self.folder / "events.db"
        self.store = SQLiteEventStore(self.db_path)
        self.addCleanup(self.store.close)
        self.store.open()

    def test_schema_reopens_and_writer_uses_wal_full(self):
        """STORE-01, STORE-02: verify durability settings on the writer itself."""
        connection = self.store._connection
        self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        self.assertEqual(connection.execute("PRAGMA synchronous").fetchone()[0], 2)
        self.store.append(event_fixture())
        self.store.close()
        self.store.open()
        self.assertEqual(read_database(self.db_path), [event_fixture()])
        self.assertEqual(
            self.store._connection.execute("PRAGMA synchronous").fetchone()[0], 2
        )

    def test_incompatible_schema_does_not_modify_existing_content(self):
        """STORE-01: reject incompatible metadata, columns, tables, indexes, and triggers."""
        changes = (
            "PRAGMA user_version=99",
            "PRAGMA application_id=123",
            "ALTER TABLE events ADD COLUMN extra TEXT",
            "CREATE TABLE unrelated (value TEXT)",
            "CREATE INDEX extra_index ON events(sequence_number)",
            "CREATE TRIGGER extra_trigger AFTER INSERT ON events BEGIN SELECT 1; END",
        )
        for index, change in enumerate(changes):
            with self.subTest(change=change):
                path = self.folder / f"incompatible-{index}.db"
                store = SQLiteEventStore(path)
                store.open()
                store.append(event_fixture())
                store.close()
                with closing(sqlite3.connect(path, isolation_level=None)) as db:
                    db.execute(change)
                    before = list(db.iterdump())
                    version = db.execute("PRAGMA user_version").fetchone()
                    application_id = db.execute("PRAGMA application_id").fetchone()
                with self.assertRaises(LoggingConfigurationError):
                    store.open()
                with closing(sqlite3.connect(path)) as db:
                    self.assertEqual(list(db.iterdump()), before)
                    self.assertEqual(
                        db.execute("PRAGMA user_version").fetchone(), version
                    )
                    self.assertEqual(
                        db.execute("PRAGMA application_id").fetchone(), application_id
                    )

    def test_open_and_close_failures_preserve_causes(self):
        """STORE-02: expected filesystem/SQLite failures retain their original causes."""
        inaccessible = SQLiteEventStore(self.folder / "new" / "events.db")
        failure = PermissionError("Controlled directory denial")
        with (
            patch.object(Path, "mkdir", side_effect=failure),
            self.assertRaises(LoggingStorageError) as caught,
        ):
            inaccessible.open()
        self.assertIs(caught.exception.__cause__, failure)
        self.assertFalse((self.folder / "new").exists())
        with self.assertRaises(LoggingStorageError):
            SQLiteEventStore(self.folder).open()
        connection = self.store._connection
        failure = sqlite3.OperationalError("Controlled close failure")
        failing = Mock(wraps=connection)
        failing.close.side_effect = failure
        with (
            patch.object(self.store, "_connection", failing),
            self.assertRaises(LoggingStorageError) as caught,
        ):
            self.store.close()
        self.assertIs(caught.exception.__cause__, failure)
        self.store.close()

    def test_cleanup_does_not_replace_open_failure(self):
        """STORE-02: secondary close failure cannot replace the failed open operation."""
        store = SQLiteEventStore(self.folder / "failed-open.db")
        primary = sqlite3.OperationalError("Controlled schema read failure")
        connection = Mock()
        connection.execute.side_effect = primary
        connection.close.side_effect = sqlite3.OperationalError(
            "Controlled close failure"
        )
        with (
            patch("core.logger_utils.storage.sqlite3.connect", return_value=connection),
            self.assertRaises(LoggingStorageError) as caught,
        ):
            store.open()
        self.assertIs(caught.exception.__cause__, primary)
        connection.close.assert_called_once()

    def test_acknowledged_append_is_visible_to_independent_reader(self):
        """STORE-03: commit is visible before the writer closes; input mutation is detached."""
        event = event_fixture()
        expected = event_fixture()
        self.store.append(event)
        event["data"]["number"] = 99
        self.assertEqual(read_database(self.db_path), [expected])

    def test_duplicate_identities_never_add_a_second_record(self):
        """STORE-03: event ID and source/sequence collisions are rejected, not counted twice."""
        self.store.append(event_fixture())
        duplicates = (
            {**event_fixture(2), "event_id": "event-1"},
            {**event_fixture(2), "sequence_number": 1},
        )
        for duplicate in duplicates:
            with self.subTest(duplicate=duplicate):
                with self.assertRaises(LoggingStorageError):
                    self.store.append(duplicate)
                self.assertEqual(read_database(self.db_path), [event_fixture()])
                with self.assertRaises(LoggingStateError):
                    self.store.append(event_fixture(3))
                self.store.close()
                self.store.open()

    def test_read_cursor_pagination_and_argument_limits(self):
        """STORE-04: page boundaries and replay retain all records without consuming them."""
        for number in range(1, 8):
            self.store.append(event_fixture(number))
        first = self.store.read_events(limit=3)
        second = self.store.read_events(after_cursor=first[-1]["cursor"], limit=3)
        third = self.store.read_events(after_cursor=second[-1]["cursor"], limit=3)
        self.assertEqual(
            [r["event"] for r in first + second + third],
            [event_fixture(n) for n in range(1, 8)],
        )
        self.assertEqual(
            [r["cursor"] for r in first + second + third], list(range(1, 8))
        )
        self.assertEqual(self.store.read_events(limit=3), first)
        self.assertEqual(self.store.read_events(after_cursor=7), [])
        for value in (-1, True, 1.5, 2**63):
            with self.assertRaises(ValueError):
                self.store.read_events(after_cursor=value)
        for value in (0, 1001, True, 1.5):
            with self.assertRaises(ValueError):
                self.store.read_events(limit=value)

    def test_read_page_is_bounded_by_encoded_bytes(self):
        """STORE-04: three six-MiB events split into two pages without a missing record."""
        self.store.close()
        store = SQLiteEventStore(self.db_path, max_event_bytes=16777216)
        self.addCleanup(store.close)
        store.open()
        for number in range(1, 4):
            event = event_fixture(number)
            event["data"] = {"text": "x" * (6 * 1024 * 1024)}
            store.append(event)
        first = store.read_events(limit=1000)
        self.assertEqual(len(first), 2)
        second = store.read_events(after_cursor=first[-1]["cursor"], limit=1000)
        self.assertEqual(
            [r["event"]["event_id"] for r in first + second],
            ["event-1", "event-2", "event-3"],
        )
        self.assertEqual(len(second), 1)

    def test_lower_write_limit_does_not_hide_old_events(self):
        """STORE-05: lowering the write limit keeps larger historic events readable."""
        event = event_fixture()
        event["data"] = {"text": "x" * 2048}
        self.store.append(event)
        self.store.close()
        limited = SQLiteEventStore(self.db_path, max_event_bytes=1024)
        self.addCleanup(limited.close)
        limited.open()
        self.assertEqual(limited.read_events()[0]["event"], event)
        with self.assertRaises(ValueError):
            limited.append({**event, "event_id": "event-2", "sequence_number": 2})
        limited.append(event_fixture(2))
        self.assertEqual(len(read_database(self.db_path)), 2)

    def test_corrupt_event_is_reported_instead_of_skipped(self):
        """STORE-05: invalid JSON, envelope shape, and indexed ID mismatch fail the read."""
        self.store.append(event_fixture())
        for encoded in (
            "{",
            "[]",
            json.dumps({**event_fixture(), "event_id": "different"}),
        ):
            with self.subTest(encoded=encoded):
                with closing(sqlite3.connect(self.db_path, isolation_level=None)) as db:
                    db.execute(
                        "UPDATE events SET event_json=? WHERE cursor=1", (encoded,)
                    )
                with self.assertRaises(LoggingStorageError):
                    self.store.read_events()

    def test_four_independent_processes_keep_all_events(self):
        """STORE-06: independent clients receive only text settings and keep their own journals."""
        processes = []
        for number in range(4):
            config = write_settings(self.folder / f"worker-{number}")
            process = LoggingProcess("write", config, "20")
            self.addCleanup(process.close)
            process.start()
            processes.append(process)
        all_ids = set()
        for process in processes:
            confirmed = process.receive()["ids"]
            self.assertEqual(process.wait(), 0)
            events = read_database(process.config.parent / "events.db")
            self.assertEqual([e["event_id"] for e in events], confirmed)
            self.assertEqual([e["sequence_number"] for e in events], list(range(1, 21)))
            self.assertEqual(
                {e["context"]["process_id"] for e in events}, {process.pid}
            )
            all_ids.update(confirmed)
        self.assertEqual(len(all_ids), 80)

    def test_forked_client_is_rejected_but_new_child_client_works(self):
        """STORE-06: the approved POSIX-only inheritance check never reuses a connection."""
        if not hasattr(os, "fork"):
            self.skipTest(
                "fork inheritance is specific to platforms that provide fork."
            )
        config = write_settings(self.folder / "parent")
        child_config = write_settings(self.folder / "child")
        process = LoggingProcess("fork", config, str(child_config))
        self.addCleanup(process.close)
        process.start()
        self.assertEqual(process.receive()["child_exit"], 0)
        self.assertEqual(process.wait(), 0)
        self.assertEqual(
            read_database(child_config.parent / "events.db")[0]["event_type"],
            "test.child",
        )

    def test_write_lock_failure_requires_reopen_but_keeps_reads(self):
        """STORE-07: real SQLite contention fails explicitly, preserves reads, then recovers."""
        config = write_settings(self.folder / "locked", busy_timeout_seconds=0.05)
        process = LoggingProcess("lock", config)
        self.addCleanup(process.close)
        process.start()
        result = process.receive()
        self.assertEqual(process.wait(), 0)
        self.assertEqual(result["storage_error"], "OperationalError")
        self.assertTrue(result["blocked_after_failure"])
        self.assertEqual(result["read_ids"], [result["confirmed_id"]])
        self.assertEqual(
            [e["event_type"] for e in read_database(config.parent / "events.db")],
            ["test.before", "test.after"],
        )
