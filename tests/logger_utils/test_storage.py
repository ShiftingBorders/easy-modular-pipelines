"""STORE-01..07: independent SQLite checks for the approved local journal contract."""

import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

from core.logger import OperationLogger
from core.logger_utils.events import (
    JournalGenerationChanged,
    LoggingConfigurationError,
    LoggingStateError,
    LoggingStorageError,
)
from tests.helpers.logging_fixtures import context_event, write_context_settings
from tests.helpers.logging_process import (
    SCRATCH_ROOT,
    LoggingProcess,
    checkpoint_for,
    cleanup_directory,
    event_fixture,
    journal_identity,
    make_store,
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
        self.store = make_store(self.db_path)
        self.addCleanup(self.store.close)
        self.store.open()

    def test_schema_reopens_and_writer_uses_wal_full(self):
        """STORE-01, STORE-02: verify durability settings on the writer itself."""
        connection = self.store._connection
        self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        self.assertEqual(connection.execute("PRAGMA synchronous").fetchone()[0], 2)
        identity = journal_identity(self.db_path)
        for value in identity.values():
            self.assertEqual(UUID(value).hex, value)
        self.assertNotEqual(identity["journal_id"], identity["generation"])
        self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
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
            "DROP TABLE journal_changes",
            "CREATE TABLE unrelated (value TEXT)",
            "CREATE INDEX extra_index ON events(sequence_number)",
            "CREATE TRIGGER extra_trigger AFTER INSERT ON events BEGIN SELECT 1; END",
        )
        for index, change in enumerate(changes):
            with self.subTest(change=change):
                path = self.folder / f"incompatible-{index}.db"
                store = make_store(path)
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
        inaccessible = make_store(self.folder / "new" / "events.db")
        failure = PermissionError("Controlled directory denial")
        with (
            patch.object(Path, "mkdir", side_effect=failure),
            self.assertRaises(LoggingStorageError) as caught,
        ):
            inaccessible.open()
        self.assertIs(caught.exception.__cause__, failure)
        self.assertFalse((self.folder / "new").exists())
        with self.assertRaises(LoggingConfigurationError):
            make_store(self.folder).open()
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
        store = make_store(self.folder / "failed-open.db")
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
        first = self.store.read_events(limit=3)["events"]
        second = self.store.read_events(
            checkpoint=checkpoint_for(self.store, first[-1]["cursor"]), limit=3
        )["events"]
        third = self.store.read_events(
            checkpoint=checkpoint_for(self.store, second[-1]["cursor"]), limit=3
        )["events"]
        self.assertEqual(
            [r["event"] for r in first + second + third],
            [event_fixture(n) for n in range(1, 8)],
        )
        self.assertEqual(
            [r["cursor"] for r in first + second + third], list(range(1, 8))
        )
        self.assertEqual(self.store.read_events(limit=3)["events"], first)
        self.assertEqual(
            self.store.read_events(checkpoint=checkpoint_for(self.store, 7))["events"],
            [],
        )
        for value in (-1, True, 1.5, 2**63):
            with self.assertRaises(ValueError):
                self.store.read_events(checkpoint=checkpoint_for(self.store, value))[
                    "events"
                ]
        for value in (0, 1001, True, 1.5):
            with self.assertRaises(ValueError):
                self.store.read_events(limit=value)["events"]

    def test_read_page_is_bounded_by_encoded_bytes(self):
        """STORE-04: three six-MiB events split into two pages without a missing record."""
        self.store.close()
        store = make_store(self.db_path, open_mode="existing", max_event_bytes=16777216)
        self.addCleanup(store.close)
        store.open()
        for number in range(1, 4):
            event = event_fixture(number)
            event["data"] = {"text": "x" * (6 * 1024 * 1024)}
            store.append(event)
        first = store.read_events(limit=1000)["events"]
        self.assertEqual(len(first), 2)
        second = store.read_events(
            checkpoint=checkpoint_for(store, first[-1]["cursor"]), limit=1000
        )["events"]
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
        limited = make_store(self.db_path, open_mode="existing", max_event_bytes=1024)
        self.addCleanup(limited.close)
        limited.open()
        self.assertEqual(limited.read_events()["events"][0]["event"], event)
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
                    self.store.read_events()["events"]

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

    def test_bad_result_arguments_are_rejected_without_poisoning_store(self):
        event = context_event()
        event["event_type"] = "command.result"
        event["context"]["request_id"] = "request"
        event["data"] = {
            "request_id": "request",
            "author": "runner",
            "outcome": "succeeded",
            "response": {},
            "ignored": "forged",
            "supersedes": [],
        }
        with self.assertRaises(ValueError):
            self.store.append_command_result(event)
        self.store.append(context_event(2))
        self.assertEqual(len(self.store.read_events()["events"]), 1)

    def test_corrupt_payload_blocks_writes_but_repaired_data_remains_readable(self):
        event = context_event()
        self.store.append(event)
        with closing(sqlite3.connect(self.db_path, isolation_level=None)) as db:
            db.execute("UPDATE events SET event_json='{'")
        with self.assertRaises(LoggingStorageError):
            self.store.read_events()["events"]
        with self.assertRaises(LoggingStateError):
            self.store.append(context_event(2))
        with closing(sqlite3.connect(self.db_path, isolation_level=None)) as db:
            db.execute("UPDATE events SET event_json=?", (json.dumps(event),))
        self.assertEqual(self.store.read_events()["events"][0]["event"], event)
        with self.assertRaises(LoggingStateError):
            self.store.append(context_event(2))
        self.store.close()
        self.store.open()
        self.store.append(context_event(2))
        self.assertEqual(len(self.store.read_events()["events"]), 2)

    def test_changed_identity_and_generation_reject_stale_clients(self):
        for field, value in (("generation", uuid4().hex), ("journal_id", uuid4().hex)):
            with self.subTest(field=field):
                folder = self.folder / field
                config = write_context_settings(folder)
                with OperationLogger(config) as logger:
                    logger.record_event("prefix")
                    with closing(
                        sqlite3.connect(folder / "events.db", isolation_level=None)
                    ) as db:
                        db.execute(f"UPDATE journal_info SET {field}=?", (value,))
                    with self.assertRaises(LoggingStorageError):
                        logger.get_journal_info()
                    with self.assertRaises(LoggingStateError):
                        logger.record_event("must-not-write")
                    logger.close()
                    with self.assertRaises(JournalGenerationChanged):
                        logger.open()
                self.assertEqual(len(read_database(folder / "events.db")), 1)

    def test_live_schema_changes_fail_closed(self):
        self.store.append(context_event())
        with closing(sqlite3.connect(self.db_path, isolation_level=None)) as db:
            db.execute("CREATE INDEX unexpected ON events(sequence_number)")
        with self.assertRaises(LoggingStorageError):
            self.store.append(context_event(2))
        with self.assertRaises(LoggingStateError):
            self.store.append(context_event(3))
        self.assertEqual(read_database(self.db_path), [context_event()])

    def test_replaced_file_is_not_adopted_on_reopen(self):
        self.store.append(context_event())
        self.store.close()
        replacement = self.folder / "replacement.db"
        other = make_store(replacement)
        other.open()
        other.append(context_event(2))
        other.close()
        os.replace(replacement, self.db_path)
        before = self.db_path.read_bytes()
        with self.assertRaises(LoggingStorageError):
            self.store.open()
        self.assertEqual(self.db_path.read_bytes(), before)
        self.assertEqual(read_database(self.db_path), [context_event(2)])

    def test_corrupt_command_index_and_request_links_fail_closed(self):
        mutations = (
            ("missing-event", "UPDATE command_results SET runner_event_id='missing'"),
            ("bad-json", "UPDATE command_results SET identity_json='{'"),
            (
                "wrong-authority",
                "UPDATE command_results SET effective_author='service'",
            ),
            ("wrong-request", None),
            ("wrong-author", None),
        )
        for label, statement in mutations:
            with self.subTest(label=label):
                folder = self.folder / label
                config = write_context_settings(folder)
                with OperationLogger(config) as logger:
                    logger.record_command_result(
                        "request", {}, author="runner", outcome="succeeded"
                    )
                    path = folder / "events.db"
                    with closing(sqlite3.connect(path, isolation_level=None)) as db:
                        if statement is not None:
                            db.execute(statement)
                        else:
                            event = json.loads(
                                db.execute("SELECT event_json FROM events").fetchone()[
                                    0
                                ]
                            )
                            if label == "wrong-request":
                                event["context"]["request_id"] = "other"
                            else:
                                event["data"]["author"] = "service"
                            db.execute(
                                "UPDATE events SET event_json=?", (json.dumps(event),)
                            )
                    with self.assertRaises(LoggingStorageError):
                        logger.read_command_result("request")
                    with self.assertRaises(LoggingStateError):
                        logger.record_event("must-not-write")
                    with closing(sqlite3.connect(path)) as db:
                        self.assertEqual(
                            db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 1
                        )
