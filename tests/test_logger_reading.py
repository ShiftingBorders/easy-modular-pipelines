"""Approved A4/A6, C3/C7/C8, D1-D5: one format and identified reads."""

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from uuid import uuid4

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
    cleanup_directory,
    journal_identity,
    make_store,
    read_database,
)


class JournalReadingTests(unittest.TestCase):
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

    def test_create_existing_and_unknown_files_are_not_adopted(self):
        """A4/A5: neither create nor existing may invent a replacement journal."""
        before = self.path.read_bytes()
        with self.assertRaises(LoggingConfigurationError):
            make_store(self.path).open()
        self.assertEqual(self.path.read_bytes(), before)
        identity = journal_identity(self.path)
        for name, content in (
            ("missing.db", None),
            ("empty.db", b""),
            ("foreign.db", b"not SQLite"),
        ):
            with self.subTest(name=name):
                path = self.folder / name
                if content is not None:
                    path.write_bytes(content)
                store = make_store(
                    path, open_mode="existing", expected_journal=identity
                )
                self.addCleanup(store.close)
                with self.assertRaises(
                    (LoggingConfigurationError, LoggingStorageError)
                ):
                    store.open()
                if content is None:
                    self.assertFalse(path.exists())
                else:
                    self.assertEqual(path.read_bytes(), content)
        self.logger.record_event("still.healthy")

        # A valid SQLite file from the former event-only layout is still an
        # incompatible journal. Rejection must not silently add missing tables.
        legacy = self.folder / "former-layout.db"
        with closing(sqlite3.connect(legacy, isolation_level=None)) as db:
            db.execute(
                "CREATE TABLE events (cursor INTEGER PRIMARY KEY AUTOINCREMENT, "
                "event_id TEXT NOT NULL UNIQUE, producer_instance_id TEXT NOT NULL, "
                "sequence_number INTEGER NOT NULL CHECK (sequence_number > 0), "
                "event_json TEXT NOT NULL, UNIQUE (producer_instance_id, sequence_number))"
            )
            db.execute("PRAGMA application_id=1162694732")
            db.execute("PRAGMA user_version=1")
            before = list(db.iterdump())
        old = make_store(legacy, open_mode="existing", expected_journal=identity)
        self.addCleanup(old.close)
        with self.assertRaises(LoggingConfigurationError):
            old.open()
        with closing(sqlite3.connect(legacy)) as db:
            self.assertEqual(list(db.iterdump()), before)

    def test_new_client_with_old_configuration_rejects_generation(self):
        """A6/F9: expected UUIDs protect even a newly constructed writer."""
        config = write_context_settings(
            self.folder / "stale",
            db_path=str(self.path),
            open_mode="existing",
            expected_journal={**journal_identity(self.path), "generation": uuid4().hex},
        )
        with self.assertRaises(JournalGenerationChanged) as caught:
            OperationLogger(config).open()
        self.assertEqual(caught.exception.code, "journal_generation_changed")
        self.assertEqual(caught.exception.actual, journal_identity(self.path))
        self.assertEqual(read_database(self.path), [])

    def test_raw_pages_have_non_consuming_identified_checkpoints(self):
        """D1/D3: a checkpoint resumes exactly after the last emitted event."""
        expected = [
            self.logger.record_event("page", {"index": index}) for index in range(5)
        ]
        first = self.logger.read_events(limit=2)
        second = self.logger.read_events(first["checkpoint"], limit=2)
        third = self.logger.read_events(second["checkpoint"], limit=2)
        self.assertEqual(
            [
                row["event"]["event_id"]
                for page in (first, second, third)
                for row in page["events"]
            ],
            expected,
        )
        self.assertEqual(
            [page["has_more"] for page in (first, second, third)], [True, True, False]
        )
        self.assertEqual(
            first["checkpoint"] | {"cursor": 0},
            {**journal_identity(self.path), "cursor": 0},
        )
        self.assertEqual(self.logger.read_events(limit=2), first)

    def test_invalid_and_stale_checkpoints_do_not_poison_readers(self):
        """D1/D2: argument/ownership rejection is not a primary storage incident."""
        identity = journal_identity(self.path)
        for key, reader in (
            ("cursor", self.logger.read_events),
            ("change_cursor", self.logger.read_changes),
        ):
            for owner_field in ("generation", "journal_id"):
                checkpoint = {**identity, owner_field: uuid4().hex, key: 0}
                with (
                    self.subTest(key=key, owner=owner_field),
                    self.assertRaises(JournalGenerationChanged),
                ):
                    reader(checkpoint)
            with self.assertRaises(LoggingStateError):
                reader({**identity, key: 1})
            for value in (-1, True, "1", 2**63):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    reader({**identity, key: value})
        self.assertEqual(list(self.folder.glob("*.emergency-*.jsonl")), [])
        self.logger.record_event("reader.still.healthy")

    def test_confirmation_has_its_own_change_without_a_second_event(self):
        """C3/C8/D5: historical service state and runner confirmation remain observable."""
        event_id = self.logger.record_command_result(
            "request", {"n": 1}, author="service", outcome="succeeded"
        )
        first = self.logger.read_changes()
        self.assertEqual(len(first["changes"]), 1)
        self.assertTrue(first["changes"][0]["provisional"])
        raw_checkpoint = self.logger.read_events()["checkpoint"]
        confirmed = self.logger.record_command_result(
            "request", {"n": 1}, author="runner", outcome="succeeded"
        )
        self.assertEqual(confirmed, event_id)
        self.assertEqual(self.logger.read_events(raw_checkpoint)["events"], [])
        second = self.logger.read_changes(first["checkpoint"])
        self.assertEqual(len(second["changes"]), 1)
        change = second["changes"][0]
        self.assertEqual(change["effective_author"], "runner")
        self.assertFalse(change["provisional"])
        self.assertTrue(change["result_changed"])
        self.assertEqual(change["entry"]["event"]["data"]["author"], "service")
        self.assertEqual(change["observation"]["author"], "runner")
        history = self.logger.read_changes()["changes"]
        self.assertEqual([item["provisional"] for item in history], [True, False])
        self.logger.record_command_result(
            "request", {"n": 1}, author="runner", outcome="succeeded"
        )
        self.assertEqual(self.logger.read_changes(second["checkpoint"])["changes"], [])

    def test_ignored_response_does_not_create_another_effective_result(self):
        """C4/C8/D4: late diagnostics change the journal, not the settled outcome."""
        runner = self.logger.record_command_result(
            "request", {}, author="runner", outcome="timed_out"
        )
        checkpoint = self.logger.read_changes()["checkpoint"]
        service = self.logger.record_command_result(
            "request", {"late": True}, author="service", outcome="succeeded"
        )
        changes = self.logger.read_changes(checkpoint)["changes"]
        self.assertEqual(len(changes), 1)
        self.assertFalse(changes[0]["result_changed"])
        self.assertEqual(changes[0]["effective_event_id"], runner)
        self.assertEqual(changes[0]["observed_event"]["event_id"], service)
        self.assertEqual(len(self.logger.read_events()["events"]), 2)
        effective = self.logger.read_events(view="effective")["events"]
        self.assertEqual([row["event"]["event_id"] for row in effective], [runner])

    def test_corrupt_change_rows_fail_closed(self):
        """C7: invalid changes must not silently disappear from a dashboard/alert feed."""
        for index, mutation in enumerate(
            ("json", "missing-event", "bad-state", "bad-owner")
        ):
            with self.subTest(mutation=mutation):
                config = write_context_settings(self.folder / str(index))
                with OperationLogger(config) as logger:
                    logger.record_command_result(
                        "request", {}, author="runner", outcome="succeeded"
                    )
                    path = config.parent / "events.db"
                    with closing(sqlite3.connect(path, isolation_level=None)) as db:
                        data = json.loads(
                            db.execute(
                                "SELECT change_json FROM journal_changes"
                            ).fetchone()[0]
                        )
                        if mutation == "missing-event":
                            data["effective_event_id"] = "missing"
                        elif mutation == "bad-state":
                            data["provisional"] = "yes"
                        elif mutation == "bad-owner":
                            data["observation"]["context"]["service_id"] = "other"
                        db.execute(
                            "UPDATE journal_changes SET change_json=?",
                            ("{" if mutation == "json" else json.dumps(data),),
                        )
                    with self.assertRaises(LoggingStorageError):
                        logger.read_changes()
                    with self.assertRaises(LoggingStateError):
                        logger.record_event("must.not.write")
                    self.assertEqual(len(read_database(path)), 1)

    def test_managed_results_cannot_be_appended_as_opaque_events(self):
        """C7/A5: one managed command-result contract, without a legacy bypass."""
        event = context_event()
        event["event_type"] = "command.result"
        event["data"] = {"opaque": True}
        with self.assertRaises(ValueError):
            self.logger._store.append(event)
        for kind in ("template.applied", "attempt.parameters", "command.result"):
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                self.logger.record_event(kind, {})
        self.logger.record_event("allowed.fact")
        self.assertEqual(len(read_database(self.path)), 1)

    def test_empty_effective_page_still_advances_to_later_results(self):
        """D3/D4: an entire skipped scan must still expose a resumable checkpoint."""
        for number in range(1000):
            self.logger.record_command_result(
                str(number), {}, author="service", outcome="succeeded"
            )
        winners = [
            self.logger.record_command_result(
                str(number), {}, author="runner", outcome="failed"
            )
            for number in range(1000)
        ]
        first = self.logger.read_events(view="effective", limit=10)
        self.assertEqual(first["events"], [])
        self.assertTrue(first["has_more"])
        self.assertGreater(first["checkpoint"]["cursor"], 0)
        second = self.logger.read_events(
            first["checkpoint"], view="effective", limit=10
        )
        self.assertEqual(
            [entry["event"]["event_id"] for entry in second["events"]], winners[:10]
        )
