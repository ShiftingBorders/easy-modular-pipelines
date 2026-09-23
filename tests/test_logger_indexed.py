"""Approved T006–T011: indexed raw event reads without source mutations."""

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import LoggingError, LoggingStateError
from tests.dashboard_tests.helpers import cleanup_directory, temporary_directory
from tests.helpers.logging_process import existing_settings, write_settings


class IndexedLoggerTests(unittest.TestCase):
    def setUp(self):
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        self.root = Path(temporary.name)
        self.config = write_settings(self.root)
        self.writer = OperationLogger(self.config)
        self.writer.open()
        self.addCleanup(self.writer.close)
        existing_settings(self.config)
        self.reader = OperationLogger(self.config, read_only=True)
        self.reader.open()
        self.addCleanup(self.reader.close)

    def test_id_lookup_tail_and_empty_boundary(self):
        """T006/T007: original envelopes, cursor ordering, missing IDs and pages."""
        self.assertEqual(self.reader.read_event_batch()["events"], [])
        identifiers = [
            self.writer.record_event("sample", {"number": i}) for i in range(5)
        ]
        indexed = self.reader.read_event_batch(
            [identifiers[4], "missing", identifiers[1]]
        )
        self.assertEqual(
            [row["event"]["event_id"] for row in indexed["events"]],
            [identifiers[1], identifiers[4]],
        )
        raw = self.reader.read_events(limit=100)["events"]
        self.assertEqual(indexed["events"], [raw[1], raw[4]])
        tail = self.reader.read_event_batch(limit=2)
        self.assertEqual([row["cursor"] for row in tail["events"]], [5, 4])
        self.assertEqual(
            [
                row["cursor"]
                for row in self.reader.read_event_batch(before=4, limit=2)["events"]
            ],
            [3, 2],
        )
        self.assertEqual(self.reader.read_event_batch(before=1)["events"], [])
        empty = self.reader.read_event_batch([])
        self.assertEqual(empty["events"], [])
        self.assertEqual(empty["boundary"], tail["boundary"])

    def test_invalid_arguments_are_rejected_without_poisoning_reader(self):
        """T008: input types, duplicate IDs, mutually exclusive modes and boundaries."""
        for options in (
            {"limit": 0},
            {"limit": 1001},
            {"limit": True},
            {"limit": "1"},
            {"before": 0},
            {"before": -1},
            {"before": True},
            {"before": 1.5},
            {"event_ids": "one"},
            {"event_ids": [""]},
            {"event_ids": [1]},
            {"event_ids": ["same", "same"]},
            {"event_ids": ["a", "b"], "limit": 1},
            {"event_ids": [], "before": 1},
        ):
            with (
                self.subTest(options=options),
                self.assertRaises((ValueError, TypeError)),
            ):
                self.reader.read_event_batch(**options)
        self.assertEqual(self.reader.read_event_batch(limit=1)["events"], [])
        self.assertEqual(self.reader.read_event_batch(limit=1000)["events"], [])

    def test_byte_limit_returns_whole_events_and_allows_continuation(self):
        """T009: UTF-8 byte limits do not truncate the first oversized event."""
        identifiers = [
            self.writer.record_event("large", {"text": "Ж" * 100}) for _ in range(3)
        ]
        with patch("core.logger_utils.storage._PAGE_BYTES", 1):
            tail = self.reader.read_event_batch()
            first = self.reader.read_event_batch(identifiers)
            self.assertEqual(len(tail["events"]), 1)
            self.assertEqual(tail["events"][0]["event"]["data"]["text"], "Ж" * 100)
            self.assertEqual(first["events"][0]["event"]["event_id"], identifiers[0])
            next_page = self.reader.read_event_batch(before=tail["events"][0]["cursor"])
            self.assertEqual(
                next_page["events"][0]["event"]["event_id"], identifiers[1]
            )
        self.assertFalse(self.reader._store._connection.in_transaction)

    def test_interrupted_read_releases_transaction_and_checks_owner(self):
        """T010: interruption, closed clients and process ownership are explicit."""
        self.writer.record_event("sample")
        with (
            patch.object(
                self.reader._store, "_decode_row", side_effect=KeyboardInterrupt
            ),
            self.assertRaises((KeyboardInterrupt, LoggingError)),
        ):
            self.reader.read_event_batch()
        self.assertFalse(self.reader._store._connection.in_transaction)
        with (
            patch("os.getpid", return_value=os.getpid() + 1),
            self.assertRaises(LoggingStateError),
        ):
            self.reader.read_event_batch()
        self.reader.close()
        with self.assertRaises(LoggingStateError):
            self.reader.read_event_batch()

    def test_reads_use_source_indexes_without_schema_or_content_writes(self):
        """T001/T011: SELECT by ID/cursor uses source indexes and preserves rows."""
        identity = self.writer.record_event("sample", {"text": "original"})
        connection = self.writer._store._connection
        before = list(connection.iterdump())
        self.reader.read_event_batch([identity])
        self.reader.read_event_batch()
        self.assertEqual(list(connection.iterdump()), before)
        for sql, args in (
            (
                "SELECT event_json FROM events WHERE event_id IN (?) ORDER BY cursor",
                (identity,),
            ),
            (
                "SELECT event_json FROM events WHERE cursor<? ORDER BY cursor DESC LIMIT ?",
                (10, 1),
            ),
        ):
            with self.subTest(sql=sql):
                plan = " ".join(
                    str(row)
                    for row in connection.execute("EXPLAIN QUERY PLAN " + sql, args)
                )
                self.assertNotIn("SCAN events", plan)
                self.assertIn("SEARCH events", plan)
