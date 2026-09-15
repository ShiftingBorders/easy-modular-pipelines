"""Approved resource_collector.md D: optional journal writes and failure boundaries."""

import contextlib
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import LoggingStorageError
from core.resource_utils.sampling import ResourceWriter
from tests.helpers.resources import TEMP_ROOT, events, journal, settings


class ResourceWriterTests(unittest.TestCase):
    def setUp(self):
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=TEMP_ROOT)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.config, self.reader, self.context = journal(self.root)
        self.addCleanup(self.reader.close)
        self.settings = settings(self.root)
        self.writer = ResourceWriter(self.settings, "writer-test")
        self.addCleanup(self.writer.close)
        self.sample = {
            "context": self.context,
            "resources": {
                "host_cpu_percent": {
                    "value": 25,
                    "unit": "percent",
                    "kind": "gauge",
                    "scope": "host",
                    "attributes": {
                        "observed_at": "2020-01-01T00:00:00+00:00",
                        "interval_seconds": 2,
                    },
                }
            },
        }

    def test_idle_samples_make_no_files_and_do_not_get_backfilled(self):
        """D: idle data never reaches disk, including after selecting a journal."""
        before = set(self.root.iterdir())
        self.writer.record(self.sample)
        self.assertEqual(set(self.root.iterdir()), before)
        self.assertEqual(events(self.reader, "resources.recorded"), [])
        self.writer.select(str(self.config))
        self.sample["resources"]["host_cpu_percent"]["value"] = 50
        self.writer.record(self.sample)
        records = events(self.reader, "resources.recorded")
        self.assertEqual(len(records), 1)
        self.assertEqual(
            records[0]["data"]["resources"]["host_cpu_percent"]["value"], 50
        )
        self.writer.select(None)
        self.writer.record(self.sample)
        self.assertEqual(len(events(self.reader, "resources.recorded")), 1)

    def test_relative_journal_path_and_observation_time_are_preserved(self):
        """D/A: copied client configuration anchors DB paths at the original file."""
        before = Path.cwd()
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir()
        try:
            os.chdir(elsewhere)
            self.writer.select(str(self.config))
            self.writer.record(self.sample)
        finally:
            os.chdir(before)
        record = events(self.reader, "resources.recorded")[0]
        self.assertEqual(
            record["context"]["experiment_id"], self.context["experiment_id"]
        )
        self.assertEqual(record["context"]["source"], "resource_collector")
        self.assertEqual(
            record["data"]["resources"],
            self.sample["resources"]
            | {
                "host_cpu_percent": {
                    **self.sample["resources"]["host_cpu_percent"],
                    "estimated": False,
                }
            },
        )
        self.assertGreater(
            datetime.fromisoformat(record["occurred_at"]),
            datetime.fromisoformat("2020-01-01T00:00:00+00:00"),
        )
        self.assertEqual(list(elsewhere.iterdir()), [])

    def test_real_sqlite_lock_drops_unconfirmed_sample_and_reports_a_gap(self):
        """D: another connection holds the write lock; retry remains bounded."""
        self.writer.select(str(self.config))
        self.writer.record(self.sample)
        with contextlib.closing(sqlite3.connect(self.root / "events.sqlite")) as locked:
            locked.execute("BEGIN IMMEDIATE")
            try:
                self.writer.record(self.sample)
                self.assertIsNotNone(self.writer.error)
                self.assertEqual(self.writer.unconfirmed_samples, 1)
                self.writer.record(self.sample)
                self.assertEqual(self.writer.unconfirmed_samples, 2)
            finally:
                locked.rollback()
        with patch(
            "core.resource_utils.sampling.time.monotonic",
            return_value=self.writer._retry_at + 1,
        ):
            self.writer.record(self.sample)
        self.assertEqual(len(events(self.reader, "resources.recorded")), 2)
        gaps = events(self.reader, "resources.gap")
        self.assertEqual(gaps[-1]["data"]["unconfirmed_samples"], 2)
        self.assertIsNone(self.writer.error)

    def test_open_failure_is_local_and_retry_is_throttled(self):
        """D: a failed optional client does not recurse into logging or retry immediately."""
        self.writer.select(str(self.config))
        with patch.object(
            OperationLogger,
            "open",
            side_effect=LoggingStorageError("collector open failed"),
        ) as opened:
            self.writer.record(self.sample)
            for _ in range(10):
                self.writer.record(self.sample)
            self.assertEqual(opened.call_count, 1)
        self.assertIn("collector open failed", self.writer.error)
        self.assertEqual(self.writer.unconfirmed_samples, 11)
        self.reader.record_event("runner.still_available", {}, context=self.context)
        self.assertEqual(len(events(self.reader, "runner.still_available")), 1)

    def test_committed_but_unconfirmed_sample_is_never_replayed(self):
        """D: an ambiguous commit is not duplicated after reconnecting the client."""
        self.writer.select(str(self.config))
        record = OperationLogger.record_resources

        def ambiguous(client, *args, **kwargs):
            record(client, *args, **kwargs)
            raise LoggingStorageError("commit acknowledgement lost")

        with patch.object(OperationLogger, "record_resources", ambiguous):
            self.writer.record(self.sample)
        self.assertEqual(self.writer.unconfirmed_samples, 1)
        self.sample["resources"]["host_cpu_percent"]["value"] = 75
        with patch(
            "core.resource_utils.sampling.time.monotonic",
            return_value=self.writer._retry_at + 1,
        ):
            self.writer.record(self.sample)
        self.assertEqual(
            [
                event["data"]["resources"]["host_cpu_percent"]["value"]
                for event in events(self.reader, "resources.recorded")
            ],
            [25, 75],
        )

    def test_low_disk_and_corrupt_journal_make_monitoring_unavailable(self):
        """D: storage failure is exposed without endless synchronous retries."""
        self.writer.select(str(self.config))
        with patch("core.logger_utils.storage.shutil.disk_usage") as usage:
            from collections import namedtuple

            usage.return_value = namedtuple("Usage", "total used free")(100, 100, 0)
            config = json.loads(self.config.read_text())
            config["logging"]["min_free_bytes"] = 1
            self.config.write_text(json.dumps(config), encoding="utf-8")
            self.writer.record(self.sample)
        self.assertIsNotNone(self.writer.error)
        self.writer.close()
        self.reader.close()
        (self.root / "events.sqlite").write_bytes(b"not a sqlite database")
        with patch(
            "core.resource_utils.sampling.time.monotonic",
            return_value=self.writer._retry_at + 1,
        ):
            self.writer.record(self.sample)
        self.assertIsNotNone(self.writer.error)
        self.assertIsNone(self.writer._client)

    def test_switching_journals_closes_the_old_client_and_separates_runs(self):
        """D: changing the selected experiment cannot write later samples into the old DB."""
        other_root = self.root / "second"
        other_root.mkdir()
        config, reader, context = journal(other_root)
        self.addCleanup(reader.close)
        self.writer.select(str(self.config))
        self.writer.record(self.sample)
        first_client = self.writer._client
        self.writer.select(str(config))
        self.assertIsNone(first_client._store)
        self.writer.record({**self.sample, "context": context})
        self.assertEqual(len(events(self.reader, "resources.recorded")), 1)
        second = events(reader, "resources.recorded")
        self.assertEqual(len(second), 1)
        self.assertEqual(
            second[0]["context"]["experiment_id"], context["experiment_id"]
        )
