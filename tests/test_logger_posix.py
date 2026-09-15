"""Approved POSIX-only A7/E3/F3 cases; never launch Linux or WSL from Windows."""

import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import LoggingStateError, LoggingStorageError
from tests.helpers.logging_fixtures import write_context_settings
from tests.helpers.logging_process import (
    SCRATCH_ROOT,
    LoggingProcess,
    cleanup_directory,
    existing_settings,
    read_database,
)


@unittest.skipUnless(
    os.name == "posix", "POSIX/Linux-specific scenario; skipped on Windows"
)
class PosixJournalTests(unittest.TestCase):
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

    def test_forked_clients_are_rejected_and_child_owns_a_new_connection(self):
        """A7: inherited clients fail before using locks; an explicit child client works."""
        self.logger.close()
        existing_settings(self.config)
        child_config = write_context_settings(self.folder / "child")
        worker = LoggingProcess(
            "fork",
            self.config,
            str(child_config),
            module_name="tests.helpers.logging_fixtures",
        )
        self.addCleanup(worker.close)
        worker.start()
        result = worker.receive()
        self.assertEqual(result["child_exit"], 0)
        self.assertEqual(worker.wait(), 0)
        events = read_database(child_config.parent / "events.db")
        self.assertEqual(events[0]["context"]["process_id"], result["child_pid"])
        self.assertNotEqual(result["child_pid"], worker.pid)

    def test_unlinked_open_database_is_detected_without_recreation(self):
        """E3: the old descriptor cannot hide removal of the journal pathname."""
        self.logger.record_event("prefix")
        self.path.unlink()
        with self.assertRaises(LoggingStorageError):
            self.logger.record_event("must.not.write")
        with self.assertRaises(LoggingStorageError):
            self.logger.read_events()
        with self.assertRaises(LoggingStateError):
            self.logger.record_event("blocked")
        self.logger.close()
        with self.assertRaises(LoggingStorageError):
            self.logger.open()
        self.assertFalse(self.path.exists())

    def test_snapshot_publication_syncs_the_destination_directory(self):
        """F3: POSIX publication includes the containing directory's durability step."""
        self.logger.record_event("prefix")
        original = os.fsync
        synced = []

        def observe(descriptor):
            synced.append(stat.S_ISDIR(os.fstat(descriptor).st_mode))
            return original(descriptor)

        with patch("os.fsync", side_effect=observe):
            manifest = self.logger.export_snapshot(
                self.folder / "snapshot", min_free_bytes=0
            )
        self.assertIn(True, synced)
        self.assertEqual(manifest["event_count"], 1)
