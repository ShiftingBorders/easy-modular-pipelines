"""Approved B7/D3/F5/G4: large events traverse every journal read/export path."""

import hashlib
import shutil
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from core.logger import OperationLogger
from core.logger_utils.filtered import FilteredJournal
from tests.helpers.logging_fixtures import write_context_settings
from tests.helpers.logging_process import SCRATCH_ROOT, cleanup_directory, make_store


class LargeJournalEventTests(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.config = write_context_settings(self.folder)
        self.logger = OperationLogger(self.config)
        self.addCleanup(self.logger.close)
        self.logger.open()

    def test_event_above_old_limit_survives_pages_exports_restore_and_projection(self):
        """B7/D3/F5/G4: one >16-MiB event stays whole, not silently split or truncated."""
        operation = self.logger.start_operation("rebuild", "large event")
        snapshot = self.folder / "before"
        manifest = self.logger.export_snapshot(snapshot, min_free_bytes=0)
        text = "x" * (16 * 1024 * 1024 + 1)
        expected_hash = hashlib.sha256(text.encode()).hexdigest()
        event_id = self.logger.record_event(
            "large", {"text": text}, operation=operation
        )
        self.logger.finish_operation(operation)
        first = self.logger.read_events(limit=1000)
        second = self.logger.read_events(first["checkpoint"], limit=1000)
        self.assertEqual(len(first["events"]), 1)
        self.assertEqual(len(second["events"]), 1)
        observed = second["events"][0]["event"]
        self.assertEqual(observed["event_id"], event_id)
        self.assertEqual(
            hashlib.sha256(observed["data"]["text"].encode()).hexdigest(), expected_hash
        )
        changes = self.logger.read_changes(limit=1)
        large_change = self.logger.read_changes(changes["checkpoint"], limit=1000)[
            "changes"
        ]
        self.assertEqual(len(large_change), 1)
        self.assertEqual(
            hashlib.sha256(
                large_change[0]["entry"]["event"]["data"]["text"].encode()
            ).hexdigest(),
            expected_hash,
        )
        del large_change, second, observed

        after = self.logger.export_snapshot(self.folder / "after", min_free_bytes=0)
        self.assertEqual(after["event_count"], 3)
        diagnostics = self.folder / "diagnostics"
        self.logger.export_diagnostics([operation.get_operation_id()], diagnostics)
        restored_path = self.folder / "restored.db"
        shutil.copyfile(snapshot / "journal.sqlite", restored_path)
        store = make_store(restored_path, open_mode="existing")
        self.addCleanup(store.close)
        store.complete_restore(
            manifest,
            restoration_id=uuid4().hex,
            new_generation=uuid4().hex,
            diagnostics=diagnostics,
        )
        store.open()
        small = store.read_events(limit=1)
        recovered = store.read_events(small["checkpoint"], limit=1000)["events"]
        self.assertEqual(len(recovered), 1)
        self.assertEqual(
            hashlib.sha256(recovered[0]["event"]["data"]["text"].encode()).hexdigest(),
            expected_hash,
        )
        del recovered

        config = write_context_settings(
            self.folder / "reader", db_path=str(restored_path), open_mode="existing"
        )
        view = FilteredJournal(config, self.folder / "view.db")
        self.addCleanup(view.close)
        view.open()
        self.assertIsNotNone(view.refresh())
        page = view.read_events(limit=1)
        large = view.read_events(page["checkpoint"], limit=1000)
        self.assertEqual(large["source"], "filtered")
        self.assertEqual(len(large["events"]), 1)
        self.assertEqual(
            hashlib.sha256(
                large["events"][0]["event"]["data"]["text"].encode()
            ).hexdigest(),
            expected_hash,
        )
