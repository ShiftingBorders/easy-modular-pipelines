"""Minimum approved checkpoint-history coverage M1-M5."""

import json
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

from core.journal.events import LoggingStorageError
from core.journal.filtered import FilteredJournal
from core.journal.logger import OperationLogger
from core.models.journal_diagnostics import JournalSnapshotManifest
from tests.helpers.journal_owner import WriterOwner
from tests.helpers.logging_fixtures import context_event, write_context_settings
from tests.helpers.logging_process import SCRATCH_ROOT, cleanup_directory, make_store


def checkpoint_event(number, count=1000, *, run="run-A", kind="runner.checkpoint"):
    event = context_event(number)
    event["event_type"] = kind
    event["context"] = {"experiment_id": "experiment-A", "run_id": run}
    event["data"] = {
        "schema_version": 4,
        "experiment_id": "experiment-A",
        "run_id": run,
        "used_request_ids": [str(UUID(int=index + 1)) for index in range(count)],
        "other": {"unicode": "История", "ordered": [None, True, 1.0]},
    }
    return event


def legacy_database(path):
    schema = Path(__file__).with_name("fixtures") / "journal_v2.sql"
    with closing(sqlite3.connect(path)) as db:
        db.executescript(schema.read_text(encoding="utf-8"))


class CheckpointRequestIdTests(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.path = self.folder / "events.db"

    def open_store(self, path=None, **options):
        store = make_store(path or self.path, **options)
        self.addCleanup(store.close)
        store.open()
        return store

    def test_exact_history_boundaries_and_physical_savings(self):
        """M1: full history and random reads survive bounded compact storage."""
        store = self.open_store()
        expected = [checkpoint_event(i + 1, 1000 + i // 2) for i in range(70)]
        expected += [
            checkpoint_event(71, 5),
            checkpoint_event(72, 6, run="run-B"),
            checkpoint_event(73, 7, kind="rebuild.checkpoint"),
            {**context_event(74), "data": {"used_request_ids": ["custom"]}},
            {**checkpoint_event(75), "data": {"used_request_ids": ["custom"]}},
        ]
        for event in expected:
            store.append(event)
        self.assertEqual(
            store._connection.execute("PRAGMA user_version").fetchone()[0], 3
        )
        rows = store._connection.execute(
            "SELECT event_id, depth FROM checkpoint_request_ids ORDER BY event_cursor"
        ).fetchall()
        self.assertEqual(len(rows), 73)
        self.assertEqual([row[1] for row in rows[:70]], [i % 32 for i in range(70)])
        self.assertEqual([row[1] for row in rows[70:]], [0, 0, 0])
        self.assertEqual(
            [item["event"] for item in store.read_events(limit=1000)["events"]],
            expected,
        )
        self.assertEqual(
            [
                item["event"]
                for item in store.read_events(limit=1000, view="effective")["events"]
            ],
            expected,
        )
        self.assertEqual(
            [
                item["entry"]["event"]
                for item in store.read_changes(limit=1000)["changes"]
            ],
            expected,
        )
        encoded = json.dumps(expected[31], ensure_ascii=False, separators=(",", ":"))
        entry = store._event_entry(expected[31]["event_id"], store._connection)
        self.assertEqual(entry.encoded_event, encoded)
        with patch(
            "core.journal.storage._PAGE_BYTES", len(encoded.encode("utf-8")) + 100
        ):
            self.assertEqual(len(store.read_events(limit=1000)["events"]), 1)
            ids = [event["event_id"] for event in expected[:2]]
            self.assertEqual(len(store.read_event_batch(ids)["events"]), 1)
        store.close()
        reader = self.open_store(open_mode="existing", read_only=True)
        self.assertEqual(
            reader.read_event_batch([expected[31]["event_id"]])["events"][0]["event"],
            expected[31],
        )
        self.assertEqual(
            reader.read_event_batch(limit=1, before=33)["events"][0]["event"],
            expected[31],
        )
        reader.close()
        legacy_path = self.folder / "legacy.db"
        legacy_database(legacy_path)
        legacy = self.open_store(legacy_path, open_mode="existing")
        for event in expected:
            legacy.append(event)
        legacy.close()
        self.assertLess(self.path.stat().st_size, legacy_path.stat().st_size / 3)

    def test_legacy_writes_and_snapshot_restore_remain_compatible(self):
        """M2: format 2 remains usable without an implicit migration."""
        legacy_database(self.path)
        store = self.open_store(open_mode="existing")
        event = checkpoint_event(1)
        store.append(event)
        self.assertEqual(
            store._connection.execute("PRAGMA user_version").fetchone()[0], 2
        )
        self.assertEqual(
            json.loads(
                store._connection.execute("SELECT event_json FROM events").fetchone()[0]
            ),
            event,
        )
        manifest = store.export_snapshot(self.folder / "snapshot", min_free_bytes=0)
        self.assertEqual(manifest["storage_schema_version"], 2)
        self.assertEqual(store.get_journal_info()["schema_version"], 2)
        restored_path = self.folder / "restored.db"
        shutil.copyfile(self.folder / "snapshot/journal.sqlite", restored_path)
        restored = make_store(restored_path, open_mode="existing")
        self.addCleanup(restored.close)
        restored.complete_restore(
            manifest, new_generation=uuid4().hex, restoration_id=uuid4().hex
        )
        restored.open()
        restored.append(checkpoint_event(2, 1001))
        self.assertEqual(
            restored._connection.execute("PRAGMA user_version").fetchone()[0], 2
        )
        self.assertEqual(restored.read_events()["events"][0]["event"], event)

    def test_atomic_append_and_replay_after_reopening(self):
        """M3: compact history is part of the existing commit/replay boundary."""
        store = self.open_store()
        first, second = checkpoint_event(1), checkpoint_event(2, 1001)
        store.append(first)
        stored = json.loads(
            store._connection.execute("SELECT event_json FROM events").fetchone()[0]
        )
        self.assertEqual(stored["data"]["used_request_ids"], [])
        with (
            patch.object(
                store, "_record_change", side_effect=OSError("publication failed")
            ),
            self.assertRaises(LoggingStorageError),
        ):
            store.append(second)
        store.close()
        store = self.open_store(open_mode="existing")
        for table in ("events", "journal_changes", "checkpoint_request_ids"):
            self.assertEqual(
                store._connection.execute(f"SELECT count(*) FROM {table}").fetchone()[
                    0
                ],
                1,
            )
        store.append(second)
        store.close()
        store = self.open_store(open_mode="existing")
        store._append_replayed(second)
        for table in ("events", "journal_changes", "checkpoint_request_ids"):
            self.assertEqual(
                store._connection.execute(f"SELECT count(*) FROM {table}").fetchone()[
                    0
                ],
                2,
            )
        with self.assertRaises(LoggingStorageError) as conflict:
            store._append_replayed(checkpoint_event(2, 1002))
        self.assertIsInstance(conflict.exception.__cause__, ValueError)
        self.assertIn("different contents", str(conflict.exception.__cause__))

    def test_corrupt_history_never_returns_an_incomplete_checkpoint(self):
        """M4: dependencies and reconstructed contents must be trustworthy."""
        store = self.open_store()
        store.append(checkpoint_event(1))
        store.append(checkpoint_event(2, 1001))
        self.assertEqual(
            store._connection.execute("PRAGMA user_version").fetchone()[0], 3
        )
        store.close()
        corruptions = {
            "missing_history": "DELETE FROM checkpoint_request_ids WHERE event_id='context-event-2'",
            "missing_parent": "DELETE FROM checkpoint_request_ids WHERE event_id='context-event-1'",
            "cycle": "UPDATE checkpoint_request_ids SET base_event_id=event_id WHERE event_id='context-event-2'",
            "scope": "UPDATE checkpoint_request_ids SET run_id='another-run' WHERE event_id='context-event-1'",
            "depth": "UPDATE checkpoint_request_ids SET depth=30 WHERE event_id='context-event-2'",
            "count": "UPDATE checkpoint_request_ids SET item_count=item_count+1 WHERE event_id='context-event-2'",
            "content": "UPDATE checkpoint_request_ids SET ids_json='[]' WHERE event_id='context-event-2'",
            "digest": "UPDATE checkpoint_request_ids SET event_sha256=replace(event_sha256,substr(event_sha256,1,1),'z') WHERE event_id='context-event-2'",
        }
        for name, statement in corruptions.items():
            with self.subTest(name=name):
                path = self.folder / f"{name}.db"
                shutil.copyfile(self.path, path)
                with closing(sqlite3.connect(path)) as db:
                    db.execute(statement)
                    db.commit()
                reader = make_store(path, open_mode="existing", read_only=True)
                try:
                    with self.assertRaises(LoggingStorageError):
                        reader.open()
                        reader.read_event_batch(["context-event-2"])
                finally:
                    reader.close()

    def test_snapshots_subset_exports_and_restored_generation(self):
        """M5: selected exports carry full states without their storage ancestors."""
        config = write_context_settings(self.folder)
        owner = WriterOwner(config).open()
        self.addCleanup(owner.close)
        logger = OperationLogger(config)
        self.addCleanup(logger.close)
        logger.open()
        before = logger.export_snapshot(self.folder / "before", min_free_bytes=0)
        self.assertEqual(before["storage_schema_version"], 3)
        data = checkpoint_event(1)["data"]
        logger.record_event("runner.checkpoint", data)
        operation = logger.start_operation("rebuild", "selected checkpoint")
        data = checkpoint_event(2, 1001)["data"]
        identifier = logger.record_event("runner.checkpoint", data, operation=operation)
        logger.finish_operation(operation)
        after = logger.export_snapshot(self.folder / "after", min_free_bytes=0)
        copied = self.open_store(
            self.folder / "after/journal.sqlite", open_mode="existing", read_only=True
        )
        copied._check_snapshot_contents(JournalSnapshotManifest.model_validate(after))
        self.assertEqual(
            copied.read_event_batch([identifier])["events"][0]["event"]["data"], data
        )
        diagnostics = self.folder / "diagnostics"
        logger.export_diagnostics([operation.get_operation_id()], diagnostics)
        records = [
            json.loads(line)
            for line in (diagnostics / "records.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        selected = next(
            row["event"]
            for row in records
            if row.get("event", {}).get("event_id") == identifier
        )
        self.assertEqual(selected["data"], data)
        restored_path = self.folder / "restored.db"
        shutil.copyfile(self.folder / "before/journal.sqlite", restored_path)
        restored = make_store(restored_path, open_mode="existing")
        self.addCleanup(restored.close)
        restored.complete_restore(
            before,
            new_generation=uuid4().hex,
            restoration_id=uuid4().hex,
            diagnostics=diagnostics,
        )
        restored.open()
        self.assertEqual(
            restored.read_event_batch([identifier])["events"][0]["event"], selected
        )
        restored.append(checkpoint_event(100, 1002))
        self.assertEqual(
            restored._connection.execute(
                "SELECT depth FROM checkpoint_request_ids WHERE event_id='context-event-100'"
            ).fetchone()[0],
            0,
        )
        restored.close()
        view_config = write_context_settings(
            self.folder / "view-config",
            db_path=str(restored_path),
            open_mode="existing",
        )
        view = FilteredJournal(view_config, self.folder / "view.db")
        self.addCleanup(view.close)
        view.open()
        view.refresh()
        events = [row["event"] for row in view.read_events(limit=1000)["events"]]
        self.assertIn(selected, events)
