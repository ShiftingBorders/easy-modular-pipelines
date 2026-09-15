"""Approved B7/B8 and F4-F10: complete diagnostics and identified restoration."""

import hashlib
import json
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.logger import OperationLogger
from core.logger_utils.events import (
    JournalGenerationChanged,
    LoggingStateError,
    LoggingStorageError,
)
from tests.helpers.logging_fixtures import write_context_settings
from tests.helpers.logging_process import (
    SCRATCH_ROOT,
    LoggingProcess,
    cleanup_directory,
    journal_identity,
    make_store,
    read_database,
)


class JournalRestorationTests(unittest.TestCase):
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
        self.restoration_id = uuid4().hex
        self.new_generation = uuid4().hex

    def prepare_diagnostics(self, *, shared_response=False):
        parent = self.logger.start_operation("runner", "parent")
        self.snapshot = self.folder / "snapshot"
        self.manifest = self.logger.export_snapshot(self.snapshot, min_free_bytes=0)
        failed = self.logger.start_operation(
            "rebuild", "failed", context=parent.get_child_context()
        )
        child = self.logger.start_operation(
            "copy", "child", context=failed.get_child_context()
        )
        # Service observation is outside the operation tree; result references must include it.
        self.service_id = self.logger.record_command_result(
            "request", {"value": "service"}, author="service", outcome="succeeded"
        )
        self.runner_id = self.logger.record_command_result(
            "request",
            {"value": "service" if shared_response else "runner"},
            author="runner",
            outcome="succeeded",
            operation=child,
        )
        self.logger.record_error(ValueError("copy failed"), operation=child)
        self.logger.finish_operation(child, status="failed")
        self.logger.finish_operation(failed, status="failed")
        self.unrelated_id = self.logger.record_event("unrelated.late")
        recovery = self.logger.start_operation("restore", "recover")
        self.intent_id = self.logger.record_event(
            "control.intent",
            {
                "action": "journal.restore",
                "restoration_id": self.restoration_id,
                "new_generation": self.new_generation,
                "snapshot_id": self.manifest["snapshot_id"],
            },
            operation=recovery,
        )
        self.roots = [failed.get_operation_id(), recovery.get_operation_id()]
        self.selected_operations = {
            failed.get_operation_id(),
            child.get_operation_id(),
            recovery.get_operation_id(),
        }
        self.before = read_database(self.path)
        self.diagnostics = self.folder / "diagnostics"
        self.bundle = self.logger.export_diagnostics(self.roots, self.diagnostics)
        self.parent = parent

    def restored_store(self, name="restored.db"):
        path = self.folder / name
        shutil.copyfile(self.snapshot / "journal.sqlite", path)
        store = make_store(path, open_mode="existing")
        self.addCleanup(store.close)
        return store

    def restore(self, store, **overrides):
        options = {
            "restoration_id": self.restoration_id,
            "new_generation": self.new_generation,
            "diagnostics": self.diagnostics,
        }
        options.update(overrides)
        return store.complete_restore(self.manifest, **options)

    def test_bundle_includes_tree_ancestors_command_dependencies_and_restore_intent(
        self,
    ):
        """F4: selection follows actual dependencies, not the entire late history."""
        self.prepare_diagnostics()
        records = [
            json.loads(line)
            for line in (self.diagnostics / "records.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        events = [record["event"] for record in records if record["kind"] == "event"]
        commands = [record for record in records if record["kind"] == "command"]
        expected = [
            event
            for event in self.before
            if (
                event["operation_id"] in self.selected_operations
                or event["event_id"] == self.service_id
                or event["operation_id"] == self.parent.get_operation_id()
            )
        ]
        self.assertEqual(events, expected)
        ids = {event["event_id"] for event in events}
        self.assertIn(self.intent_id, ids)
        self.assertNotIn(self.unrelated_id, ids)
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["service"]["event_id"], self.service_id)
        self.assertEqual(commands[0]["runner"]["event_id"], self.runner_id)
        self.assertEqual(
            self.bundle["records_sha256"],
            hashlib.sha256(
                (self.diagnostics / "records.jsonl").read_bytes()
            ).hexdigest(),
        )
        self.assertEqual(read_database(self.path), self.before)

    def test_restore_preserves_original_facts_and_assigns_a_fresh_generation(self):
        """F6/F7/F10: imported IDs/observers survive; new local cursors are allowed."""
        self.prepare_diagnostics()
        store = self.restored_store()
        result = self.restore(store)
        self.assertEqual(result["generation"], self.new_generation)
        self.assertEqual(result["journal_id"], self.manifest["journal_id"])
        imported = read_database(store.db_path)
        original = {event["event_id"]: event for event in self.before}
        self.assertNotIn(self.unrelated_id, {event["event_id"] for event in imported})
        self.assertIn(self.intent_id, {event["event_id"] for event in imported})
        for event in imported:
            self.assertEqual(event, original[event["event_id"]])
        store.open()
        command = store.read_command_result("request")
        self.assertEqual(command["event_id"], self.runner_id)
        self.assertEqual(command["author"], "runner")
        self.assertEqual(
            {item["author"] for item in command["observations"]}, {"runner", "service"}
        )
        self.assertTrue(store.read_changes()["changes"])
        self.assertEqual(read_database(self.path), self.before)

    def test_shared_event_confirmation_is_transferred_without_duplicate_event(self):
        """F4/F10: observer metadata exists even when runner reused the service event."""
        self.prepare_diagnostics(shared_response=True)
        store = self.restored_store()
        self.restore(store)
        store.open()
        result = store.read_command_result("request")
        self.assertEqual(result["author"], "runner")
        self.assertFalse(result["provisional"])
        self.assertEqual(result["event"]["data"]["author"], "service")
        self.assertEqual(len(result["observations"]), 2)
        self.assertEqual(
            len(
                [
                    event
                    for event in read_database(store.db_path)
                    if event["event_type"] == "command.result"
                ]
            ),
            1,
        )

    def test_confirmation_on_a_selected_root_remains_attached_to_that_operation(self):
        """F4: root selection works even without a parent_operation_id in the confirmation."""
        event_id = self.logger.record_command_result(
            "request", {}, author="service", outcome="succeeded"
        )
        root = self.logger.start_operation("rebuild", "root")
        self.logger.record_command_result(
            "request", {}, author="runner", outcome="succeeded", operation=root
        )
        destination = self.folder / "root-diagnostics"
        self.logger.export_diagnostics([root.get_operation_id()], destination)
        records = [
            json.loads(line)
            for line in (destination / "records.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.assertIn(
            event_id,
            {
                record["event"]["event_id"]
                for record in records
                if record["kind"] == "event"
            },
        )
        command = next(record for record in records if record["kind"] == "command")
        self.assertEqual(
            command["runner"]["observation"]["operation_id"], root.get_operation_id()
        )

    def test_kill_before_and_after_restore_commit_leaves_a_complete_state(self):
        """F7/F8/E2: actual process death cannot leave a partial generation or transfer."""
        self.prepare_diagnostics()
        for mode in ("restore_before_commit", "restore_after_commit"):
            with self.subTest(mode=mode):
                store = self.restored_store(mode + ".db")
                config = write_context_settings(
                    self.folder / mode, db_path=str(store.db_path), open_mode="existing"
                )
                worker = LoggingProcess(
                    mode,
                    config,
                    str(self.snapshot / "manifest.json"),
                    self.new_generation,
                    self.restoration_id,
                    str(self.diagnostics),
                    module_name="tests.helpers.logging_fixtures",
                )
                self.addCleanup(worker.close)
                worker.start()
                status = worker.receive()
                self.assertEqual(
                    status["phase"],
                    "restore_uncommitted"
                    if mode == "restore_before_commit"
                    else "restore_committed",
                )
                worker.kill_writer()
                self.assertNotEqual(worker.wait(), 0)
                expected = {
                    "journal_id": self.manifest["journal_id"],
                    "generation": self.manifest["generation"]
                    if mode == "restore_before_commit"
                    else self.new_generation,
                }
                reader = make_store(
                    store.db_path, open_mode="existing", expected_journal=expected
                )
                self.addCleanup(reader.close)
                reader.open()  # Allows SQLite to recover any hot rollback journal.
                events = read_database(store.db_path)
                if mode == "restore_before_commit":
                    self.assertEqual(len(events), self.manifest["event_count"])
                    self.assertIsNone(reader.read_command_result("request"))
                else:
                    self.assertIn(
                        self.intent_id, {event["event_id"] for event in events}
                    )
                    self.assertEqual(
                        reader.read_command_result("request")["author"], "runner"
                    )
                with closing(sqlite3.connect(store.db_path)) as db:
                    self.assertEqual(
                        db.execute("PRAGMA integrity_check").fetchone()[0], "ok"
                    )
                    self.assertEqual(
                        db.execute("PRAGMA foreign_key_check").fetchall(), []
                    )
                reader.close()
                result = self.restore(reader)
                self.assertEqual(result["generation"], self.new_generation)
                if mode == "restore_after_commit":
                    self.assertEqual(result, status["result"])

    def test_repeat_is_idempotent_and_other_parameters_are_rejected(self):
        """F8: retry cannot rotate generation twice, even from a fresh client."""
        self.prepare_diagnostics()
        store = self.restored_store()
        first = self.restore(store)
        before = read_database(store.db_path)
        self.assertEqual(self.restore(store), first)
        fresh = make_store(store.db_path, open_mode="existing")
        self.addCleanup(fresh.close)
        self.assertEqual(self.restore(fresh), first)
        fresh.open()
        self.assertEqual(fresh.get_journal_info()["generation"], self.new_generation)
        fresh.close()
        with self.assertRaises(ValueError):
            self.restore(store, new_generation=uuid4().hex)
        self.assertEqual(read_database(store.db_path), before)
        with closing(sqlite3.connect(store.db_path, isolation_level=None)) as db:
            db.execute("UPDATE journal_info SET generation=?", (uuid4().hex,))
        later = make_store(store.db_path, open_mode="existing")
        self.addCleanup(later.close)
        with self.assertRaises(LoggingStateError):
            self.restore(later)

    def test_snapshot_boundary_and_content_are_both_verified(self):
        """F6: same event count/cursor cannot disguise different snapshot content."""
        self.prepare_diagnostics()
        for label in ("cursor", "content", "generation", "open"):
            with self.subTest(label=label):
                store = self.restored_store(label + ".db")
                manifest = dict(self.manifest)
                if label == "cursor":
                    manifest["cursor"] += 1
                elif label == "generation":
                    manifest["generation"] = uuid4().hex
                elif label == "content":
                    with closing(
                        sqlite3.connect(store.db_path, isolation_level=None)
                    ) as db:
                        event = json.loads(
                            db.execute(
                                "SELECT event_json FROM events LIMIT 1"
                            ).fetchone()[0]
                        )
                        event["data"]["attributes"] = {"changed": True}
                        db.execute(
                            "UPDATE events SET event_json=?", (json.dumps(event),)
                        )
                else:
                    store.open()
                before = read_database(store.db_path)
                with self.assertRaises((ValueError, LoggingStateError)):
                    store.complete_restore(
                        manifest,
                        restoration_id=self.restoration_id,
                        new_generation=self.new_generation,
                        diagnostics=self.diagnostics,
                    )
                self.assertEqual(read_database(store.db_path), before)

    def test_bundle_corruption_and_foreign_ownership_do_not_change_the_target(self):
        """F5: manifest/checksum/count/reference failures precede a committed transfer."""
        self.prepare_diagnostics()
        for label in ("checksum", "count", "journal", "duplicate", "missing-reference"):
            with self.subTest(label=label):
                bundle = self.folder / ("bundle-" + label)
                shutil.copytree(self.diagnostics, bundle)
                manifest = json.loads(
                    (bundle / "manifest.json").read_text(encoding="utf-8")
                )
                records = [
                    json.loads(line)
                    for line in (bundle / "records.jsonl")
                    .read_text(encoding="utf-8")
                    .splitlines()
                ]
                if label == "checksum":
                    manifest["records_sha256"] = "0" * 64
                elif label == "count":
                    manifest["event_count"] += 1
                elif label == "journal":
                    manifest["journal_id"] = uuid4().hex
                else:
                    if label == "duplicate":
                        records.append(records[0])
                        manifest["event_count"] += 1
                    else:
                        command = next(
                            record for record in records if record["kind"] == "command"
                        )
                        command["runner"]["event_id"] = "missing"
                    text = "".join(json.dumps(record) + "\n" for record in records)
                    (bundle / "records.jsonl").write_text(text, encoding="utf-8")
                    manifest["records_sha256"] = hashlib.sha256(
                        (bundle / "records.jsonl").read_bytes()
                    ).hexdigest()
                (bundle / "manifest.json").write_text(
                    json.dumps(manifest), encoding="utf-8"
                )
                store = self.restored_store(label + ".db")
                before = read_database(store.db_path)
                with self.assertRaises(ValueError):
                    self.restore(store, diagnostics=bundle)
                self.assertEqual(read_database(store.db_path), before)
                self.assertEqual(
                    journal_identity(store.db_path)["generation"],
                    self.manifest["generation"],
                )

    def test_partial_transfer_rolls_back_without_advancing_generation(self):
        """F7/E6: a fault after a real insertion leaves no imported prefix or new generation."""
        self.prepare_diagnostics()
        store = self.restored_store()
        original = store._insert_event
        calls = []

        def insert_then_fail(*args, **kwargs):
            original(*args, **kwargs)
            calls.append(True)
            raise sqlite3.OperationalError("controlled transfer failure")

        before = read_database(store.db_path)
        with (
            patch.object(store, "_insert_event", side_effect=insert_then_fail),
            self.assertRaises(LoggingStorageError),
        ):
            self.restore(store)
        self.assertEqual(calls, [True])
        self.assertEqual(read_database(store.db_path), before)
        self.assertEqual(
            journal_identity(store.db_path)["generation"], self.manifest["generation"]
        )
        fresh = make_store(store.db_path, open_mode="existing")
        self.addCleanup(fresh.close)
        self.restore(fresh)

    def test_conflicting_event_ids_and_producer_sequences_reject_the_whole_transfer(
        self,
    ):
        """F7: a valid bundle checksum cannot authorize replacing existing facts."""
        self.prepare_diagnostics()
        for label in ("event", "producer"):
            with self.subTest(label=label):
                directory = self.folder / ("conflict-" + label)
                shutil.copytree(self.diagnostics, directory)
                records = [
                    json.loads(line)
                    for line in (directory / "records.jsonl")
                    .read_text(encoding="utf-8")
                    .splitlines()
                ]
                first = next(
                    record["event"] for record in records if record["kind"] == "event"
                )
                if label == "event":
                    first["data"]["attributes"] = {"forged": True}
                else:
                    first["event_id"] = uuid4().hex
                (directory / "records.jsonl").write_text(
                    "".join(json.dumps(record) + "\n" for record in records),
                    encoding="utf-8",
                )
                metadata = json.loads(
                    (directory / "manifest.json").read_text(encoding="utf-8")
                )
                metadata["records_sha256"] = hashlib.sha256(
                    (directory / "records.jsonl").read_bytes()
                ).hexdigest()
                (directory / "manifest.json").write_text(
                    json.dumps(metadata), encoding="utf-8"
                )
                store = self.restored_store(label + ".db")
                before = read_database(store.db_path)
                with self.assertRaisesRegex(
                    ValueError, "event ID" if label == "event" else "producer sequence"
                ):
                    self.restore(store, diagnostics=directory)
                self.assertEqual(read_database(store.db_path), before)
                self.assertEqual(
                    journal_identity(store.db_path)["generation"],
                    self.manifest["generation"],
                )

    def test_existing_observer_cannot_be_replaced_by_an_imported_observation(self):
        """F7: matching event data does not permit rewriting an observer's original metadata."""
        self.logger.record_command_result(
            "request", {}, author="service", outcome="succeeded"
        )
        self.snapshot = self.folder / "snapshot"
        self.manifest = self.logger.export_snapshot(self.snapshot, min_free_bytes=0)
        root = self.logger.start_operation("restore", "root")
        self.logger.record_command_result(
            "request", {}, author="runner", outcome="succeeded", operation=root
        )
        self.diagnostics = self.folder / "diagnostics"
        self.logger.export_diagnostics([root.get_operation_id()], self.diagnostics)
        records = [
            json.loads(line)
            for line in (self.diagnostics / "records.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        command = next(record for record in records if record["kind"] == "command")
        command["service"]["observation"]["occurred_at"] = "2026-01-01T00:00:00+00:00"
        (self.diagnostics / "records.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
        )
        metadata = json.loads(
            (self.diagnostics / "manifest.json").read_text(encoding="utf-8")
        )
        metadata["records_sha256"] = hashlib.sha256(
            (self.diagnostics / "records.jsonl").read_bytes()
        ).hexdigest()
        (self.diagnostics / "manifest.json").write_text(
            json.dumps(metadata), encoding="utf-8"
        )
        store = self.restored_store()
        before = read_database(store.db_path)
        with self.assertRaisesRegex(ValueError, "observation conflicts"):
            self.restore(store)
        self.assertEqual(read_database(store.db_path), before)

    def test_invalid_manifest_fields_are_rejected_before_restoration(self):
        """F6: schema, UUID, counts and hashes must be valid before changing persistent data."""
        self.prepare_diagnostics()
        store = self.restored_store()
        before = read_database(store.db_path)
        for field, value in (
            ("schema_version", True),
            ("storage_schema_version", 99),
            ("snapshot_id", "bad"),
            ("generation", None),
            ("cursor", True),
            ("content_sha256", "bad"),
            ("database", "../elsewhere"),
            ("created_at", "2026-01-01T00:00:00"),
        ):
            with self.subTest(field=field), self.assertRaises((TypeError, ValueError)):
                store.complete_restore(
                    {**self.manifest, field: value},
                    restoration_id=self.restoration_id,
                    new_generation=self.new_generation,
                    diagnostics=self.diagnostics,
                )
            self.assertEqual(read_database(store.db_path), before)
        self.assertEqual(self.restore(store)["generation"], self.new_generation)

    def test_old_connection_and_checkpoint_are_rejected_after_generation_changes(self):
        """F9/D2: an idle stale client cannot append into restored state."""
        self.prepare_diagnostics()
        target = self.restored_store()
        stale = make_store(target.db_path, open_mode="existing")
        self.addCleanup(stale.close)
        stale.open()
        checkpoint = stale.read_events()["checkpoint"]
        self.restore(target)
        target.open()
        with self.assertRaises(JournalGenerationChanged):
            target.read_events(checkpoint)
        with self.assertRaises(LoggingStorageError):
            stale.append(
                {
                    "schema_version": 1,
                    "event_id": "stale",
                    "producer_instance_id": "stale",
                    "sequence_number": 1,
                    "occurred_at": "2026-09-15T00:00:00+00:00",
                    "event_type": "stale.attempt",
                    "context": {},
                    "operation_id": None,
                    "data": {},
                }
            )
        self.assertNotIn(
            "stale", {event["event_id"] for event in read_database(target.db_path)}
        )

    def test_deep_accepted_event_survives_diagnostic_envelope_and_restore(self):
        """B8/F5: a JSONL wrapper does not reduce the event's accepted nesting depth."""
        operation = self.logger.start_operation("rebuild", "deep")
        self.snapshot = self.folder / "snapshot"
        self.manifest = self.logger.export_snapshot(self.snapshot, min_free_bytes=0)
        nested = None
        for _ in range(30):
            nested = [nested]
        event_id = self.logger.record_event(
            "deep", {"nested": nested}, operation=operation
        )
        self.diagnostics = self.folder / "diagnostics"
        self.logger.export_diagnostics([operation.get_operation_id()], self.diagnostics)
        store = self.restored_store()
        self.restore(store)
        event = next(
            event
            for event in read_database(store.db_path)
            if event["event_id"] == event_id
        )
        self.assertEqual(event["data"], {"nested": nested})
