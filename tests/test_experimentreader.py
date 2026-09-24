"""Approved discovery scenarios Q2-01–09, Q2-19–25 and Q2-33."""

import copy
import json
import os
import shutil
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.experimentreader import ExperimentReader
from core.logger import OperationLogger
from core.logger_utils.events import LoggingError
from core.logger_utils.storage import SQLiteEventStore
from core.runner_utils.runtimeio import read_json, write_json
from tests import test_modulemanager
from tests.helpers.dag import DagWorkspace
from tests.helpers.logging_fixtures import BASE_CONTEXT, write_context_settings
from tests.helpers.logging_process import journal_identity


class ExperimentReaderTests(unittest.TestCase):
    def setUp(self):
        self.files = DagWorkspace()
        self.addCleanup(self.files.close)
        self.root = self.files.root
        self.directory = self.root / "experiments" / "saved"
        (self.directory / "runner").mkdir(parents=True)
        self.state = {
            "schema_version": 3,
            "experiment_id": "saved",
            "phase": "stopped",
            "mode": "paused",
            "template": {"name": "Saved experiment"},
            "cycle_number": 3,
            "stage_position": 2,
        }
        self.state_path = self.directory / "runner/state.json"
        write_json(self.state_path, self.state)
        write_json(self.root / "experiments.json", {"saved": "saved"})
        self.reader = ExperimentReader(self.root)
        self.snapshot_id = str(uuid4())
        self.manifest_path = (
            self.root / "snapshots/saved" / self.snapshot_id / "manifest.json"
        )
        self.manifest_path.parent.mkdir(parents=True)
        self.manifest = {
            "schema_version": 2,
            "snapshot_id": self.snapshot_id,
            "experiment_id": "saved",
            "state": copy.deepcopy(self.state),
            "label": "before",
            "created_at": "2026-09-24T00:00:00+00:00",
        }
        write_json(self.manifest_path, self.manifest)

    def test_registry_absence_and_corruption_q2_01(self):
        registry = self.root / "experiments.json"
        registry.unlink()
        self.assertEqual(self.reader.list_experiments(), {"items": []})
        for text in ("{broken", "[]", "null"):
            with self.subTest(text=text):
                registry.write_text(text, encoding="utf-8")
                with self.assertRaises((ValueError, TypeError)):
                    self.reader.list_experiments()

    def test_state_failures_remain_per_item_but_inspection_fails_q2_02_03(self):
        good = self.directory / "../good"
        (good / "runner").mkdir(parents=True)
        write_json(good / "runner/state.json", {**self.state, "experiment_id": "good"})
        write_json(self.root / "experiments.json", {"saved": "saved", "good": "good"})
        cases = [
            None,
            "{",
            {**self.state, "schema_version": 999},
            {**self.state, "experiment_id": "other"},
        ]
        for state in cases:
            with self.subTest(state=state):
                if state is None:
                    self.state_path.unlink()
                else:
                    self.state_path.write_text(
                        state if isinstance(state, str) else json.dumps(state),
                        encoding="utf-8",
                    )
                result = self.reader.read("stats.experiments", {}, "good")["items"]
                self.assertEqual(
                    [item["experiment_id"] for item in result], ["good", "saved"]
                )
                self.assertTrue(result[0]["available"])
                self.assertTrue(result[0]["selected"])
                self.assertFalse(result[1]["available"])
                self.assertTrue(result[1]["error"])
                with self.assertRaises((OSError, ValueError, TypeError)):
                    self.reader.inspect_experiment("saved")
        with self.assertRaises(FileNotFoundError):
            self.reader.inspect_experiment("unknown")

    def test_registry_path_escape_q2_04(self):
        for folder in ("..", "../outside", str(self.root.parent)):
            with self.subTest(folder=folder):
                write_json(self.root / "experiments.json", {"saved": folder})
                self.assertFalse(
                    self.reader.list_experiments()["items"][0]["available"]
                )
                with self.assertRaises(ValueError):
                    self.reader.inspect_experiment("saved")

    def test_snapshot_corruption_and_identity_q2_05_06(self):
        bad_id = str(uuid4())
        bad = self.manifest_path.parent.parent / bad_id / "manifest.json"
        bad.parent.mkdir()
        for document in (
            "{",
            {**self.manifest, "schema_version": 99},
            self.manifest,
            {**self.manifest, "snapshot_id": bad_id, "experiment_id": "other"},
            {
                **self.manifest,
                "snapshot_id": bad_id,
                "state": {"experiment_id": "other"},
            },
        ):
            with self.subTest(document=document):
                bad.write_text(
                    document if isinstance(document, str) else json.dumps(document),
                    encoding="utf-8",
                )
                rows = {
                    item["snapshot_id"]: item
                    for item in self.reader.list_snapshots("saved")["items"]
                }
                self.assertTrue(rows[self.snapshot_id]["available"])
                self.assertEqual(rows[self.snapshot_id]["integrity"], "not_checked")
                self.assertFalse(rows[bad_id]["available"])
                with self.assertRaises((ValueError, TypeError)):
                    self.reader.inspect_snapshot("saved", bad_id)
        with self.assertRaises(ValueError):
            self.reader.inspect_snapshot("saved", "not-a-uuid")
        with self.assertRaises(FileNotFoundError):
            self.reader.inspect_snapshot("saved", str(uuid4()))

    def test_snapshot_selection_and_missing_experiment_q2_07(self):
        self.assertEqual(
            self.reader.read("stats.snapshots", {}, "saved")["experiment_id"], "saved"
        )
        with self.assertRaises((ValueError, TypeError)):
            self.reader.read("stats.snapshots", {}, None)
        with self.assertRaises(FileNotFoundError):
            self.reader.list_snapshots("unknown")
        self.manifest_path.unlink()
        self.assertEqual(self.reader.list_snapshots("saved")["items"], [])

    def test_access_denied_q2_08(self):
        for denied in (
            self.root / "experiments.json",
            self.state_path,
            self.manifest_path,
        ):

            def read(path, denied=denied, **kwargs):
                if path == denied:
                    raise PermissionError("denied by host filesystem")
                return read_json(path, **kwargs)

            with (
                self.subTest(denied=denied),
                patch("core.experimentreader.read_json", side_effect=read),
            ):
                if denied.name == "experiments.json":
                    with self.assertRaises(PermissionError):
                        self.reader.list_experiments()
                elif denied == self.state_path:
                    self.assertFalse(
                        self.reader.list_experiments()["items"][0]["available"]
                    )
                    with self.assertRaises(PermissionError):
                        self.reader.inspect_experiment("saved")
                else:
                    self.assertFalse(
                        self.reader.list_snapshots("saved")["items"][0]["available"]
                    )
                    with self.assertRaises(PermissionError):
                        self.reader.inspect_snapshot("saved", self.snapshot_id)

    def test_atomic_metadata_replacement_q2_09(self):
        for path, operation, key in (
            (self.state_path, lambda: self.reader.inspect_experiment("saved"), "state"),
            (
                self.manifest_path,
                lambda: self.reader.inspect_snapshot("saved", self.snapshot_id),
                "manifest",
            ),
        ):
            original = read_json(path)
            replacement = {**original, "publication_marker": "new"}

            def read(selected, path=path, replacement=replacement, **kwargs):
                document = read_json(selected, **kwargs)
                if selected == path:
                    write_json(path, replacement)
                return document

            with (
                self.subTest(path=path),
                patch("core.experimentreader.read_json", side_effect=read),
            ):
                self.assertEqual(operation()[key], original)
            self.assertEqual(operation()[key], replacement)

    def make_journal(self):
        context = {**BASE_CONTEXT, "experiment_id": "saved"}
        config = write_context_settings(
            self.directory / "journals", context=context, db_path="events.sqlite"
        )
        logger = OperationLogger(config)
        logger.open()
        self.addCleanup(logger.close)
        write_json(
            self.directory / "runner/journal.json",
            journal_identity(self.directory / "journals/events.sqlite"),
        )
        logger.record_attempt_parameters({}, {}, template_yaml="stages: []")
        artifact = (
            self.directory
            / "shared_artifacts/epoch_3/prepare/stage-A/attempt_1/output.bin"
        )
        artifact.parent.mkdir(parents=True)
        artifact.write_bytes(b"recorded bytes")
        logger.record_artifact("output.bin", "test output", artifact_id="output")
        return logger, artifact

    def test_artifact_records_availability_and_unknown_ids_q2_20_21(self):
        _, path = self.make_journal()
        self.assertEqual(
            Path(self.reader.get_artifact("saved", "output")["path"]).read_bytes(),
            b"recorded bytes",
        )
        with patch.object(
            Path, "is_file", side_effect=PermissionError("artifact access denied")
        ):
            self.assertFalse(
                self.reader.list_artifacts("saved")["items"][0]["available"]
            )
            with self.assertRaises(PermissionError):
                self.reader.get_artifact("saved", "output")
        path.unlink()
        row = self.reader.list_artifacts("saved")["items"][0]
        self.assertEqual(row["artifact_id"], "output")
        self.assertFalse(row["available"])
        for identifier in ("output", "unknown"):
            with (
                self.subTest(identifier=identifier),
                self.assertRaises(FileNotFoundError),
            ):
                self.reader.get_artifact("saved", identifier)

    def test_continuation_reads_inherited_and_new_artifacts_without_source_fallback(
        self,
    ):
        logger, source = self.make_journal()
        logger.close()
        continuation = self.root / "experiments/continued"
        shutil.copytree(self.directory, continuation)
        write_json(
            self.root / "experiments.json", {"saved": "saved", "continued": "continued"}
        )
        write_json(
            continuation / "runner/state.json",
            {**self.state, "experiment_id": "continued"},
        )
        context = {
            **BASE_CONTEXT,
            "experiment_id": "continued",
            "attempt_id": str(uuid4()),
            "attempt_number": 2,
        }
        config = write_context_settings(
            continuation / "journals",
            context=context,
            db_path="events.sqlite",
            open_mode="existing",
            expected_journal=journal_identity(continuation / "journals/events.sqlite"),
        )
        continued_logger = OperationLogger(config)
        continued_logger.open()
        self.addCleanup(continued_logger.close)
        continued_logger.record_attempt_parameters({}, {}, template_yaml="stages: []")
        new_file = (
            continuation / "shared_artifacts/epoch_3/prepare/stage-A/attempt_2/new.bin"
        )
        new_file.parent.mkdir(parents=True)
        new_file.write_bytes(b"new bytes")
        continued_logger.record_artifact("new.bin", "new artifact", artifact_id="new")
        inherited = continuation / source.relative_to(self.directory)
        inherited.write_bytes(b"restored bytes")
        rows = self.reader.list_artifacts("continued")["items"]
        self.assertEqual([row["artifact_id"] for row in rows], ["output", "new"])
        self.assertEqual(
            [row["context"]["experiment_id"] for row in rows], ["saved", "continued"]
        )
        for identifier, expected in (
            ("output", b"restored bytes"),
            ("new", b"new bytes"),
        ):
            result = self.reader.get_artifact("continued", identifier)
            path = Path(result["path"])
            self.assertTrue(path.is_relative_to(continuation))
            self.assertEqual(path.read_bytes(), expected)
        inherited.unlink()
        self.assertEqual(
            [
                row["available"]
                for row in self.reader.list_artifacts("continued")["items"]
            ],
            [False, True],
        )
        with self.assertRaises(FileNotFoundError):
            self.reader.get_artifact("continued", "output")
        self.assertEqual(
            Path(self.reader.get_artifact("continued", "new")["path"]), new_file
        )
        self.assertEqual(source.read_bytes(), b"recorded bytes")
        identity_path = continuation / "runner/journal.json"
        identity = read_json(identity_path)
        for field in ("journal_id", "generation"):
            write_json(identity_path, {**identity, field: str(uuid4())})
            with self.subTest(field=field), self.assertRaises(LoggingError):
                self.reader.get_artifact("continued", "new")
        write_json(identity_path, identity)

    def test_inherited_artifacts_reject_escaping_paths(self):
        logger, source = self.make_journal()
        logger.close()
        continuation = self.root / "experiments/continued"
        shutil.copytree(self.directory, continuation)
        write_json(
            self.root / "experiments.json", {"saved": "saved", "continued": "continued"}
        )
        with (
            closing(
                sqlite3.connect(continuation / "journals/events.sqlite")
            ) as database,
            database,
        ):
            cursor, text = database.execute(
                "SELECT cursor, event_json FROM events WHERE event_json LIKE '%artifact.recorded%'"
            ).fetchone()
            event = json.loads(text)
            for path in ("../outside", str(self.root / "outside"), "C:\\outside"):
                event["data"]["path"] = path
                database.execute(
                    "UPDATE events SET event_json=? WHERE cursor=?",
                    (json.dumps(event), cursor),
                )
                database.commit()
                self.assertFalse(
                    self.reader.list_artifacts("continued")["items"][0]["available"]
                )
                with self.assertRaises(ValueError):
                    self.reader.get_artifact("continued", "output")
        self.assertEqual(source.read_bytes(), b"recorded bytes")

    def test_journal_missing_corrupt_and_wrong_identity_q2_19(self):
        with self.assertRaises(FileNotFoundError):
            self.reader.list_artifacts("unknown")
        logger, _ = self.make_journal()
        logger.close()
        identity_path = self.directory / "runner/journal.json"
        identity = read_json(identity_path)
        for field in ("journal_id", "generation"):
            write_json(identity_path, {**identity, field: str(uuid4())})
            with self.subTest(field=field), self.assertRaises(LoggingError):
                self.reader.list_artifacts("saved")
        write_json(identity_path, identity)
        database = self.directory / "journals/events.sqlite"
        database.write_bytes(b"broken sqlite")
        with self.assertRaises(LoggingError):
            self.reader.get_artifact("saved", "output")
        database.unlink()
        with self.assertRaises(LoggingError):
            self.reader.list_artifacts("saved")
        self.assertFalse(database.exists())

    def test_artifact_invalid_context_and_path_q2_20_22(self):
        _, path = self.make_journal()
        records = list(self.reader._artifact_records("saved"))
        event, context = records[0]
        for changes, data in (
            ({"attempt_number": None}, event["data"]),
            ({}, {**event["data"], "path": "../../outside"}),
        ):
            with (
                self.subTest(changes=changes, data=data),
                patch.object(
                    self.reader,
                    "_artifact_records",
                    side_effect=lambda _, data=data, changes=changes: (
                        row
                        for row in [({**event, "data": data}, {**context, **changes})]
                    ),
                ),
            ):
                self.assertFalse(
                    self.reader.list_artifacts("saved")["items"][0]["available"]
                )
                with self.assertRaises(ValueError):
                    self.reader.get_artifact("saved", "output")
        self.assertEqual(path.read_bytes(), b"recorded bytes")

    def test_artifact_scan_stops_at_initial_boundary_q2_23(self):
        logger, _ = self.make_journal()
        real_read = SQLiteEventStore.read_events
        appended = False

        def read(store, checkpoint=None, **kwargs):
            nonlocal appended
            page = real_read(store, checkpoint, limit=1)
            if not appended:
                appended = True
                logger.record_artifact("output.bin", "late", artifact_id="late")
            return page

        with patch.object(SQLiteEventStore, "read_events", read):
            self.assertEqual(
                [
                    row["artifact_id"]
                    for row in self.reader.list_artifacts("saved")["items"]
                ],
                ["output"],
            )
        self.assertEqual(len(self.reader.list_artifacts("saved")["items"]), 2)

    def test_generation_change_during_scan_q2_24(self):
        logger, _ = self.make_journal()
        logger.close()
        real_read = SQLiteEventStore.read_events
        changed = False

        def read(store, checkpoint=None, **kwargs):
            nonlocal changed
            page = real_read(store, checkpoint, limit=1)
            if not changed:
                changed = True
                with (
                    closing(
                        sqlite3.connect(self.directory / "journals/events.sqlite")
                    ) as database,
                    database,
                ):
                    database.execute(
                        "UPDATE journal_info SET generation=?", (str(uuid4()),)
                    )
            return page

        with (
            patch.object(SQLiteEventStore, "read_events", read),
            self.assertRaises(LoggingError),
        ):
            self.reader.list_artifacts("saved")

    def test_native_symlinks_cannot_escape_metadata_or_artifacts_q2_04_07_22_33(self):
        _, artifact = self.make_journal()
        outside = self.root / "outside.json"
        outside.write_text("external bytes", encoding="utf-8")
        probe = self.root / "probe-link"
        try:
            probe.symlink_to(outside)
        except OSError as error:
            if os.name == "nt" and getattr(error, "winerror", None) == 1314:
                self.skipTest("Windows symlink privilege is unavailable.")
            raise
        probe.unlink()
        for path, listing, inspect in (
            (
                self.state_path,
                self.reader.list_experiments,
                lambda: self.reader.inspect_experiment("saved"),
            ),
            (
                self.manifest_path,
                lambda: self.reader.list_snapshots("saved"),
                lambda: self.reader.inspect_snapshot("saved", self.snapshot_id),
            ),
            (
                artifact,
                lambda: self.reader.list_artifacts("saved"),
                lambda: self.reader.get_artifact("saved", "output"),
            ),
        ):
            saved = path.read_bytes()
            path.unlink()
            try:
                path.symlink_to(outside)
                with self.subTest(path=path):
                    self.assertFalse(listing()["items"][0]["available"])
                    with self.assertRaises(ValueError):
                        inspect()
            finally:
                path.unlink(missing_ok=True)
                path.write_bytes(saved)
        self.assertEqual(outside.read_text(encoding="utf-8"), "external bytes")

    def test_caller_cwd_does_not_affect_reads_q2_33(self):
        previous = Path.cwd()
        try:
            os.chdir(self.directory)
            self.assertEqual(
                self.reader.inspect_experiment("saved")["state"], self.state
            )
            self.assertEqual(
                self.reader.inspect_snapshot("saved", self.snapshot_id)["manifest"],
                self.manifest,
            )
        finally:
            os.chdir(previous)
        with self.assertRaises(ValueError):
            ExperimentReader(Path("relative"))

    def test_native_directory_links_escape_checks_q2_04_07_22_33(self):
        logger, artifact = self.make_journal()
        outside = self.root / "outside-directory"
        outside.mkdir()
        (outside / "output.bin").write_bytes(b"outside")
        write_json(outside / "state.json", self.state)
        write_json(outside / "manifest.json", self.manifest)
        links = []
        link_directory = test_modulemanager.PlatformTests._directory_link
        try:
            runner = self.directory / "runner"
            runner.rename(self.directory / "runner-original")
            link_directory(self, runner, outside)
            links.append(runner)
            self.assertFalse(self.reader.list_experiments()["items"][0]["available"])
            with self.assertRaises(ValueError):
                self.reader.inspect_experiment("saved")
            if os.name == "nt":
                runner.rmdir()
            else:
                runner.unlink()
            links.remove(runner)
            (self.directory / "runner-original").rename(runner)
            snapshot_id = str(uuid4())
            link = self.manifest_path.parent.parent / snapshot_id
            link_directory(self, link, outside)
            links.append(link)
            with self.assertRaises(ValueError):
                self.reader.inspect_snapshot("saved", snapshot_id)
            row = next(
                item
                for item in self.reader.list_snapshots("saved")["items"]
                if item["snapshot_id"] == snapshot_id
            )
            self.assertFalse(row["available"])
            link = artifact.parent / "linked"
            link_directory(self, link, outside)
            links.append(link)
            logger.record_artifact("linked/output.bin", "outside", artifact_id="escape")
            with self.assertRaises(ValueError):
                self.reader.get_artifact("saved", "escape")
            self.assertEqual((outside / "output.bin").read_bytes(), b"outside")
        finally:
            for link in reversed(links):
                if os.name == "nt":
                    link.rmdir()
                else:
                    link.unlink()
