"""Approved basic_dag.md D1/D2/D7: durable state and publication failures."""

import copy
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.runner_utils.runtimeio import process_identity, read_json, write_json
from core.runner_utils.state import RunnerState, RunnerStateStore, StageAttempt
from tests.helpers.dag import DagWorkspace


class RunnerStateTests(unittest.TestCase):
    def setUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.root = self.workspace.root / "experiment"
        self.root.mkdir()
        self.state = RunnerState(
            "experiment",
            self.root,
            "run",
            self.root / "experiment.yaml",
            str(uuid4()),
            "stages: []\n",
            {"stages": []},
            "paused",
        )
        self.store = RunnerStateStore()

    def test_round_trip_preserves_position_counts_inputs_and_attempt_paths(self):
        """D1: portable internal references and scalar execution state survive reload."""
        stage_id, request_id = str(uuid4()), str(uuid4())
        self.state.cycle_number = 2
        self.state.stage_position = 3
        self.state.phase = "stage_running"
        self.state.pause_requested = True
        self.state.stage_retry_counts = {stage_id: 2}
        self.state.stage_attempt_numbers = {stage_id: 4}
        self.state.used_request_ids = {request_id}
        self.state.last_result = {"value": [1, None, "данные"]}
        self.state.last_result_id = request_id
        self.state.stage_result_ids = {stage_id: request_id}
        self.state.active_attempt = StageAttempt(
            str(uuid4()),
            stage_id,
            str(uuid4()),
            2,
            4,
            self.root / "attempt",
            {"value": 7},
            {"enabled": False},
            None,
        )
        self.state.active_attempt.result_request_id = request_id
        self.state.active_attempt.participant = {
            "experiment_id": self.state.experiment_id,
            "participant_id": stage_id,
            "participant_instance_id": self.state.active_attempt.attempt_id,
        }
        self.state.active_attempt.endpoint_path = (
            self.root / "attempt/executor.lock.json"
        )
        self.store.save(self.state)
        document = read_json(self.root / "runner/state.json")
        self.assertEqual(document["last_result_id"], request_id)
        self.assertEqual(document["active_attempt"]["artifacts_directory"], "attempt")
        self.assertTrue(
            {"control_queue", "current_command", "tasks", "connections"}.isdisjoint(
                document
            )
        )
        restored = self.store.load(self.root)
        self.assertEqual(
            (
                restored.mode,
                restored.phase,
                restored.cycle_number,
                restored.stage_position,
            ),
            ("paused", "stage_running", 2, 3),
        )
        self.assertEqual(restored.stage_retry_counts, {stage_id: 2})
        self.assertEqual(restored.used_request_ids, {request_id})
        self.assertEqual(restored.last_result, {"value": [1, None, "данные"]})
        self.assertEqual(restored.active_attempt.input_data, {"value": 7})
        self.assertEqual(restored.active_attempt.result_request_id, request_id)

    def test_invalid_schema_corruption_and_escaping_paths_are_rejected(self):
        """D1: damaged and incompatible state cannot be mistaken for a valid checkpoint."""
        self.store.save(self.state)
        path = self.root / "runner/state.json"
        valid = read_json(path)
        for key, value in (
            ("schema_version", True),
            ("schema_version", 1),
            ("schema_version", 2),
            ("mode", "unknown"),
            ("cycle_number", True),
            ("stage_position", 0),
            ("pause_requested", 1),
            ("last_result_id", "../outside.json"),
            ("used_request_ids", ["bad"]),
        ):
            with self.subTest(key=key, value=value):
                candidate = copy.deepcopy(valid)
                candidate[key] = value
                path.write_text(json.dumps(candidate), encoding="utf-8")
                with self.assertRaises((TypeError, ValueError)):
                    self.store.load(self.root)
        path.write_text('{"schema_version":', encoding="utf-8")
        with self.assertRaises(json.JSONDecodeError):
            self.store.load(self.root)

    def test_snapshot_cursor_owner_checkpoint_and_origins_survive_relocation(self):
        """Snapshots A1/A3: version 2 preserves progress and experiment-relative paths."""
        stage_id = str(uuid4())
        self.state.pending_advance = True
        self.state.checkpoint_id = str(uuid4())
        self.state.owner_identity = process_identity(os.getpid())
        self.state.stage_result_ids[stage_id] = stage_id
        self.state.stage_result_origins[stage_id] = "source-experiment"
        self.store.save(self.state)
        relocated = self.root / "relocated"
        relocated.mkdir()
        write_json(
            relocated / "runner/state.json", read_json(self.root / "runner/state.json")
        )
        restored = self.store.load(relocated)
        self.assertEqual(restored.template_path, relocated / "experiment.yaml")
        self.assertTrue(restored.pending_advance)
        self.assertEqual(restored.checkpoint_id, self.state.checkpoint_id)
        self.assertEqual(restored.owner_identity, self.state.owner_identity)
        self.assertEqual(restored.stage_result_ids[stage_id], stage_id)
        self.assertEqual(restored.stage_result_origins[stage_id], "source-experiment")
        self.state.template_path = self.workspace.root / "external.yaml"
        self.store.save(self.state)
        self.assertEqual(
            self.store.load(self.root).template_path, self.state.template_path
        )

    def test_invalid_snapshot_fields_never_replace_valid_state(self):
        """Snapshots A2/A5: malformed ownership, origins and checkpoint are rejected."""
        self.store.save(self.state)
        path = self.root / "runner/state.json"
        document = read_json(path)
        for key, value in (
            ("pending_advance", 1),
            ("checkpoint_id", "bad"),
            ("owner_identity", {"pid": os.getpid()}),
            ("owner_identity", {**process_identity(os.getpid()), "pid": True}),
            ("stage_result_origins", {str(uuid4()): "unmatched-result"}),
            ("stage_result_origins", []),
        ):
            with self.subTest(key=key, value=value):
                write_json(path, {**document, key: value})
                with self.assertRaises((TypeError, ValueError)):
                    self.store.load(self.root)

    def test_validation_precedes_replacement_of_persisted_state(self):
        """D1/D2: invalid in-memory counts do not overwrite the previous document."""
        self.store.save(self.state)
        path = self.root / "runner/state.json"
        before = path.read_bytes()
        self.state.stage_position = False
        with self.assertRaises(ValueError):
            self.store.save(self.state)
        self.assertEqual(path.read_bytes(), before)

    def test_publication_exposes_complete_old_then_new_json(self):
        """D2: fsync and replacement occur after a complete temporary document exists."""
        path = self.root / "document.json"
        write_json(path, {"value": "old"})
        original_replace = os.replace
        order = []

        def replace(source, destination):
            self.assertEqual(read_json(path), {"value": "old"})
            self.assertEqual(read_json(Path(source)), {"value": "new"})
            self.assertEqual(order, ["fsync"])
            original_replace(source, destination)

        original_fsync = os.fsync

        def sync(descriptor):
            original_fsync(descriptor)
            order.append("fsync")

        with (
            patch("core.runner_utils.runtimeio.os.fsync", side_effect=sync),
            patch("core.runner_utils.runtimeio.os.replace", side_effect=replace),
        ):
            write_json(path, {"value": "new"})
        self.assertEqual(read_json(path), {"value": "new"})
        self.assertEqual(list(self.root.glob(".publish-*")), [])

    def test_write_fsync_and_replace_failures_preserve_the_previous_json(self):
        """D2: all publication failure points leave the old document readable."""
        path = self.root / "document.json"
        write_json(path, {"value": "old"})
        for target in ("tempfile.NamedTemporaryFile", "os.fsync", "os.replace"):
            with (
                self.subTest(target=target),
                patch(
                    f"core.runner_utils.runtimeio.{target}",
                    side_effect=OSError("disk failure"),
                ),
                self.assertRaisesRegex(OSError, "disk failure"),
            ):
                write_json(path, {"value": "new"})
            self.assertEqual(read_json(path), {"value": "old"})
            self.assertEqual(list(self.root.glob(".publish-*")), [])

    def test_cleanup_error_does_not_replace_original_publication_failure(self):
        """D7: an additional unlink failure keeps the initial exception and diagnostic."""
        primary = OSError("original replace failure")
        with (
            patch("core.runner_utils.runtimeio.os.replace", side_effect=primary),
            patch.object(Path, "unlink", side_effect=OSError("cleanup failure")),
            self.assertRaises(OSError) as raised,
        ):
            write_json(self.root / "document.json", {"value": 1})
        self.assertIs(raised.exception, primary)
        self.assertIn("cleanup failure", " ".join(raised.exception.__notes__))
