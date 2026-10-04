"""Validated replacements and typed experiment inputs retain boundary guarantees."""

import copy
import unittest
from pathlib import Path
from uuid import uuid4

import yaml

from core.models.dashboard_queries import PublicationCursor
from core.models.experiment_template import (
    ExperimentTemplate,
    HeartbeatPolicy,
    ServiceCallDefinition,
    ServiceDefinition,
    StageDefinition,
)
from core.models.updates import _update_model
from core.primitives.json_values import copy_json_object


class ModelRetentionTests(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).resolve().parents[2] / "default_settings/experiment_template.yaml"
        self.document = yaml.safe_load(path.read_text(encoding="utf-8"))
        self.document["stages"] = [{
            "module": {"name": "worker", "version": "1", "hash": "a" * 64},
            "settings": {"nested": [1]},
            "timeout_seconds": None,
            "errors": {"retries": 0, "retry_delay_seconds": 0, "on_exhausted": "stop"},
        }]
        self.template = ExperimentTemplate.model_validate(self.document)

    def test_checked_replacement_preserves_original_and_omitted_fields(self):
        original = self.template.model_dump(exclude_unset=True)
        replacement = _update_model(self.template, cycles=2)
        self.assertEqual(replacement.cycles, 2)
        self.assertEqual(self.template.model_dump(exclude_unset=True), original)
        self.assertNotIn("stage_id", replacement.stages[0].model_fields_set)
        self.assertNotIn("returns_data", replacement.stages[0].model_fields_set)
        replacement.stages[0].settings["nested"].append(2)
        self.assertEqual(self.template.stages[0].settings["nested"], [1])
        with self.assertRaises(ValueError):
            _update_model(self.template, cycles=0)
        self.assertEqual(self.template.model_dump(exclude_unset=True), original)

    def test_typed_nested_replacements_check_references_together(self):
        stage = self.template.stages[0]
        service_id = str(uuid4())
        service = ServiceDefinition(
            service_id=service_id,
            module=stage.module,
            settings={},
            heartbeat=HeartbeatPolicy(interval_seconds=1, grace_seconds=2),
            command_timeout_seconds=3,
            on_command_timeout="stop",
            state_required=False,
            errors=stage.errors,
        )
        call = ServiceCallDefinition(
            service_id=service_id, settings={}, timeout_seconds=None, errors=stage.errors
        )
        original = self.template.model_dump(exclude_unset=True)
        with self.assertRaisesRegex(ValueError, "explicit service_id"):
            _update_model(self.template, stages=[call])
        replacement = _update_model(self.template, stages=[call], services=[service])
        self.assertIs(replacement.stages[0], call)
        self.assertIs(replacement.services[0], service)
        self.assertEqual(self.template.model_dump(exclude_unset=True), original)

    def test_mixed_typed_and_json_inputs_detach_json_and_keep_models(self):
        document = copy.deepcopy(self.document)
        stage = self.template.stages[0]
        document["stages"] = [stage, document["stages"][0]]
        document["logging"] = self.template.logging
        replacement = ExperimentTemplate.model_validate(document)
        self.assertIs(replacement.stages[0], stage)
        self.assertIs(replacement.logging, self.template.logging)
        document["stages"][1]["settings"]["nested"].append(2)
        self.assertEqual(replacement.stages[1].settings["nested"], [1])

    def test_typed_input_keeps_json_constraints_and_empty_dag_check(self):
        for bad in (float("inf"), "\ud800", {1: "value"}, self.template.logging):
            with self.subTest(bad=type(bad).__name__):
                document = {**self.document, "name": bad, "stages": self.template.stages}
                with self.assertRaises((TypeError, ValueError)):
                    ExperimentTemplate.model_validate(document)
        with self.assertRaises(ValueError):
            _update_model(self.template, stages=[])
        # The general JSON primitive still rejects models anywhere in a document.
        with self.assertRaises(TypeError):
            copy_json_object({"stage": self.template.stages[0]}, "JSON only")

    def test_nested_update_retains_validation_and_explicit_values(self):
        stage = self.template.stages[0]
        self.assertIsInstance(stage, StageDefinition)
        errors = _update_model(stage.errors, retries=2)
        updated = _update_model(stage, errors=errors, returns_data=False)
        self.assertEqual(updated.errors.retries, 2)
        self.assertIn("returns_data", updated.model_fields_set)
        self.assertIs(updated.returns_data, False)
        self.assertEqual(stage.errors.retries, 0)
        with self.assertRaises(ValueError):
            _update_model(stage, errors={"retries": 1})

    def test_cursor_update_uses_explicit_json_representation(self):
        cursor = PublicationCursor.model_validate({"position": [1, "a", "b"], "publication": "p"})
        replacement = _update_model(cursor, _dump_mode="json", publication="next")
        self.assertEqual(replacement.position, (1, "a", "b"))
        self.assertEqual(replacement.publication, "next")
        self.assertEqual(cursor.publication, "p")
