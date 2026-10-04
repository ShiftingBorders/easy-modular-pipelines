"""Validated replacements and typed experiment inputs retain boundary guarantees."""

import asyncio
import copy
import unittest
from pathlib import Path
from unittest.mock import patch
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
from core.models.participant_identity import ParticipantIdentity
from core.models.participant_launch import (
    ExecutionCall,
    ModuleContext,
    ModulePreparation,
    PreparedLaunch,
    PreparedModuleContext,
    StageExecutionIdentity,
    StageLaunch,
)
from core.models.updates import _update_model
from core.primitives.json_values import copy_json_object
from tests.helpers.services import ServiceWorkspace


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

    def test_typed_definitions_keep_whole_template_depth_and_unicode_limits(self):
        nested = None
        for _ in range(30):
            nested = [nested]
        stage = _update_model(self.template.stages[0], settings={"deep": nested})
        for definition in (stage, stage.model_dump(exclude_unset=True)):
            with self.subTest(kind=type(definition).__name__), self.assertRaises(ValueError):
                ExperimentTemplate.model_validate({**self.document, "stages": [definition]})
        module = _update_model(self.template.stages[0].module, name="worker\ud800")
        stage = _update_model(self.template.stages[0], module=module)
        with self.assertRaises(ValueError):
            ExperimentTemplate.model_validate({**self.document, "stages": [stage]})

    def _execution_values(self, identity):
        root = Path(__file__).resolve().parents[2]
        return {
            "context": identity,
            "input_data": {"data": [1, None]},
            "settings": {"application": [True, "данные"]},
            "experiment_directory": root,
            "resources_directory": root / "resources",
            "settings_directory": root / "settings",
            "module_data_directory": root / "data",
            "artifacts_directory": root / "artifacts",
        }

    def test_execution_context_retains_models_and_native_paths_until_output(self):
        identity = StageExecutionIdentity(
            experiment_id="experiment",
            participant_id=str(uuid4()),
            participant_instance_id=str(uuid4()),
            request_id=str(uuid4()),
            historical={"values": [1, 1.0, True, None]},
        )
        values = self._execution_values(identity)
        call = ExecutionCall.model_validate(values)
        self.assertIs(call.context, identity)
        self.assertEqual(call.experiment_directory, values["experiment_directory"])
        document = call.model_dump(mode="json", exclude_unset=True)
        self.assertEqual(document["context"], identity.model_dump())
        self.assertEqual(
            document["experiment_directory"], str(values["experiment_directory"])
        )
        values["settings"]["application"].append("external")
        self.assertNotIn("external", call.settings["application"])
        root = values["experiment_directory"]
        context = PreparedModuleContext(
            **self._execution_values(identity),
            protocol_version=2,
            logging_config_path=None,
            endpoint_path=root / "endpoint.json",
            control_timeout_seconds=1,
        )
        self.assertIsNone(context.logging_config_path)
        with self.assertRaises((TypeError, ValueError)):
            ModuleContext.model_validate(
                context.model_dump(mode="json", exclude_unset=True)
            )

    def test_launch_identity_comparison_keeps_extras_and_rejects_mismatch(self):
        identity = StageExecutionIdentity(
            experiment_id="experiment",
            participant_id=str(uuid4()),
            participant_instance_id=str(uuid4()),
            request_id=str(uuid4()),
            historical={"value": 1},
        )
        root = Path(__file__).resolve().parents[2]
        call = ExecutionCall.model_validate(self._execution_values(identity))
        context = ModuleContext(
            **self._execution_values(identity),
            protocol_version=2,
            logging_config_path=root / "logger.json",
            endpoint_path=root / "endpoint.json",
            control_timeout_seconds=1,
        )
        document = {
            "argv": ["python"],
            "code_directory": str(root),
            "executor_logging_config": str(root / "executor.json"),
            "context": identity.model_dump(),
            "runtime_context": context.model_dump(mode="json", exclude_unset=True),
            "call": call.model_dump(mode="json", exclude_unset=True),
            "endpoint_path": str(context.endpoint_path),
            "control_timeout_seconds": 1,
            "stop_timeout_seconds": 1,
            "runner_timeout_margin_seconds": 0,
        }
        launch = StageLaunch.model_validate(document)
        self.assertEqual(launch.context.request_id, identity.request_id)
        for field, value in (
            ("request_id", str(uuid4())),
            ("historical", {"value": 2}),
        ):
            candidate = copy.deepcopy(document)
            candidate["call"]["context"][field] = value
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(ValueError, "same fixed call"),
            ):
                StageLaunch.model_validate(candidate)

    def test_typed_execution_inputs_preserve_json_constraints(self):
        identity = ParticipantIdentity(
            experiment_id="experiment",
            participant_id=str(uuid4()),
            participant_instance_id=str(uuid4()),
        )
        values = self._execution_values(identity)
        for bad in (float("inf"), "\ud800", {1: "invalid"}, identity):
            with (
                self.subTest(bad=type(bad).__name__),
                self.assertRaises((TypeError, ValueError)),
            ):
                ExecutionCall.model_validate({**values, "input_data": bad})
        nested = None
        for _ in range(31):
            nested = [nested]
        too_deep = _update_model(identity, historical=nested)
        with self.assertRaises(ValueError):
            ExecutionCall.model_validate({**values, "context": too_deep})
        with self.assertRaises(TypeError):
            ExecutionCall.model_validate(
                {**values, "arbitrary_path": values["experiment_directory"]}
            )
        with self.assertRaises(TypeError):
            copy_json_object({"context": identity}, "JSON only")

    def test_preparation_retains_models_and_public_service_call_stays_json(self):
        workspace = ServiceWorkspace()
        self.addCleanup(lambda: asyncio.run(workspace.close()))
        service = workspace.service()
        definition = workspace.state.template.services[0]
        identity = ParticipantIdentity(
            experiment_id=workspace.state.experiment_id,
            participant_id=definition.service_id,
            participant_instance_id=str(uuid4()),
        )
        inputs = ModulePreparation(
            context=identity,
            artifacts_directory=workspace.root / "typed-preparation",
            input_data=None,
        )
        prepared = workspace.launcher._prepare(workspace.state, definition, inputs)
        self.assertIsInstance(prepared, PreparedLaunch)
        self.assertIsInstance(prepared.runtime_context, PreparedModuleContext)
        self.assertIs(prepared.context, identity)
        self.assertIs(prepared.call.context, identity)
        self.assertIs(prepared.runtime_context.context, identity)
        self.assertNotIn("request_id", prepared.context.model_fields_set)
        call = ServiceCallDefinition(
            stage_id=str(uuid4()),
            service_id=definition.service_id,
            settings={},
            timeout_seconds=None,
            errors=definition.errors,
        )
        document = workspace.launcher.prepare(
            workspace.state,
            call.model_dump(exclude_unset=True),
            identity.model_dump(),
            workspace.root / "public-preparation",
            None,
        )
        self.assertIsInstance(document, dict)
        self.assertIsNone(document["runtime_context"]["logging_config_path"])
        self.assertIsNone(document["executor_logging_config"])
        self.assertEqual(document["service_id"], service["service_id"])
        self.assertEqual(document["context"], identity.model_dump())

    def test_service_consumer_calls_public_prepare_hook_and_retains_launch(self):
        workspace = ServiceWorkspace()
        self.addCleanup(lambda: asyncio.run(workspace.close()))
        workspace.service()
        definition = workspace.state.template.services[0]
        original = workspace.launcher.prepare
        captured = []

        def prepare(*args, **kwargs):
            self.assertIsInstance(args[1], dict)
            self.assertIsInstance(args[2], dict)
            document = original(*args, **kwargs)
            document["callback_metadata"] = {"value": [1, None]}
            captured.append(document)
            return document

        with patch.object(workspace.launcher, "prepare", prepare):
            instance, directory, launch, context = (
                workspace.manager._prepare_service_start(workspace.state, definition)
            )
        self.assertEqual(len(captured), 1)
        self.assertIsInstance(launch, PreparedLaunch)
        self.assertEqual(launch.model_extra["callback_metadata"], {"value": [1, None]})
        self.assertEqual(instance.endpoint_path, launch.endpoint_path)
        self.assertEqual(instance.artifacts_directory, directory)
        self.assertEqual(launch.context.experiment_id, context["experiment_id"])
        self.assertEqual(
            launch.model_dump(mode="json", exclude_unset=True), captured[0]
        )
