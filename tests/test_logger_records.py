"""Approved v2 configuration facts, resources and runner/service result semantics."""

import json
import os
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import LoggingStateError
from tests.helpers.logging_fixtures import (
    BASE_CONTEXT,
    TEMPLATE_YAML,
    write_context_settings,
)
from tests.helpers.logging_process import SCRATCH_ROOT, cleanup_directory, read_database


class OperationLoggerRecordsTests(unittest.TestCase):
    def test_yaml_is_required_and_invalid_values_leave_the_client_usable(self):
        """B4/B5: missing or invalid YAML never produces a partial parameters event."""
        with self.assertRaises(TypeError):
            self.logger.record_template_applied({}, template_revision_id="revision")
        with self.assertRaises(TypeError):
            self.logger.record_attempt_parameters({}, {})
        for value in (None, 1, "", " ", "bad\x00"):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                self.logger.record_attempt_parameters({}, {}, template_yaml=value)
        self.assertEqual(read_database(self.db_path), [])
        self.logger.record_attempt_parameters({}, {}, template_yaml="# retained\n{}\n")
        self.assertEqual(
            read_database(self.db_path)[0]["data"]["template_yaml"], "# retained\n{}\n"
        )

    def test_control_intent_observation_and_unknown_reconciliation_remain_distinct(
        self,
    ):
        """B6: recording empty command observations never invents a completed action."""
        with patch("subprocess.Popen") as spawn:
            with self.logger.operation("launch", "stage") as operation:
                parameters = self.logger.record_attempt_parameters(
                    {}, {}, template_yaml="{}", operation=operation
                )
                intent = self.logger.record_event(
                    "control.intent",
                    {
                        "action": "stage.start",
                        "parameters_event_id": parameters,
                        "arguments": ["--input", "payload"],
                        "timeout_seconds": 30,
                    },
                    operation=operation,
                )
                self.logger.record_event(
                    "control.observed",
                    {"intent_event_id": intent, "state": "sent"},
                    operation=operation,
                )
                self.logger.record_event(
                    "control.reconciled",
                    {
                        "intent_event_id": intent,
                        "state": "unknown",
                        "observations": {"pending_commands": []},
                    },
                    operation=operation,
                )
            spawn.assert_not_called()
        facts = [
            event
            for event in read_database(self.db_path)
            if event["event_type"].startswith("control.")
        ]
        self.assertEqual(
            [event["event_type"] for event in facts],
            ["control.intent", "control.observed", "control.reconciled"],
        )
        self.assertEqual(facts[-1]["data"]["state"], "unknown")
        self.assertEqual(facts[-1]["data"]["intent_event_id"], intent)
        self.assertEqual(facts[0]["data"]["parameters_event_id"], parameters)

    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.config = write_context_settings(self.folder)
        self.db_path = self.folder / "events.db"
        self.logger = OperationLogger(self.config)
        self.addCleanup(self.logger.close)
        self.logger.open()

    def service_client(self):
        config = write_context_settings(
            self.folder / "participant",
            db_path=str(self.db_path),
            open_mode="existing",
            context={
                **BASE_CONTEXT,
                "source": "participant",
                "host_name": "wrong-host",
                "process_id": 1,
            },
        )
        client = OperationLogger(config)
        self.addCleanup(client.close)
        client.open()
        return client

    def test_full_template_and_effective_settings_are_detached_without_reference_reads(
        self,
    ):
        referenced = self.folder / "external-settings.json"
        referenced.write_text('{"private": "file-only"}', encoding="utf-8")
        template = {
            "stages": [{"id": "stage-A", "position": 2}],
            "literal_value": "preserve-this-verbatim",
            "settings_file": str(referenced),
            "optional": None,
        }
        effective = {"nested": {"values": [None, False, 0, 2.5]}, "enabled": True}
        expected_template = json.loads(json.dumps(template))
        expected_effective = json.loads(json.dumps(effective))
        template_yaml = "# original comment\n" + json.dumps(template, indent=2) + "\n"
        original_open = Path.open

        def guard_reference(path, *args, **kwargs):
            self.assertNotEqual(
                path, referenced, "Logger must not read referenced settings"
            )
            return original_open(path, *args, **kwargs)

        with patch.object(Path, "open", guard_reference):
            applied = self.logger.record_template_applied(
                template,
                template_revision_id="template-2",
                previous_template_revision_id="template-1",
                reason="reload_template",
                template_yaml=template_yaml,
            )
            parameters = self.logger.record_attempt_parameters(
                template, effective, template_yaml=template_yaml
            )
        template["stages"][0]["position"] = 99
        effective["nested"]["values"].append("changed")
        records = read_database(self.db_path)
        self.assertEqual([e["event_id"] for e in records], [applied, parameters])
        self.assertEqual(
            [e["event_type"] for e in records],
            ["template.applied", "attempt.parameters"],
        )
        self.assertEqual(records[0]["data"]["template"], expected_template)
        self.assertEqual(
            records[1]["data"],
            {
                "template": expected_template,
                "effective_settings": expected_effective,
                "template_yaml": template_yaml,
            },
        )
        self.assertEqual(
            records[0]["data"]["previous_template_revision_id"], "template-1"
        )
        self.assertFalse(any(e["event_type"] == "operation.started" for e in records))

    def test_required_attempt_context_and_oversized_parameters_do_not_write(self):
        for field in (
            "experiment_id",
            "run_id",
            "stage_id",
            "stage_execution_id",
            "attempt_id",
            "template_revision_id",
            "cycle_number",
            "attempt_number",
            "module_name",
            "module_version",
            "module_hash",
        ):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.logger.record_attempt_parameters(
                    {}, {}, context={field: None}, template_yaml=TEMPLATE_YAML
                )
        with self.assertRaises(ValueError):
            self.logger.record_template_applied(
                {},
                template_revision_id="next",
                context={"experiment_id": None},
                template_yaml=TEMPLATE_YAML,
            )
        with self.assertRaises(ValueError):
            self.logger.record_template_applied(
                {},
                template_revision_id="same",
                previous_template_revision_id="same",
                template_yaml=TEMPLATE_YAML,
            )
        self.logger.close()
        write_context_settings(self.folder, open_mode="existing", max_event_bytes=2048)
        self.logger.open()
        with self.assertRaises(ValueError):
            self.logger.record_attempt_parameters(
                {"large": "x" * 1048576}, {}, template_yaml=TEMPLATE_YAML
            )
        self.assertEqual(read_database(self.db_path), [])
        self.logger.record_attempt_parameters(
            {}, {"recovered": True}, template_yaml=TEMPLATE_YAML
        )
        self.assertEqual(len(read_database(self.db_path)), 1)

    def test_context_propagation_keeps_full_ids_and_actual_writer_identity(self):
        with self.logger.operation("attempt", "prepare") as parent:
            child_context = parent.get_child_context()
            self.assertEqual(
                child_context["experiment_id"], BASE_CONTEXT["experiment_id"]
            )
            self.assertEqual(child_context["cycle_number"], 3)
            with self.logger.operation("internal", "child", context=child_context):
                pass
        service = self.service_client()
        event_id = service.record_event(
            "service.observed", context={"host_name": "fake", "process_id": 7}
        )
        event = next(
            e for e in read_database(self.db_path) if e["event_id"] == event_id
        )
        self.assertEqual(event["context"]["host_name"], socket.gethostname())
        self.assertEqual(event["context"]["process_id"], os.getpid())
        starts = [
            e
            for e in read_database(self.db_path)
            if e["event_type"] == "operation.started"
        ]
        self.assertEqual(
            starts[1]["context"]["parent_operation_id"], starts[0]["operation_id"]
        )

    def test_generic_lifecycle_output_and_notification_facts_keep_all_data(self):
        facts = {
            "dag.paused": {"confirmed": True, "reason": "operator"},
            "command.output": {
                "stream": "stderr",
                "text": "first\nsecond",
                "observed_process_id": 123,
            },
            "service.message": {"message": {"result": "fail", "data": [1, None]}},
            "notification.delivery": {
                "incident_id": "incident",
                "channel": "desktop",
                "status": "failed",
            },
        }
        for kind, payload in facts.items():
            self.logger.record_event(kind, payload)
        records = read_database(self.db_path)
        self.assertEqual({e["event_type"]: e["data"] for e in records}, facts)
        for kind in ("template.applied", "attempt.parameters", "command.result"):
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                self.logger.record_event(kind, {})
        self.assertEqual(len(read_database(self.db_path)), len(facts))

    def test_host_resources_distinguish_zero_missing_and_estimated_measurements(self):
        self.logger.record_resources(
            {
                "cpu": {"value": 0, "unit": "%", "kind": "gauge", "scope": "host"},
                "vram": {
                    "value": None,
                    "unit": "byte",
                    "kind": "gauge",
                    "scope": "host",
                },
                "internet_rx": {
                    "value": 125000,
                    "unit": "byte/s",
                    "kind": "gauge",
                    "scope": "host",
                    "estimated": True,
                    "attributes": {"interface": "wan"},
                },
            }
        )
        measurements = read_database(self.db_path)[0]["data"]["resources"]
        self.assertEqual(measurements["cpu"]["value"], 0)
        self.assertIsNone(measurements["vram"]["value"])
        self.assertTrue(measurements["internet_rx"]["estimated"])
        self.assertEqual(
            measurements["internet_rx"]["attributes"], {"interface": "wan"}
        )

    def test_matching_responses_in_both_orders_share_an_event_and_keep_authors(self):
        service = self.service_client()
        clients = {"runner": self.logger, "participant": service}
        for first, second in (("runner", "participant"), ("participant", "runner")):
            with self.subTest(first=first):
                request = "equal-" + first
                response = {"data": {"a": 1, "b": [None, False]}, "result": "success"}
                first_id = clients[first].record_command_result(
                    request, response, author=first, outcome="succeeded"
                )
                before = read_database(self.db_path)
                reordered = {"result": "success", "data": {"b": [None, False], "a": 1}}
                second_id = clients[second].record_command_result(
                    request, reordered, author=second, outcome="succeeded"
                )
                self.assertEqual(first_id, second_id)
                self.assertEqual(read_database(self.db_path), before)
                result = self.logger.read_command_result(request)
                self.assertEqual(result["author"], "runner")
                self.assertFalse(result["provisional"])
                self.assertEqual(result["event"]["data"]["author"], first)
                self.assertEqual(result["event"]["context"]["source"], first)
                self.assertEqual(
                    {o["author"] for o in result["observations"]}, {"runner", "participant"}
                )
                self.assertTrue(
                    all(o["ignored"] is None for o in result["observations"])
                )
                self.assertNotEqual(
                    result["observations"][0]["observation"]["producer_instance_id"],
                    result["observations"][1]["observation"]["producer_instance_id"],
                )
        self.logger.record_event("after.duplicates")
        own_events = [
            e for e in read_database(self.db_path) if e["context"]["source"] == "runner"
        ]
        self.assertEqual(
            [e["sequence_number"] for e in own_events],
            list(range(1, len(own_events) + 1)),
        )

    def test_service_result_is_provisional_until_runner_confirmation(self):
        service = self.service_client()
        self.assertIsNone(self.logger.read_command_result("unknown"))
        service.record_command_result(
            "provisional", {}, author="participant", outcome="succeeded"
        )
        result = self.logger.read_command_result("provisional")
        self.assertTrue(result["provisional"])
        self.assertEqual(result["author"], "participant")

    def test_conflicting_responses_keep_runner_and_retain_ignored_service_payload(self):
        service = self.service_client()
        clients = {"runner": self.logger, "participant": service}
        for first, second in (("runner", "participant"), ("participant", "runner")):
            with self.subTest(first=first):
                request = "conflict-" + first
                ids = {}
                ids[first] = clients[first].record_command_result(
                    request,
                    {"origin": first},
                    author=first,
                    outcome="succeeded" if first == "participant" else "failed",
                )
                initial = read_database(self.db_path)
                ids[second] = clients[second].record_command_result(
                    request,
                    {"origin": second},
                    author=second,
                    outcome="succeeded" if second == "participant" else "failed",
                )
                records = read_database(self.db_path)
                self.assertEqual(records[: len(initial)], initial)
                result = self.logger.read_command_result(request)
                self.assertEqual(result["event_id"], ids["runner"])
                self.assertEqual(result["outcome"], "failed")
                ignored = next(
                    o for o in result["observations"] if o["author"] == "participant"
                )
                self.assertEqual(ignored["ignored"], "runner_result_precedence")
                self.assertEqual(
                    ignored["event"]["data"]["response"], {"origin": "participant"}
                )
                if first == "participant":
                    self.assertIsNone(ignored["event"]["data"]["ignored"])
                    self.assertEqual(
                        result["event"]["data"]["supersedes"],
                        [
                            {
                                "event_id": ids["participant"],
                                "ignored": "runner_result_precedence",
                            }
                        ],
                    )
                else:
                    self.assertEqual(
                        ignored["event"]["data"]["ignored"], "runner_result_precedence"
                    )

    def test_late_service_success_cannot_replace_invalidated_runner_outcomes(self):
        service = self.service_client()
        for outcome, reason in (
            ("timed_out", "request_timed_out"),
            ("cancelled", "request_cancelled"),
            ("invalidated", "request_invalidated"),
        ):
            with self.subTest(outcome=outcome):
                self.logger.record_command_result(
                    outcome, {"reason": outcome}, author="runner", outcome=outcome
                )
                service.record_command_result(
                    outcome,
                    {"result": "success"},
                    author="participant",
                    outcome="succeeded",
                )
                result = self.logger.read_command_result(outcome)
                self.assertEqual(result["outcome"], outcome)
                self.assertEqual(result["observations"][1]["ignored"], reason)

    def test_response_comparison_preserves_scalar_types_array_order_and_outcome(self):
        service = self.service_client()
        for index, (left, right) in enumerate(((True, 1), (1, 1.0), ([1, 2], [2, 1]))):
            request = f"types-{index}"
            with self.subTest(values=(left, right)):
                first = service.record_command_result(
                    request, {"value": left}, author="participant", outcome="succeeded"
                )
                second = self.logger.record_command_result(
                    request, {"value": right}, author="runner", outcome="succeeded"
                )
                self.assertNotEqual(first, second)
        first = service.record_command_result(
            "outcome", {}, author="participant", outcome="succeeded"
        )
        second = self.logger.record_command_result(
            "outcome", {}, author="runner", outcome="failed"
        )
        self.assertNotEqual(first, second)

    def test_repeated_author_and_request_ownership_errors_are_nonfatal(self):
        first = self.logger.record_command_result(
            "owned", {"x": 1}, author="runner", outcome="succeeded"
        )
        second = self.logger.record_command_result(
            "owned", {"x": 1}, author="runner", outcome="succeeded"
        )
        self.assertEqual(first, second)
        for options in (
            {"response": {"x": 2}},
            {"context": {"experiment_id": "other"}},
            {"context": {"participant_id": "other"}},
            {"context": {"participant_instance_id": "other"}},
            {"context": {"request_id": "different"}},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                kwargs = {
                    "response": {"x": 1},
                    "author": "runner",
                    "outcome": "succeeded",
                    **options,
                }
                self.logger.record_command_result("owned", **kwargs)
        self.logger.record_event("after.rejected.results")
        self.assertEqual(len(read_database(self.db_path)), 2)

    def test_invalid_command_inputs_leave_client_usable(self):
        for options in (
            {"author": "stage"},
            {"outcome": "success"},
            {"response": []},
            {"context": {"experiment_id": None}},
            {"context": {"participant_id": None}},
        ):
            with (
                self.subTest(options=options),
                self.assertRaises((TypeError, ValueError)),
            ):
                self.logger.record_command_result(
                    "request",
                    **{
                        "response": {},
                        "author": "runner",
                        "outcome": "succeeded",
                        **options,
                    },
                )
        self.assertEqual(read_database(self.db_path), [])
        self.logger.record_event("still.healthy")

    def test_operation_bound_records_and_old_handles_keep_session_rules(self):
        operation = self.logger.start_operation("attempt", "stage-A")
        self.logger.record_attempt_parameters(
            {}, {}, operation=operation, template_yaml=TEMPLATE_YAML
        )
        self.logger.record_command_result(
            "bound", {}, author="runner", outcome="succeeded", operation=operation
        )
        records = read_database(self.db_path)
        self.assertEqual(
            {e["operation_id"] for e in records}, {operation.get_operation_id()}
        )
        self.logger.close()
        self.logger.open()
        for call in (
            lambda: self.logger.record_attempt_parameters(
                {}, {}, operation=operation, template_yaml=TEMPLATE_YAML
            ),
            lambda: self.logger.record_command_result(
                "new", {}, author="runner", outcome="succeeded", operation=operation
            ),
            lambda: self.logger.finish_operation(operation),
        ):
            with self.assertRaises(LoggingStateError):
                call()
