"""Approved basic_dag.md D1/D2/D7: durable state and publication failures."""

import copy
import errno
import json
import os
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

import yaml

from core.experiments.results import read_result
from core.experiments.stages import _finish_executor_status
from core.experiments.state import (
    RunnerState,
    RunnerStateStore,
    ServiceInstance,
    StageAttempt,
    _attempt_result_identity,
    _relative_state_path,
    state_from_document,
    state_to_document,
)
from core.models.experiment_template import (
    ExperimentTemplate,
    HeartbeatPolicy,
    ServiceDefinition,
)
from core.models.participant_identity import ParticipantIdentity
from core.models.participant_observations import (
    ExecutorCommandState,
    RetainedExecutorStatus,
    RetainedServiceStatus,
    ServiceObservation,
)
from core.models.process_identity import ProcessIdentity
from core.models.runner_state import (
    DagDecision,
    LastDecision,
    PendingInput,
    PendingRebuild,
    ServiceCallExecutorStatus,
    ServiceFailure,
    ServiceFailureDetails,
    ServiceRequest,
    WorkingServiceRequest,
)
from core.models.updates import _update_model
from core.participants.protocol import error_details
from core.primitives.json_files import read_json, write_json
from core.primitives.processes import process_identity
from tests.helpers.dag import DagWorkspace


class RunnerStateTests(unittest.TestCase):
    def setUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.root = self.workspace.root / "experiment"
        self.root.mkdir()
        template = self.workspace.template()
        self.state = RunnerState(
            "experiment",
            self.root,
            "run",
            self.root / "experiment.yaml",
            str(uuid4()),
            yaml.safe_dump(template),
            template,
            "paused",
        )
        self.store = RunnerStateStore()

    def test_state_round_trip_retains_models_and_detaches_public_json(self):
        _, service = self._add_path_participants()
        self.store.save(self.state)
        restored = self.store.load(self.root)
        self.assertIsInstance(restored.template, ExperimentTemplate)
        self.assertIsInstance(
            restored.services[service.service_id].definition, ServiceDefinition
        )
        self.assertEqual(state_to_document(restored), state_to_document(self.state))
        document = state_to_document(restored)
        document["template"]["stages"][0]["settings"]["external"] = True
        self.assertNotIn("external", restored.template.stages[0].settings)

    def test_recovery_rejects_incomplete_or_unassigned_runtime_templates(self):
        document = state_to_document(self.state)
        incomplete = self.state.template.model_dump(exclude_unset=True)
        del incomplete["stages"][0]["stage_id"]
        for template in (
            {},
            {"stages": []},
            {"stages": None},
            {"stages": [{}]},
            incomplete,
        ):
            with (
                self.subTest(template=template),
                self.assertRaises((TypeError, ValueError)),
            ):
                state_from_document(self.root, {**document, "template": template})

    def test_managed_records_remain_models_after_persistence(self):
        _, service = self._add_path_participants()
        stage_id = self.state.template.stages[0].stage_id
        request_id, snapshot_id = str(uuid4()), str(uuid4())
        decision = DagDecision.model_validate({"command": "move", "stage_id": stage_id})
        self.state.last_dag_decision = LastDecision(
            request_id=request_id,
            source_stage_id=stage_id,
            experiment_id=self.state.experiment_id,
            decision=decision,
        )
        self.state.pending_input = PendingInput(
            request_id=request_id,
            source_stage_id=stage_id,
            experiment_id=self.state.experiment_id,
            stage_id=stage_id,
        )
        self.state.stable_snapshot_id = snapshot_id
        self.state.pending_rebuild = PendingRebuild(
            operation_id=str(uuid4()),
            snapshot_id=snapshot_id,
            template_revision_id=self.state.template_revision_id,
            run_id=self.state.run_id,
        )
        self.state.owner_identity = ProcessIdentity.model_validate(
            process_identity(os.getpid())
        )
        service.failure = ServiceFailureDetails(
            code="service_failure", message="failed"
        )
        document = state_to_document(self.state)
        self.store.save(self.state)
        restored = self.store.load(self.root)
        for value, expected in (
            (restored.pending_input, PendingInput),
            (restored.pending_rebuild, PendingRebuild),
            (restored.last_dag_decision, LastDecision),
            (restored.last_dag_decision.decision, DagDecision),
            (restored.owner_identity, ProcessIdentity),
            (restored.services[service.service_id].failure, ServiceFailureDetails),
        ):
            self.assertIsInstance(value, expected)
        self.assertEqual(state_to_document(restored), document)
        self.assertIs(self.state.last_dag_decision.decision, decision)

    def test_live_failure_retains_empty_message_and_saved_contract(self):
        failure = ServiceFailureDetails(
            code="service_failure", message=str(TimeoutError())
        )
        self.assertEqual(
            failure.model_dump(), error_details("service_failure", TimeoutError())
        )
        with self.assertRaises(ValueError):
            ServiceFailure.model_validate(failure.model_dump())

    def test_service_request_replacements_and_presence_survive_restore(self):
        _, service = self._add_path_participants()
        request_id = str(uuid4())
        request = WorkingServiceRequest.model_validate(
            {
                "owner": "service",
                "request_id": request_id,
                "command": "echo",
                "args": {"nested": [1]},
                "queued_monotonic": 1,
                "sent_monotonic": None,
                "timed_out": False,
                "compatibility": {"value": None},
            }
        )
        service.pending_requests = [request]
        self.state.used_request_ids.add(request_id)
        document = state_to_document(self.state)
        self.store.save(self.state)
        restored = self.store.load(self.root)
        pending = restored.services[service.service_id].pending_requests[0]
        self.assertIsInstance(pending, WorkingServiceRequest)
        self.assertNotIn("deadline_monotonic", pending.model_fields_set)
        self.assertNotIn("service_instance_id", pending.model_fields_set)
        self.assertEqual(state_to_document(restored), document)
        sent = _update_model(
            request,
            service_instance_id=service.service_instance_id,
            sent_monotonic=2,
            sent_at="now",
        )
        self.assertIsNone(request.sent_monotonic)
        self.assertEqual(sent.sent_monotonic, 2)
        self.assertEqual(sent.model_extra["compatibility"], {"value": None})
        with self.assertRaises(ValueError):
            _update_model(request, request_id="invalid")
        self.assertEqual(request.request_id, request_id)

    def test_live_request_keeps_saved_deadline_constraint_separate(self):
        request = WorkingServiceRequest.model_validate(
            {
                "owner": "service",
                "request_id": str(uuid4()),
                "command": "echo",
                "args": {},
                "queued_monotonic": 1,
                "deadline_monotonic": -1,
                "sent_monotonic": None,
                "timed_out": False,
            }
        )
        self.assertEqual(request.deadline_monotonic, -1)
        with self.assertRaises(ValueError):
            ServiceRequest.model_validate(request.model_dump(exclude_unset=True))

    def test_retained_service_status_preserves_sparse_documents_and_observation(self):
        _, service = self._add_path_participants()
        for document in (
            {},
            {"legacy": {"value": None}},
            {"request_id": "historical", "observed_monotonic": None},
        ):
            service.last_status = RetainedServiceStatus.model_validate(document)
            self.store.save(self.state)
            restored = (
                self.store.load(self.root).services[service.service_id].last_status
            )
            self.assertIsInstance(restored, RetainedServiceStatus)
            self.assertEqual(restored.model_dump(exclude_unset=True), document)
        observation = ServiceObservation.model_validate(
            {
                "protocol_version": 2,
                "request_id": str(uuid4()),
                "result": "success",
                "data": {"value": 1},
                "command": "heartbeat",
                "legacy": [None, False],
            }
        )
        retained = RetainedServiceStatus.from_observation(observation, "observed", 3)
        self.assertEqual(
            retained.model_dump(exclude_unset=True),
            {
                **observation.model_dump(exclude_unset=True),
                "observed_at": "observed",
                "observed_monotonic": 3,
            },
        )
        self.assertNotIn("observed_at", observation.model_extra or {})

    def _add_path_participants(self):
        stage_id = str(uuid4())
        attempt = StageAttempt(
            str(uuid4()),
            stage_id,
            str(uuid4()),
            1,
            1,
            self.root / "attempt",
            None,
            {},
            None,
        )
        attempt.participant = ParticipantIdentity(
            experiment_id=self.state.experiment_id,
            participant_id=stage_id,
            participant_instance_id=attempt.attempt_id,
        )
        attempt.endpoint_path = self.root / "stage-endpoint.json"
        self.state.active_attempt = attempt
        self.state.used_request_ids.add(attempt.request_id)
        service_id = str(uuid4())
        stage = self.state.template.stages[0]
        definition = ServiceDefinition(
            service_id=service_id,
            module=stage.module,
            settings={},
            heartbeat=HeartbeatPolicy(interval_seconds=1, grace_seconds=2),
            command_timeout_seconds=3,
            on_command_timeout="stop",
            state_required=False,
            errors=stage.errors,
        )
        service = ServiceInstance(service_id, str(uuid4()), definition)
        self.state.template = _update_model(self.state.template, services=[definition])
        service.endpoint_path = self.root / "service-endpoint.json"
        self.state.services[service_id] = service
        return attempt, service

    def test_executor_status_models_survive_json_publication_and_recovery(self):
        attempt, _ = self._add_path_participants()
        identity = process_identity(os.getpid())
        request = WorkingServiceRequest(
            owner="caller",
            request_id=attempt.request_id,
            command="execute",
            args={"value": [1, None]},
            queued_monotonic=1,
            sent_monotonic=None,
            timed_out=False,
        )
        full = ExecutorCommandState.model_validate(
            {
                "current": {"request_id": attempt.request_id, "command": "execute"},
                "pending": [],
                "process": identity,
                "started_at": "started",
                "started_monotonic": 1,
                "finished": False,
                "exit_code": None,
                "progress": {"value": 0.5, "detail": {"value": 1}},
                "module_state": {"working": True},
                "extra": [1, 1.0, True],
            }
        )
        service = ServiceCallExecutorStatus(
            participant=attempt.participant,
            request_id=attempt.request_id,
            process=identity,
            finished=False,
            current=request,
        )
        self.assertIs(service.current, request)
        self.assertIs(service.participant, attempt.participant)
        for status in (
            full,
            service,
            RetainedExecutorStatus.model_validate({}),
            RetainedExecutorStatus.model_validate(
                {"finished": "historical", "process": {}, "unknown": None}
            ),
        ):
            with self.subTest(status=type(status).__name__):
                attempt.executor_status = status
                document = state_to_document(self.state)
                expected = status.model_dump(exclude_unset=True)
                self.assertEqual(
                    document["active_attempt"]["executor_status"], expected
                )
                json.dumps(document, allow_nan=False)
                restored = state_from_document(self.root, document)
                self.assertIsInstance(
                    restored.active_attempt.executor_status, type(status)
                )
                self.assertEqual(state_to_document(restored), document)
                finished = _finish_executor_status(status)
                self.assertEqual(
                    finished.model_dump(exclude_unset=True),
                    {**expected, "finished": True, "current": None, "pending": []},
                )
                self.assertEqual(status.model_dump(exclude_unset=True), expected)
        attempt.executor_status = full
        document = state_to_document(self.state)
        document["active_attempt"]["executor_status"]["progress"]["detail"]["value"] = 9
        self.assertEqual(full.progress.model_extra["detail"]["value"], 1)

    def test_service_process_identity_remains_a_model_after_persistence(self):
        attempt, service = self._add_path_participants()
        identity = ProcessIdentity.model_validate(process_identity(os.getpid()))
        service.process_identity = identity
        attempt.process_identity = identity
        self.store.save(self.state)
        restored = self.store.load(self.root)
        self.assertIsInstance(
            restored.services[service.service_id].process_identity, ProcessIdentity
        )
        self.assertIsInstance(restored.active_attempt.process_identity, ProcessIdentity)
        self.assertEqual(
            restored.services[service.service_id].process_identity, identity
        )
        self.assertEqual(state_to_document(restored), state_to_document(self.state))
        document = state_to_document(restored)
        self.assertEqual(
            document["active_attempt"]["process_identity"], identity.model_dump()
        )
        document["services"][service.service_id]["process_identity"]["pid"] += 1
        self.assertEqual(
            restored.services[service.service_id].process_identity.pid, os.getpid()
        )

    def test_attempt_identity_retains_extras_and_detaches_saved_json(self):
        attempt, _ = self._add_path_participants()
        attempt.participant = _update_model(
            attempt.participant, historical={"values": [1, 1.0, True, None]}
        )
        document = state_to_document(self.state)
        restored = state_from_document(self.root, document)
        self.assertIsInstance(restored.active_attempt.participant, ParticipantIdentity)
        self.assertEqual(
            restored.active_attempt.participant.participant_instance_id,
            attempt.participant.participant_instance_id,
        )
        self.assertEqual(state_to_document(restored), document)
        document["active_attempt"]["participant"]["historical"]["values"].append(
            "external"
        )
        self.assertNotIn(
            "external",
            restored.active_attempt.participant.model_extra["historical"]["values"],
        )

    def test_partial_attempt_process_identity_keeps_historical_document_contract(self):
        attempt, _ = self._add_path_participants()
        for identity in (
            None,
            {},
            {"pid": os.getpid()},
            {"pid": True, "historical": {"value": [1, None]}},
            {**process_identity(os.getpid()), "historical": "kept"},
        ):
            with self.subTest(identity=identity):
                attempt.process_identity = identity
                document = state_to_document(self.state)
                restored = state_from_document(self.root, document)
                self.assertEqual(restored.active_attempt.process_identity, identity)
                self.assertEqual(state_to_document(restored), document)

    def test_journal_result_checks_model_identity_and_current_attempt_coordinates(self):
        attempt, _ = self._add_path_participants()
        attempt.participant = _update_model(
            attempt.participant,
            stage_id="historical-stage",
            attempt_id="historical-attempt",
            historical={"version": 1},
        )
        expected = _attempt_result_identity(attempt)
        self.assertEqual(expected.stage_id, attempt.stage_id)
        self.assertEqual(expected.attempt_id, attempt.attempt_id)
        self.assertEqual(expected.model_extra["historical"], {"version": 1})
        context = expected.model_dump(exclude_unset=True)
        record = {
            "event": {"context": context},
            "response": {"result": "success", "data": None},
            "author": "runner",
        }
        reader = Mock()
        reader.read_command_result.return_value = record
        for identity in (expected, context):
            with self.subTest(identity=type(identity).__name__):
                self.assertIs(
                    read_result(
                        reader, attempt.request_id, expected=identity, accepted=True
                    ),
                    record,
                )
        context["stage_id"] = str(uuid4())
        with self.assertRaisesRegex(ValueError, "identity mismatch: stage_id"):
            read_result(reader, attempt.request_id, expected=expected)

    def test_recovery_keeps_non_object_legacy_executor_status_values(self):
        attempt, _ = self._add_path_participants()
        for value in (None, False, 1, "historical", [], ["legacy"]):
            with self.subTest(value=value):
                attempt.executor_status = value
                document = state_to_document(self.state)
                restored = state_from_document(self.root, document)
                self.assertEqual(restored.active_attempt.executor_status, value)
                self.assertEqual(state_to_document(restored), document)
                if not value:
                    self.assertEqual(
                        _finish_executor_status(value).model_dump(exclude_unset=True),
                        {"finished": True, "current": None, "pending": []},
                    )
                else:
                    with self.assertRaises(TypeError):
                        _finish_executor_status(value)

    def test_finishing_executor_status_preserves_metadata_and_original_observation(
        self,
    ):
        status = ExecutorCommandState.model_validate(
            {
                "current": {"request_id": str(uuid4()), "command": "execute"},
                "pending": [{"request_id": str(uuid4()), "command": "next"}],
                "process": process_identity(os.getpid()),
                "started_at": "started",
                "started_monotonic": 1,
                "finished": False,
                "exit_code": None,
                "progress": {"value": 0.5},
                "module_state": {"working": True},
                "extra": {"kept": True},
            }
        )
        original = status.model_dump(exclude_unset=True)
        finished = _finish_executor_status(status)
        self.assertIsInstance(finished, ExecutorCommandState)
        self.assertEqual(
            finished.model_dump(exclude_unset=True),
            {**original, "finished": True, "current": None, "pending": []},
        )
        self.assertEqual(status.model_dump(exclude_unset=True), original)

    @unittest.skipUnless(os.name == "nt", "Windows realpath namespace race")
    def test_disappearing_stage_and_service_endpoints_survive_state_round_trip(self):
        """Delete the real file between realpath's two native calls."""
        import ntpath

        participants = self._add_path_participants()
        native = ntpath._getfinalpathname
        for participant in participants:
            endpoint = participant.endpoint_path
            for operation in ("save", "load"):
                with self.subTest(participant=endpoint.name, operation=operation):
                    endpoint.write_text("{}", encoding="utf-8")
                    document = state_to_document(self.state)
                    removed = []

                    def resolve_and_remove(path, endpoint=endpoint, removed=removed):
                        result = native(path)
                        if not removed and Path(path) == endpoint:
                            endpoint.unlink()
                            removed.append(path)
                        return result

                    with patch.object(ntpath, "_getfinalpathname", resolve_and_remove):
                        if operation == "save":
                            saved = state_to_document(self.state)
                            restored = state_from_document(self.root, saved)
                        else:
                            restored = state_from_document(self.root, document)
                    self.assertEqual(removed, [str(endpoint)])
                    self.assertFalse(endpoint.exists())
                    self.assertEqual(state_to_document(restored), document)

    def test_saved_participant_paths_still_reject_absolute_and_parent_paths(self):
        """Both stage and service paths retain their experiment boundary."""
        self._add_path_participants()
        original = state_to_document(self.state)
        service_id = next(iter(self.state.services))
        for owner in ("stage", "service"):
            for path in (str(self.root / "endpoint.json"), "../outside.json"):
                with self.subTest(owner=owner, path=path):
                    document = copy.deepcopy(original)
                    participant = (
                        document["active_attempt"]
                        if owner == "stage"
                        else document["services"][service_id]
                    )
                    participant["endpoint_path"] = path
                    with self.assertRaisesRegex(ValueError, "escapes the experiment"):
                        state_from_document(self.root, document)

    def test_participant_endpoint_links_cannot_escape_the_experiment(self):
        participants = self._add_path_participants()
        document = state_to_document(self.state)
        outside = self.workspace.root / "outside.json"
        outside.write_text("{}", encoding="utf-8")
        for participant in participants:
            endpoint = participant.endpoint_path
            try:
                endpoint.symlink_to(outside)
            except OSError as error:
                self.skipTest(f"File symlinks unavailable: {error}")
            try:
                with self.subTest(participant=endpoint.name):
                    with self.assertRaises(ValueError):
                        state_to_document(self.state)
                    with self.assertRaisesRegex(ValueError, "escapes the experiment"):
                        state_from_document(self.root, document)
            finally:
                endpoint.unlink()

    @unittest.skipUnless(os.name == "nt", "Windows local and UNC namespace spelling")
    def test_windows_namespace_comparison_keeps_drive_and_share_boundaries(self):
        for root, extended in (
            (Path("C:/experiment"), Path("//?/C:/experiment")),
            (
                Path("//server/share/experiment"),
                Path("//?/UNC/server/share/experiment"),
            ),
        ):
            for parent in (root, extended):
                for child in (root / "endpoint.json", extended / "endpoint.json"):
                    with self.subTest(parent=parent, child=child):
                        self.assertEqual(
                            _relative_state_path(child, parent), Path("endpoint.json")
                        )
            with self.assertRaises(ValueError):
                _relative_state_path(extended.parent / "outside.json", root)
        with self.assertRaises(ValueError):
            _relative_state_path(
                Path("//?/UNC/server/other/experiment/endpoint.json"),
                Path("//server/share/experiment"),
            )

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
        self.state.active_attempt.participant = ParticipantIdentity(
            experiment_id=self.state.experiment_id,
            participant_id=stage_id,
            participant_instance_id=self.state.active_attempt.attempt_id,
        )
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
        self.state.owner_identity = ProcessIdentity.model_validate(
            process_identity(os.getpid())
        )
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
            patch("core.primitives.json_files.os.fsync", side_effect=sync),
            patch("core.primitives.json_files.os.replace", side_effect=replace),
        ):
            write_json(path, {"value": "new"})
        self.assertEqual(read_json(path), {"value": "new"})
        self.assertEqual(list(self.root.glob(".publish-*")), [])

    def test_json_readers_close_before_decoding_on_both_platforms(self):
        """Parsing a complete old document does not keep its file locked."""
        from dashboard.journals import read_object

        path = self.root / "document.json"
        for read in (read_json, read_object):
            with self.subTest(reader=read.__name__):
                write_json(path, {"value": "old"})
                original_loads = json.loads
                replaced = False

                def decode(*args, original_loads=original_loads, **kwargs):
                    nonlocal replaced
                    if not replaced:
                        replaced = True
                        write_json(path, {"value": "new"})
                    return original_loads(*args, **kwargs)

                with patch("core.primitives.json_files.json.loads", side_effect=decode):
                    self.assertEqual(read(path), {"value": "old"})
                self.assertTrue(replaced)
                self.assertEqual(read(path), {"value": "new"})
                self.assertEqual(list(self.root.glob(".publish-*")), [])

    @unittest.skipUnless(os.name == "nt", "Windows deny-delete file sharing")
    def test_windows_shared_reader_keeps_old_unicode_path_contents(self):
        from dashboard.journals import read_object

        path = self.root / "состояние-🌦.json"
        original_open = os.fdopen
        for read in (read_json, read_object):
            with self.subTest(reader=read.__name__):
                write_json(path, {"value": "old"})

                def opened(*args, **kwargs):
                    stream = original_open(*args, **kwargs)
                    try:
                        write_json(path, {"value": "new"})
                    except BaseException:
                        stream.close()
                        raise
                    return stream

                with patch("core.primitives.json_files.os.fdopen", side_effect=opened):
                    self.assertEqual(read(path), {"value": "old"})
                self.assertEqual(read(path), {"value": "new"})
                self.assertEqual(list(self.root.glob(".publish-*")), [])

    @unittest.skipUnless(os.name == "nt", "Windows deny-delete file sharing")
    def test_short_external_reader_lock_is_retried(self):
        path = self.root / "document.json"
        write_json(path, {"value": "old"})
        with path.open("rb") as reader:
            release = threading.Timer(0.04, reader.close)
            release.start()
            try:
                write_json(path, {"value": "new"})
            finally:
                release.cancel()
                release.join()
        self.assertEqual(read_json(path), {"value": "new"})
        self.assertEqual(list(self.root.glob(".publish-*")), [])

    @unittest.skipUnless(os.name == "nt", "Windows deny-delete file sharing")
    def test_persistent_reader_lock_is_bounded_and_preserves_old_json(self):
        path = self.root / "document.json"
        write_json(path, {"value": "old"})
        started = time.monotonic()
        with path.open("rb"), self.assertRaises(PermissionError) as raised:
            write_json(path, {"value": "new"})
        self.assertIn(raised.exception.winerror, (5, 32, 33))
        self.assertGreaterEqual(time.monotonic() - started, 0.9)
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(read_json(path), {"value": "old"})
        self.assertEqual(list(self.root.glob(".publish-*")), [])
        write_json(path, {"value": "after unlock"})
        self.assertEqual(read_json(path), {"value": "after unlock"})

    def test_unrelated_access_error_is_not_retried(self):
        path = self.root / "document.json"
        write_json(path, {"value": "old"})
        denied = PermissionError(errno.EACCES, "access denied", str(path))
        with (
            patch(
                "core.primitives.json_files.os.replace", side_effect=denied
            ) as replace,
            patch("core.primitives.json_files.time.sleep") as sleep,
            self.assertRaises(PermissionError) as raised,
        ):
            write_json(path, {"value": "new"})
        self.assertIs(raised.exception, denied)
        replace.assert_called_once()
        sleep.assert_not_called()
        self.assertEqual(read_json(path), {"value": "old"})
        self.assertEqual(list(self.root.glob(".publish-*")), [])

    def test_reader_permission_failure_is_bounded_and_not_hidden(self):
        path = self.root / "document.json"
        write_json(path, {"value": "old"})
        denied = PermissionError(errno.EACCES, "access denied", str(path))
        target = "os.fdopen" if os.name == "nt" else "pathlib.Path.open"
        started = time.monotonic()
        with (
            patch(target, side_effect=denied),
            self.assertRaises(PermissionError) as error,
        ):
            read_json(path)
        self.assertIs(error.exception, denied)
        self.assertLess(time.monotonic() - started, 5)
        write_json(path, {"value": "after denial"})
        self.assertEqual(read_json(path), {"value": "after denial"})

    @unittest.skipUnless(os.name != "nt", "POSIX open-file replacement")
    def test_posix_external_reader_does_not_prevent_replacement(self):
        path = self.root / "document.json"
        write_json(path, {"value": "old"})
        with path.open("rb") as reader:
            write_json(path, {"value": "new"})
            self.assertEqual(json.load(reader), {"value": "old"})
        self.assertEqual(read_json(path), {"value": "new"})

    def test_write_fsync_and_replace_failures_preserve_the_previous_json(self):
        """D2: all publication failure points leave the old document readable."""
        path = self.root / "document.json"
        write_json(path, {"value": "old"})
        for target in ("tempfile.NamedTemporaryFile", "os.fsync", "os.replace"):
            with (
                self.subTest(target=target),
                patch(
                    f"core.primitives.json_files.{target}",
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
            patch("core.primitives.json_files.os.replace", side_effect=primary),
            patch.object(Path, "unlink", side_effect=OSError("cleanup failure")),
            self.assertRaises(OSError) as raised,
        ):
            write_json(self.root / "document.json", {"value": 1})
        self.assertIs(raised.exception, primary)
        self.assertIn("cleanup failure", " ".join(raised.exception.__notes__))
