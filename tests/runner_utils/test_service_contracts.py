"""Approved services.md A/E/G: schemas, launch contract and persistent state."""

import asyncio
import copy
import json
import os
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import yaml

from core.logger_utils.events import LoggingError
from core.runner_utils.connection import ParticipantConnection
from core.runner_utils.state import state_from_document, state_to_document
from tests.helpers.dag import process_running
from tests.helpers.services import ServiceWorkspace, wait_for


class ServiceContractTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ServiceWorkspace()
        self.addAsyncCleanup(self.w.close)

    async def test_module_variants_and_invalid_schemas(self):
        """A: module metadata distinguishes full/socket, action/socket and action/commands."""
        for implementation, interface in (
            ("full", "socket"),
            ("action", "socket"),
            ("action", "commands"),
        ):
            definition = self.w.service(
                implementation=implementation, interface=interface
            )
            path = self.w.experiment / "modules" / definition["module"]["name"] / "1"
            valid = self.w.assembler.read_module(path)
            self.assertEqual(valid["implementation"], implementation)
            self.assertNotIn("service_interface", valid)
            for update in (
                {"service_interface": "unknown"},
                {"schema_version": True},
                {"commands": {}},
                {"extra": 1},
            ):
                with self.subTest(implementation=implementation, update=update):
                    (path / "module.yaml").write_text(
                        yaml.safe_dump({**valid, **update}), encoding="utf-8"
                    )
                    with self.assertRaises((TypeError, ValueError)):
                        self.w.assembler.read_module(path)
            (path / "module.yaml").write_text(yaml.safe_dump(valid), encoding="utf-8")

    async def test_definition_validation_precedes_runtime_file_creation(self):
        """A: explicit policies reject malformed values before launch side effects."""
        w = self.w
        valid = w.service()
        candidates = [
            {key: value for key, value in valid.items() if key != missing}
            for missing in valid
        ]
        for field, values in {
            "heartbeat": [
                {},
                {"interval_seconds": False, "grace_seconds": 10},
                {"interval_seconds": 1, "grace_seconds": 0},
            ],
            "command_timeout_seconds": [0, -1, True, "30"],
            "on_command_timeout": [None, "ignore"],
            "state_required": [0, None],
            "errors": [
                {},
                {"retries": True, "retry_delay_seconds": 1, "on_exhausted": "pause"},
                {"retries": 3, "retry_delay_seconds": -1, "on_exhausted": "pause"},
            ],
        }.items():
            candidates.extend({**valid, field: value} for value in values)
        candidates.append({**valid, "extra": 1})
        for index, definition in enumerate(candidates):
            directory = w.experiment / "shared_artifacts" / f"invalid-{index}"
            with (
                self.subTest(index=index),
                self.assertRaises((TypeError, ValueError, KeyError)),
            ):
                w.launcher.prepare(
                    w.state,
                    definition,
                    {"service_instance_id": str(uuid4())},
                    directory,
                    None,
                )
            self.assertFalse(directory.exists())
        self.assertEqual(w.manager._processes, {})

    async def test_launch_from_another_cwd_preserves_code_and_context(self):
        """A/G: the caller's cwd does not select module code, paths, or logger configuration."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            previous = Path.cwd()
            try:
                os.chdir(w.root)
                self.assertEqual(await w.manager.start_all(w.state), "ready")
            finally:
                os.chdir(previous)
            instance = w.state.services[definition["service_id"]]
            token = (instance.artifacts_directory / "service.token").read_text()
            self.assertNotIn(token, json.dumps(w.events()))
            w.assembler.check_module(w.state, definition)
            self.assertEqual(w.state.stage_position, 1)
            self.assertIsNone(w.state.last_result)

    async def test_integrity_failure_and_missing_registry_record_prevent_spawn(self):
        """A: actual code/HashDB mismatches prohibit service processes."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            path = w.experiment / "modules" / definition["module"]["name"] / "1/main.py"
            source = path.read_bytes()
            path.write_bytes(source + b"\n# changed\n")
            with self.assertRaises(ValueError):
                await w.manager.start_all(w.state)
            self.assertEqual(w.trace(definition), [])
            path.write_bytes(source)
            w.hashes.remove_module_hash(definition["module"]["name"], "1")
            with self.assertRaises(ValueError):
                await w.replacement_manager().start_all(w.state)
            self.assertEqual(w.trace(definition), [])

    async def test_locked_mandatory_journal_prevents_spawn(self):
        """A/D: a real conflicting SQLite transaction blocks mandatory intent, not just its acknowledgement."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            locked = sqlite3.connect(w.experiment / "journals/events.sqlite")
            locked.execute("BEGIN IMMEDIATE")
            try:
                with self.assertRaises(LoggingError):
                    await w.manager.start_all(w.state)
            finally:
                locked.rollback()
                locked.close()
            self.assertEqual(w.trace(definition), [])
            self.assertEqual(w.manager._processes, {})
            self.assertTrue(
                all(instance.stopped for instance in w.state.services.values())
            )

    async def test_partial_startup_failure_stops_previously_started_service(self):
        """A: a later invalid module cannot leave an earlier owned service running."""
        async with asyncio.timeout(120):
            w = self.w
            first, invalid = w.service(), w.service()
            w.hashes.remove_module_hash(invalid["module"]["name"], "1")
            with self.assertRaises(ValueError):
                await w.manager.start_all(w.state)
            instance = w.state.services[first["service_id"]]
            self.assertTrue(instance.stopped)
            self.assertFalse(process_running(instance.process_identity["pid"]))
            self.assertEqual(w.trace(invalid), [])

    async def test_logger_failure_during_stop_still_attempts_all_owned_services(self):
        """D: real SQLite contention cannot prevent shutdown and emergency diagnostics."""
        async with asyncio.timeout(120):
            w = self.w
            w.service()
            w.service()
            await w.manager.start_all(w.state)
            locked = sqlite3.connect(w.experiment / "journals/events.sqlite")
            locked.execute("BEGIN IMMEDIATE")
            try:
                results = await w.manager.stop_all(w.state)
            finally:
                locked.rollback()
                locked.close()
            for sid, result in results.items():
                self.assertTrue(result["stopped"], result)
                self.assertTrue(result["error"])
                self.assertTrue(
                    (
                        w.experiment / "runner" / f"service-stop-{sid}.emergency.json"
                    ).exists()
                )
                self.assertFalse(
                    process_running(w.state.services[sid].process_identity["pid"])
                )

    async def test_state_write_failure_is_reported_without_rejecting_service_work(self):
        """E: isolated publication failure is reported while mandatory journal writes remain real."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            with patch.object(
                w.store, "save", side_effect=OSError("test state storage unavailable")
            ):
                self.assertEqual(await w.manager.start_all(w.state), "ready")
                reply = await w.manager.request(
                    w.state, definition["service_id"], "echo", {"value": 1}
                )
                self.assertEqual(reply["result"], "success")
            self.assertTrue(w.events("error.recorded"))

    async def test_real_handshake_rejects_token_identity_and_old_process_metadata(self):
        """B: authentic endpoint succeeds; altered token/identity/creation value cannot authenticate."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            await w.manager.start_all(w.state)
            instance = w.state.services[definition["service_id"]]
            endpoint = json.loads(instance.endpoint_path.read_text())
            identity = {
                key: endpoint[key]
                for key in (
                    "experiment_id",
                    "participant_id",
                    "participant_instance_id",
                )
            }
            for update in (
                {"participant_instance_id": str(uuid4())},
                {"experiment_id": str(uuid4())},
            ):
                connection = ParticipantConnection(
                    instance.endpoint_path,
                    {**identity, **update},
                )
                with self.assertRaises(ValueError):
                    await connection.connect(timeout_seconds=30)
                await connection.close()
            from core.runner_utils.runtimeio import write_json

            altered = copy.deepcopy(endpoint)
            altered["process"]["created_at_os"] += 1
            bad = w.root / "bad-endpoint.json"
            write_json(bad, altered)
            connection = ParticipantConnection(bad, identity)
            with self.assertRaises(ValueError):
                await connection.connect(timeout_seconds=30)
            await connection.close()
            altered = copy.deepcopy(endpoint)
            token = w.root / "bad.token"
            token.write_text("incorrect")
            altered["endpoint"]["token_file"] = str(token)
            write_json(bad, altered)
            connection = ParticipantConnection(bad, identity)
            with self.assertRaises((ValueError, EOFError, OSError)):
                await connection.connect(timeout_seconds=30)
            await connection.close()
            self.assertTrue(process_running(instance.process_identity["pid"]))

    async def test_live_queue_roundtrip_and_invalid_saved_requests(self):
        """E: active and pending real requests roundtrip without tasks, paths escaping, or replay IDs."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            active = asyncio.create_task(
                w.manager.request(w.state, sid, "echo", {"gate": str(w.root / "gate")})
            )
            pending = asyncio.create_task(w.manager.request(w.state, sid, "echo", {}))
            await wait_for(
                lambda: (
                    w.state.services[sid].active_request is not None
                    and w.state.services[sid].pending_requests
                )
            )
            w.store.save(w.state)
            restored = w.store.load(w.experiment)
            self.assertEqual(
                restored.services[sid].active_request,
                w.state.services[sid].active_request,
            )
            self.assertEqual(
                restored.services[sid].pending_requests,
                w.state.services[sid].pending_requests,
            )
            document = state_to_document(w.state)
            for field, value in (
                ("endpoint_path", "../outside.json"),
                ("artifacts_directory", str(w.root)),
                ("restart_count", True),
                ("ready", 1),
            ):
                changed = copy.deepcopy(document)
                changed["services"][sid][field] = value
                with (
                    self.subTest(field=field),
                    self.assertRaises((ValueError, TypeError)),
                ):
                    state_from_document(w.experiment, changed)
            changed = copy.deepcopy(document)
            saved = changed["services"][sid]
            saved["pending_requests"][0]["request_id"] = saved["active_request"][
                "request_id"
            ]
            with self.assertRaises(ValueError):
                state_from_document(w.experiment, changed)
            for field, value in (
                ("service_instance_id", str(uuid4())),
                ("sent_monotonic", None),
            ):
                changed = copy.deepcopy(document)
                changed["services"][sid]["active_request"][field] = value
                with self.subTest(active_field=field), self.assertRaises(ValueError):
                    state_from_document(w.experiment, changed)
            changed = copy.deepcopy(document)
            changed["services"][sid]["pending_requests"][0]["sent_monotonic"] = 1
            with self.assertRaises(ValueError):
                state_from_document(w.experiment, changed)
            changed = copy.deepcopy(document)
            other = str(uuid4())
            changed["services"][other] = copy.deepcopy(changed["services"][sid])
            changed["services"][other]["service_id"] = other
            changed["services"][other]["definition"]["service_id"] = other
            with self.assertRaises(ValueError):
                state_from_document(w.experiment, changed)
            (w.root / "gate").touch()
            self.assertTrue(
                all(
                    reply["result"] == "success"
                    for reply in await asyncio.gather(active, pending)
                )
            )
