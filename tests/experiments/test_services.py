"""Approved services.md A-D/G: actual services, queues and default timers."""

import asyncio
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from core.experiments.state import _process_identity_pid
from core.models.participant_protocol import ParticipantResult
from core.primitives.json_files import read_json
from tests.helpers.dag import process_running, terminate_owned
from tests.helpers.services import ServiceWorkspace, wait_for


class ServiceRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = ServiceWorkspace()
        self.addAsyncCleanup(self.workspace.close)

    async def test_foreign_cwd_lifecycle_preserves_context_and_typed_shutdown(self):
        """A/B/D/G: caller paths, immutable code, context and typed, confirmed shutdown."""
        async with asyncio.timeout(120):
            w = self.workspace
            definition = w.service()
            sid = definition["service_id"]
            previous = Path.cwd()
            try:
                os.chdir(w.root)
                w.publish_service_definitions()
                self.assertEqual(await w.manager.start_all(w.state), "ready")
            finally:
                os.chdir(previous)
            instance = w.state.services[sid]
            self.assertNotEqual(
                _process_identity_pid(instance.process_identity), os.getpid()
            )
            context = read_json(instance.artifacts_directory / "received-context.json")
            self.assertEqual(context["settings"]["nested"], {"keep": 1, "replace": [2]})
            self.assertIsNone(context["settings"]["nullable"])
            self.assertIsNone(context["input_data"])
            self.assertEqual(
                context["context"]["service_instance_id"], instance.service_instance_id
            )
            self.assertTrue(Path(context["logging_config_path"]).is_file())
            self.assertTrue(instance.endpoint_path.is_file())
            w.assembler.check_module(w.state, definition)
            self.assertEqual(w.state.mode, "paused")
            self.assertEqual(w.state.stage_position, 1)
            self.assertIsNone(w.state.last_result)
            reply = await w.manager.request(w.state, sid, "echo", {"value": 42})
            self.assertEqual(reply["data"], {"value": 42})
            token = (instance.artifacts_directory / "service.token").read_text(
                encoding="utf-8"
            )
            self.assertNotIn(token, json.dumps(w.events()))
            with patch.object(
                w.manager,
                "_record_service_stop_outcome",
                wraps=w.manager._record_service_stop_outcome,
            ) as record:
                result = await w.manager.stop_all(w.state)
            state, service_id, request_id, response, _, _ = record.call_args.args
            self.assertIs(state, w.state)
            self.assertIsInstance(response, ParticipantResult)
            self.assertTrue(result[sid]["stopped"], result)
            self.assertEqual(service_id, sid)
            stored = w.journal.client.read_command_result(request_id)
            self.assertEqual(
                stored["response"], response.model_dump(exclude_unset=True)
            )
            self.assertEqual(set(stored["response"]), {"result", "data"})
            self.assertEqual(stored["outcome"], "succeeded")
            self.assertFalse(
                process_running(_process_identity_pid(instance.process_identity))
            )

    async def test_start_order_and_independent_work_queues(self):
        """A/B/C: ordered readiness and independent services while one request waits."""
        async with asyncio.timeout(120):
            w = self.workspace
            first, second = w.service(), w.service()
            w.publish_service_definitions()
            self.assertEqual(await w.manager.start_all(w.state), "ready")
            self.assertLess(w.trace(first)[0]["at"], w.trace(second)[0]["at"])
            gate = w.root / "release-work"
            requests = [
                asyncio.create_task(
                    w.manager.request(w.state, first["service_id"], "echo", args)
                )
                for args in ({"gate": str(gate), "n": 1}, {"n": 2})
            ]
            await wait_for(
                lambda: any(row["event"] == "work_started" for row in w.trace(first))
            )
            reply = await w.manager.request(
                w.state, second["service_id"], "echo", {"other": True}
            )
            self.assertEqual(reply["result"], "success")
            before = w.state.services[first["service_id"]].last_status.request_id
            await wait_for(
                lambda: (
                    w.state.services[first["service_id"]].last_status.request_id
                    != before
                )
            )
            self.assertFalse(any(task.done() for task in requests))
            self.assertEqual(
                len([row for row in w.trace(first) if row["event"] == "work_started"]),
                1,
            )
            gate.touch()
            replies = await asyncio.gather(*requests)
            self.assertEqual([item["data"]["n"] for item in replies], [1, 2])
            work = [
                row["event"]
                for row in w.trace(first)
                if row["event"].startswith("work_")
            ]
            self.assertEqual(
                work, ["work_started", "work_finished", "work_started", "work_finished"]
            )
            self.assertTrue(
                all(
                    item["stopped"]
                    for item in (await w.manager.stop_all(w.state)).values()
                )
            )

    async def test_work_failure_does_not_restart_service(self):
        """C: a command failure leaves the same healthy service instance alive."""
        async with asyncio.timeout(120):
            w = self.workspace
            definition = w.service()
            w.publish_service_definitions()
            await w.manager.start_all(w.state)
            instance = w.state.services[definition["service_id"]]
            result = await w.manager.request(w.state, instance.service_id, "fail", {})
            self.assertEqual(result["result"], "fail")
            self.assertIs(w.state.services[instance.service_id], instance)
            self.assertEqual(instance.restart_count, 0)
            self.assertTrue(
                process_running(_process_identity_pid(instance.process_identity))
            )
            accepted = w.journal.client.read_command_result(result["request_id"])
            self.assertEqual(accepted["author"], "runner")
            self.assertEqual(accepted["outcome"], "failed")

    async def test_action_services_wait_for_full_readiness_and_confirm_shutdown(self):
        """D10/D11: command preparation precedes readiness; shutdown confirms exit."""
        async with asyncio.timeout(120):
            w = self.workspace
            definition = w.service(implementation="action")
            control = w.controls[definition["service_id"]]
            (control / "hold-start").touch()
            w.publish_service_definitions()
            starting = asyncio.create_task(w.manager.start_all(w.state))
            await wait_for(lambda: (control / "action-start.json").exists())
            command_pid = read_json(control / "action-start.json")["pid"]
            self.assertTrue(process_running(command_pid))
            self.assertFalse(starting.done())
            (control / "hold-start").unlink()
            self.assertEqual(await starting, "ready")
            results = await w.manager.stop_all(w.state)
            self.assertTrue(results[definition["service_id"]]["stopped"], results)
            await wait_for(lambda: (control / "action-stop.json").exists())
            self.assertFalse(process_running(command_pid))

    async def test_real_crash_restarts_with_new_instance_and_preserves_pending_requests(
        self,
    ):
        """C/D: sent work fails once; unsent work survives replacement with unique IDs."""
        async with asyncio.timeout(120):
            w = self.workspace
            definition = w.service()
            sid = definition["service_id"]
            w.publish_service_definitions()
            await w.manager.start_all(w.state)
            old = w.state.services[sid]
            first = asyncio.create_task(
                w.manager.request(w.state, sid, "echo", {"gate": str(w.root / "never")})
            )
            second = asyncio.create_task(
                w.manager.request(w.state, sid, "echo", {"n": 2})
            )
            await wait_for(
                lambda: any(
                    row["event"] == "work_started" for row in w.trace(definition)
                )
            )
            terminate_owned(old.process_identity)
            self.assertEqual((await first)["result"], "fail")
            self.assertEqual((await second)["data"], {"n": 2})
            self.assertNotEqual(
                w.state.services[sid].service_instance_id, old.service_instance_id
            )
            self.assertEqual(w.state.services[sid].restart_count, 1)
            self.assertFalse(
                process_running(_process_identity_pid(old.process_identity))
            )
            self.assertFalse(
                any(row["event"] == "duplicate" for row in w.trace(definition))
            )
