"""Approved services.md A-D/G: actual services, queues and default timers."""

import asyncio
import os
import unittest
from pathlib import Path

from core.runner_utils.runtimeio import read_json
from tests.helpers.dag import process_running, terminate_owned
from tests.helpers.services import ServiceWorkspace, wait_for


class ServiceRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = ServiceWorkspace()
        self.addAsyncCleanup(self.workspace.close)

    async def test_socket_start_context_streams_and_real_shutdown(self):
        """A/B/D: exact identities, immutable code, actual context and confirmed shutdown."""
        async with asyncio.timeout(120):
            w = self.workspace
            definition = w.service()
            sid = definition["service_id"]
            self.assertEqual(await w.manager.start_all(w.state), "ready")
            instance = w.state.services[sid]
            self.assertNotEqual(instance.process_identity["pid"], os.getpid())
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
            reply = await w.manager.request(w.state, sid, "echo", {"value": 42})
            self.assertEqual(reply["data"], {"value": 42})
            result = await w.manager.stop_all(w.state)
            self.assertTrue(result[sid]["stopped"], result)
            self.assertFalse(process_running(instance.process_identity["pid"]))

    async def test_start_order_and_independent_work_queues(self):
        """A/B/C: ordered readiness and independent services while one request waits."""
        async with asyncio.timeout(120):
            w = self.workspace
            first, second = w.service(), w.service()
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
            before = w.state.services[first["service_id"]].last_status["request_id"]
            await wait_for(
                lambda: (
                    w.state.services[first["service_id"]].last_status["request_id"]
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
            await w.manager.start_all(w.state)
            instance = w.state.services[definition["service_id"]]
            result = await w.manager.request(w.state, instance.service_id, "fail", {})
            self.assertEqual(result["result"], "fail")
            self.assertIs(w.state.services[instance.service_id], instance)
            self.assertEqual(instance.restart_count, 0)
            self.assertTrue(process_running(instance.process_identity["pid"]))
            accepted = w.journal.client.read_command_result(result["request_id"])
            self.assertEqual(accepted["author"], "runner")
            self.assertEqual(accepted["outcome"], "failed")

    async def test_action_socket_and_commands_only_lifecycles(self):
        """A/D: action stop is called; commands-only readiness never waits for command exit."""
        async with asyncio.timeout(120):
            w = self.workspace
            socket = w.service(implementation="action")
            commands = w.service(interface="commands", implementation="action")
            control = w.controls[commands["service_id"]]
            (control / "hold-start").touch()
            self.assertEqual(await w.manager.start_all(w.state), "ready")
            await wait_for(lambda: (control / "action-start.json").exists())
            command_pid = read_json(control / "action-start.json")["pid"]
            self.assertTrue(process_running(command_pid))
            with self.assertRaises(ValueError):
                await w.manager.restart(w.state, commands["service_id"], automatic=True)
            results = await w.manager.stop_all(w.state)
            self.assertTrue(results[socket["service_id"]]["stopped"], results)
            self.assertTrue(results[commands["service_id"]]["stopped"], results)
            await wait_for(lambda: (control / "action-stop.json").exists())
            self.assertTrue(process_running(command_pid))
            (control / "hold-start").unlink()

    async def test_real_crash_restarts_with_new_instance_and_preserves_pending_requests(
        self,
    ):
        """C/D: sent work fails once; unsent work survives replacement with unique IDs."""
        async with asyncio.timeout(120):
            w = self.workspace
            definition = w.service()
            sid = definition["service_id"]
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
            self.assertFalse(process_running(old.process_identity["pid"]))
            self.assertFalse(
                any(row["event"] == "duplicate" for row in w.trace(definition))
            )
