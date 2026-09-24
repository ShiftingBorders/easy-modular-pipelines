"""Approved service-control review regressions; reuse process and receipt fixtures."""

import asyncio
import copy
import unittest
from unittest.mock import patch
from uuid import uuid4

from core.serverruntime import ServerError
from tests import test_server_results
from tests.helpers.dag import process_running, terminate_owned
from tests.helpers.http_runtime import ServerTestCase
from tests.helpers.service_integration import ServiceDagWorkspace
from tests.helpers.services import ServiceWorkspace, wait_for


class ServiceControlReviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_stop_cancels_delayed_restart_and_allows_explicit_start(self):
        workspace = ServiceDagWorkspace()
        self.addAsyncCleanup(workspace.close)
        first, second = workspace.service(), workspace.service()
        delay = 123.456
        first["errors"]["retry_delay_seconds"] = delay
        runner = await workspace.launch(workspace.template())
        state = runner._state
        service_id = first["service_id"]
        original = state.services[service_id]
        unrelated = state.services[second["service_id"]]
        entered, release = asyncio.Event(), asyncio.Event()
        sleep = asyncio.sleep

        async def delayed(seconds, result=None):
            if seconds == delay:
                entered.set()
                await release.wait()
                return result
            return await sleep(seconds, result)

        with patch("core.runner_utils.services.asyncio.sleep", side_effect=delayed):
            terminate_owned(original.process_identity)
            try:
                await asyncio.wait_for(entered.wait(), 15)
                restarting = runner._services._restarts[service_id]
                self.assertTrue(original.stopped)
                self.assertFalse(restarting.done())
                stopped = await runner.stop_service(1)
                self.assertTrue(stopped["stopped"])
                self.assertTrue(stopped["manually_stopped"])
                self.assertTrue(restarting.cancelled())
                release.set()
                await sleep(0.1)
                self.assertIs(state.services[service_id], original)
                self.assertTrue(original.manually_stopped)
                self.assertEqual(
                    len(
                        [
                            row
                            for row in workspace.trace(first)
                            if row["event"] == "started"
                        ]
                    ),
                    1,
                )
                shutdowns = [
                    row
                    for row in workspace.trace(first)
                    if row.get("command") == "shutdown"
                ]
                repeated = await runner.stop_service(1)
                self.assertEqual(repeated, stopped)
                self.assertEqual(
                    [
                        row
                        for row in workspace.trace(first)
                        if row.get("command") == "shutdown"
                    ],
                    shutdowns,
                )
                self.assertIs(state.services[second["service_id"]], unrelated)
                self.assertTrue(process_running(unrelated.process_identity["pid"]))
                self.assertEqual(workspace.stage_trace(), [])
            finally:
                release.set()
        started = await runner.start_service(1)
        self.assertTrue(started["ready"])
        self.assertFalse(started["manually_stopped"])
        self.assertNotEqual(
            started["service_instance_id"], original.service_instance_id
        )
        self.assertIs(state.services[second["service_id"]], unrelated)
        self.assertEqual(
            len([row for row in workspace.trace(first) if row["event"] == "started"]), 2
        )
        self.assertEqual(workspace.stage_trace(), [])

    async def test_direct_restart_rechecks_manual_stop_after_delay_and_cancels_waiter(
        self,
    ):
        workspace = ServiceWorkspace()
        self.addAsyncCleanup(workspace.close)
        definition = workspace.service(policy="restart")
        delay = 123.456
        definition["errors"]["retry_delay_seconds"] = delay
        manager, state = workspace.manager, workspace.state
        service_id = definition["service_id"]
        await manager.start_all(state)
        instance = state.services[service_id]
        waiter = manager.enqueue(
            state,
            service_id,
            str(uuid4()),
            "echo",
            {"gate": str(workspace.root / "release-work")},
            owner="service",
        )
        await wait_for(lambda: instance.active_request is not None)
        manager._monitor_task.cancel()
        await asyncio.gather(manager._monitor_task, return_exceptions=True)
        instance.active_request["timed_out"] = True
        entered, release = asyncio.Event(), asyncio.Event()
        sleep = asyncio.sleep

        async def delayed(seconds, result=None):
            if seconds == delay:
                entered.set()
                await release.wait()
                return result
            return await sleep(seconds, result)

        with patch("core.runner_utils.services.asyncio.sleep", side_effect=delayed):
            restarting = asyncio.create_task(
                manager.restart(state, service_id, automatic=True)
            )
            try:
                await asyncio.wait_for(entered.wait(), 15)
                self.assertNotIn(service_id, manager._restarts)
                self.assertFalse(waiter.done())
                self.assertTrue(instance.stopped)
                instance.manually_stopped = True
                release.set()
                self.assertEqual(await asyncio.wait_for(restarting, 3), "pause")
                self.assertTrue(waiter.cancelled())
                self.assertIs(state.services[service_id], instance)
                self.assertTrue(instance.manually_stopped)
                self.assertEqual(
                    len(
                        [
                            row
                            for row in workspace.trace(definition)
                            if row["event"] == "started"
                        ]
                    ),
                    1,
                )
            finally:
                release.set()
                restarting.cancel()
                await asyncio.gather(restarting, return_exceptions=True)
                waiter.cancel()

    async def test_partial_startup_blocks_dag_until_all_services_are_explicitly_started(
        self,
    ):
        workspace = ServiceDagWorkspace()
        self.addAsyncCleanup(workspace.close)
        first, second = workspace.service(retries=0), workspace.service()
        failed_health = workspace.files.controls[first["service_id"]] / "fail-health"
        failed_health.touch()
        template = workspace.template([workspace.stage(), workspace.stage()])
        runner = await workspace.launch(template, paused=False)
        state = runner._state
        self.assertEqual(state.mode, "paused")
        self.assertEqual(state.services[first["service_id"]].blocked_action, "pause")
        self.assertNotIn(second["service_id"], state.services)
        failed_health.unlink()
        self.assertTrue((await runner.start_service(1))["ready"])
        self.assertNotIn(second["service_id"], state.services)
        for operation in (runner.resume, runner.step):
            with (
                self.subTest(operation=operation.__name__),
                self.assertRaisesRegex(RuntimeError, "all declared services"),
            ):
                await operation()
            self.assertEqual(state.mode, "paused")
            self.assertEqual(state.stage_position, 1)
            self.assertIsNone(state.active_attempt)
        self.assertEqual(
            await asyncio.wait_for(runner._services.wait_ready(state), 2), "pause"
        )
        self.assertEqual(workspace.trace(second), [])
        self.assertEqual(workspace.stage_trace(), [])
        self.assertTrue((await runner.start_service(2))["ready"])
        self.assertEqual(
            await asyncio.wait_for(runner._services.wait_ready(state), 2), "ready"
        )
        await runner.step()
        self.assertEqual(
            len([row for row in workspace.stage_trace() if row["event"] == "start"]), 1
        )
        await runner.resume()
        await wait_for(lambda: runner.get_state()["phase"] == "completed", timeout=30)
        self.assertEqual(
            len([row for row in workspace.stage_trace() if row["event"] == "start"]), 2
        )


class ReceiptModeReviewTests(unittest.IsolatedAsyncioTestCase):
    setUp = test_server_results.ResultCacheTests.setUp
    reply = test_server_results.ResultCacheTests.reply

    async def test_retained_commands_replay_after_mode_change_but_new_ids_do_not(self):
        cases = [
            (name, "run", "maintenance", {}, {"kind": "service", "position": 1})
            for name in ("service.start", "service.stop")
        ] + [
            (
                name,
                "maintenance",
                "run",
                {"folder": str(self.runtime.settings.project_root / "source")}
                if name == "module.add"
                else {"name": "module", "version": "1"},
                None,
            )
            for name in ("module.add", "module.validate", "module.remove")
        ]
        for name, old_mode, new_mode, args, target in cases:
            for outcome in ("succeeded", "failed"):
                with self.subTest(command=name, outcome=outcome):
                    self.setUp()
                    runtime = self.runtime
                    runtime.settings.server_mode = old_mode
                    document = {
                        "command": name,
                        "command_id": str(uuid4()),
                        "args": args,
                    }
                    if target is not None:
                        document["target"] = target
                    runtime.submit(document)
                    runtime._requests.get_nowait()
                    self.reply(document["command_id"], state=outcome)
                    retained = runtime.result(document["command_id"])
                    runtime.settings.server_mode = new_mode
                    self.assertEqual(runtime.submit(document), retained)
                    self.assertTrue(runtime._requests.empty())
                    changed = copy.deepcopy(document)
                    if target is not None:
                        changed["target"]["position"] = 2
                    elif name == "module.add":
                        changed["args"]["folder"] = str(
                            runtime.settings.project_root / "other"
                        )
                    else:
                        changed["args"]["version"] = "2"
                    with self.assertRaises(ServerError) as conflict:
                        runtime.submit(changed)
                    self.assertEqual(conflict.exception.code, "command_id_conflict")
                    with self.assertRaises(ServerError) as rejected:
                        runtime.submit({**document, "command_id": str(uuid4())})
                    self.assertEqual(rejected.exception.code, "invalid_mode")
                    runtime.settings.result_ttl = 0
                    with self.assertRaises(ServerError) as expired:
                        runtime.submit(document)
                    self.assertEqual(expired.exception.code, "invalid_mode")
                    self.assertEqual(runtime._records, {})
                    self.assertTrue(runtime._requests.empty())

    async def test_retained_chains_replay_and_new_invalid_chains_are_atomic(self):
        for old_mode, new_mode, old_command, allowed in (
            (
                "run",
                "maintenance",
                {
                    "command": "service.stop",
                    "target": {"kind": "service", "position": 1},
                },
                {
                    "command": "module.remove",
                    "args": {"name": "module", "version": "1"},
                },
            ),
            (
                "maintenance",
                "run",
                {
                    "command": "module.remove",
                    "args": {"name": "module", "version": "1"},
                },
                {
                    "command": "service.stop",
                    "target": {"kind": "service", "position": 1},
                },
            ),
        ):
            with self.subTest(mode=old_mode):
                self.setUp()
                runtime = self.runtime
                runtime.settings.server_mode = old_mode
                document = {
                    "chain_id": str(uuid4()),
                    "commands": [
                        {**old_command, "command_id": str(uuid4())} for _ in range(2)
                    ],
                }
                runtime.submit(document, chain=True)
                runtime._requests.get_nowait()
                for item in document["commands"]:
                    runtime._accept_response(
                        {
                            "command_id": item["command_id"],
                            "chain_id": document["chain_id"],
                            "state": "succeeded",
                            "result": "success",
                            "data": {},
                            "error": None,
                        }
                    )
                retained = runtime.submit(document, chain=True)
                runtime.settings.server_mode = new_mode
                self.assertEqual(runtime.submit(document, chain=True), retained)
                changed = copy.deepcopy(document)
                changed["commands"].reverse()
                with self.assertRaises(ServerError) as conflict:
                    runtime.submit(changed, chain=True)
                self.assertEqual(conflict.exception.code, "chain_id_conflict")
                previous = list(runtime._records)
                with self.assertRaises(ServerError) as invalid:
                    runtime.submit({"commands": [allowed, old_command]}, chain=True)
                self.assertEqual(invalid.exception.code, "invalid_mode")
                self.assertEqual(list(runtime._records), previous)
                self.assertTrue(runtime._requests.empty())


class ReceiptModeProcessReviewTests(ServerTestCase):
    async def test_http_receipts_survive_real_mode_switch_in_both_directions(self):
        server = await self.start_server()
        await server.launch(self.w.template(services=True))
        service = {
            "command": "service.stop",
            "command_id": str(uuid4()),
            "args": {},
            "target": {"kind": "service", "position": 1},
        }
        admitted = await server.client.post("/commands", json=service)
        self.assertEqual(admitted.status_code, 202)
        stopped = await server.result(service["command_id"])
        self.assertEqual(stopped["state"], "succeeded", stopped)
        switched = await server.command("server.mode", {"mode": "maintenance"})
        self.assertEqual(switched["state"], "succeeded", switched)
        server.observe_children()
        replay = await server.client.post("/commands", json=service)
        self.assertEqual(replay.status_code, 202)
        self.assertEqual(replay.json(), stopped)
        denied = await server.client.post(
            "/commands", json={**service, "command_id": str(uuid4())}
        )
        self.assertEqual(denied.status_code, 409)
        self.assertEqual(denied.json()["error"]["code"], "invalid_mode")
        module = {
            "command": "module.validate",
            "command_id": str(uuid4()),
            "args": {"name": "absent", "version": "1"},
        }
        self.assertEqual(
            (await server.client.post("/commands", json=module)).status_code, 202
        )
        failed = await server.result(module["command_id"])
        self.assertEqual(failed["error"]["code"], "not_found")
        switched = await server.command("server.mode", {"mode": "run"})
        self.assertEqual(switched["state"], "succeeded", switched)
        server.observe_children()
        replay = await server.client.post("/commands", json=module)
        self.assertEqual(replay.status_code, 202)
        self.assertEqual(replay.json(), failed)
        denied = await server.client.post(
            "/commands", json={**module, "command_id": str(uuid4())}
        )
        self.assertEqual(denied.status_code, 409)
        self.assertEqual(denied.json()["error"]["code"], "invalid_mode")
        self.assertIsNone((await server.get("/state"))["experiment_id"])
