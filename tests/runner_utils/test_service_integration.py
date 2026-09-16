"""Approved service_integration.md B-E: real DAG, services, queues and telemetry."""

import asyncio
import sqlite3
import time
import unittest
from unittest.mock import patch

from core.runner_utils.runtimeio import process_identity, write_json
from tests.helpers.dag import DagSession, process_running, terminate_owned
from tests.helpers.service_integration import ServiceDagWorkspace
from tests.helpers.services import wait_for


class ServiceIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ServiceDagWorkspace()
        self.addAsyncCleanup(self.w.close)

    async def test_unconfirmed_stop_prevents_a_second_experiment(self):
        """D3/D4: denied termination of an owned, unresponsive peer cannot permit another DAG."""
        async with asyncio.timeout(180):
            w = self.w
            definition = w.service()
            runner = await w.launch(w.template())
            sid = definition["service_id"]
            original = runner._state.services[sid]
            ignore = w.files.controls[sid] / "ignore-shutdown"
            ignore.touch()
            process = runner._services._processes[sid]
            with patch.object(
                process,
                "kill",
                side_effect=PermissionError("isolated owned kill refusal"),
            ):
                began = time.monotonic()
                response = await w.session.post("stop")
                self.assertGreaterEqual(time.monotonic() - began, 30)
                self.assertEqual(response["result"], "fail", response)
                self.assertFalse(runner.get_state()["termination_confirmed"])
                self.assertTrue(process_running(original.process_identity["pid"]))
                denied = await w.session.send(
                    "run", {"template_path": str(w.write_template(w.template()))}
                )
                self.assertEqual(denied["result"], "fail", denied)
                self.assertEqual(len(list((w.root / "experiments").iterdir())), 1)
            ignore.unlink()
            self.assertEqual((await w.session.post("stop"))["result"], "success")
            self.assertFalse(process_running(original.process_identity["pid"]))

    async def test_stage_error_policies_keep_or_stop_services_with_the_experiment(self):
        """C3/D4: a stage pause preserves services; fatal and skipped final runs shut them down."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            for action in ("pause", "stop", "skip"):
                with self.subTest(action=action):
                    stages = [
                        w.stage(settings={"mode": "fail"}, on_exhausted=action),
                        w.stage(),
                    ]
                    runner = await w.launch(w.template(stages), paused=False)
                    phase = {"pause": "waiting", "stop": "failed", "skip": "completed"}[
                        action
                    ]
                    await wait_for(
                        lambda runner=runner, phase=phase: (
                            runner.get_state()["phase"] == phase
                        )
                    )
                    instance = runner._state.services[definition["service_id"]]
                    self.assertEqual(
                        process_running(instance.process_identity["pid"]),
                        action == "pause",
                    )
                    self.assertEqual(instance.stopped, action != "pause")
                    await w.session.post("stop")

    async def test_collector_crash_does_not_restart_services_or_stage(self):
        """E3: the actual optional measurement process restarts independently of experiment work."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            gate = w.gate()
            runner = await w.launch(
                w.template([w.stage(settings={"gate": str(gate)})]), paused=False
            )
            _, stage = await w.session.ready_attempt()
            service = runner._state.services[definition["service_id"]]
            collector = w.session.controller.resources
            await wait_for(lambda: collector.get_status()["collector_id"])
            original = collector.get_status()
            await wait_for(
                lambda: {
                    service.service_instance_id,
                    runner._state.active_attempt.attempt_id,
                }.issubset(
                    {item["series_id"] for item in collector.get_status()["latest"]}
                ),
                timeout=20,
            )
            terminate_owned(process_identity(original["pid"]))
            await wait_for(
                lambda: (
                    collector.get_status()["collector_id"]
                    not in (None, original["collector_id"])
                ),
                timeout=20,
            )
            self.assertIs(runner._state.services[definition["service_id"]], service)
            self.assertTrue(process_running(stage["pid"]))
            self.assertTrue(service.ready)
            gate.touch()
            await wait_for(lambda: runner.get_state()["phase"] == "completed")

    async def test_ordered_services_pause_cycles_and_confirmed_completion(self):
        """B1/C5/D3: all interfaces live across stages/cycles; the final step confirms shutdown."""
        async with asyncio.timeout(120):
            w = self.w
            first = w.service()
            second = w.service(implementation="action")
            commands = w.service(implementation="action", interface="commands")
            stages = [
                w.stage(settings={"label": "A"}),
                w.stage(settings={"label": "B"}),
            ]
            runner = await w.launch(w.template(stages, cycles=2))
            self.assertEqual(runner.get_state()["mode"], "paused")
            self.assertEqual(w.stage_trace(), [])
            self.assertLess(w.trace(first)[0]["at"], w.trace(second)[0]["at"])
            await wait_for(
                lambda: (
                    w.files.controls[commands["service_id"]] / "action-start.json"
                ).exists()
            )
            sid = first["service_id"]
            old = runner._state.services[sid]
            response = await w.session.send("step")
            self.assertEqual(response["result"], "success", response)
            terminate_owned(old.process_identity)
            await wait_for(
                lambda: (
                    runner._state.services[sid] is not old
                    and runner._state.services[sid].ready
                )
            )
            replacement = runner._state.services[sid]
            self.assertEqual(replacement.restart_count, 1)
            self.assertEqual(
                (await w.session.send("step"))["data"]["result"]["data"]["trail"],
                ["A", "B"],
            )
            self.assertEqual((await w.session.send("step"))["result"], "success")
            self.assertIs(runner._state.services[sid], replacement)
            self.assertEqual(replacement.restart_count, 0)
            last = await w.session.send("step")
            self.assertEqual(last["data"]["phase"], "completed", last)
            self.assertTrue(
                all(item["stopped"] for item in runner.get_state()["services"])
            )
            for item in runner._state.services.values():
                if item.interface == "socket":
                    self.assertFalse(process_running(item.process_identity["pid"]))
            await wait_for(
                lambda: (
                    w.files.controls[commands["service_id"]] / "action-stop.json"
                ).exists()
            )

    async def test_startup_blocks_stage_and_tail_but_not_reads_or_priority_stop(self):
        """B1/B4/D4: an actual service awaiting readiness leaves command intake responsive."""
        async with asyncio.timeout(120):
            w = self.w
            first, second = w.service(), w.service()
            (w.files.controls[first["service_id"]] / "hold-start").touch()
            w.session = DagSession(w)
            await w.session.start()
            ack = await w.session.send(
                "run",
                {
                    "template_path": str(w.write_template(w.template())),
                    "delayed_start": False,
                },
            )
            self.assertEqual(ack["result"], "success")
            await wait_for(lambda: w.trace(first))
            reply = await w.session.send("stats.state")
            self.assertEqual(reply["data"]["phase"], "starting")
            self.assertEqual(w.stage_trace(), [])
            self.assertEqual(w.trace(second), [])
            self.assertIsNone(reply["data"]["services"][1]["service_instance_id"])
            stopped = await w.session.post("stop")
            self.assertEqual(stopped["result"], "success", stopped)
            self.assertTrue(
                w.session.runner._state.services[first["service_id"]].stopped
            )

    async def test_startup_timeout_pause_and_retry_start_the_remaining_services(self):
        """B2/B3: the real 30-second deadline pauses startup; manual recovery starts its tail."""
        async with asyncio.timeout(120):
            w = self.w
            first, second = w.service(retries=0), w.service()
            hold = w.files.controls[first["service_id"]] / "hold-start"
            hold.touch()
            began = time.monotonic()
            runner = await w.launch(w.template(), paused=False)
            self.assertGreaterEqual(time.monotonic() - began, 30)
            self.assertEqual(runner.get_state()["mode"], "paused")
            self.assertEqual(w.stage_trace(), [])
            self.assertEqual(w.trace(second), [])
            hold.unlink()
            retry = await w.session.send(
                "retry", target={"kind": "service", "position": 1}
            )
            self.assertEqual(retry["result"], "success", retry)
            self.assertEqual(retry["data"]["action"], "ready")
            self.assertEqual(len(runner._state.services), 2)
            self.assertTrue(all(item.ready for item in runner._state.services.values()))
            self.assertEqual(w.stage_trace(), [])
            self.assertEqual(runner.get_state()["mode"], "paused")
            await w.session.send("resume")
            await wait_for(lambda: runner.get_state()["phase"] == "completed")

    async def test_service_crash_does_not_replace_the_running_stage(self):
        """C1/E1: observation and independent recovery continue during a long stage."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            gate = w.gate()
            runner = await w.launch(
                w.template([w.stage(settings={"gate": str(gate)})]), paused=False
            )
            _, started = await w.session.ready_attempt()
            old = runner._state.services[definition["service_id"]]
            terminate_owned(old.process_identity)
            await wait_for(
                lambda: (
                    runner._state.services[definition["service_id"]] is not old
                    and runner._state.services[definition["service_id"]].ready
                )
            )
            observed = (await w.session.send("stats.state"))["data"]
            self.assertEqual(observed["phase"], "stage_running")
            self.assertTrue(process_running(started["pid"]))
            self.assertEqual(
                len([row for row in w.stage_trace() if row["event"] == "start"]), 1
            )
            gate.touch()
            await wait_for(lambda: runner.get_state()["phase"] == "completed")

    async def test_service_pause_releases_step_and_allows_retry_before_stage_finishes(
        self,
    ):
        """C3/D1: a paused service can recover without destroying the actual in-flight stage."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service(retries=0)
            gate = w.gate()
            runner = await w.launch(
                w.template(
                    [
                        w.stage(settings={"gate": str(gate), "label": "first"}),
                        w.stage(settings={"label": "second"}),
                    ]
                )
            )
            step = w.session.post("step")
            _, stage = await w.session.ready_attempt()
            original = runner._state.services[definition["service_id"]]
            terminate_owned(original.process_identity)
            interrupted = await step
            self.assertEqual(interrupted["result"], "fail", interrupted)
            self.assertTrue(process_running(stage["pid"]))
            self.assertEqual(runner.get_state()["mode"], "paused")
            reset = await w.session.send(
                "reset_retries", target={"kind": "service", "position": 1}
            )
            self.assertEqual(reset["result"], "success", reset)
            self.assertEqual(original.blocked_action, "pause")
            self.assertIs(runner._state.services[definition["service_id"]], original)
            retry = await w.session.send(
                "retry", target={"kind": "service", "position": 1}
            )
            self.assertEqual(retry["result"], "success", retry)
            self.assertTrue(process_running(stage["pid"]))
            gate.touch()
            await wait_for(
                lambda: (
                    runner.get_state()["phase"] == "waiting"
                    and runner._state.active_attempt is None
                )
            )
            self.assertEqual(runner.get_state()["result"]["trail"], ["first"])
            self.assertEqual(
                len([row for row in w.stage_trace() if row["event"] == "start"]), 1
            )
            await w.session.send("resume")
            await wait_for(lambda: runner.get_state()["phase"] == "completed")
            self.assertEqual(runner.get_state()["result"]["trail"], ["first", "second"])

    async def test_stage_retry_waits_for_service_without_spending_attempts(self):
        """C2: recovery readiness also gates automatic retries inside StageRunner."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            gate = w.gate()
            stage = w.stage(settings={"gate": str(gate), "failures": 1}, retries=1)
            runner = await w.launch(w.template([stage]), paused=False)
            await w.session.ready_attempt()
            old = runner._state.services[sid]
            hold = w.files.controls[sid] / "hold-start"
            hold.touch()
            terminate_owned(old.process_identity)
            await wait_for(lambda: runner._state.services[sid] is not old)
            gate.touch()
            await wait_for(
                lambda: any(row["event"] == "finish" for row in w.stage_trace())
            )
            await asyncio.sleep(1.2)
            self.assertEqual(
                len([row for row in w.stage_trace() if row["event"] == "start"]), 1
            )
            self.assertEqual(
                runner._state.stage_retry_counts.get(stage["stage_id"], 0), 0
            )
            hold.unlink()
            await wait_for(lambda: runner.get_state()["phase"] == "completed")
            self.assertEqual(runner._state.stage_retry_counts[stage["stage_id"]], 1)

    async def test_hung_service_stop_policy_interrupts_stage_and_stops_other_services(
        self,
    ):
        """C1/C4/D4: a genuine hung event loop uses normal deadlines and cannot block reads."""
        async with asyncio.timeout(180):
            w = self.w
            definition, healthy = w.service(retries=0), w.service()
            definition["errors"]["on_exhausted"] = "stop"
            gate = w.gate()
            runner = await w.launch(
                w.template([w.stage(settings={"gate": str(gate)})]), paused=False
            )
            _, stage = await w.session.ready_attempt()
            original = runner._state.services[definition["service_id"]]
            write_json(
                w.files.controls[definition["service_id"]] / "fault.json",
                {"phase": "idle", "action": "hang"},
            )
            await wait_for(
                lambda: (
                    w.files.controls[definition["service_id"]] / "fault-entered.json"
                ).exists()
            )
            began = time.monotonic()
            other = runner._state.services[healthy["service_id"]]
            probe = other.last_status["request_id"]
            self.assertEqual((await w.session.send("stats.state"))["result"], "success")
            await wait_for(lambda: other.last_status["request_id"] != probe)
            await wait_for(lambda: runner.get_state()["phase"] == "failed", timeout=90)
            self.assertGreaterEqual(time.monotonic() - began, 10)
            self.assertFalse(process_running(stage["pid"]))
            self.assertFalse(process_running(original.process_identity["pid"]))
            self.assertFalse(process_running(other.process_identity["pid"]))

    async def test_priority_stop_cancels_manual_retry_during_readiness(self):
        """D4: a pending manual restart cannot obstruct standalone stop or leave its spawned copy."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            runner = await w.launch(w.template())
            sid = definition["service_id"]
            original = runner._state.services[sid]
            (w.files.controls[sid] / "hold-start").touch()
            retry = w.session.post("retry", target={"kind": "service", "position": 1})
            await wait_for(
                lambda: (
                    runner._state.services[sid] is not original
                    and runner._state.services[sid].process_identity
                )
            )
            replacement = runner._state.services[sid]
            self.assertEqual((await w.session.send("stats.state"))["result"], "success")
            stopped = await w.session.post("stop")
            self.assertEqual(stopped["result"], "success", stopped)
            self.assertEqual((await retry)["error"]["code"], "command_cancelled")
            self.assertFalse(process_running(replacement.process_identity["pid"]))
            self.assertFalse(runner._service_retrying)

    async def test_retry_counters_and_invalid_commands_preserve_pause(self):
        """D1/D2: manual recovery preserves spent budget; reset changes only that count."""
        async with asyncio.timeout(120):
            w = self.w
            socket, commands = (
                w.service(),
                w.service(implementation="action", interface="commands"),
            )
            runner = await w.launch(w.template())
            sid = socket["service_id"]
            original = runner._state.services[sid]
            terminate_owned(original.process_identity)
            await wait_for(
                lambda: (
                    runner._state.services[sid] is not original
                    and runner._state.services[sid].ready
                )
            )
            retry = await w.session.send(
                "retry", target={"kind": "service", "position": 1}
            )
            self.assertEqual(retry["result"], "success", retry)
            current = runner._state.services[sid]
            self.assertEqual(current.restart_count, 1)
            reset = await w.session.send(
                "reset_retries", target={"kind": "service", "position": 1}
            )
            self.assertEqual(
                reset["data"], {"service_id": sid, "previous": 1, "current": 0}
            )
            self.assertIs(runner._state.services[sid], current)
            for position in (0, 3, True, 2):
                response = await w.session.send("retry", {"position": position})
                self.assertEqual(response["result"], "fail", response)
            self.assertEqual(runner.get_state()["mode"], "paused")
            self.assertEqual(w.stage_trace(), [])
            self.assertIn(commands["service_id"], runner._state.services)

    async def test_close_detaches_from_service_and_stage_without_implicit_stop(self):
        """D5: explicit detach preserves independent participants; cleanup owns later shutdown."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            runner = await w.launch(
                w.template([w.stage(settings={"gate": str(w.gate())})]), paused=False
            )
            _, stage = await w.session.ready_attempt()
            service = runner._state.services[definition["service_id"]]
            await runner.close()
            self.assertTrue(process_running(stage["pid"]))
            self.assertTrue(process_running(service.process_identity["pid"]))
            self.assertIsNone(runner._service_task)
            self.assertFalse(runner.get_state()["fresh"])

    async def test_optional_state_write_failure_preserves_service_and_stage_results(
        self,
    ):
        """D5: failed state.json publication cannot replace mandatory journal outcomes."""
        async with asyncio.timeout(120):
            w = self.w
            w.service()
            runner = await w.launch(w.template())
            with patch.object(
                runner._state_store,
                "save",
                side_effect=OSError("isolated state publication failure"),
            ):
                result = await w.session.send("step")
            self.assertEqual(result["result"], "success", result)
            self.assertEqual(runner.get_state()["phase"], "completed")
            self.assertTrue(
                all(item.stopped for item in runner._state.services.values())
            )

    async def test_mandatory_journal_failure_stops_every_participant(self):
        """C4/D5: actual SQLite writer contention fails supervision and shuts down all owned work."""
        async with asyncio.timeout(120):
            w = self.w
            w.service()
            w.service()
            runner = await w.launch(
                w.template([w.stage(settings={"gate": str(w.gate())})]), paused=False
            )
            _, stage = await w.session.ready_attempt()
            identities = [
                item.process_identity for item in runner._state.services.values()
            ]
            lock = sqlite3.connect(
                runner._state.experiment_directory / "journals/events.sqlite"
            )
            lock.execute("BEGIN IMMEDIATE")
            try:
                await wait_for(
                    lambda: runner.get_state()["phase"] == "failed", timeout=60
                )
            finally:
                lock.rollback()
                lock.close()
            self.assertFalse(process_running(stage["pid"]))
            self.assertTrue(
                all(not process_running(item["pid"]) for item in identities)
            )
            self.assertIsNotNone(runner.get_state()["error"])

    async def test_state_is_detached_and_resource_series_follow_instances_and_experiments(
        self,
    ):
        """D6/E: real telemetry gains service targets, follows replacements and leaves stopped experiments."""
        async with asyncio.timeout(120):
            w = self.w
            service = w.service()
            w.service(implementation="action", interface="commands")
            runner = await w.launch(w.template())
            collector = w.session.controller.resources
            first = runner._state.services[service["service_id"]]
            await wait_for(
                lambda: any(
                    item["context"].get("service_instance_id")
                    == first.service_instance_id
                    for item in collector.get_status()["latest"]
                ),
                timeout=20,
            )
            state = (await w.session.send("stats.state"))["data"]
            state["services"][0]["process"]["pid"] = 0
            state["services"][0]["module"]["name"] = "changed"
            self.assertNotEqual(
                runner._state.services[service["service_id"]].process_identity["pid"], 0
            )
            self.assertNotEqual(
                runner._state.services[service["service_id"]].definition["module"][
                    "name"
                ],
                "changed",
            )
            self.assertEqual(len(runner.get_resource_snapshot()["targets"]), 1)
            terminate_owned(first.process_identity)
            await wait_for(
                lambda: (
                    runner._state.services[service["service_id"]] is not first
                    and runner._state.services[service["service_id"]].ready
                )
            )
            current = runner._state.services[service["service_id"]]
            await wait_for(
                lambda: any(
                    item["context"].get("service_instance_id")
                    == current.service_instance_id
                    for item in collector.get_status()["latest"]
                ),
                timeout=20,
            )
            old_manager = runner._services
            old_experiment = runner._state.experiment_id
            old_requests = set(runner._state.used_request_ids)
            self.assertEqual((await w.session.send("step"))["result"], "success")
            self.assertEqual(runner.get_resource_snapshot()["targets"], [])
            await wait_for(
                lambda: any(
                    not item["context"] for item in collector.get_status()["latest"]
                )
            )
            next_service = w.service()
            await w.launch(w.template(services=[next_service]))
            self.assertIsNot(runner._services, old_manager)
            self.assertNotEqual(runner._state.experiment_id, old_experiment)
            self.assertEqual(list(runner._state.services), [next_service["service_id"]])
            self.assertEqual(runner._state.used_request_ids & old_requests, set())
            self.assertEqual((await w.session.send("step"))["result"], "success")
