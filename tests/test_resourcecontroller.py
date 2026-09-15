"""Approved resource_collector.md C/D/E: real DAG, queues, telemetry and failures."""

import asyncio
import functools
import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from core.experimentcontroller import ExperimentController
from core.logger import OperationLogger
from core.runner_utils.runtimeio import process_identity
from tests.helpers.dag import (
    DagSession,
    DagWorkspace,
    process_running,
    terminate_owned,
    wait_until,
)
from tests.helpers.resources import (
    DEFAULT_CONFIG,
    controlled_collector,
    events,
    write_settings,
)


class ResourceControllerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.session = None
        self.addAsyncCleanup(self.close_session)

    async def close_session(self):
        if self.session is not None:
            await self.session.close()

    async def start(self, path=None):
        self.session = DagSession(self.workspace)
        self.session.controller = ExperimentController(
            self.workspace.root,
            self.session.runner,
            self.session.requests,
            self.session.responses,
            resource_config_path=path or write_settings(self.workspace.root),
        )
        await self.session.start()
        return self.session.controller.resources

    async def launch_gated(self, *, failures=0, retries=0):
        gate = self.workspace.gate()
        stage = self.workspace.stage(
            settings={"gate": str(gate), "failures": failures},
            retries=retries,
            timeout=None,
        )
        await self.session.launch(self.workspace.template([stage]), paused=False)
        directory, ready = await self.session.ready_attempt()
        return gate, directory, ready

    async def test_default_configuration_covers_idle_crash_completion_and_next_experiment(
        self,
    ):
        """C/D/E: default intervals, real worker crash, and two disjoint experiment journals."""
        async with asyncio.timeout(60):
            collector = await self.start(DEFAULT_CONFIG)
            await wait_until(lambda: collector.get_status()["latest"])
            self.assertEqual(
                list((self.workspace.root / "experiments").glob("*/journals/*")), []
            )
            idle_history = collector.read_history()["samples"]
            self.assertTrue(all(sample["context"] == {} for sample in idle_history))
            gate, directory, ready = await self.launch_gated()
            runner = self.session.runner
            attempt = runner._state.active_attempt.attempt_id
            await wait_until(
                lambda: any(
                    item["context"].get("attempt_id") == attempt
                    for item in collector.get_status()["latest"]
                )
            )
            old_collector = collector.get_status()
            terminate_owned(process_identity(old_collector["pid"]))
            await wait_until(
                lambda: (
                    collector.get_status()["collector_id"]
                    not in (None, old_collector["collector_id"])
                )
            )
            await wait_until(
                lambda: any(
                    item["context"].get("attempt_id") == attempt
                    for item in collector.get_status()["latest"]
                )
            )
            self.assertTrue(process_running(ready["pid"]))
            self.assertEqual(runner._state.active_attempt.attempt_id, attempt)
            self.assertEqual(runner._state.active_attempt.attempt_number, 1)
            status = await self.session.send("stats.resources")
            self.assertEqual(status["result"], "success")
            page = await self.session.send("stats.resources.history", {"limit": 2})
            self.assertEqual(page["result"], "success")
            self.assertLessEqual(len(page["data"]["samples"]), 2)
            old_reader = OperationLogger(runner._journal.reader_config_path)
            old_reader.open()
            self.addCleanup(old_reader.close)
            gate.touch()
            await wait_until(lambda: runner.get_state()["phase"] == "completed")
            self.assertTrue((directory / "output.json").is_file())
            await wait_until(
                lambda: (
                    collector.get_status()["latest"]
                    and all(
                        item["context"] == {}
                        for item in collector.get_status()["latest"]
                    )
                )
            )
            records = events(old_reader, "resources.recorded")
            self.assertTrue(records)
            process_records = [
                item
                for item in records
                if "process_cpu_percent" in item["data"]["resources"]
            ]
            self.assertTrue(process_records)
            for item in process_records:
                self.assertEqual(item["context"]["attempt_id"], attempt)
                identity = item["data"]["resources"]["process_cpu_percent"][
                    "attributes"
                ]["observed_process"]
                self.assertEqual(identity["pid"], ready["pid"])
                self.assertNotEqual(item["context"]["process_id"], ready["pid"])
            self.assertTrue(
                any(
                    item["data"].get("reason") == "collector_restarted"
                    for item in events(old_reader, "resources.gap")
                )
            )
            count = len(records)
            started = datetime.now(UTC)
            module = runner._state.template["stages"][0]["module"]
            second = await self.session.launch(
                self.workspace.template([self.workspace.stage(module)]), paused=True
            )
            await wait_until(
                lambda: events(runner._journal.client, "resources.recorded")
            )
            self.assertEqual(len(events(old_reader, "resources.recorded")), count)
            for item in events(runner._journal.client, "resources.recorded"):
                self.assertEqual(
                    item["context"]["experiment_id"], second["experiment_id"]
                )
                for measurement in item["data"]["resources"].values():
                    self.assertGreaterEqual(
                        datetime.fromisoformat(
                            measurement["attributes"]["observed_at"]
                        ),
                        started,
                    )
            self.assertEqual((await self.session.send("step"))["result"], "success")

    async def check_optional_failure(self, flag):
        root = self.workspace.root
        (root / flag).touch()
        worker = functools.partial(controlled_collector, controls=str(root))
        with patch("core.resourcecollector.collect_resources", worker):
            collector = await self.start()
            gate, _, ready = await self.launch_gated()
            attempt = self.session.runner._state.active_attempt.attempt_id
            await wait_until(lambda: collector.get_status()["journal_error"])
            reply = await self.session.send("stats.state")
            self.assertEqual(reply["result"], "success")
            self.assertEqual(reply["data"]["phase"], "stage_running")
            self.assertTrue(process_running(ready["pid"]))
            (root / flag).unlink()
            await wait_until(
                lambda: (
                    collector.get_status()["journal_error"] is None
                    and events(
                        self.session.runner._journal.client, "resources.recorded"
                    )
                )
            )
            self.assertEqual(
                self.session.runner._state.active_attempt.attempt_id, attempt
            )
            gate.touch()
            await wait_until(
                lambda: self.session.runner.get_state()["phase"] == "completed"
            )
            self.assertEqual(self.session.runner.get_state()["result"]["attempt"], 1)

    async def test_collector_open_failure_does_not_stop_the_dag(self):
        """D/E: collector-only open failure does not affect mandatory runner writes."""
        async with asyncio.timeout(30):
            await self.check_optional_failure("fail-open")

    async def test_collector_write_failure_does_not_restart_the_stage(self):
        """D/E: optional write failures preserve the active PID/attempt and final result."""
        async with asyncio.timeout(30):
            await self.check_optional_failure("fail-write")

    async def test_read_and_stop_commands_work_while_the_collector_is_hung(self):
        """C/E: actual queue reads and priority stop remain responsive during a sample hang."""
        async with asyncio.timeout(30):
            root = self.workspace.root
            worker = functools.partial(controlled_collector, controls=str(root))
            with patch("core.resourcecollector.collect_resources", worker):
                collector = await self.start(
                    write_settings(root, heartbeat_timeout_seconds=5)
                )
                _, _, ready = await self.launch_gated()
                await wait_until(lambda: collector.get_status()["latest"])
                (root / "hang").touch()
                await wait_until(lambda: (root / "hanging").exists())
                for command, args in (
                    ("stats.resources", {"bad": 1}),
                    ("stats.resources.history", {"after": -1}),
                    ("stats.resources.history", {"limit": 1001}),
                ):
                    reply = await asyncio.wait_for(self.session.send(command, args), 2)
                    self.assertEqual(reply["error"]["code"], "invalid_request")
                self.assertEqual(
                    (await asyncio.wait_for(self.session.send("stats.state"), 2))[
                        "data"
                    ]["phase"],
                    "stage_running",
                )
                result = await asyncio.wait_for(self.session.send("stop"), 5)
                self.assertEqual(result["result"], "success")
                self.assertFalse(process_running(ready["pid"]))
                (root / "release").touch()

    async def test_bad_monitoring_config_leaves_normal_dag_commands_available(self):
        """A/E: missing and malformed collector settings are isolated from execution."""
        async with asyncio.timeout(30):
            template = self.workspace.template()
            for contents in (None, "{"):
                path = self.workspace.root / "invalid-monitoring.json"
                if contents is not None:
                    path.write_text(contents, encoding="utf-8")
                collector = await self.start(path)
                await wait_until(
                    lambda collector=collector: (
                        collector.get_status()["state"] == "configuration_error"
                    )
                )
                await self.session.launch(template)
                self.assertEqual((await self.session.send("step"))["result"], "success")
                self.assertEqual(self.session.runner.get_state()["phase"], "completed")
                await self.close_session()

    async def test_paused_dag_keeps_logging_and_retry_uses_a_new_series(self):
        """C/D/E: pause retains host monitoring; automatic retry has a separate process series."""
        async with asyncio.timeout(30):
            collector = await self.start()
            gate = self.workspace.gate()
            stage = self.workspace.stage(
                settings={"gate": str(gate), "failures": 1}, retries=1, timeout=None
            )
            stage["errors"]["retry_delay_seconds"] = 0.5
            await self.session.launch(self.workspace.template([stage]), paused=True)
            reader = self.session.runner._journal.client
            await wait_until(lambda: events(reader, "resources.recorded"))
            count = len(events(reader, "resources.recorded"))
            await wait_until(lambda: len(events(reader, "resources.recorded")) > count)
            self.assertIsNone(self.session.runner._state.active_attempt)
            self.assertEqual((await self.session.send("resume"))["result"], "success")
            _, first = await self.session.ready_attempt()
            first_attempt = self.session.runner._state.active_attempt.attempt_id
            await wait_until(
                lambda: any(
                    item["context"].get("attempt_id") == first_attempt
                    for item in collector.get_status()["latest"]
                )
            )
            gate.touch()
            await wait_until(
                lambda: any(
                    item["event_type"] == "stage.finished"
                    for item in self.workspace.events(self.session.runner)
                )
            )
            gate.unlink()
            await wait_until(
                lambda: self.session.runner._state.active_attempt.attempt_number == 2
            )
            _, second = await self.session.ready_attempt()
            second_attempt = self.session.runner._state.active_attempt.attempt_id
            await wait_until(
                lambda: any(
                    item["context"].get("attempt_id") == second_attempt
                    for item in collector.get_status()["latest"]
                )
            )
            self.assertNotEqual(first_attempt, second_attempt)
            self.assertFalse(process_running(first["pid"]))
            self.assertTrue(process_running(second["pid"]))
            gate.touch()
            await wait_until(
                lambda: self.session.runner.get_state()["phase"] == "completed"
            )
            self.assertEqual(self.session.runner.get_state()["result"]["attempt"], 2)
