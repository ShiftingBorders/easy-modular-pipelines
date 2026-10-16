"""Approved N1-N5/N7-N8: executor startup, policy and independent evidence."""

import asyncio
import json
import subprocess
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch

from tests.helpers.dag import DagSession, DagWorkspace, wait_until
from tests.helpers.journal_writer_faults import WriterFaults


class ExecutorStartupTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.faults = WriterFaults(self.workspace.root / "startup-writer-faults")
        self.addAsyncCleanup(self.faults.close)
        self.session = DagSession(self.workspace)
        await self.session.start()
        self.addAsyncCleanup(self.session.close)
        self.addCleanup(self.faults.release_all)
        self.launches = []

    def _redirect_executor(self, fault, gate=None, *, limit=None):
        original = subprocess.Popen

        def launch(argv, *args, **kwargs):
            argv = list(argv)
            if "core.participants.executor" in argv:
                directory = Path(argv[argv.index("--launch") + 1]).parent
                if limit is None or len(self.launches) < limit:
                    argv[argv.index("core.participants.executor")] = (
                        "tests.helpers.dag_executor"
                    )
                    argv.extend(["--fault", fault])
                    if gate is not None:
                        argv.extend(["--gate", str(gate)])
                self.launches.append(directory)
            return original(argv, *args, **kwargs)

        return patch("core.experiments.stages.subprocess.Popen", side_effect=launch)

    async def _prepare(
        self,
        *,
        startup=8,
        control=1.5,
        timeout=None,
        retries=0,
        action="pause",
        following=False,
    ):
        stage = self.workspace.stage(
            timeout=timeout, retries=retries, on_exhausted=action
        )
        stages = [stage]
        if following:
            stages.append(self.workspace.stage(stage["module"]))
        template = self.workspace.template(stages, keep_attempts=2)
        template["start_timeout"] = startup
        template["unknown_state"]["timeout_seconds"] = control
        template["runner_timeout_margin_seconds"] = 2
        await self.session.launch(template)
        return stage

    def _records(self, directory, component):
        path = directory / f"startup-{component}.jsonl"
        self.assertTrue(path.is_file(), f"Missing independent diagnostics: {path}")
        return [
            json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
        ]

    def _starts(self):
        path = (
            self.session.runner._state.experiment_directory / "shared_data/trace.jsonl"
        )
        if not path.is_file():
            return []
        return [
            row
            for line in path.read_text(encoding="utf-8").splitlines()
            if (row := json.loads(line))["event"] == "start"
        ]

    async def test_start_timeout_controls_readiness_and_success_trace_n1(self):
        stage = await self._prepare()
        gate = self.workspace.gate()
        try:
            with self._redirect_executor("startup_hold", gate):
                step = self.session.post("step")
                await wait_until(gate.with_suffix(".entered").is_file)
                await asyncio.sleep(2)
                gate.touch()
                reply = await asyncio.wait_for(asyncio.shield(step), 30)
            self.assertEqual(reply["result"], "success", reply)
            self.assertEqual(len(self._starts()), 1)
            state = self.session.runner._state
            self.assertEqual(state.stage_attempt_numbers[stage["stage_id"]], 1)
            runner = self._records(self.launches[0], "runner")
            phases = [row["phase"] for row in runner]
            for phase in (
                "spawn.requested",
                "spawn.returned",
                "endpoint.connected",
                "execute.sent",
            ):
                self.assertIn(phase, phases)
            self.assertLess(
                phases.index("spawn.requested"), phases.index("execute.sent")
            )
            budget = next(row for row in runner if row["phase"] == "spawn.requested")
            self.assertEqual(budget["details"]["timeout_seconds"], 8)
            self.assertEqual(budget["details"]["timeout_source"], "start_timeout")
            executor = self._records(self.launches[0], "executor")
            phases = [row["phase"] for row in executor]
            ordered = [
                "process.entered",
                "launch.validated",
                "logger.open.begin",
                "logger.open.end",
                "listener.ready",
                "endpoint.published",
                "execute.received",
                "module.spawned",
            ]
            positions = [phases.index(phase) for phase in ordered]
            self.assertEqual(positions, sorted(positions))
            self.assertTrue(
                all(
                    row["context"]["attempt_id"] == budget["context"]["attempt_id"]
                    for row in executor
                )
            )
        finally:
            gate.touch(exist_ok=True)

    async def _check_spawn_budget(self, *, startup, timeout):
        await self._prepare(startup=startup, control=5, timeout=timeout)
        stages = self.session.runner._stages
        original = stages._spawn_executor

        async def delayed_spawn(*args, **kwargs):
            await original(*args, **kwargs)
            await asyncio.sleep(3)

        with patch.object(stages, "_spawn_executor", delayed_spawn):
            reply = await asyncio.wait_for(self.session.post("step"), 30)
        self.assertEqual(reply["result"], "fail", reply)
        self.assertEqual(
            self._starts(), [], "Expired startup must never dispatch execute."
        )
        self.assertEqual(self.session.runner.get_state()["mode"], "paused")
        if timeout is not None:
            finishes = [
                row
                for row in self.workspace.events(self.session.runner)
                if row["event_type"] == "stage.finished"
            ]
            self.assertEqual(finishes[-1]["data"]["outcome"], "timed_out")

    async def test_spawn_consumes_startup_budget_without_stage_limit_n2(self):
        await self._check_spawn_budget(startup=2, timeout=None)

    async def test_earlier_stage_budget_prevents_dispatch_n2(self):
        await self._check_spawn_budget(startup=8, timeout=2)

    async def test_handshake_returning_after_deadline_cannot_dispatch_n2(self):
        await self._prepare(startup=2, control=5)
        stages = self.session.runner._stages
        connect = stages._connect
        held = False

        async def delayed_connect(attempt, timeout):
            nonlocal held
            await connect(attempt, timeout)
            if not held:
                held = True
                await asyncio.sleep(3)

        with patch.object(stages, "_connect", delayed_connect):
            reply = await asyncio.wait_for(self.session.post("step"), 30)
        self.assertEqual(reply["result"], "fail", reply)
        self.assertEqual(self._starts(), [])

    async def test_stalled_journal_read_cannot_hide_startup_expiry_n2_n6(self):
        self.faults.open()
        await self._prepare(startup=2, control=5)
        gate = self.faults.hold_quick_check()
        step = self.session.post("step")
        try:
            await gate.entered()
            await asyncio.sleep(3)
            directory = self.session.runner._state.active_attempt.artifacts_directory
            records = self._records(directory, "runner")
            self.assertIn("startup.failed", [row["phase"] for row in records])
            self.assertEqual(
                self.session.runner._journal._supervisor.get_status()["state"], "ready"
            )
            self.assertEqual(self._starts(), [])
        finally:
            gate.release()
            await asyncio.wait_for(asyncio.gather(step, return_exceptions=True), 30)

    async def _writer_outage(self, seconds=2.5):
        owner = self.session.runner._journal._supervisor
        held = self.faults.hold_start()
        try:
            await self.faults.kill(owner.get_status()["process"])
            await held.entered()
            await asyncio.sleep(seconds)
        finally:
            held.release()
        await wait_until(lambda: owner.get_status()["state"] == "ready", timeout=30)

    async def test_writer_recovery_during_bootstrap_keeps_attempt_n3(self):
        self.faults.open()
        stage = await self._prepare(startup=3, control=1.5)
        gate = self.workspace.gate()
        try:
            with self._redirect_executor("startup_hold", gate):
                step = self.session.post("step")
                await wait_until(gate.with_suffix(".entered").is_file)
                attempt_id = self.session.runner._state.active_attempt.attempt_id
                outage = asyncio.create_task(self._writer_outage(3.5))
                await wait_until(
                    lambda: (
                        self.session.runner._journal._supervisor.get_status()["state"]
                        != "ready"
                    )
                )
                gate.touch()
                await outage
                reply = await asyncio.wait_for(asyncio.shield(step), 30)
            self.assertEqual(reply["result"], "success", reply)
            state = self.session.runner._state
            self.assertEqual(state.stage_result_ids[stage["stage_id"]], attempt_id)
            self.assertEqual(state.stage_attempt_numbers[stage["stage_id"]], 1)
            self.assertEqual(len(self._starts()), 1)
        finally:
            gate.touch(exist_ok=True)

    async def test_writer_recovery_while_executor_awaits_execute_n3(self):
        self.faults.open()
        stage = await self._prepare(startup=8, control=1.5)
        stages = self.session.runner._stages
        connect = stages._connect
        held = False

        async def connect_during_recovery(attempt, timeout):
            nonlocal held
            await connect(attempt, timeout)
            if not held:
                held = True
                await self._writer_outage()

        with patch.object(stages, "_connect", connect_during_recovery):
            reply = await asyncio.wait_for(self.session.post("step"), 30)
        self.assertEqual(reply["result"], "success", reply)
        self.assertEqual(
            self.session.runner._state.stage_attempt_numbers[stage["stage_id"]], 1
        )
        self.assertEqual(len(self._starts()), 1)

    async def _check_failure_policy(self, action):
        stage = await self._prepare(control=1, action=action, following=True)
        with self._redirect_executor("startup_exit", limit=1):
            reply = await asyncio.wait_for(self.session.post("step"), 30)
        self._assert_failure_policy(action, stage, reply)
        return reply

    def _assert_failure_policy(self, action, stage, reply):
        state = self.session.runner.get_state()
        expected = {"pause": "waiting", "stop": "failed", "skip": "waiting"}[action]
        self.assertEqual(state["phase"], expected, reply)
        self.assertEqual(state["stage_position"], 1)
        self.assertEqual(state["pending_advance"], action == "skip")
        self.assertEqual(
            self.session.runner._state.stage_attempt_numbers[stage["stage_id"]], 1
        )
        self.assertEqual(self._starts(), [])

    async def test_startup_exit_applies_stop_n4(self):
        await self._check_failure_policy("stop")

    async def test_startup_exit_applies_skip_n4(self):
        await self._check_failure_policy("skip")
        following = self.session.runner._state.template.stages[1].stage_id
        reply = await asyncio.wait_for(self.session.post("step"), 30)
        self.assertEqual(reply["result"], "success", reply)
        self.assertEqual([row["stage_id"] for row in self._starts()], [following])

    async def test_retry_preserves_input_and_changes_attempt_after_startup_exit_n4(
        self,
    ):
        predecessor = self.workspace.stage(settings={"label": "predecessor"})
        stage = self.workspace.stage(predecessor["module"], retries=1, timeout=None)
        template = self.workspace.template([predecessor, stage], keep_attempts=2)
        template["start_timeout"] = 8
        template["unknown_state"]["timeout_seconds"] = 1
        await self.session.launch(template)
        previous = await asyncio.wait_for(self.session.post("step"), 30)
        self.assertEqual(previous["result"], "success", previous)
        expected_input = self.session.runner._state.last_result
        self.assertEqual(expected_input["trail"], ["predecessor"])
        with self._redirect_executor("startup_exit", limit=1):
            reply = await asyncio.wait_for(self.session.post("step"), 30)
        self.assertEqual(reply["result"], "success", reply)
        state = self.session.runner._state
        self.assertEqual(state.stage_attempt_numbers[stage["stage_id"]], 2)
        starts = self._starts()
        self.assertEqual(
            [row["stage_id"] for row in starts],
            [predecessor["stage_id"], stage["stage_id"]],
        )
        self.assertEqual(starts[-1]["input"], expected_input)
        contexts = [
            json.loads((path / "context.json").read_text(encoding="utf-8"))
            for path in self.launches
        ]
        self.assertNotEqual(
            contexts[0]["context"]["attempt_id"], contexts[1]["context"]["attempt_id"]
        )
        self.assertEqual(contexts[0]["input_data"], expected_input)
        self.assertEqual(contexts[1]["input_data"], expected_input)

    async def test_unconfirmed_startup_stop_blocks_retry_and_skip_n5(self):
        stage = await self._prepare(control=1, retries=1, action="skip", following=True)
        with (
            self._redirect_executor("startup_exit", limit=1),
            patch.object(self.session.runner._stages, "interrupt", return_value=False),
        ):
            await asyncio.wait_for(self.session.post("step"), 30)
        state = self.session.runner._state
        self.assertEqual(state.stage_position, 1)
        self.assertFalse(state.pending_advance)
        self.assertEqual(state.stage_attempt_numbers[stage["stage_id"]], 1)
        self.assertEqual(self._starts(), [])

    async def test_stop_before_late_endpoint_never_executes_module_n5(self):
        await self._prepare(control=5)
        gate = self.workspace.gate()
        try:
            with self._redirect_executor("startup_hold", gate):
                step = self.session.post("step")
                await wait_until(gate.with_suffix(".entered").is_file)
                attempt = self.session.runner._state.active_attempt
                stopping = asyncio.create_task(self.session.runner.stop())
                await wait_until(lambda: self.session.runner._stop_requested)
                await asyncio.sleep(0.1)
                gate.touch()
                stopped = await asyncio.wait_for(stopping, 30)
                await asyncio.wait_for(asyncio.gather(step, return_exceptions=True), 30)
            self.assertTrue(stopped["termination_confirmed"])
            self.assertEqual(self._starts(), [])
            self.assertFalse(attempt.endpoint_path.exists())
        finally:
            gate.touch(exist_ok=True)

    async def test_early_output_is_bounded_and_preserved_without_executor_logger_n7(
        self,
    ):
        await self._prepare(control=1)
        with self._redirect_executor("startup_flood"):
            await asyncio.wait_for(self.session.post("step"), 30)
        directory = self.launches[0]
        for name in ("stdout", "stderr"):
            path = directory / f"executor.{name}.log"
            self.assertTrue(path.is_file(), path)
            self.assertLessEqual(path.stat().st_size, 1024 * 1024)
            self.assertIn(
                f"executor bootstrap {name}", path.read_text(encoding="utf-8")
            )
        records = self._records(directory, "runner")
        losses = [row for row in records if row["phase"] == "output.truncated"]
        self.assertEqual(
            {row["details"]["stream"] for row in losses}, {"stdout", "stderr"}
        )
        self.assertEqual(self._starts(), [])

    async def test_pause_preserves_failure_evidence_before_cleanup_n4_n8(self):
        stage = await self._prepare(control=1, following=True)
        stages = self.session.runner._stages
        interrupt = stages.interrupt
        entered, release = asyncio.Event(), asyncio.Event()

        async def delayed_stop(*args, **kwargs):
            entered.set()
            await release.wait()
            return await interrupt(*args, **kwargs)

        with (
            self._redirect_executor("startup_exit"),
            patch.object(stages, "interrupt", delayed_stop),
        ):
            step = self.session.post("step")
            try:
                await asyncio.wait_for(entered.wait(), 30)
                before_cleanup = self._records(self.launches[0], "runner")
                failure = next(
                    row for row in before_cleanup if row["phase"] == "startup.failed"
                )
                self.assertNotIn(
                    "cleanup.finished", [row["phase"] for row in before_cleanup]
                )
                self.assertFalse(step.done())
            finally:
                release.set()
                reply = await asyncio.wait_for(step, 30)
        self._assert_failure_policy("pause", stage, reply)
        records = self._records(self.launches[0], "runner")
        self.assertEqual(
            next(row for row in records if row["phase"] == "startup.failed"), failure
        )
        cleanup = next(row for row in records if row["phase"] == "cleanup.finished")
        self.assertLess(failure["monotonic"], cleanup["monotonic"])
        self.assertTrue(cleanup["details"]["stopped"])
        self.assertTrue(failure["details"]["error"]["traceback"])
        self.assertEqual(failure["details"]["timeout_source"], "start_timeout")
        self.assertTrue(failure["details"]["last_phase"])
        errors = [
            row
            for row in self.workspace.events(self.session.runner)
            if row["event_type"] == "error.recorded"
        ]
        error = next(
            row
            for row in errors
            if row["context"].get("attempt_id") == failure["context"]["attempt_id"]
        )
        self.assertTrue(error["data"]["traceback"])
        self.assertEqual(error["data"]["error_id"], failure["details"]["error_id"])

    async def test_cleanup_exception_preserves_startup_failure_and_blocks_retry_n8(
        self,
    ):
        stage = await self._prepare(control=1, retries=1)
        with (
            self._redirect_executor("startup_exit"),
            patch.object(
                self.session.runner._stages,
                "interrupt",
                side_effect=OSError("injected startup cleanup failure"),
            ),
        ):
            reply = await asyncio.wait_for(self.session.post("step"), 30)
        self.assertEqual(reply["result"], "fail", reply)
        self.assertEqual(len(self.launches), 1)
        self.assertEqual(self._starts(), [])
        self.assertEqual(
            self.session.runner._state.stage_attempt_numbers[stage["stage_id"]], 1
        )
        records = self._records(self.launches[0], "runner")
        failure = next(row for row in records if row["phase"] == "startup.failed")
        cleanup = next(row for row in records if row["phase"] == "cleanup.finished")
        original = failure["details"]["error"]
        self.assertEqual(original["error_type"], "_ExecutorExited")
        self.assertIn("Executor exited", original["message"])
        self.assertIn("_ExecutorExited", original["traceback"])
        self.assertLess(failure["monotonic"], cleanup["monotonic"])
        self.assertFalse(cleanup["details"]["stopped"])
        self.assertEqual(cleanup["details"]["error"]["error_type"], "OSError")
        error_id = failure["details"]["error_id"]
        self.assertEqual(cleanup["details"]["error_id"], error_id)
        errors = [
            row["data"]
            for row in self.workspace.events(self.session.runner)
            if row["event_type"] == "error.recorded"
        ]
        primary = next(row for row in errors if row["error_id"] == error_id)
        secondary = next(
            row for row in errors if row.get("caused_by_error_id") == error_id
        )
        self.assertEqual(primary["error_code"], "executor_startup_failed")
        self.assertIn("_ExecutorExited", primary["traceback"])
        self.assertIn("injected startup cleanup failure", secondary["traceback"])
