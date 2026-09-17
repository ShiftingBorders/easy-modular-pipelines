"""Approved B/C/J: StageClient with real executor, subprocess, TCP and journal."""

import unittest
from pathlib import Path
from unittest.mock import patch

from core.runner_utils.stage_client import StageClient
from tests.helpers.dag import DagSession, DagWorkspace, wait_until


class StageClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = DagWorkspace()
        self.addCleanup(self.w.close)
        self.session = DagSession(self.w)
        await self.session.start()
        self.addAsyncCleanup(self.session.close)
        self.module = self.w.module(
            source=Path(__file__).parents[1] / "helpers/stage_client_probe.py"
        )

    def test_constructor_has_no_io_and_unopened_calls_fail(self):
        with patch.object(Path, "open", side_effect=AssertionError("constructor I/O")):
            client = StageClient(self.w.root / "missing.json")
        with self.assertRaises(RuntimeError):
            client.cancel_requested()
        with self.assertRaises(RuntimeError):
            client.succeed({})
        client.close()

    async def test_progress_state_and_cooperative_cancellation(self):
        gate = self.w.gate()
        await self.session.launch(
            self.w.template([self.w.stage(self.module, settings={"gate": str(gate)})])
        )
        step = self.session.post("step")
        runner = self.session.runner
        await wait_until(
            lambda: (
                runner._state.active_attempt is not None
                and (
                    runner._state.active_attempt.artifacts_directory / "client.ready"
                ).exists()
            )
        )
        directory = runner._state.active_attempt.artifacts_directory
        events = self.w.events(runner)
        self.assertTrue(
            any(event["event_type"] == "progress.recorded" for event in events)
        )
        self.assertTrue(
            any(
                event["event_type"] == "module.report_state"
                and event["data"]["phase"] == "working"
                for event in events
            )
        )
        await runner.stop()
        await step
        self.assertTrue((directory / "client.cancelled").exists())

    async def test_result_validation_duplicates_and_exit_code(self):
        for mode, expected in (
            ("invalid", "success"),
            ("duplicate", "success"),
            ("no_result", "fail"),
            ("nonzero", "fail"),
        ):
            with self.subTest(mode=mode):
                await self.session.launch(
                    self.w.template(
                        [self.w.stage(self.module, settings={"mode": mode})]
                    )
                )
                response = await self.session.send("step")
                self.assertEqual(response["result"], expected, response)
                if mode == "invalid":
                    self.assertEqual(response["data"]["result"]["data"]["rejected"], 7)
                if mode == "duplicate":
                    self.assertEqual(
                        len(
                            list(
                                self.session.runner._state.experiment_directory.rglob(
                                    "duplicate.rejected"
                                )
                            )
                        ),
                        1,
                    )
                await self.session.send("stop")

    async def test_shipped_counter_example_runs_through_core_sdk(self):
        from tests.helpers.dag import REPOSITORY

        module = self.w.module(
            "counter",
            source=REPOSITORY / "examples/basic_dag/modules/counter/1.0/main.py",
        )
        await self.session.launch(
            self.w.template(
                [self.w.stage(module, settings={"ticks": 2, "delay_seconds": 0})]
            )
        )
        response = await self.session.send("step")
        self.assertEqual(response["result"], "success", response)
        result = response["data"]["result"]["data"]
        self.assertEqual(result["ticks"], 2)
        self.assertTrue(
            (
                self.session.runner._state.experiment_directory / result["artifact"]
            ).exists()
        )
