"""Approved B/C/J: StageClient with real executor, subprocess, TCP and journal."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from core.models.participant_protocol import ModuleProgress
from core.participants.stage_client import StageClient
from tests.helpers.dag import TEMP_ROOT, DagSession, DagWorkspace, wait_until


class StageClientModelTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=TEMP_ROOT)
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()

    def test_constructor_has_no_io_and_unopened_calls_fail(self):
        with patch.object(Path, "open", side_effect=AssertionError("constructor I/O")):
            client = StageClient(self.root / "missing.json")
        with self.assertRaises(RuntimeError):
            client.cancel_requested()
        with self.assertRaises(RuntimeError):
            client.succeed({})
        client.close()

    async def test_progress_model_reaches_logger_before_public_json_request(self):
        client = StageClient(self.root / "context.json")
        client._logger = Mock()
        client._connection = Mock()

        async def request(request_id, command, data, *, timeout_seconds):
            client._logger.record_progress.assert_called_once_with(
                0.25, total=1, stage=None, unit="fraction"
            )
            self.assertEqual(command, "report_progress")
            self.assertEqual(data, {"value": 0.25, "message": None})
            self.assertIsInstance(data, dict)
            return {"result": "success", "data": {}}

        client._connection.request = AsyncMock(side_effect=request)
        with patch.object(client, "_report") as report:
            client.report_progress(0.25)
        command, progress = report.call_args.args
        self.assertIsInstance(progress, ModuleProgress)
        await client._send(command, progress)
        client._connection.request.assert_awaited_once()


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
