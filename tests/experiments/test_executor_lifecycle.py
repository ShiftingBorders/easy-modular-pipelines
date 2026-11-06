"""Approved E01-E08: accepted results and executor exit are separate facts."""

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import psutil

from core.experiments.stages import StageRunner
from core.primitives import processes
from tests.helpers.processes import WindowsProcessCalls


@unittest.skipUnless(os.name == "nt", "Windows executor lifecycle regressions")
class ExecutorLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        scratch = (
            Path(__file__).resolve().parents[2] / ".artifacts/tmp/executor-identity"
        )
        scratch.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.calls = WindowsProcessCalls()
        self.calls.install(self)
        self.records = {}
        journal = SimpleNamespace(
            client=SimpleNamespace(
                read_command_result=Mock(
                    side_effect=lambda request: self.records[request]
                )
            )
        )
        self.runner = StageRunner(None, journal, None)
        self.state = SimpleNamespace(
            experiment_id="experiment-1",
            experiment_directory=self.root,
            stage_result_ids={},
            pending_input=None,
            last_dag_decision=None,
            active_attempt=None,
            template=SimpleNamespace(start_timeout=0.05),
        )
        # The baseline waits through psutil after its own native identity check.
        # The corrected Windows wait must use the verified native handle instead.
        old_wait = patch("core.experiments.stages.psutil.Process")
        old_wait.start().return_value.wait.side_effect = self._baseline_wait
        self.addCleanup(old_wait.stop)

    def _baseline_wait(self, timeout=None):
        if self.calls.wait_result == 258:
            raise psutil.TimeoutExpired(timeout)
        return 0

    def accepted(self, request="request-1", pid=101):
        context = {
            "module_name": "example",
            "stage_id": "stage-1",
            "participant_id": "stage-1",
            "cycle_number": 1,
            "attempt_number": 1,
        }
        self.records[request] = {"event": {"context": context}}
        self.state.stage_result_ids["stage-1"] = request
        directory = self.root / "shared_artifacts/epoch_1/example/stage-1/attempt_1"
        directory.mkdir(parents=True, exist_ok=True)
        identity = processes.process_identity(pid)
        (directory / "process.json").write_text(
            json.dumps({"experiment_id": "experiment-1", "executor": identity}),
            encoding="utf-8",
        )
        return identity

    async def test_completed_executor_is_not_reopened_from_retained_result(self):
        self.accepted()
        await self.runner._wait_accepted_executors(self.state)
        self.calls.denied.add(101)
        self.calls.kernel.OpenProcess.reset_mock()
        for _ in range(3):
            await self.runner._wait_accepted_executors(self.state)
        self.calls.kernel.OpenProcess.assert_not_called()
        self.assertEqual(self.state.stage_result_ids, {"stage-1": "request-1"})

    async def test_uncertain_executor_is_retained_and_error_names_its_identity(self):
        self.accepted()
        self.calls.denied.add(101)
        with self.assertRaises(PermissionError) as caught:
            await self.runner._wait_accepted_executors(self.state)
        notes = " ".join(getattr(caught.exception, "__notes__", []))
        for detail in ("101", "request-1", "created_at_os"):
            self.assertIn(detail, notes)
        self.assertIn("request-1", self.runner._executor_identities)
        self.calls.denied.clear()
        await self.runner._wait_accepted_executors(self.state)
        self.assertNotIn("request-1", self.runner._executor_identities)

    async def test_published_result_with_live_executor_does_not_confirm_exit(self):
        self.accepted()
        self.calls.wait_result = 258
        with self.assertRaises(psutil.TimeoutExpired):
            await self.runner._wait_accepted_executors(self.state)
        self.assertIn("request-1", self.runner._executor_identities)
        self.calls.wait_result = 0
        await self.runner._wait_accepted_executors(self.state)

    async def test_finished_launcher_does_not_hide_its_live_executor(self):
        self.accepted()
        launcher = Mock(pid=202)
        launcher.poll.return_value = 0
        launcher.wait.return_value = 0
        self.runner._executor_processes.append((launcher, "request-1"))
        self.calls.wait_result = 258
        with self.assertRaises(psutil.TimeoutExpired):
            await self.runner.close(self.state)
        launcher.wait.assert_called_once()
        self.assertIn("request-1", self.runner._executor_identities)

    async def test_new_identity_for_same_request_is_not_hidden_by_confirmation(self):
        self.accepted()
        await self.runner._wait_accepted_executors(self.state)
        self.accepted(pid=202)
        self.calls.wait_result = 258
        with self.assertRaises(psutil.TimeoutExpired):
            await self.runner._wait_accepted_executors(self.state)

    async def test_confirmations_are_bounded_by_retained_results(self):
        for number in range(12):
            self.accepted(request=f"request-{number}")
            await self.runner._wait_accepted_executors(self.state)
            self.assertLessEqual(len(self.runner._completed_executor_identities), 1)
        self.state.stage_result_ids.clear()
        await self.runner._wait_accepted_executors(self.state)
        self.assertFalse(self.runner._completed_executor_identities)

    async def test_new_controller_rechecks_process_instead_of_inheriting_confirmation(
        self,
    ):
        self.accepted()
        await self.runner._wait_accepted_executors(self.state)
        replacement = StageRunner(None, self.runner._journal, None)
        self.calls.wait_result = 258
        with self.assertRaises(psutil.TimeoutExpired):
            await replacement._wait_accepted_executors(self.state)

    async def test_cancellation_does_not_confirm_a_wait_still_in_progress(self):
        self.accepted()
        entered, release = asyncio.Event(), asyncio.Event()
        original = asyncio.to_thread

        async def block_wait(function, *args, **kwargs):
            if getattr(function, "__name__", None) == "_wait_process_exit":
                entered.set()
                await release.wait()
            return await original(function, *args, **kwargs)

        with patch("core.experiments.stages.asyncio.to_thread", side_effect=block_wait):
            pending = asyncio.create_task(
                self.runner._wait_accepted_executors(self.state)
            )
            try:
                await asyncio.wait_for(entered.wait(), 2)
                pending.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await pending
            finally:
                release.set()
                await asyncio.gather(pending, return_exceptions=True)
        self.calls.wait_result = 258
        with self.assertRaises(psutil.TimeoutExpired):
            await self.runner._wait_accepted_executors(self.state)
