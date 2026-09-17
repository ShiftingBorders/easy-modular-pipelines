"""Approved snapshots.md B/E/F/H: real mixed-service DAG and rollback."""

import asyncio
import os
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import LoggingError
from core.runner_utils.runtimeio import read_json
from tests.helpers.dag import process_running
from tests.helpers.services import wait_for
from tests.helpers.snapshots import SnapshotWorkspace, file_inventory


class ExperimentSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = SnapshotWorkspace()
        self.addAsyncCleanup(self.w.close)

    @unittest.skipUnless(os.name == "nt", "Windows file sharing")
    async def test_temporary_endpoint_sharing_violation_does_not_fail_stage_startup(
        self,
    ):
        """I2/I7: a real exclusive OS handle delays startup without causing a retry."""
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        release = w.files.gate()
        entered = w.root / "endpoint-locked"
        original = subprocess.Popen

        def spawn(arguments, **kwargs):
            arguments = list(arguments)
            arguments[arguments.index("core.runner_utils.executor")] = (
                "tests.helpers.snapshot_executor"
            )
            kwargs["env"] = {
                **os.environ,
                "EMP_TEST_EXECUTOR_RELEASE": str(release),
                "EMP_TEST_EXECUTOR_ENTERED": str(entered),
                "EMP_TEST_EXECUTOR_MODE": "lock_publication",
            }
            return original(arguments, **kwargs)

        with patch("core.runner_utils.stages.subprocess.Popen", side_effect=spawn):
            step = asyncio.create_task(runner.step())
            try:
                await wait_for(entered.exists, 15)
                with self.assertRaises(PermissionError):
                    read_json(runner._state.active_attempt.endpoint_path)
                await asyncio.sleep(0.2)
                self.assertIsNone(runner.get_state()["error"])
                self.assertFalse(step.done())
            finally:
                release.touch(exist_ok=True)
            response = await asyncio.wait_for(step, 30)
        self.assertEqual(response["result"]["data"]["trail"], ["A"])
        self.assertEqual(
            runner._state.stage_attempt_numbers[w.stages[0]["stage_id"]], 1
        )

    async def test_snapshot_waits_for_executor_cleanup_and_excludes_its_token(self):
        """B5/E4: a real completed executor must close before snapshot file enumeration."""
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        release = w.files.gate()
        entered = w.root / "executor-cleanup-entered"
        original = subprocess.Popen

        def spawn(arguments, **kwargs):
            arguments = list(arguments)
            arguments[arguments.index("core.runner_utils.executor")] = (
                "tests.helpers.snapshot_executor"
            )
            kwargs["env"] = {
                **os.environ,
                "EMP_TEST_EXECUTOR_RELEASE": str(release),
                "EMP_TEST_EXECUTOR_ENTERED": str(entered),
            }
            return original(arguments, **kwargs)

        pending = None
        try:
            with patch("core.runner_utils.stages.subprocess.Popen", side_effect=spawn):
                await runner.step()
            await wait_for(entered.exists, 15)
            token = next(
                runner._state.experiment_directory.glob(
                    "shared_artifacts/**/executor.lock.*.token"
                )
            )
            self.assertTrue(token.is_file())
            pending = asyncio.create_task(runner.snapshot("after cleanup"))
            await asyncio.sleep(0.2)
            self.assertFalse(pending.done())
        finally:
            release.touch(exist_ok=True)
        result = await asyncio.wait_for(pending, 30)
        self.assertFalse(token.exists())
        self.assertFalse(
            any(
                path.name.startswith("executor.lock.") and path.suffix == ".token"
                for path in w.archive(result["snapshot_id"]).rglob("*")
            )
        )

    async def test_manual_snapshot_rollback_restores_cursor_files_and_both_services(
        self,
    ):
        """B1/B5/B6/F3-F5: S1 after A, mutate B/RAM, rollback, then both cycles."""
        async with asyncio.timeout(150):
            w = self.w
            runner = await w.launch()
            first = await runner.step()
            self.assertEqual(first["result"]["data"]["trail"], ["A"])
            self.assertEqual(await w.value(10), 10)
            snapshot = await runner.snapshot("S1")
            archive = w.archive(snapshot["snapshot_id"])
            saved = file_inventory(archive)
            manifest = read_json(archive / "manifest.json")
            self.assertTrue(manifest["state"]["pending_advance"])
            self.assertIsNone(manifest["state"]["active_attempt"])
            self.assertEqual(
                set(manifest["services"]),
                {w.socket["service_id"], w.commands["service_id"]},
            )
            self.assertFalse(any("/runner/" in key for key in saved))
            self.assertFalse(any("shared_artifacts/services/" in key for key in saved))
            self.assertFalse(
                any(
                    Path(key).name
                    in {
                        "service.token",
                        "launch.json",
                        "context.json",
                        "process.json",
                        "ready.json",
                        "executor.token",
                    }
                    for key in saved
                )
            )
            self.assertFalse(any(key.endswith(("-wal", "-shm")) for key in saved))
            old = runner._state.services[w.socket["service_id"]]
            old_client = OperationLogger(runner._journal.reader_config_path)
            old_client.open()
            self.addCleanup(old_client.close)
            generation = old_client.get_journal_info()["generation"]
            old_client.close()
            await runner.step()
            second_result = runner._state.last_result_id
            await w.value(20)
            await runner.snapshot("S2")
            reply = await runner.rollback(snapshot["snapshot_id"])
            self.assertEqual(reply["mode"], "paused")
            await asyncio.wait_for(runner._ready.wait(), 30)
            self.assertEqual(runner.get_state()["phase"], "waiting")
            self.assertEqual(runner._state.cycle_number, 1)
            self.assertEqual(runner._state.stage_position, 1)
            self.assertTrue(runner._state.pending_advance)
            self.assertIsNone(runner._journal.client.read_command_result(second_result))
            self.assertIsNotNone(
                runner._journal.client.read_command_result(runner._state.last_result_id)
            )
            self.assertFalse(process_running(old.process_identity["pid"]))
            self.assertNotEqual(
                runner._state.services[old.service_id].service_instance_id,
                old.service_instance_id,
            )
            self.assertEqual(await w.value(), 10)
            await wait_for(lambda: len(w.actions()) >= 3)
            self.assertEqual(
                [item["action"] for item in w.actions()], ["start", "stop", "start"]
            )
            self.assertNotEqual(
                runner._journal.client.get_journal_info()["generation"], generation
            )
            with self.assertRaises(LoggingError):
                old_client.open()
            self.assertEqual(file_inventory(archive), saved)
            after = await runner.step()
            self.assertEqual(after["result"]["data"]["trail"], ["A", "B"])
            for expected in (["A", "B", "C"], ["A"], ["A", "B"], ["A", "B", "C"]):
                reply = await runner.step()
                self.assertEqual(reply["result"]["data"]["trail"], expected)
            self.assertEqual(runner.get_state()["phase"], "completed")
            final = w.manifests()[-1]
            self.assertEqual(final["kind"], "final")
            self.assertEqual(final["state"]["phase"], "completed")
            self.assertTrue(
                all(item.stopped for item in runner._state.services.values())
            )

    async def test_manual_snapshot_at_start_and_periodic_modes(self):
        """B3/B6: off/after_stage/after_epoch have exact boundaries and final snapshot."""
        for mode, expected_regular in (
            ("off", 0),
            ("after_stage", 5),
            ("after_epoch", 1),
        ):
            with self.subTest(mode=mode):
                w = SnapshotWorkspace(mode=mode)
                try:
                    runner = await w.launch()
                    initial = await runner.snapshot("initial")
                    self.assertFalse(
                        read_json(w.archive(initial["snapshot_id"]) / "manifest.json")[
                            "state"
                        ]["pending_advance"]
                    )
                    for _ in range(6):
                        await runner.step()
                    manifests = w.manifests()
                    self.assertEqual(len(manifests), expected_regular + 2)
                    periodic = [
                        item
                        for item in manifests
                        if item["kind"] == "regular" and item["label"] is None
                    ]
                    self.assertEqual(len(periodic), expected_regular)
                    if mode == "after_epoch":
                        self.assertEqual(
                            (
                                periodic[0]["state"]["cycle_number"],
                                periodic[0]["state"]["stage_position"],
                            ),
                            (1, 3),
                        )
                    self.assertEqual(manifests[-1]["kind"], "final")
                finally:
                    await w.close()

    async def test_continue_creates_paused_clone_without_changing_source(self):
        """H1/H2/H4: inherited artifacts keep origin; clone resumes at B."""
        w = self.w
        runner = await w.launch()
        await runner.step()
        await w.value(10)
        stopped = await runner.stop()
        self.assertEqual(stopped["phase"], "stopped")
        source = runner._state.experiment_directory
        source_id = runner._state.experiment_id
        # Closing the source completes SQLite's WAL checkpoint before measuring
        # immutability of continuation performed by a different runner owner.
        await runner.close()
        runner = w.replacement()
        before = file_inventory(source)
        archive_root = w.root / "snapshots" / source.name
        archive_before = file_inventory(archive_root)
        response = await runner.run(experiment_id=source_id, continue_run=True)
        await asyncio.wait_for(runner._ready.wait(), 90)
        self.assertEqual(runner.get_state()["phase"], "waiting", runner.get_state())
        self.assertNotEqual(response["experiment_id"], source_id)
        self.assertNotEqual(runner._state.experiment_directory, source)
        self.assertEqual(runner._state.mode, "paused")
        self.assertEqual(await w.value(), 10)
        self.assertEqual(
            runner._state.stage_result_origins[w.stages[0]["stage_id"]], source_id
        )
        artifact = runner._journal.client.read_command_result(
            runner._state.last_result_id
        )["event"]["context"]
        self.assertEqual(artifact["experiment_id"], source_id)
        self.assertEqual((await runner.step())["result"]["data"]["trail"], ["A", "B"])
        self.assertEqual(file_inventory(source), before)
        self.assertEqual(file_inventory(archive_root), archive_before)

    async def test_rollback_at_cycle_boundary_and_repeated_rollback_keep_pause(self):
        """B6/F4/G5: a saved completed cycle advances once after each explicit rollback."""
        w = self.w
        runner = await w.launch()
        for _ in range(3):
            await runner.step()
        await w.value(10)
        snapshot = await runner.snapshot("end of first cycle")
        for _ in range(2):
            await runner.step()
        generations = set()
        for _ in range(2):
            await runner.rollback(snapshot["snapshot_id"])
            await asyncio.wait_for(runner._ready.wait(), 30)
            generations.add(runner._journal.client.get_journal_info()["generation"])
            self.assertEqual(runner._state.mode, "paused")
            self.assertEqual(
                (runner._state.cycle_number, runner._state.stage_position), (1, 3)
            )
            self.assertEqual(await w.value(), 10)
            result = await runner.step()
            self.assertEqual(result["result"]["data"]["trail"], ["A"])
            self.assertEqual(
                (runner._state.cycle_number, runner._state.stage_position), (2, 1)
            )
        self.assertEqual(len(generations), 2)

    async def test_continue_rejects_active_or_finished_source_and_rerun_starts_cycle_one(
        self,
    ):
        """H3/H4: continue requires remaining work; rerun resets execution in a new run."""
        w = SnapshotWorkspace(services=False, cycles=1)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        source_id = runner._state.experiment_id
        with self.assertRaises(RuntimeError):
            await runner.run(experiment_id=source_id, continue_run=True)
        with self.assertRaises(ValueError):
            await runner.run(
                w.template_path, experiment_id=source_id, continue_run=True
            )
        for _ in range(3):
            await runner.step()
        await asyncio.wait_for(asyncio.shield(runner._task), 15)
        registry = read_json(w.root / "experiments.json")
        await runner.run(experiment_id=source_id, continue_run=True)
        await asyncio.wait_for(runner._ready.wait(), 30)
        self.assertEqual(runner.get_state()["phase"], "failed")
        self.assertIn("no remaining cycles", runner.get_state()["error"]["message"])
        self.assertEqual(read_json(w.root / "experiments.json"), registry)
        await runner.rerun("experiment", experiment_id=source_id)
        await asyncio.wait_for(runner._ready.wait(), 30)
        self.assertEqual(runner.get_state()["phase"], "waiting")
        self.assertNotEqual(runner._state.experiment_id, source_id)
        self.assertEqual(
            (runner._state.cycle_number, runner._state.stage_position), (1, 1)
        )
        self.assertIsNone(runner._state.last_result)
        self.assertEqual((await runner.step())["result"]["data"]["trail"], ["A"])

    async def test_running_stage_rejects_snapshot_without_interrupting_it(self):
        """B2: a stage-free boundary is required even when pause has been requested."""
        w = self.w
        gate = w.files.gate()
        w.stages[0]["settings"]["gate"] = str(gate)
        runner = await w.launch()
        step = asyncio.create_task(runner.step())
        await wait_for(
            lambda: (
                runner._state.active_attempt is not None
                and runner._state.active_attempt.process_identity is not None
            )
        )
        pid = runner._state.active_attempt.process_identity["pid"]
        with self.assertRaises(RuntimeError):
            await runner.snapshot()
        self.assertTrue(process_running(pid))
        self.assertEqual(w.manifests(), [])
        gate.touch()
        self.assertEqual((await step)["result"]["data"]["trail"], ["A"])
        self.assertTrue((await runner.snapshot())["valid"])
