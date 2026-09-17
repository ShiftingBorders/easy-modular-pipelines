"""Approved snapshots.md A5/I: actual owner loss, live work and journal recovery."""

import asyncio
import unittest
from unittest.mock import patch
from uuid import uuid4

import psutil

from core.runner_utils.runtimeio import read_json, write_json
from tests.helpers.dag import process_running, terminate_owned
from tests.helpers.services import wait_for
from tests.helpers.snapshots import SnapshotWorkspace


class ExperimentRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_partially_stopped_services_stay_paused_with_remaining_peer_observed(
        self,
    ):
        """I1/I7: one stopped service must not prevent observing the still-live peer."""
        w = SnapshotWorkspace()
        self.addAsyncCleanup(w.close)
        w.template["services"].reverse()
        runner = await w.launch()
        commands_id = w.commands["service_id"]
        socket_id = w.socket["service_id"]
        socket = runner._state.services[socket_id]
        before = socket.last_status["observed_monotonic"]
        self.assertTrue(
            (await runner._services.stop_all(runner._state, service_ids={commands_id}))[
                commands_id
            ]["stopped"]
        )
        experiment_id = runner._state.experiment_id
        await runner.close()
        runner = w.replacement()
        await asyncio.wait_for(runner.recover(experiment_id), 30)
        recovered = runner._state.services[socket_id]
        await wait_for(lambda: recovered.last_status["observed_monotonic"] > before, 5)
        self.assertEqual(recovered.service_instance_id, socket.service_instance_id)
        self.assertTrue(process_running(recovered.process_identity["pid"]))
        self.assertTrue(runner._state.services[commands_id].stopped)
        self.assertEqual(runner.get_state()["mode"], "paused")
        with self.assertRaises(RuntimeError):
            await runner.resume()

    async def test_unresponsive_executor_cannot_rerun_a_stage_whose_stop_is_unconfirmed(
        self,
    ):
        """I3: a suspended real executor cannot confirm stopping its live stage."""
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        gate = w.files.gate()
        w.stages[0]["settings"]["gate"] = str(gate)
        w.template["unknown_state"]["on_timeout"] = "rerun"
        runner = await w.launch()
        step = asyncio.create_task(runner.step())
        await wait_for(
            lambda: (
                runner._state.active_attempt is not None
                and runner._state.active_attempt.process_identity is not None
            )
        )
        attempt = runner._state.active_attempt
        record = read_json(attempt.artifacts_directory / "process.json")
        experiment_id = runner._state.experiment_id
        await runner.close()
        await asyncio.gather(step, return_exceptions=True)
        executor = psutil.Process(record["executor"]["pid"])
        executor.suspend()
        try:
            self.assertTrue(process_running(record["stage"]["pid"]))
            runner = w.replacement()
            await runner.recover(experiment_id)
            await wait_for(lambda: runner.get_state()["phase"] == "failed", 60)
            self.assertFalse(runner.get_state()["termination_confirmed"])
            self.assertTrue(process_running(record["stage"]["pid"]))
            self.assertEqual(runner._state.stage_attempt_numbers[attempt.stage_id], 1)
            with self.assertRaisesRegex(RuntimeError, "unconfirmed"):
                await runner.run(w.template_path)
        finally:
            executor.resume()
            gate.touch()

    async def test_stage_survives_actual_owner_crash_and_is_not_repeated(self):
        """I1/I2: an independently owned executor survives os._exit of its runner."""
        w = SnapshotWorkspace()
        self.addAsyncCleanup(w.close)
        gate = w.files.gate()
        w.stages[0]["settings"]["gate"] = str(gate)
        runner = await w.launch()
        experiment_id = runner._state.experiment_id
        await runner.close()
        owner = await w.start_owner("stage", "stage_running")
        self.assertEqual(await asyncio.wait_for(owner.wait(), 90), 23)
        saved = read_json(runner._state.experiment_directory / "runner/state.json")
        attempt = saved["active_attempt"]
        pid = attempt["process_identity"]["pid"]
        self.assertTrue(process_running(pid))
        runner = w.replacement()
        await runner.recover(experiment_id)
        self.assertEqual(runner._state.active_attempt.attempt_id, attempt["attempt_id"])
        self.assertEqual(runner._state.active_attempt.process_identity["pid"], pid)
        self.assertEqual(runner._state.mode, "paused")
        gate.touch()
        await wait_for(lambda runner=runner: runner._state.active_attempt is None, 30)
        self.assertEqual(runner._state.last_result["trail"], ["A"])
        self.assertEqual(
            runner._state.stage_attempt_numbers[w.stages[0]["stage_id"]], 1
        )
        self.assertEqual(runner._state.stage_position, 1)
        self.assertTrue(runner._state.pending_advance)
        self.assertEqual((await runner.step())["result"]["data"]["trail"], ["A", "B"])

    async def test_missing_and_corrupt_optional_state_use_committed_checkpoint(self):
        """I4/A4: restore progress and applied YAML without trusting the optional file."""
        for fault in ("missing", "corrupt", "stale"):
            with self.subTest(fault=fault):
                w = SnapshotWorkspace(services=False)
                self.addAsyncCleanup(w.close)
                runner = await w.launch()
                initial = read_json(
                    runner._state.experiment_directory / "runner/state.json"
                )
                await runner.step()
                root, experiment_id = (
                    runner._state.experiment_directory,
                    runner._state.experiment_id,
                )
                await runner.close()
                path = root / "runner/state.json"
                if fault == "corrupt":
                    path.write_text("{broken", encoding="utf-8")
                elif fault == "stale":
                    write_json(path, initial)
                else:
                    path.unlink()
                (root / "experiment.yaml").write_text(
                    "unapplied: edit", encoding="utf-8"
                )
                runner = w.replacement()
                await runner.recover(experiment_id)
                self.assertEqual(runner._state.last_result["trail"], ["A"])
                self.assertNotIn("unapplied", runner._state.template)
                self.assertEqual(
                    (await runner.step())["result"]["data"]["trail"], ["A", "B"]
                )
                await w.close()

    async def test_committed_start_intent_recovers_stage_missing_from_optional_state(
        self,
    ):
        """I4: crash recovery finds a real launched attempt despite failed state writes."""
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        gate = w.files.gate()
        w.stages[0]["settings"]["gate"] = str(gate)
        runner = await w.launch()
        experiment_id = runner._state.experiment_id
        root = runner._state.experiment_directory
        await runner.close()
        owner = await w.start_owner("stage", "stage_without_state")
        self.assertEqual(await asyncio.wait_for(owner.wait(), 90), 23)
        self.assertIsNone(read_json(root / "runner/state.json")["active_attempt"])
        process_files = list(root.glob("shared_artifacts/epoch_1/**/process.json"))
        self.assertEqual(len(process_files), 1)
        record = read_json(process_files[0])
        runner = w.replacement()
        await runner.recover(experiment_id)
        self.assertEqual(runner._state.active_attempt.attempt_id, record["attempt_id"])
        self.assertEqual(runner._state.active_attempt.process_identity, record["stage"])
        gate.touch()
        await wait_for(lambda: runner._state.active_attempt is None, 30)
        self.assertEqual(runner._state.last_result["trail"], ["A"])
        self.assertEqual(
            runner._state.stage_attempt_numbers[w.stages[0]["stage_id"]], 1
        )

    async def test_live_service_work_and_pending_request_ids_survive_detach(self):
        """I1/I5: queue ownership and original sent request survive a new runner."""
        w = SnapshotWorkspace()
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        sid = w.socket["service_id"]
        gate = w.files.gate()
        request = asyncio.create_task(
            runner._services.request(
                runner._state, sid, "echo", {"gate": str(gate), "n": 1}
            )
        )
        await wait_for(
            lambda runner=runner: runner._state.services[sid].active_request is not None
        )
        queued = asyncio.create_task(
            runner._services.request(runner._state, sid, "echo", {"n": 2})
        )
        await wait_for(lambda: len(runner._state.services[sid].pending_requests) == 1)
        instance = runner._state.services[sid]
        active_id = instance.active_request["request_id"]
        queued_id = instance.pending_requests[0]["request_id"]
        sent_at = instance.active_request["sent_monotonic"]
        experiment_id = runner._state.experiment_id
        await runner.close()
        await asyncio.gather(request, queued, return_exceptions=True)
        runner = w.replacement()
        await runner.recover(experiment_id)
        recovered = runner._state.services[sid]
        self.assertEqual(recovered.service_instance_id, instance.service_instance_id)
        self.assertEqual(recovered.active_request["request_id"], active_id)
        self.assertEqual(recovered.active_request["sent_monotonic"], sent_at)
        self.assertEqual(recovered.pending_requests[0]["request_id"], queued_id)
        gate.touch()
        await wait_for(
            lambda: recovered.active_request is None and not recovered.pending_requests,
            30,
        )
        starts = [
            row["request_id"]
            for row in w.files.trace(w.socket)
            if row["event"] == "work_started" and row["command"] == "echo"
        ]
        self.assertEqual(starts, [active_id, queued_id])

    async def test_stale_unsent_queue_cannot_replay_an_already_sent_command(self):
        """I5: a failed state publication must not turn an uncertain send into new work."""
        w = SnapshotWorkspace()
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        sid = w.socket["service_id"]
        gate = w.files.gate()
        save = runner._state_store.save

        def publish(state):
            if state.services[sid].active_request is not None:
                raise OSError("optional state disk failure")
            return save(state)

        with patch.object(runner._state_store, "save", side_effect=publish):
            request = asyncio.create_task(
                runner._services.request(
                    runner._state, sid, "echo", {"gate": str(gate)}
                )
            )
            await wait_for(
                lambda: any(
                    row["event"] == "work_started" and row["command"] == "echo"
                    for row in w.files.trace(w.socket)
                )
            )
            experiment_id = runner._state.experiment_id
            await runner.close()
            await asyncio.gather(request, return_exceptions=True)
        runner = w.replacement()
        with self.assertRaisesRegex(RuntimeError, "stop"):
            await asyncio.wait_for(runner.recover(experiment_id), 60)
        starts = [
            row
            for row in w.files.trace(w.socket)
            if row["event"] == "work_started" and row["command"] == "echo"
        ]
        self.assertEqual(len(starts), 1)
        self.assertEqual(runner.get_state()["phase"], "failed")

    async def test_live_foreign_owner_is_refused_and_dead_owner_can_be_recovered(self):
        """A5/I7: actual live ownership rejects recovery without hanging or killing it."""
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        experiment_id = str(uuid4())
        owner = await w.start_owner(
            "run", "ready", action="hang", experiment_id=experiment_id
        )
        await wait_for(lambda: (w.root / "owner-fault.json").is_file(), 30)
        identity = read_json(w.root / "owner-fault.json")["process"]
        with self.assertRaisesRegex(RuntimeError, "still alive"):
            await w.runner.recover(experiment_id)
        self.assertTrue(process_running(identity["pid"]))
        terminate_owned(identity)
        await asyncio.wait_for(owner.wait(), 30)
        await w.runner.recover(experiment_id)
        self.assertEqual(w.runner.get_state()["phase"], "waiting")

    async def test_reused_pid_identity_does_not_authorize_killing_foreign_process(self):
        """A5: an OS identity with a different creation value is a different owner."""
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        experiment_id = runner._state.experiment_id
        await runner.close()
        state_path = runner._state.experiment_directory / "runner/state.json"
        state = read_json(state_path)
        await w.start_owner("run", "ready", action="hang", experiment_id=str(uuid4()))
        await wait_for(lambda: (w.root / "owner-fault.json").exists(), 30)
        unrelated = read_json(w.root / "owner-fault.json")["process"]
        state["owner_identity"] = {**unrelated, "created_at_os": 1}
        write_json(state_path, state)
        recovered = w.replacement()
        await recovered.recover(experiment_id)
        self.assertEqual(recovered.get_state()["phase"], "waiting")
        self.assertTrue(process_running(unrelated["pid"]))

    async def test_unknown_stage_policies_require_confirmed_old_termination(self):
        """I3: pause, skip, rerun and the recovery limit use the original attempt."""
        for action, limit in (("pause", 3), ("skip", 3), ("rerun", 3), ("rerun", 1)):
            with self.subTest(action=action, limit=limit):
                w = SnapshotWorkspace(services=False)
                self.addAsyncCleanup(w.close)
                gate = w.files.gate()
                w.stages[0]["settings"]["gate"] = str(gate)
                w.template["unknown_state"]["on_timeout"] = action
                w.template["unknown_state"]["recovery_limit"] = limit
                runner = await w.launch()
                step = asyncio.create_task(runner.step())
                await wait_for(
                    lambda runner=runner: (
                        runner._state.active_attempt is not None
                        and runner._state.active_attempt.process_identity is not None
                    )
                )
                original = runner._state.active_attempt
                experiment_id = runner._state.experiment_id
                await runner.close()
                await asyncio.gather(step, return_exceptions=True)
                record = read_json(original.artifacts_directory / "process.json")
                # Lose both participants before result publication; a missing file
                # can no longer simulate unknown state in the journal protocol.
                terminate_owned(record["executor"])
                terminate_owned(record["stage"])
                await wait_for(
                    lambda record=record: (
                        not process_running(record["executor"]["pid"])
                    ),
                    30,
                )
                gate.touch()
                runner = w.replacement()
                await runner.recover(experiment_id)
                await wait_for(
                    lambda runner=runner: (
                        runner._state.unknown_state_recovery_count == 1
                    ),
                    30,
                )
                if limit == 1:
                    await wait_for(
                        lambda runner=runner: runner.get_state()["phase"] == "failed",
                        40,
                    )
                    self.assertEqual(
                        runner._state.stage_attempt_numbers[original.stage_id], 1
                    )
                elif action == "pause":
                    await wait_for(
                        lambda runner=runner: (
                            runner._state.active_attempt.outcome == "unknown"
                        ),
                        10,
                    )
                    with self.assertRaises(RuntimeError):
                        await runner.step()
                    self.assertEqual(
                        runner._state.stage_attempt_numbers[original.stage_id], 1
                    )
                else:
                    await wait_for(
                        lambda runner=runner: runner._state.active_attempt is None, 45
                    )
                    self.assertTrue(runner._state.pending_advance)
                    self.assertEqual(
                        runner._state.stage_attempt_numbers[original.stage_id],
                        2 if action == "rerun" else 1,
                    )
                    self.assertFalse(process_running(original.process_identity["pid"]))
                await w.close()
