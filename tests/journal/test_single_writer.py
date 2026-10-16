"""Approved W01-W08: real writer recovery without changing the logging API."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import time
import unittest
from contextlib import closing, suppress
from unittest.mock import patch
from uuid import uuid4

from core.experiments.snapshots import ExperimentSnapshots
from core.journal.events import JournalGenerationChanged, LoggingStorageError
from core.journal.logger import OperationLogger
from core.participants.connection import ParticipantConnection
from core.primitives.json_files import read_json
from tests.helpers.dag import process_running, wait_until
from tests.helpers.journal_writer import (
    WriterFixture,
    database_snapshot,
    require_writer,
    stored_events,
)
from tests.helpers.logging_fixtures import write_context_settings
from tests.helpers.logging_process import LoggingProcess, existing_settings
from tests.helpers.snapshots import SnapshotWorkspace


class SingleWriterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        supervisor_type, settings_type = require_writer(self)
        self.w = WriterFixture(supervisor_type, settings_type)
        self.addAsyncCleanup(self.w.close)
        await self.w.open()

    async def test_recovers_before_commit_and_continues_same_attempt_w01(self):
        """W01: kill at an uncommitted real stage result, without rerunning it."""
        w = self.w
        workspace = await w.experiment()
        runner = workspace.runner
        database = runner._state.experiment_directory / "journals/events.sqlite"
        first_stage = workspace.stages[0]["stage_id"]
        gate = w.faults.arm("stage.finished", "before_commit", stage_id=first_stage)
        step = w.track(runner.step())
        entered = await gate.entered()
        event_id = entered["event"]["event_id"]
        self.assertNotEqual(entered["process"]["pid"], os.getpid())
        self.assertFalse(step.done(), "The stage must still await COMMIT.")
        self.assertNotIn(
            event_id, [event["event_id"] for event in stored_events(database)]
        )

        failure_started = time.monotonic()
        await w.faults.kill(entered["process"])
        supervisor = runner._journal._supervisor
        status = await wait_until(
            lambda: (
                supervisor.get_status()
                if supervisor.get_status().get("failure_detected_at", 0)
                >= failure_started
                else None
            ),
            timeout=2,
        )
        self.assertLessEqual(status["failure_detected_at"] - failure_started, 2)
        result = await asyncio.wait_for(
            asyncio.shield(step), max(0.1, 60 - (time.monotonic() - failure_started))
        )
        self.assertEqual(result["result"]["data"]["trail"], ["A"])
        self.assertEqual(runner.get_state()["phase"], "waiting")
        events = stored_events(database)
        self.assertEqual(sum(event["event_id"] == event_id for event in events), 1)
        self.assertEqual(runner._state.stage_attempt_numbers[first_stage], 1)
        trace_path = runner._state.experiment_directory / "shared_data/trace.jsonl"
        trace = [
            json.loads(line)
            for line in trace_path.read_text(encoding="utf-8").splitlines()
        ]
        starts = [entry for entry in trace if entry["event"] == "start"]
        self.assertEqual([entry["label"] for entry in starts], ["A"])
        following = await asyncio.wait_for(runner.step(), 30)
        self.assertEqual(following["result"]["data"]["trail"], ["A", "B"])

    async def test_lost_commit_ack_preserves_event_and_canonical_result_w02(self):
        """W02: replay ordinary writes and an aliasing runner confirmation once."""
        w = self.w
        identity = w.logger.get_journal_info()
        for kind in ("ordinary", "command"):
            with self.subTest(kind=kind):
                canonical_id = None
                request_id = "single-writer-confirmation"
                if kind == "command":
                    canonical_id = w.logger.record_command_result(
                        request_id,
                        {"value": 7},
                        author="participant",
                        outcome="succeeded",
                    )
                gate = w.faults.arm(
                    "single_writer.ack" if kind == "ordinary" else "command.result",
                    "after_commit",
                    request_id=None if kind == "ordinary" else request_id,
                )
                if kind == "ordinary":
                    pending = w.record(
                        w.logger.record_event, "single_writer.ack", {"value": 7}
                    )
                else:
                    pending = w.record(
                        w.logger.record_command_result,
                        request_id,
                        {"value": 7},
                        author="runner",
                        outcome="succeeded",
                    )
                entered = await gate.entered()
                committed = database_snapshot(w.database)
                expected_id = canonical_id or entered["event"]["event_id"]
                self.assertIn(
                    expected_id,
                    [json.loads(row[-1])["event_id"] for row in committed["events"]],
                )
                self.assertFalse(
                    pending.done(), "COMMIT is not yet an ACK to the client."
                )
                if canonical_id is not None:
                    self.assertNotEqual(entered["event"]["event_id"], canonical_id)

                await w.faults.kill(entered["process"])
                self.assertEqual(
                    await asyncio.wait_for(asyncio.shield(pending), 30), expected_id
                )
                self.assertEqual(database_snapshot(w.database), committed)
                self.assertEqual(w.logger.get_journal_info(), identity)
                if kind == "command":
                    result = w.logger.read_command_result(request_id)
                    self.assertEqual(result["author"], "runner")
                    self.assertEqual(result["event_id"], canonical_id)
                    self.assertEqual(len(result["observations"]), 2)

    async def test_watchdog_replaces_hung_writers_without_overlap_w03(self):
        """W03: the supervisor, not the test, terminates two stuck writers."""
        w = self.w
        gate = w.faults.arm("single_writer.hung", "before_commit", occurrences=2)
        pending = w.record(w.logger.record_event, "single_writer.hung", {"value": 3})
        first = await gate.entered(0)
        second = await gate.entered(1)
        self.assertNotEqual(first["process"], second["process"])
        self.assertFalse(process_running(first["process"]["pid"]))
        self.assertEqual(w.faults.alive(), [second["process"]["pid"]])
        event_id = await asyncio.wait_for(asyncio.shield(pending), 30)
        self.assertFalse(process_running(second["process"]["pid"]))
        self.assertEqual(len(w.faults.alive()), 1)
        self.assertEqual(first["event"], second["event"])
        self.assertEqual(event_id, first["event"]["event_id"])
        self.assertEqual(
            sum(event["event_id"] == event_id for event in stored_events(w.database)), 1
        )
        w.logger.record_event("single_writer.after_recovery")

    async def test_bad_request_and_handler_failure_are_isolated_w04(self):
        """W04: malformed transport and a failed transaction leave peers usable."""
        w = self.w
        original = w.supervisor.get_status()["writer_instance_id"]
        endpoint = read_json(w.supervisor.endpoint_path)["endpoint"]
        reader, writer = await asyncio.open_connection(
            endpoint["host"], endpoint["port"]
        )
        try:
            writer.write((1).to_bytes(8, "big") + b"{")
            await writer.drain()
            try:
                received = await asyncio.wait_for(reader.read(1), 5)
            except ConnectionResetError:
                received = b""
            self.assertEqual(received, b"")
        finally:
            writer.close()
            with suppress(ConnectionResetError):
                await writer.wait_closed()

        good = await w.client(w.supervisor.client_config_path)
        gate = w.faults.arm("single_writer.rejected", "before_commit", action="raise")
        rejected = w.record(w.logger.record_event, "single_writer.rejected")
        entered = await gate.entered()
        with self.assertRaises(LoggingStorageError):
            await asyncio.wait_for(asyncio.shield(rejected), 15)
        accepted = await asyncio.wait_for(
            w.record(good.record_event, "single_writer.healthy"), 15
        )
        self.assertEqual(w.supervisor.get_status()["writer_instance_id"], original)
        ids = [event["event_id"] for event in stored_events(w.database)]
        self.assertNotIn(entered["event"]["event_id"], ids)
        self.assertEqual(ids.count(accepted), 1)

    async def test_full_queue_keeps_control_and_shutdown_responsive_w05(self):
        """W05: pressure retains writes; a later outage cannot prevent stopping."""
        w = self.w
        self.assertEqual(w.settings_type().queue_capacity, 100)
        w.settings = w.policy(stall_timeout=30)
        await self._assert_restart_freezes_deadline()
        workspace = await w.experiment(services=True)
        await self._assert_queue_retains_writes(workspace)
        await self._assert_stop_during_outage(workspace)

    async def _assert_restart_freezes_deadline(self):
        w = self.w
        workspace = SnapshotWorkspace(services=True)
        w.workspaces.append(workspace)
        gate_path = workspace.files.gate()
        workspace.template["stages"][0]["timeout_seconds"] = 60
        workspace.template["stages"][0]["settings"]["gate"] = str(gate_path)
        runner = await workspace.launch()
        step = w.track(runner.step())
        await wait_until(lambda: runner._state.active_attempt is not None, timeout=20)
        attempt = runner._state.active_attempt
        await wait_until(
            (attempt.artifacts_directory / "ready.json").is_file, timeout=30
        )
        deadline_before = runner._stages._deadline(attempt)
        held = w.faults.hold_start()
        await w.faults.kill(runner._journal._supervisor.get_status()["process"])
        await held.entered()
        paused_at = time.monotonic()
        await self._assert_heartbeat(workspace)
        await asyncio.sleep(2.5)
        elapsed_pause = time.monotonic() - paused_at
        held.release()
        await wait_until(
            lambda: runner._journal._supervisor.get_status()["state"] == "ready",
            timeout=30,
        )
        await wait_until(
            lambda: (
                runner._stages._deadline(attempt) >= deadline_before + elapsed_pause
            ),
            timeout=5,
        )
        self.assertGreaterEqual(
            runner._stages._deadline(attempt) - deadline_before, elapsed_pause
        )
        gate_path.touch()
        result = await asyncio.wait_for(asyncio.shield(step), 30)
        self.assertEqual(result["result"]["data"]["trail"], ["A"])

    async def _assert_queue_retains_writes(self, workspace):
        w = self.w
        runner = workspace.runner
        supervisor = runner._journal._supervisor
        clients = [await w.client(runner._journal.reader_config_path) for _ in range(3)]
        database = runner._state.experiment_directory / "journals/events.sqlite"
        gate = w.faults.arm(
            "stage.finished", "before_commit", stage_id=workspace.stages[0]["stage_id"]
        )
        step = w.track(runner.step())
        await gate.entered()
        writes = [
            w.record(client.record_event, "single_writer.queued", {"number": number})
            for number, client in enumerate(clients)
        ]
        await wait_until(
            lambda: supervisor.get_status()["queue_depth"] == 2, timeout=10
        )
        self.assertEqual(supervisor.get_status()["queue_capacity"], 2)
        self.assertTrue(all(not task.done() for task in writes))
        self.assertNotEqual(runner.get_state()["phase"], "failed")
        await self._assert_heartbeat(workspace)
        gate.release()
        identifiers = await asyncio.wait_for(asyncio.gather(*writes), 30)
        await asyncio.wait_for(asyncio.shield(step), 30)
        events = stored_events(database)
        for identifier in identifiers:
            self.assertEqual(
                sum(event["event_id"] == identifier for event in events), 1
            )

    async def _assert_heartbeat(self, workspace):
        runner = workspace.runner
        service = runner._state.services[workspace.socket["service_id"]]
        identity = {
            "experiment_id": runner._state.experiment_id,
            "participant_id": service.service_id,
            "participant_instance_id": service.service_instance_id,
        }
        connection = ParticipantConnection(service.endpoint_path, identity)
        await connection.connect(timeout_seconds=5)
        try:
            response = await connection.request(
                str(uuid4()), "heartbeat", {}, timeout_seconds=5
            )
            self.assertEqual(response["result"], "success")
        finally:
            await connection.close()

    async def _assert_stop_during_outage(self, workspace):
        w = self.w
        runner = workspace.runner
        stopped_gate = w.faults.arm(
            "stage.finished",
            "before_commit",
            stage_id=workspace.stages[1]["stage_id"],
            occurrences=20,
        )
        interrupted_step = w.track(runner.step())
        await stopped_gate.entered()
        stopped = await asyncio.wait_for(runner.stop(), 15)
        self.assertEqual(stopped["phase"], "stopped")
        self.assertTrue(stopped["termination_confirmed"])
        self.assertTrue(
            all(service.stopped for service in runner._state.services.values())
        )
        position = runner.get_state()["stage_position"]
        stopped_gate.release()
        await asyncio.wait_for(
            asyncio.gather(interrupted_step, return_exceptions=True), 30
        )
        self.assertEqual(runner.get_state()["phase"], "stopped")
        self.assertEqual(runner.get_state()["stage_position"], position)

    async def test_rollback_closes_readers_and_fences_old_generation_w06(self):
        """W06: keep clients open across the real directory replacement barrier."""
        w = self.w
        workspace = await w.experiment()
        runner = workspace.runner
        root = runner._state.experiment_directory
        snapshot = await runner.snapshot("single-writer rollback")
        old_writer = await w.client(runner._journal.reader_config_path)
        old_reader = await w.client(runner._journal.reader_config_path, read_only=True)
        previous = old_reader.get_journal_info()
        await runner.step()
        entered, release = asyncio.Event(), asyncio.Event()
        original = ExperimentSnapshots._replace_restore_directory

        async def replace(owner, source, target):
            if source == root:
                entered.set()
                await asyncio.wait_for(release.wait(), 15)
            await original(owner, source, target)

        with patch.object(ExperimentSnapshots, "_replace_restore_directory", replace):
            rollback = w.track(runner.rollback(snapshot["snapshot_id"]))
            try:
                await asyncio.wait_for(entered.wait(), 30)
                supervisor = runner._journal._supervisor
                status = supervisor.get_status()
                self.assertIn(status["state"], ("quiescing", "stopped"))
                if status["process"] is not None and process_running(
                    status["process"]["pid"]
                ):
                    await w.faults.kill(status["process"])
                starts = len(w.faults.processes)
                # Observe five configured heartbeat intervals after the confirmed
                # barrier. This does not guess when the filesystem operation ran.
                await asyncio.sleep(0.5)
                self.assertEqual(len(w.faults.processes), starts)
            finally:
                release.set()
            await asyncio.wait_for(asyncio.shield(rollback), 30)

        current = runner._journal.client.get_journal_info()
        self.assertEqual(current["journal_id"], previous["journal_id"])
        self.assertNotEqual(current["generation"], previous["generation"])
        with self.assertRaises(JournalGenerationChanged):
            await asyncio.wait_for(
                w.record(old_writer.record_event, "single_writer.stale"), 10
            )
        with self.assertRaises(JournalGenerationChanged):
            await asyncio.wait_for(w.record(old_reader.read_events), 10)
        self.assertNotIn(
            "single_writer.stale",
            [
                event["event_type"]
                for event in stored_events(root / "journals/events.sqlite")
            ],
        )
        self.assertEqual(runner.get_state()["phase"], "waiting")

    async def test_new_journal_preserves_old_file_and_pauses_for_history_w07(self):
        """W07: new storage records its predecessor; missing history remains paused."""
        w = self.w
        workspace = await w.experiment()
        runner = workspace.runner
        supervisor = runner._journal._supervisor
        old = runner._state.experiment_directory / "journals/events.sqlite"
        await runner.step()
        w.faults.refuse_storage(old)
        await w.faults.kill(supervisor.get_status()["process"])
        preserved = hashlib.sha256(old.read_bytes()).hexdigest()
        status = await wait_until(
            lambda: (
                supervisor.get_status()
                if not supervisor.get_status().get("history_available", True)
                else None
            ),
            timeout=60,
        )
        from pathlib import Path

        replacement = Path(status["db_path"])
        self.assertNotEqual(replacement, old)
        self.assertTrue(old.is_file())
        self.assertEqual(hashlib.sha256(old.read_bytes()).hexdigest(), preserved)
        events = stored_events(replacement)
        self.assertEqual(events[0]["event_type"], "journal.created_after_failure")
        self.assertEqual(events[0]["data"]["previous_path"], str(old))
        await wait_until(lambda: runner.get_state()["mode"] == "paused", timeout=5)
        self.assertEqual(runner.get_state()["phase"], "waiting")

        w.faults.refuse_storage(old, replacement_too=True)
        await w.faults.kill(supervisor.get_status()["process"])
        await wait_until(
            lambda: supervisor.get_status()["state"] == "storage_unavailable",
            timeout=60,
        )
        self.assertEqual(runner.get_state()["mode"], "paused")
        self.assertNotEqual(runner.get_state()["phase"], "failed")
        pending = w.record(
            runner._journal.client.record_event, "single_writer.not_acknowledged"
        )
        await asyncio.sleep(0.1)
        if pending.done():
            with self.assertRaises(LoggingStorageError):
                pending.result()
        w.faults.clear_storage_failure()

    async def test_standalone_outlives_launcher_expires_and_restarts_once_w08(self):
        """W08: standalone lifetime, concurrent reconnect and non-idle SQL work."""
        w = self.w
        self.assertEqual(w.settings_type().idle_timeout_seconds, 300)
        config = write_context_settings(
            w.root / "standalone", writer_settings={"idle_timeout_seconds": 3.0}
        )
        application = LoggingProcess(
            "standalone", config, module_name="tests.helpers.journal_standalone"
        )
        self.addCleanup(application.close)
        await asyncio.to_thread(application.start)
        first = await asyncio.to_thread(application.receive)
        self.assertEqual(await asyncio.to_thread(application.wait), 0)
        self.assertTrue(process_running(first["writer"]["pid"]))
        await wait_until(
            lambda: not process_running(first["writer"]["pid"]), timeout=10
        )
        existing_settings(config)
        clients = [OperationLogger(config), OperationLogger(config)]
        w.loggers.extend(clients)
        await asyncio.wait_for(
            asyncio.gather(*(asyncio.to_thread(client.open) for client in clients)), 15
        )
        statuses = [client._store.get_status() for client in clients]
        self.assertEqual(statuses[0]["process"], statuses[1]["process"])
        self.assertNotEqual(statuses[0]["process"], first["writer"])
        database = config.parent / "events.db"
        with closing(sqlite3.connect(database, isolation_level=None)) as blocker:
            blocker.execute("BEGIN IMMEDIATE")
            pending = w.record(clients[0].record_event, "single_writer.long_work")
            await wait_until(lambda: clients[1]._store.get_status()["busy"], timeout=5)
            await asyncio.sleep(3.2)
            self.assertTrue(process_running(statuses[0]["process"]["pid"]))
            self.assertFalse(pending.done())
            blocker.execute("ROLLBACK")
        acknowledged = await asyncio.wait_for(asyncio.shield(pending), 15)
        await asyncio.sleep(3.0)
        results = await asyncio.wait_for(
            asyncio.gather(
                *(
                    w.record(
                        client.record_event,
                        "single_writer.idle_boundary",
                        {"index": index},
                    )
                    for index, client in enumerate(clients)
                )
            ),
            15,
        )
        ids = [event["event_id"] for event in stored_events(database)]
        for identifier in [first["event_id"], acknowledged, *results]:
            self.assertEqual(ids.count(identifier), 1)
