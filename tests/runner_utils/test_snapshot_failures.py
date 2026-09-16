"""Approved snapshots.md B/D/E/G: failure never publishes a false ready archive."""

import asyncio
import time
import unittest
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import LoggingStorageError
from core.runner_utils.runtimeio import read_json, write_json
from tests.helpers.dag import process_running, terminate_owned
from tests.helpers.services import wait_for
from tests.helpers.snapshots import SnapshotWorkspace


class SnapshotFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_copy_manifest_and_mandatory_journal_failures_never_publish_ready_snapshot(
        self,
    ):
        """B4/D4/E3: errors after export/copy do not produce a selectable archive."""
        for fault in ("copy", "manifest", "journal"):
            with self.subTest(fault=fault):
                w = SnapshotWorkspace()
                self.addAsyncCleanup(w.close)
                runner = await w.launch()
                await runner.step()
                original = runner._snapshots._build_snapshot

                def build(*args, original=original):
                    original(*args)
                    raise OSError("copy failed after creating payload")

                def publish(path, document, runner=runner):
                    if (
                        path.name == "manifest.json"
                        and path.parent.parent.name
                        == runner._state.experiment_directory.name
                    ):
                        raise PermissionError("manifest publication refused")
                    return write_json(path, document)

                if fault == "copy":
                    failing = patch.object(
                        runner._snapshots, "_build_snapshot", side_effect=build
                    )
                elif fault == "manifest":
                    failing = patch(
                        "core.runner_utils.snapshots.write_json", side_effect=publish
                    )
                else:
                    failing = patch.object(
                        OperationLogger,
                        "export_snapshot",
                        side_effect=LoggingStorageError("journal export refused"),
                    )
                with failing, self.assertRaises((OSError, LoggingStorageError)):
                    await runner.snapshot("cannot publish")
                self.assertEqual(w.manifests(), [])
                self.assertEqual(runner.get_state()["phase"], "failed")
                self.assertTrue(
                    all(item.stopped for item in runner._state.services.values())
                )
                commands = [
                    row["command"]
                    for row in w.files.trace(w.socket)
                    if row["event"] == "work_started"
                ]
                self.assertIn("unfreeze_writes", commands)
                await w.close()

    async def test_unfreeze_rejection_invalidates_copy_and_stops_experiment(self):
        """D4: a copied archive is unusable when write resumption is unconfirmed."""
        w = SnapshotWorkspace()
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        controls = w.files.files.controls[w.socket["service_id"]]
        (controls / "fail-unfreeze").touch()
        with self.assertRaisesRegex(RuntimeError, "resumption"):
            await runner.snapshot()
        self.assertEqual(w.manifests(), [])
        self.assertEqual(runner.get_state()["phase"], "failed")
        self.assertTrue(all(item.stopped for item in runner._state.services.values()))

    async def test_final_export_failure_reports_invalid_stop_and_prevents_completion(
        self,
    ):
        """E1/E2: explicit stop may confirm shutdown; completion requires a valid export."""
        for completion in (False, True):
            with self.subTest(completion=completion):
                w = SnapshotWorkspace(cycles=1)
                self.addAsyncCleanup(w.close)
                runner = await w.launch()
                if completion:
                    await runner.step()
                    await runner.step()
                controls = w.files.files.controls[w.socket["service_id"]]
                (controls / "fail-save").touch()
                if completion:
                    with self.assertRaisesRegex(RuntimeError, "Final snapshot"):
                        await runner.step()
                    self.assertEqual(runner.get_state()["phase"], "failed")
                else:
                    self.assertEqual((await runner.stop())["phase"], "stopped")
                self.assertFalse(runner._last_snapshot["valid"])
                self.assertEqual(w.manifests(), [])
                self.assertTrue(
                    all(item.stopped for item in runner._state.services.values())
                )
                diagnostics = list(
                    (w.root / "controller/snapshot_failures").glob("*.json")
                )
                self.assertEqual(len(diagnostics), 1)
                self.assertFalse(read_json(diagnostics[0])["valid"])
                await w.close()

    async def test_crashed_or_hung_service_before_freeze_ack_never_produces_snapshot(
        self,
    ):
        """D2/D3: actual service crash/hang invalidates the pending freeze instance."""
        for action in ("crash", "hang"):
            with self.subTest(action=action):
                w = SnapshotWorkspace()
                self.addAsyncCleanup(w.close)
                runner = await w.launch()
                instance = runner._state.services[w.socket["service_id"]]
                controls = w.files.files.controls[instance.service_id]
                write_json(
                    controls / "fault.json",
                    {"phase": "before_freeze", "action": action},
                )
                with self.assertRaises((RuntimeError, ConnectionError)):
                    await asyncio.wait_for(runner.snapshot(), 100)
                self.assertEqual(w.manifests(), [])
                self.assertFalse(process_running(instance.process_identity["pid"]))
                self.assertEqual(runner.get_state()["phase"], "failed")
                await w.close()

    async def test_crashed_and_hung_owner_recover_the_prepared_freeze_without_replay(
        self,
    ):
        """D3/I6: real owner loss between freeze acknowledgement and state publication."""
        for action in ("crash", "hang"):
            with self.subTest(action=action):
                w = SnapshotWorkspace()
                self.addAsyncCleanup(w.close)
                runner = await w.launch()
                experiment_id = runner._state.experiment_id
                instance = runner._state.services[w.socket["service_id"]]
                await runner.close()
                owner = await w.start_owner(
                    "snapshot", "freeze_confirmed", action=action
                )
                await wait_for(lambda w=w: (w.root / "owner-fault.json").exists(), 30)
                fault = read_json(w.root / "owner-fault.json")
                if action == "hang":
                    with self.assertRaisesRegex(RuntimeError, "still alive"):
                        await w.replacement().recover(experiment_id)
                    terminate_owned(fault["process"])
                await asyncio.wait_for(owner.wait(), 30)
                runner = w.replacement()
                await asyncio.wait_for(runner.recover(experiment_id), 90)
                self.assertEqual(
                    runner._state.services[instance.service_id].service_instance_id,
                    instance.service_instance_id,
                )
                self.assertIsNone(
                    runner._state.services[instance.service_id].prepared_freeze_id
                )
                self.assertIsNone(runner._state.services[instance.service_id].freeze_id)
                self.assertEqual(w.manifests(), [])
                self.assertEqual(await w.value(10), 10)
                self.assertTrue((await runner.snapshot("after recovery"))["valid"])
                commands = [
                    row["command"]
                    for row in w.files.trace(w.socket)
                    if row["event"] == "work_started"
                ]
                self.assertEqual(commands.count("freeze_writes"), 2)
                await w.close()

    async def test_reset_refuses_live_participants_and_waits_for_commands_process(self):
        """D5/F2: start/stop action handles must be reaped before replacing runtime files."""
        w = SnapshotWorkspace()
        self.addAsyncCleanup(w.close)
        controls = w.files.files.controls[w.commands["service_id"]]
        (controls / "hold-start").touch()
        runner = await w.launch()
        with self.assertRaises(RuntimeError):
            await runner._services.reset(runner._state)
        snapshot = await runner.snapshot()
        sentinel = runner._state.experiment_directory / "shared_data/keep"
        sentinel.write_bytes(b"original")
        started = time.monotonic()
        try:
            with self.assertRaisesRegex(RuntimeError, "runtime files"):
                await asyncio.wait_for(runner.rollback(snapshot["snapshot_id"]), 90)
            self.assertGreaterEqual(time.monotonic() - started, 30)
            self.assertEqual(sentinel.read_bytes(), b"original")
        finally:
            (controls / "hold-start").unlink(missing_ok=True)
