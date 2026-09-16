"""Approved snapshots.md F/G: kill real runner owners at restoration boundaries."""

import asyncio
import os
import subprocess
import unittest
from uuid import UUID, uuid5

from core.logger import OperationLogger
from core.runner_utils.runtimeio import read_json, write_json
from tests.helpers.dag import REPOSITORY, process_running
from tests.helpers.snapshots import SnapshotWorkspace


class SnapshotTransactionTests(unittest.IsolatedAsyncioTestCase):
    async def crash_restore(self, phase, *, services=False):
        w = SnapshotWorkspace(services=services)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        await runner.step()
        if services:
            await w.value(10)
        snapshot = await runner.snapshot("transaction source")
        await runner.step()
        if services:
            await w.value(20)
        experiment_id = runner._state.experiment_id
        root = runner._state.experiment_directory
        await runner.close()
        output = (w.root / "owner-output.log").open("wb")
        owner = await asyncio.create_subprocess_exec(
            *[
                "uv",
                "run",
                "python",
                "-m",
                "tests.helpers.snapshot_owner",
                "--root",
                str(w.root),
                "--experiment",
                experiment_id,
                "--operation",
                "rollback",
                "--snapshot",
                snapshot["snapshot_id"],
                "--phase",
                phase,
            ],
            cwd=REPOSITORY,
            stdout=output,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        try:
            code = await asyncio.wait_for(owner.wait(), 90)
        finally:
            if owner.returncode is None:
                owner.terminate()
                await asyncio.wait_for(owner.wait(), 10)
            output.close()
        self.assertEqual(
            code, 23, (w.root / "owner-output.log").read_text(encoding="utf-8")
        )
        marker = w.root / "controller/restore_transactions" / f"{root.name}.json"
        self.assertTrue(marker.is_file())
        return w, snapshot, marker

    async def test_recover_resumes_every_file_and_journal_crash_boundary(self):
        """G1/G2/F6: real crashes cover copy preparation, both moves and DB commit."""
        for phase in (
            "staging",
            "prepared",
            "old_moved",
            "new_installed",
            "files_installed",
            "journal_committed",
            "journal_restored",
            "complete",
        ):
            with self.subTest(phase=phase):
                w, _snapshot, marker = await self.crash_restore(phase)
                transaction = read_json(marker)
                runner = w.replacement()
                await asyncio.wait_for(runner.recover(transaction["experiment_id"]), 90)
                self.assertEqual(
                    runner.get_state()["phase"], "waiting", runner.get_state()
                )
                self.assertEqual(runner._state.mode, "paused")
                self.assertEqual(runner._state.last_result["trail"], ["A"])
                self.assertTrue(runner._state.pending_advance)
                self.assertEqual(read_json(marker)["phase"], "complete")
                self.assertEqual(
                    runner._journal.client.get_journal_info()["generation"],
                    uuid5(
                        UUID(transaction["restoration_id"]),
                        "experiment-journal-generation",
                    ).hex,
                )
                events = runner._journal.client.read_events(limit=1000)["events"]
                self.assertEqual(
                    sum(
                        item["event"]["event_type"] == "experiment.restored"
                        for item in events
                    ),
                    1,
                )
                self.assertEqual(
                    (await runner.step())["result"]["data"]["trail"], ["A", "B"]
                )
                await w.close()

    async def test_uncertain_service_load_stops_new_instances_and_requires_fresh_rollback(
        self,
    ):
        """G4/F3: a crash after load_state cannot authorize replay of that command."""
        w, snapshot, marker = await self.crash_restore("load_applied", services=True)
        transaction = read_json(marker)
        current = w.root / "experiments" / transaction["target_folder"]
        saved = read_json(current / "runner/state.json")
        identity = saved["services"][w.socket["service_id"]]["process_identity"]
        self.assertTrue(process_running(identity["pid"]))
        loaded_before = [
            row
            for row in w.files.trace(w.socket)
            if row.get("event") == "work_started" and row.get("command") == "load_state"
        ]
        runner = w.replacement()
        with self.assertRaisesRegex(RuntimeError, "fresh rollback"):
            await asyncio.wait_for(runner.recover(transaction["experiment_id"]), 90)
        self.assertFalse(process_running(identity["pid"]))
        self.assertEqual(read_json(marker)["phase"], "failed")
        loaded_after = [
            row
            for row in w.files.trace(w.socket)
            if row.get("event") == "work_started" and row.get("command") == "load_state"
        ]
        self.assertEqual(loaded_after, loaded_before)
        await runner.rollback(snapshot["snapshot_id"])
        await asyncio.wait_for(runner._ready.wait(), 30)
        self.assertEqual(await w.value(), 10)
        self.assertEqual(read_json(marker)["phase"], "complete")

    async def test_invalid_transaction_marker_never_moves_existing_directories(self):
        """G1/G3: wrong identity, phase and paths fail before another file replacement."""
        w, _, marker = await self.crash_restore("prepared")
        original = read_json(marker)
        target = w.root / "experiments" / original["target_folder"]
        sentinel = target / "shared_data/sentinel"
        sentinel.write_bytes(b"keep")
        for key, value in (
            ("phase", "invalid"),
            ("target_folder", "../outside"),
            ("source_folder", "C:/outside"),
            ("schema_version", True),
            ("owner", {"pid": os.getpid()}),
        ):
            with self.subTest(key=key):
                write_json(marker, {**original, key: value})
                runner = w.replacement()
                with self.assertRaises((ValueError, TypeError, KeyError)):
                    await runner.recover(original["experiment_id"])
                self.assertEqual(sentinel.read_bytes(), b"keep")
                await runner.close()
        write_json(marker, original)
        await w.replacement().recover(original["experiment_id"])

    async def test_open_external_database_handle_prevents_replacement_on_windows(self):
        """F2/G3: an unclosed external journal handle cannot permit partial overwrite."""
        if os.name != "nt":
            self.skipTest("Windows replacement semantics")
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        snapshot = await runner.snapshot()
        root = runner._state.experiment_directory
        sentinel = root / "shared_data/sentinel"
        sentinel.write_bytes(b"keep")
        reader = OperationLogger(runner._journal.reader_config_path)
        reader.open()
        try:
            with self.assertRaises(PermissionError):
                await runner.rollback(snapshot["snapshot_id"])
            self.assertEqual(sentinel.read_bytes(), b"keep")
        finally:
            reader.close()
        experiment_id = runner._state.experiment_id
        await runner.close()
        recovered = w.replacement()
        await recovered.recover(experiment_id)
        self.assertEqual(recovered.get_state()["phase"], "waiting")
        self.assertFalse(sentinel.exists())

    async def test_missing_replacement_or_corrupt_cache_preserves_previous_tree(self):
        """G3: recoverable staging failures never overwrite the remaining valid tree."""
        for fault in ("replacement", "cache"):
            with self.subTest(fault=fault):
                w, _snapshot, marker = await self.crash_restore("prepared")
                transaction = read_json(marker)
                target = w.root / "experiments" / transaction["target_folder"]
                work = w.root / "controller/restores" / transaction["restoration_id"]
                sentinel = target / "shared_data/keep"
                sentinel.write_bytes(b"original")
                if fault == "replacement":
                    replacement = work / "replacement"
                    displaced = work / "displaced-replacement"
                    self.assertTrue(replacement.resolve().is_relative_to(w.root))
                    self.assertTrue(displaced.resolve().is_relative_to(w.root))
                    replacement.replace(displaced)
                else:
                    manifest = work / "snapshot/manifest.json"
                    before = manifest.read_bytes()
                    manifest.write_bytes(b"{broken")
                runner = w.replacement()
                with self.assertRaises((RuntimeError, ValueError)):
                    await runner.recover(transaction["experiment_id"])
                self.assertEqual(sentinel.read_bytes(), b"original")
                await runner.close()
                if fault == "replacement":
                    displaced.replace(replacement)
                else:
                    manifest.write_bytes(before)
                await w.replacement().recover(transaction["experiment_id"])
                self.assertFalse(sentinel.exists())
                await w.close()
