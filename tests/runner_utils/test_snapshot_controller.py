"""Approved snapshots.md D2/J: responsive commands and real resource writer barriers."""

import asyncio
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from core.runner_utils.runtimeio import read_json
from tests.helpers.service_integration import ServiceDagWorkspace
from tests.helpers.services import wait_for


class SnapshotControllerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ServiceDagWorkspace()
        self.addAsyncCleanup(self.w.close)
        self.socket = self.w.service()
        self.socket["settings"]["auto_increment"] = False
        self.commands = self.w.service(interface="commands", implementation="action")
        stages = [self.w.stage(settings={"label": label}) for label in ("A", "B", "C")]
        template = self.w.template(stages, cycles=2)
        template["snapshots"]["keep"] = 8
        self.runner = await self.w.launch(template)

    async def test_reads_heartbeats_and_priority_stop_work_during_snapshot_copy(self):
        """D2/J1/J2: real services remain observed while disk work is in a thread."""
        w, runner = self.w, self.runner
        started, release = threading.Event(), threading.Event()
        original = runner._snapshots._build_snapshot

        def copy_files(*args):
            started.set()
            if not release.wait(30):
                raise TimeoutError("Test did not release its copy gate.")
            return original(*args)

        with patch.object(runner._snapshots, "_build_snapshot", side_effect=copy_files):
            pending = w.session.post("snapshot", {"label": "cancelled-copy"})
            try:
                await wait_for(started.is_set, 15)
                instance = runner._state.services[self.socket["service_id"]]
                previous = instance.last_status["observed_monotonic"]
                self.assertIsNotNone(instance.freeze_id)
                state = await asyncio.wait_for(w.session.send("stats.state"), 5)
                self.assertEqual(state["result"], "success")
                self.assertEqual(state["data"]["phase"], "snapshotting")
                events = await asyncio.wait_for(
                    w.session.send(
                        "logs.read", {"experiment_id": runner._state.experiment_id}
                    ),
                    5,
                )
                self.assertEqual(events["result"], "success")
                await wait_for(
                    lambda: instance.last_status["observed_monotonic"] > previous, 5
                )
                with self.assertRaises(RuntimeError):
                    await runner.snapshot("conflict")
                with self.assertRaises(RuntimeError):
                    await runner.resume()
                stop = w.session.post("stop")
                await asyncio.sleep(0.1)
                self.assertFalse(stop.done())
                self.assertFalse(pending.done())
                self.assertEqual(
                    (await w.session.send("stats.state"))["result"], "success"
                )
            finally:
                release.set()
            stopped = await asyncio.wait_for(asyncio.shield(stop), 90)
            cancelled = await asyncio.wait_for(asyncio.shield(pending), 10)
            self.assertEqual(stopped["result"], "success", stopped)
            self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(runner.get_state()["phase"], "stopped")
        archives = w.root / "snapshots" / runner._state.experiment_directory.name
        manifests = [read_json(path) for path in archives.glob("*/manifest.json")]
        self.assertFalse(any(item["label"] == "cancelled-copy" for item in manifests))
        self.assertTrue(all(item["kind"] == "final" for item in manifests))

    async def test_real_collector_closes_before_replacement_and_resumes_new_generation(
        self,
    ):
        """F2/J3: the acknowledged writer barrier precedes replacement of the live DB."""
        w, runner = self.w, self.runner
        collector = w.session.controller.resources
        await wait_for(
            lambda: collector._process is not None and bool(collector._packet), 15
        )
        self.assertEqual((await w.session.send("step"))["result"], "success")
        snapshot = await w.session.send("snapshot", {"label": "resources"})
        self.assertEqual(snapshot["result"], "success", snapshot)
        root = runner._state.experiment_directory
        before = runner._journal.client.get_journal_info()["generation"]
        events = []
        suspend = runner._suspend_resources
        replace = Path.replace

        async def suspend_writer():
            await suspend()
            self.assertTrue(collector._suspended)
            self.assertTrue(
                collector._process is None or collector._packet.get("journal_closed")
            )
            events.append("closed")

        def move(source, destination):
            if source == root:
                self.assertEqual(events, ["closed"])
                events.append("replace")
            return replace(source, destination)

        runner._suspend_resources = suspend_writer
        with patch.object(Path, "replace", move):
            reply = await asyncio.wait_for(
                asyncio.shield(
                    w.session.post(
                        "rollback", {"snapshot_id": snapshot["data"]["snapshot_id"]}
                    )
                ),
                90,
            )
        self.assertEqual(reply["result"], "success", reply)
        self.assertEqual(events, ["closed", "replace"])
        self.assertFalse(collector._suspended)
        after = runner._journal.client.get_journal_info()["generation"]
        self.assertNotEqual(before, after)
        self.assertEqual(
            (await w.session.send("step"))["data"]["result"]["data"]["trail"],
            ["A", "B"],
        )
        await wait_for(lambda: bool(collector.get_status()["latest"]), 15)
        self.assertIsNone(collector.get_status()["journal_error"])

    async def test_observer_without_close_acknowledgement_cannot_restore(self):
        """J4: observation alone is not permission to replace a writer's database."""
        runner = self.runner
        snapshot = await runner.snapshot()
        root = runner._state.experiment_directory
        sentinel = root / "shared_data/keep"
        sentinel.write_bytes(b"unchanged")
        runner._suspend_resources = None
        with self.assertRaisesRegex(RuntimeError, "barrier"):
            await runner.rollback(snapshot["snapshot_id"])
        self.assertEqual(sentinel.read_bytes(), b"unchanged")
        self.assertEqual(runner.get_state()["phase"], "waiting")
        experiment_id = runner._state.experiment_id
        self.assertEqual((await self.w.session.send("stop"))["result"], "success")
        recovered = await self.w.session.send(
            "recover", {"experiment_id": experiment_id}
        )
        self.assertEqual(recovered["result"], "success", recovered)
        self.assertEqual(runner.get_state()["phase"], "stopped")
