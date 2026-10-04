"""Approved A04-A05: conditional recovery, snapshots, reload and artifact lifetime."""

import asyncio
import copy
import subprocess
import unittest
from pathlib import Path

from core.primitives.json_files import read_json
from tests.helpers.dag import REPOSITORY, process_running
from tests.helpers.reload import events, trace
from tests.helpers.services import wait_for
from tests.helpers.snapshots import SnapshotWorkspace

SOURCE = Path(__file__).resolve().parents[1] / "helpers/conditional_stage.py"


class ConditionalRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def make_work(self):
        work = SnapshotWorkspace(services=False, cycles=1)
        self.addAsyncCleanup(work.close)
        return work

    def condition(self, work, settings):
        stage = work.files.stage(
            settings={"label": "condition", **settings},
            source=SOURCE,
            stage_kind="conditional",
        )
        stage["returns_data"] = True
        return stage

    async def launch(self, work, stages):
        work.stages = stages
        work.template["stages"] = stages
        return await work.launch()

    async def start_owner(self, work, boundary, condition, target):
        output = (work.root / "conditional-owner.log").open("wb")
        try:
            process = await asyncio.create_subprocess_exec(
                "uv",
                "run",
                "python",
                "-m",
                "tests.helpers.conditional_owner",
                "--root",
                str(work.root),
                "--experiment",
                work.runner._state.experiment_id,
                "--condition",
                condition["stage_id"],
                "--target",
                target["stage_id"],
                "--boundary",
                boundary,
                cwd=REPOSITORY,
                stdout=output,
                stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except BaseException:
            output.close()
            raise
        work.owners.append((process, output))
        return process

    async def test_owner_crashes_preserve_move_and_do_not_repeat_launched_work(self):
        """A04: actual owner death before/after commit and with a live target."""
        for boundary in ("accepted", "transition", "target_started"):
            with self.subTest(boundary=boundary):
                work = self.make_work()
                try:
                    async with asyncio.timeout(180):
                        seed, skipped, target = work.stages
                        gate = work.files.gate()
                        target["settings"]["gate"] = str(gate)
                        tail = work.files.stage(settings={"label": "tail"})
                        payload = {"trail": ["carried"]}
                        condition = self.condition(
                            work,
                            {
                                "decision": {
                                    "command": "move",
                                    "stage_id": target["stage_id"],
                                    "data": payload,
                                }
                            },
                        )
                        runner = await self.launch(
                            work, [seed, condition, skipped, target, tail]
                        )
                        await runner.step()
                        experiment_id = runner._state.experiment_id
                        root = runner._state.experiment_directory
                        await runner.close()
                        owner = await self.start_owner(
                            work, boundary, condition, target
                        )
                        returncode = await asyncio.wait_for(owner.wait(), 90)
                        diagnostic = (work.root / "conditional-owner.log").read_text(
                            encoding="utf-8", errors="replace"
                        )
                        self.assertEqual(returncode, 23, diagnostic)
                        saved = read_json(root / "runner/state.json")
                        active = saved["active_attempt"]
                        if boundary == "target_started":
                            self.assertEqual(active["stage_id"], target["stage_id"])
                            self.assertTrue(
                                process_running(active["process_identity"]["pid"])
                            )
                            ready = root / active["artifacts_directory"] / "ready.json"
                            await wait_for(ready.exists, 30)
                        runner = work.replacement()
                        await runner.recover(experiment_id)
                        if boundary == "target_started":
                            self.assertEqual(
                                runner._state.active_attempt.attempt_id,
                                active["attempt_id"],
                            )
                            self.assertEqual(
                                runner._state.active_attempt.process_identity.model_dump(),
                                active["process_identity"],
                            )
                        else:
                            await wait_for(
                                lambda runner=runner: (
                                    runner._state.active_attempt is None
                                    and runner._state.pending_input is not None
                                ),
                                60,
                            )
                            self.assertEqual(
                                runner._state.pending_input.stage_id,
                                target["stage_id"],
                            )
                            self.assertEqual(runner._state.stage_position, 4)
                            self.assertNotIn(
                                target["stage_id"], runner._state.stage_attempt_numbers
                            )
                        gate.touch()
                        if boundary == "target_started":
                            await wait_for(
                                lambda runner=runner: (
                                    runner._state.active_attempt is None
                                ),
                                60,
                            )
                            self.assertEqual(
                                runner._state.stage_attempt_numbers[target["stage_id"]],
                                1,
                            )
                        await runner.resume()
                        await wait_for(
                            lambda runner=runner: (
                                runner.get_state()["phase"] == "completed"
                            ),
                            60,
                        )
                        starts = [
                            item for item in trace(runner) if item["event"] == "start"
                        ]
                        self.assertEqual(
                            [item["stage_id"] for item in starts],
                            [
                                seed["stage_id"],
                                condition["stage_id"],
                                target["stage_id"],
                                tail["stage_id"],
                            ],
                        )
                        self.assertEqual(starts[2]["input"], payload)
                        condition_results = [
                            event
                            for event in events(runner, "stage.finished")
                            if event["context"]["stage_id"] == condition["stage_id"]
                        ]
                        self.assertEqual(len(condition_results), 1)
                        self.assertEqual(
                            runner._state.stage_attempt_numbers[condition["stage_id"]],
                            1,
                        )
                finally:
                    await work.close()

    async def artifact_work(self, *, retries=0, crash_attempts=0):
        work = self.make_work()
        producer = work.files.stage(
            source=SOURCE,
            settings={
                "label": "producer",
                "artifact_probe": True,
                "crash_attempts": crash_attempts,
            },
            retries=retries,
        )
        condition = self.condition(
            work,
            {
                "decisions": [
                    {"command": "move", "stage_id": producer["stage_id"]},
                    {},
                ],
                "forward_input": True,
            },
        )
        tail = work.stages[-1]
        work.template["keep_attempts"] = 1
        runner = await self.launch(work, [producer, condition, tail])
        return work, runner, producer, condition, tail

    async def test_snapshot_rollback_and_continuation_preserve_backwards_input(self):
        """A04/A05: restore an invalidated source reference and its file in a clone."""
        async with asyncio.timeout(180):
            work, runner, _producer, condition, _tail = await self.artifact_work()
            first = (await runner.step())["result"]["data"]
            await runner.step()
            self.assertEqual(runner._state.stage_result_ids, {})
            transfer = copy.deepcopy(runner._state.pending_input)
            source_id = runner._state.experiment_id
            snapshot = await runner.snapshot("pending backwards move")
            second = (await runner.step())["result"]["data"]
            self.assertEqual(second["read"], "attempt=1\n")
            await runner.rollback(snapshot["snapshot_id"])
            self.assertEqual(runner._state.pending_input, transfer)
            self.assertEqual(runner._state.stage_position, 1)
            self.assertFalse(runner._state.pending_advance)
            self.assertFalse(
                (runner._state.experiment_directory / second["path"]).exists()
            )
            await runner.stop()
            self.assertTrue(runner._last_snapshot["valid"])
            await runner.close()
            runner = work.replacement()
            await runner.run(experiment_id=source_id, continue_run=True)
            await asyncio.wait_for(runner._ready.wait(), 90)
            self.assertEqual(runner.get_state()["phase"], "waiting", runner.get_state())
            self.assertNotEqual(runner._state.experiment_id, source_id)
            self.assertEqual(runner._state.pending_input, transfer)
            self.assertEqual(transfer.experiment_id, source_id)
            restored_file = runner._state.experiment_directory / first["path"]
            self.assertEqual(restored_file.read_text(encoding="utf-8"), "attempt=1\n")
            second = (await runner.step())["result"]["data"]
            self.assertEqual(second["read"], "attempt=1\n")
            self.assertEqual(second["input"], first)
            await runner.step()
            final = await runner.step()
            self.assertEqual(final["phase"], "completed")
            self.assertEqual(final["result"]["data"]["input"], second)
            condition_starts = [
                item
                for item in trace(runner)
                if item["stage_id"] == condition["stage_id"]
            ]
            self.assertEqual([item["attempt"] for item in condition_starts], [1, 2])

    async def test_pruning_protects_transferred_file_and_leaves_persistent_directories(
        self,
    ):
        """A05: real target reads an old attempt file while unprotected attempts expire."""
        async with asyncio.timeout(120):
            work, runner, producer, _, _ = await self.artifact_work(
                retries=1, crash_attempts=1
            )
            first = (await runner.step())["result"]["data"]
            root = runner._state.experiment_directory
            file = root / first["path"]
            self.assertEqual(file.read_text(encoding="utf-8"), "attempt=2\n")
            expired = file.parent.parent / "attempt_1"
            self.assertFalse(expired.exists())
            persistent = [
                root / "shared_data/persistent.txt",
                root / "module_data" / producer["stage_id"] / "persistent.txt",
            ]
            for path in persistent:
                path.write_text("keep", encoding="utf-8")
            await runner.step()
            second = (await runner.step())["result"]["data"]
            self.assertEqual(second["input"], first)
            self.assertEqual(second["read"], "attempt=2\n")
            self.assertEqual(file.read_text(encoding="utf-8"), "attempt=2\n")
            self.assertTrue((root / second["path"]).is_file())
            for path in persistent:
                self.assertEqual(path.read_text(encoding="utf-8"), "keep")
            snapshot = await runner.snapshot("protected artifacts")
            self.assertTrue(
                (
                    work.archive(snapshot["snapshot_id"]) / "files" / first["path"]
                ).is_file()
            )

    async def test_reload_preserves_only_valid_transfer_and_selects_actual_next_input(
        self,
    ):
        """A05: preserve a valid assignment or discard it after definition changes."""
        for change in ("tail", "source", "target", "remove_target", "reorder"):
            with self.subTest(change=change):
                work = self.make_work()
                try:
                    async with asyncio.timeout(150):
                        seed, gap, target = work.stages
                        tail = work.files.stage(settings={"label": "tail"})
                        payload = {"trail": ["moved"]}
                        condition = self.condition(
                            work,
                            {
                                "decision": {
                                    "command": "move",
                                    "stage_id": target["stage_id"],
                                    "data": payload,
                                }
                            },
                        )
                        runner = await self.launch(
                            work, [seed, condition, gap, target, tail]
                        )
                        seed_data = (await runner.step())["result"]["data"]
                        await runner.step()
                        transfer = copy.deepcopy(runner._state.pending_input)
                        candidate = copy.deepcopy(
                            runner._state.template.model_dump(exclude_unset=True)
                        )
                        if change == "tail":
                            candidate["stages"][-1]["settings"]["echo"] = "new tail"
                        elif change == "source":
                            candidate["stages"][1]["returns_data"] = False
                        elif change == "target":
                            candidate["stages"][3]["settings"]["echo"] = "new target"
                        elif change == "remove_target":
                            candidate["stages"].pop(3)
                        else:
                            candidate["stages"][2:4] = candidate["stages"][3:1:-1]
                        path = work.files.write_template(candidate, "reload.yaml")
                        self.assertTrue((await runner.reload_template(path))["changed"])
                        if change == "tail":
                            self.assertEqual(runner._state.pending_input, transfer)
                        else:
                            self.assertIsNone(runner._state.pending_input)
                        if change == "source":
                            self.assertEqual(runner._state.stage_position, 2)
                            await runner.step()
                            self.assertNotEqual(
                                runner._state.pending_input.request_id,
                                transfer.request_id,
                            )
                            expected = seed_data
                        else:
                            expected = (
                                payload if change in ("tail", "reorder") else None
                            )
                        next_id = runner._state.template.model_dump(exclude_unset=True)[
                            "stages"
                        ][runner._state.stage_position - 1]["stage_id"]
                        self.assertEqual(
                            next_id,
                            tail["stage_id"]
                            if change == "remove_target"
                            else target["stage_id"],
                        )
                        result = (await runner.step())["result"]["data"]
                        self.assertEqual(result["input"], expected)
                        if change == "target":
                            self.assertEqual(result["echo"], "new target")
                        starts = [
                            item for item in trace(runner) if item["event"] == "start"
                        ]
                        self.assertEqual(starts[-1]["stage_id"], next_id)
                finally:
                    await work.close()
