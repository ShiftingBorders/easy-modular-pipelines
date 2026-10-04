"""Approved A01-A03: actual conditional moves, visits and lifecycle commands."""

import asyncio
import copy
import unittest
from pathlib import Path

from tests.helpers.dag import process_running
from tests.helpers.reload import events, trace
from tests.helpers.services import wait_for
from tests.helpers.snapshots import SnapshotWorkspace

SOURCE = Path(__file__).resolve().parents[1] / "helpers/conditional_stage.py"


class ConditionalTransitionTests(unittest.IsolatedAsyncioTestCase):
    def make_work(self, *, services=False, cycles=1):
        work = SnapshotWorkspace(services=services, cycles=cycles)
        self.addAsyncCleanup(work.close)
        return work

    def condition(self, work, settings, *, returns_data=True, retries=0):
        stage = work.files.stage(
            settings={"label": "condition", **settings},
            retries=retries,
            source=SOURCE,
            stage_kind="conditional",
        )
        stage["returns_data"] = returns_data
        return stage

    async def launch(self, work, stages):
        work.stages = stages
        work.template["stages"] = stages
        return await work.launch()

    async def test_forward_move_transfers_exact_data_and_step_waits_at_target(self):
        """A01/A03: skip the positional predecessor, then forward target output."""
        for payload in (None, False, 0, {"trail": ["transferred"]}):
            with self.subTest(payload=payload):
                work = self.make_work()
                try:
                    async with asyncio.timeout(120):
                        seed, skipped, target = work.stages
                        tail = work.files.stage(settings={"label": "tail"})
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
                        await runner.step()
                        self.assertEqual(runner._state.stage_position, 4)
                        self.assertEqual(runner._state.mode, "paused")
                        self.assertFalse(runner._state.pending_advance)
                        self.assertNotIn(
                            target["stage_id"], runner._state.stage_attempt_numbers
                        )
                        self.assertNotIn(
                            skipped["stage_id"], runner._state.stage_attempt_numbers
                        )
                        target_result = (await runner.step())["result"]["data"]
                        self.assertEqual(target_result["input"], payload)
                        self.assertIs(type(target_result["input"]), type(payload))
                        tail_result = (await runner.step())["result"]["data"]
                        self.assertEqual(tail_result["input"], target_result)
                        self.assertEqual(runner.get_state()["phase"], "completed")
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
                finally:
                    await work.close()

    async def test_move_targets_a_service_node_and_forwards_its_actual_output(self):
        """A01: the move target is a DAG service-call node, not a service ID."""
        async with asyncio.timeout(120):
            work = self.make_work(services=True)
            work.template["services"] = [work.socket]
            seed, skipped, original = work.stages
            target = copy.deepcopy(original)
            target.pop("module")
            target["service_id"] = work.socket["service_id"]
            target["settings"] = {"marker": "service target"}
            tail = work.files.stage(settings={"label": "tail"})
            payload = {"value": 42}
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
            runner = await self.launch(work, [seed, condition, skipped, target, tail])
            instance = runner._state.services[
                work.socket["service_id"]
            ].service_instance_id
            await runner.step()
            await runner.step()
            self.assertEqual(runner._state.stage_position, 4)
            self.assertNotIn(target["stage_id"], runner._state.stage_attempt_numbers)
            result = (await runner.step())["result"]["data"]
            self.assertEqual(result["input_data"], payload)
            self.assertEqual(result["settings"], {"marker": "service target"})
            self.assertEqual(
                runner._state.services[work.socket["service_id"]].service_instance_id,
                instance,
            )
            calls = [
                row
                for row in work.files.trace(work.socket)
                if row["event"] == "work_started" and row.get("command") == "execute"
            ]
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["args"]["input_data"], payload)
            self.assertNotIn(skipped["stage_id"], runner._state.stage_attempt_numbers)
            final = await runner.step()
            self.assertEqual(final["result"]["data"]["input"], result)
            self.assertEqual(final["phase"], "completed")

    async def test_backwards_move_invalidates_results_without_advancing_cycle(self):
        """A02: two full cycles each contain an explicit backwards visit."""
        async with asyncio.timeout(180):
            work = self.make_work(cycles=2)
            first, second, tail = work.stages
            payload = {"trail": ["loop"]}
            condition = self.condition(
                work,
                {
                    "decisions": [
                        {
                            "command": "move",
                            "stage_id": first["stage_id"],
                            "data": payload,
                        },
                        {"data": None},
                    ]
                },
            )
            runner = await self.launch(work, [first, second, condition, tail])
            for cycle in (1, 2):
                await runner.step()
                await runner.step()
                previous = dict(runner._state.stage_result_ids)
                moved = await runner.step()
                self.assertEqual(runner._state.stage_position, 1)
                self.assertEqual(runner._state.cycle_number, cycle)
                self.assertEqual(runner._state.stage_result_ids, {})
                transfer = runner._state.pending_input
                self.assertEqual(transfer.source_stage_id, condition["stage_id"])
                self.assertEqual(
                    runner._journal.client.read_command_result(transfer.request_id)[
                        "response"
                    ]["data"],
                    payload,
                )
                self.assertEqual(moved["result"]["data"], payload)
                await runner.step()
                self.assertNotEqual(
                    runner._state.stage_result_ids[first["stage_id"]],
                    previous[first["stage_id"]],
                )
                await runner.step()
                self.assertNotEqual(
                    runner._state.stage_result_ids[second["stage_id"]],
                    previous[second["stage_id"]],
                )
                await runner.step()
                result = await runner.step()
                self.assertEqual(
                    result["result"]["data"]["trail"], ["loop", "A", "B", "C"]
                )
                self.assertEqual(runner._state.cycle_number, cycle)
            self.assertEqual(runner.get_state()["phase"], "completed")
            starts = [item for item in trace(runner) if item["event"] == "start"]
            self.assertEqual([item["cycle"] for item in starts], [1] * 7 + [2] * 7)
            first_starts = [
                item for item in starts if item["stage_id"] == first["stage_id"]
            ]
            self.assertEqual(
                [item["input"] for item in first_starts], [None, payload, None, payload]
            )

    async def test_self_move_starts_a_fresh_retry_budget_and_preserves_retry_input(
        self,
    ):
        """A02: both visits can spend a retry, each with its own fixed input."""
        async with asyncio.timeout(120):
            work = self.make_work()
            seed, _, tail = work.stages
            condition = self.condition(work, {"crash_on_attempts": [1, 3]}, retries=1)
            condition["settings"]["decisions"] = [
                None,
                {"command": "move", "stage_id": condition["stage_id"], "data": 7},
                None,
                {"data": None},
            ]
            runner = await self.launch(work, [seed, condition, tail])
            initial = (await runner.step())["result"]["data"]
            await runner.step()
            self.assertEqual(runner._state.stage_position, 2)
            self.assertEqual(
                runner._state.stage_retry_counts.get(condition["stage_id"], 0), 0
            )
            self.assertNotIn(condition["stage_id"], runner._state.stage_result_ids)
            await runner.step()
            self.assertEqual(runner._state.stage_retry_counts[condition["stage_id"]], 1)
            self.assertEqual(
                runner._state.stage_attempt_numbers[condition["stage_id"]], 4
            )
            self.assertEqual(runner._state.cycle_number, 1)
            starts = [
                item
                for item in trace(runner)
                if item["stage_id"] == condition["stage_id"]
            ]
            self.assertEqual(
                [item["input"] for item in starts], [initial, initial, 7, 7]
            )
            finished = [
                event
                for event in events(runner, "stage.finished")
                if event["context"]["stage_id"] == condition["stage_id"]
            ]
            execution_ids = [
                event["context"]["stage_execution_id"] for event in finished
            ]
            self.assertEqual(execution_ids[0], execution_ids[1])
            self.assertEqual(execution_ids[2], execution_ids[3])
            self.assertNotEqual(execution_ids[0], execution_ids[2])
            self.assertEqual((await runner.step())["result"]["data"]["input"], 7)
            self.assertEqual(runner.get_state()["phase"], "completed")

    async def test_pause_keeps_services_alive_and_resume_does_not_repeat_condition(
        self,
    ):
        """A03: successful pause is a boundary, while services remain usable."""
        async with asyncio.timeout(150):
            work = self.make_work(services=True)
            condition = self.condition(
                work, {"decision": {"command": "pause"}}, returns_data=False
            )
            tail = work.stages[-1]
            runner = await self.launch(work, [condition, tail])
            identities = {
                sid: item.process_identity
                for sid, item in runner._state.services.items()
            }
            instances = {
                sid: item.service_instance_id
                for sid, item in runner._state.services.items()
            }
            await runner.resume()
            await wait_for(
                lambda: (
                    runner._state.pending_advance and runner._state.mode == "paused"
                ),
                60,
            )
            self.assertEqual(runner._state.stage_position, 1)
            self.assertEqual(
                [item["stage_id"] for item in trace(runner)], [condition["stage_id"]]
            )
            self.assertEqual(await work.value(19), 19)
            for sid, identity in identities.items():
                self.assertTrue(process_running(identity["pid"]))
                self.assertTrue(runner._state.services[sid].ready)
                self.assertEqual(
                    runner._state.services[sid].service_instance_id, instances[sid]
                )
            await runner.resume()
            await wait_for(lambda: runner.get_state()["phase"] == "completed", 60)
            starts = [item for item in trace(runner) if item["event"] == "start"]
            self.assertEqual(
                [item["stage_id"] for item in starts],
                [condition["stage_id"], tail["stage_id"]],
            )
            for identity in identities.values():
                self.assertFalse(process_running(identity["pid"]))

    async def test_conditional_stop_finishes_step_only_after_service_shutdown(self):
        """A03: stop is successful control, with no successor or live services."""
        async with asyncio.timeout(150):
            work = self.make_work(services=True)
            condition = self.condition(
                work, {"decision": {"command": "stop"}}, returns_data=False
            )
            runner = await self.launch(work, [condition, work.stages[-1]])
            identities = [
                item.process_identity for item in runner._state.services.values()
            ]
            result = await runner.step()
            self.assertEqual(result["phase"], "stopped")
            self.assertEqual(result["result"]["result"], "success")
            self.assertTrue(runner._task.done())
            self.assertTrue(runner._last_snapshot["valid"])
            self.assertEqual(
                [item["stage_id"] for item in trace(runner)], [condition["stage_id"]]
            )
            for identity in identities:
                self.assertFalse(process_running(identity["pid"]))

    async def test_final_conditional_command_precedes_automatic_completion(self):
        """A03: final pause waits for resume/step; final stop stays stopped."""
        for command, continuation in (
            ("pause", "resume"),
            ("pause", "step"),
            ("stop", None),
        ):
            with self.subTest(command=command, continuation=continuation):
                work = self.make_work()
                try:
                    async with asyncio.timeout(90):
                        condition = self.condition(
                            work, {"decision": {"command": command}}, returns_data=False
                        )
                        runner = await self.launch(work, [condition])
                        result = await runner.step()
                        self.assertEqual(
                            result["phase"],
                            "waiting" if command == "pause" else "stopped",
                        )
                        self.assertEqual(
                            runner._state.stage_attempt_numbers[condition["stage_id"]],
                            1,
                        )
                        if command == "pause":
                            self.assertEqual(runner._state.mode, "paused")
                            self.assertTrue(runner._state.pending_advance)
                            if continuation == "step":
                                self.assertEqual(
                                    (await runner.step())["phase"], "completed"
                                )
                            else:
                                await runner.resume()
                                await wait_for(
                                    lambda runner=runner: (
                                        runner.get_state()["phase"] == "completed"
                                    ),
                                    60,
                                )
                        self.assertEqual(len(trace(runner)), 1)
                finally:
                    await work.close()
