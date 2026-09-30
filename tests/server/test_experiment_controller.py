"""Approved basic_dag.md C1-C11: actual queues, executors, and controlled stages."""

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from tests.helpers.dag import DagSession, DagWorkspace, process_running, wait_until


class ExperimentControllerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.session = DagSession(self.workspace)
        await self.session.start()
        self.addAsyncCleanup(self.session.close)

    def trace(self):
        path = (
            self.session.runner._state.experiment_directory / "shared_data/trace.jsonl"
        )
        return (
            [json.loads(line) for line in path.read_text().splitlines()]
            if path.exists()
            else []
        )

    async def test_three_stages_two_cycles_forward_inputs_in_serial_order(self):
        """C2/C5: real stage start/finish facts prove order and per-cycle inputs."""
        async with asyncio.timeout(30):
            module = self.workspace.module()
            stages = [
                self.workspace.stage(module, settings={"label": name})
                for name in ("A", "B", "C")
            ]
            await self.session.launch(
                self.workspace.template(stages, cycles=2), paused=False
            )
            await wait_until(
                lambda: (
                    self.session.runner.get_state()["phase"] in ("completed", "failed")
                )
            )
            state = self.session.runner.get_state()
            self.assertEqual(state["phase"], "completed", state)
            trace = self.trace()
            self.assertEqual(
                [(row["event"], row["label"]) for row in trace],
                [
                    (event, label)
                    for label in ("A", "B", "C", "A", "B", "C")
                    for event in ("start", "finish")
                ],
            )
            starts = trace[::2]
            self.assertEqual([row["cycle"] for row in starts], [1, 1, 1, 2, 2, 2])
            self.assertEqual(
                [row["input"] is None for row in starts],
                [True, False, False, True, False, False],
            )
            self.assertEqual(starts[1]["input"]["trail"], ["A"])
            self.assertEqual(starts[2]["input"]["trail"], ["A", "B"])
            self.assertEqual(state["result"]["trail"], ["A", "B", "C"])

    async def test_run_acknowledges_signal_before_preparation_and_reports_failure(self):
        """C1: run acceptance and actual readiness/failure are distinct facts."""
        async with asyncio.timeout(30):
            entered, release = asyncio.Event(), asyncio.Event()

            async def prepare(*args):
                entered.set()
                await release.wait()
                raise ValueError("preparation rejected")

            with patch.object(
                self.session.runner._assembler, "assemble", side_effect=prepare
            ):
                result = await self.session.send(
                    "run", {"template_path": str(self.workspace.root / "template.yaml")}
                )
                self.assertEqual(result["result"], "success")
                self.assertTrue(result["data"]["signaled"])
                await entered.wait()
                self.assertEqual(
                    (await self.session.send("stats.state"))["data"]["phase"],
                    "starting",
                )
                release.set()
                await wait_until(
                    lambda: self.session.runner.get_state()["phase"] == "failed"
                )
            self.assertIn(
                "preparation rejected",
                self.session.runner.get_state()["error"]["message"],
            )

    async def test_delayed_start_pause_reads_step_resume_and_completion(self):
        """C2/C3/C4: queries finish while pause is awaiting a gated stage."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            module = self.workspace.module()
            stages = [
                self.workspace.stage(
                    module, settings={"label": "A", "gate": str(gate)}
                ),
                self.workspace.stage(module, settings={"label": "B"}),
                self.workspace.stage(module, settings={"label": "C"}),
            ]
            launched = await self.session.launch(self.workspace.template(stages))
            self.assertEqual(self.trace(), [])
            self.assertEqual(self.session.runner.get_state()["mode"], "paused")
            self.assertEqual((await self.session.send("resume"))["result"], "success")
            await self.session.ready_attempt()
            pause = self.session.post("pause")
            state = (await self.session.send("stats.state"))["data"]
            self.assertEqual(state["phase"], "stage_running")
            self.assertFalse(pause.done())
            logs = await self.session.send(
                "logs.read", {"experiment_id": launched["experiment_id"]}
            )
            self.assertEqual(logs["result"], "success")
            self.assertFalse(gate.exists())
            gate.touch()
            self.assertEqual((await pause)["result"], "success")
            self.assertEqual(self.session.runner.get_state()["result"]["trail"], ["A"])
            self.assertEqual(
                [row["label"] for row in self.trace() if row["event"] == "start"], ["A"]
            )
            step = await self.session.send("step")
            self.assertEqual(step["result"], "success", step)
            self.assertEqual(step["data"]["result"]["data"]["trail"], ["A", "B"])
            await self.session.send("resume")
            await wait_until(
                lambda: self.session.runner.get_state()["phase"] == "completed"
            )

    async def test_automatic_and_manual_retries_preserve_input_and_separate_counts(
        self,
    ):
        """C6: manual attempt preserves the used automatic budget and original input."""
        async with asyncio.timeout(30):
            module = self.workspace.module()
            stages = [
                self.workspace.stage(module, settings={"label": "A"}),
                self.workspace.stage(
                    module, settings={"label": "B", "failures": 1}, retries=2
                ),
                self.workspace.stage(module, settings={"label": "C"}),
            ]
            await self.session.launch(self.workspace.template(stages))
            first = await self.session.send("step")
            self.assertEqual(first["result"], "success", first)
            second = await self.session.send("step")
            self.assertEqual(second["result"], "success", second)
            stage_id = stages[1]["stage_id"]
            self.assertEqual(self.session.runner._state.stage_retry_counts[stage_id], 1)
            rerun = await self.session.send("rerun", {"scope": "stage", "position": 2})
            self.assertEqual(rerun["result"], "success", rerun)
            self.assertEqual(self.session.runner._state.stage_retry_counts[stage_id], 1)
            inputs = [
                row["input"]
                for row in self.trace()
                if row["event"] == "start" and row["label"] == "B"
            ]
            self.assertEqual(len(inputs), 3)
            self.assertEqual(inputs, [inputs[0]] * 3)
            reset = await self.session.send(
                "reset_retries", target={"kind": "stage", "position": 2}
            )
            self.assertEqual(
                reset["data"], {"stage_id": stage_id, "previous": 1, "current": 0}
            )
            self.assertEqual(self.session.runner.get_state()["mode"], "paused")

    async def test_error_policies_pause_stop_and_skip_without_forwarding_failed_data(
        self,
    ):
        """C5/C6: each exhausted policy has its own observable DAG outcome."""
        async with asyncio.timeout(30):
            module = self.workspace.module()
            for action in ("pause", "stop", "skip"):
                with self.subTest(action=action):
                    stages = [
                        self.workspace.stage(
                            module,
                            settings={"mode": "fail", "label": "bad"},
                            on_exhausted=action,
                        ),
                        self.workspace.stage(module, settings={"label": "after"}),
                    ]
                    await self.session.launch(
                        self.workspace.template(stages), paused=False
                    )
                    expected = {
                        "pause": "waiting",
                        "stop": "failed",
                        "skip": "completed",
                    }[action]
                    await wait_until(
                        lambda expected=expected: (
                            self.session.runner.get_state()["phase"] == expected
                        )
                    )
                    starts = [row for row in self.trace() if row["event"] == "start"]
                    if action == "skip":
                        self.assertEqual(starts[1]["input"], None)
                        self.assertEqual(
                            self.session.runner.get_state()["result"]["trail"],
                            ["after"],
                        )
                    else:
                        self.assertEqual(len(starts), 1)
                    await self.session.send("stop")

    async def test_move_uses_available_predecessor_and_does_not_start_work(self):
        """C7: pointer changes alone do not execute stages; later results remain usable."""
        async with asyncio.timeout(30):
            module = self.workspace.module()
            stages = [
                self.workspace.stage(module, settings={"label": name})
                for name in ("A", "B", "C", "D")
            ]
            await self.session.launch(self.workspace.template(stages))
            for _ in range(3):
                self.assertEqual((await self.session.send("step"))["result"], "success")
            await self.session.send("move", {"position": 2})
            self.assertEqual(len(self.trace()), 6)
            self.assertEqual(
                (await self.session.send("rerun", {"scope": "stage", "position": 2}))[
                    "result"
                ],
                "success",
            )
            await self.session.send("move", {"position": 4})
            reply = await self.session.send("step")
            self.assertEqual(
                reply["data"]["result"]["data"]["trail"], ["A", "B", "C", "D"]
            )

    async def test_invalid_positions_and_states_are_rejected_without_execution(self):
        """C7/C11: direct boundary checks complement the real-queue scenarios."""
        async with asyncio.timeout(30):
            runner = self.session.runner
            for command in ("pause", "resume", "step"):
                self.assertEqual((await self.session.send(command))["result"], "fail")
            await self.session.launch(self.workspace.template())
            with patch.object(runner, "step", new_callable=AsyncMock) as execute:
                for value in (0, -1, 2, True, None):
                    with self.subTest(position=value):
                        with self.assertRaises((ValueError, RuntimeError, TypeError)):
                            runner.move(value)
                        with self.assertRaises((ValueError, RuntimeError, TypeError)):
                            await runner.rerun("stage", position=value)
                execute.assert_not_awaited()
            self.assertEqual(self.trace(), [])

    async def test_command_chains_do_not_interleave(self):
        """C8: all commands in the first chain finish before the second chain begins."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            module = self.workspace.module()
            stages = [
                self.workspace.stage(
                    module, settings={"label": "A", "gate": str(gate)}
                ),
                self.workspace.stage(module, settings={"label": "B"}),
                self.workspace.stage(module),
            ]
            await self.session.launch(self.workspace.template(stages))
            first = self.session.chain(
                [
                    {"command": "step"},
                    {"command": "move", "args": {"position": 2}},
                    {"command": "step"},
                ]
            )
            second = self.session.chain(
                [
                    {"command": "move", "args": {"position": 1}},
                    {"command": "step"},
                    {"command": "stop"},
                ]
            )
            await self.session.ready_attempt()
            self.assertTrue(all(not future.done() for future in first + second))
            gate.touch()
            replies = await asyncio.gather(*first, *second)
            self.assertTrue(
                all(reply["result"] == "success" for reply in replies), replies
            )
            ids = [reply["command_id"] for reply in replies]
            self.assertEqual(
                [
                    reply["command_id"]
                    for reply in self.session.received
                    if reply["command_id"] in ids
                ],
                ids,
            )

    async def test_chain_failure_cancels_its_tail_without_cancelling_next_chain(self):
        """C8: completed effects remain; a later independent chain may still run."""
        async with asyncio.timeout(30):
            module = self.workspace.module()
            stages = [
                self.workspace.stage(module, settings={"mode": "fail"}),
                self.workspace.stage(module),
            ]
            await self.session.launch(self.workspace.template(stages))
            first = self.session.chain([{"command": "step"}, {"command": "resume"}])
            second = self.session.chain(
                [{"command": "move", "args": {"position": 2}}, {"command": "step"}]
            )
            responses = await asyncio.gather(*first, *second)
            self.assertEqual(
                [item["state"] for item in responses],
                ["failed", "cancelled", "succeeded", "succeeded"],
            )
            self.assertEqual(len(self.trace()), 4)

    async def test_priority_stop_cancels_current_command_and_waiting_tail(self):
        """C9/D7: real process termination is checked before test cleanup."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            await self.session.launch(
                self.workspace.template(
                    [self.workspace.stage(settings={"gate": str(gate)})]
                )
            )
            active = self.session.post("step")
            _, ready = await self.session.ready_attempt()
            queued = self.session.chain(
                [{"command": "move", "args": {"position": 1}}, {"command": "step"}]
            )
            pause = self.session.post("pause")
            stop = await self.session.send("stop")
            self.assertEqual(stop["result"], "success", stop)
            self.assertEqual(stop["data"]["phase"], "stopped")
            self.assertFalse(process_running(ready["pid"]))
            self.assertEqual(
                [
                    result["state"]
                    for result in await asyncio.gather(active, *queued, pause)
                ],
                ["cancelled"] * 4,
            )

    async def test_full_response_queue_does_not_block_stop(self):
        """C10: a stalled response consumer cannot stall command intake or shutdown."""
        async with asyncio.timeout(30):
            await self.session.close()
            self.session = DagSession(self.workspace, response_capacity=1)
            self.addAsyncCleanup(self.session.close)
            self.session.controller_task = asyncio.create_task(
                self.session.controller.serve()
            )
            gate = self.workspace.gate()
            template = self.workspace.template(
                [self.workspace.stage(settings={"gate": str(gate)})]
            )
            await self.session.runner.run(
                self.workspace.write_template(template), delayed_start=True
            )
            await wait_until(
                lambda: self.session.runner.get_state()["phase"] == "waiting"
            )
            step = self.session.post("step")
            _, ready = await self.session.ready_attempt()
            query = self.session.post("stats.state")
            await wait_until(self.session.responses.full)
            stop = self.session.post("stop")
            await wait_until(
                lambda: self.session.runner.get_state()["phase"] == "stopped"
            )
            self.assertFalse(process_running(ready["pid"]))
            self.session.reader_task = asyncio.create_task(self.session._read())
            self.assertEqual((await query)["result"], "success")
            self.assertEqual((await step)["state"], "cancelled")
            self.assertEqual((await stop)["result"], "success")

    async def test_unconfirmed_stop_prevents_new_run(self):
        """C11: an explicit refusal to confirm termination cannot permit a new DAG."""
        async with asyncio.timeout(30):
            template = self.workspace.template()
            await self.session.launch(template)
            with patch.object(
                self.session.runner._stages,
                "interrupt",
                new=AsyncMock(return_value=False),
            ):
                stop = await self.session.send("stop")
                self.assertEqual(stop["result"], "fail")
                self.assertFalse(
                    self.session.runner.get_state()["termination_confirmed"]
                )
                again = await self.session.send(
                    "run",
                    {"template_path": str(self.workspace.write_template(template))},
                )
                self.assertEqual(again["result"], "fail")
            self.assertEqual(self.trace(), [])

    async def test_retry_budget_is_not_reset_by_success_or_a_new_cycle(self):
        """C5/C6: stage retries remain spent across success and epoch boundaries."""
        async with asyncio.timeout(30):
            module = self.workspace.module()
            first = self.workspace.stage(
                module,
                settings={"label": "A", "failures": 1},
                retries=1,
                on_exhausted="skip",
            )
            second = self.workspace.stage(module, settings={"label": "B"})
            await self.session.launch(
                self.workspace.template([first, second], cycles=2), paused=False
            )
            await wait_until(
                lambda: (
                    self.session.runner.get_state()["phase"] in ("completed", "failed")
                )
            )
            self.assertEqual(self.session.runner.get_state()["phase"], "completed")
            starts = [row for row in self.trace() if row["event"] == "start"]
            self.assertEqual(
                [
                    (row["cycle"], row["attempt"])
                    for row in starts
                    if row["label"] == "A"
                ],
                [(1, 1), (1, 2), (2, 1)],
            )
            self.assertEqual(
                self.session.runner._state.stage_retry_counts[first["stage_id"]], 1
            )
            self.assertIsNone(
                [row for row in starts if row["label"] == "B"][-1]["input"]
            )

    async def test_terminal_states_reject_advancement_and_id_reuse(self):
        """C11: stopped, completed, and failed instances have explicit command outcomes."""
        async with asyncio.timeout(30):
            module = self.workspace.module()
            for terminal in ("stopped", "completed", "failed"):
                with self.subTest(terminal=terminal):
                    stage = self.workspace.stage(
                        module,
                        settings={
                            "mode": "fail" if terminal == "failed" else "success"
                        },
                        on_exhausted="stop",
                    )
                    launched = await self.session.launch(
                        self.workspace.template([stage])
                    )
                    await self.session.send("stop" if terminal == "stopped" else "step")
                    self.assertEqual(self.session.runner.get_state()["phase"], terminal)
                    for command in ("pause", "resume", "step"):
                        self.assertEqual(
                            (await self.session.send(command))["result"], "fail"
                        )
                    reused = await self.session.send(
                        "run",
                        {
                            "experiment_id": launched["experiment_id"],
                            "template_path": str(
                                self.workspace.root / "experiment.yaml"
                            ),
                        },
                    )
                    self.assertEqual(reused["result"], "fail")
                    self.assertEqual(reused["error"]["code"], "experiment_id_conflict")
