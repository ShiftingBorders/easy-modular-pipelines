"""Approved G/H replacement for the removed local demo/auto integration test."""

import asyncio
import json

from core.experimentassembler import find_experiment
from core.runner_utils.runtimeio import read_json
from tests.helpers.dag import process_running
from tests.helpers.http_runtime import ServerTestCase


class FullSystemDagTests(ServerTestCase):
    async def test_pause_step_snapshot_rollback_and_resume_use_real_stage_boundaries(
        self,
    ):
        gate = self.w.root / "pause.release"
        self.w.gates.append(gate)
        server = await self.start_server()
        await server.launch(self.w.template(versions=True, gate=gate))
        self.assertEqual((await server.command("resume"))["result"], "success")
        await server.wait_state("stage_running")
        pause = await server.submit("pause")
        async with asyncio.timeout(10):
            while (await server.get("/state"))["mode"] != "paused":
                await asyncio.sleep(0.025)
        gate.touch()
        self.assertEqual(
            (await server.result(pause["command_id"]))["result"], "success"
        )
        boundary = await server.wait_state("waiting")
        self.assertEqual(boundary["stage_position"], 1)
        self.assertEqual(boundary["result"]["label"], "A")
        snapshot = await server.command("snapshot", {"label": "after A"})
        self.assertEqual(snapshot["result"], "success", snapshot)
        self.assertEqual((await server.command("step"))["result"], "success")
        after_step = await server.get("/state")
        self.assertEqual(after_step["stage_position"], 2)
        self.assertEqual(after_step["result"]["label"], "B")
        rollback = await server.command(
            "rollback", {"snapshot_id": snapshot["data"]["snapshot_id"]}
        )
        self.assertEqual(rollback["result"], "success", rollback)
        restored = await server.get("/state")
        self.assertEqual(restored["stage_position"], 1)
        self.assertEqual(restored["result"]["label"], "A")
        self.assertEqual((await server.command("resume"))["result"], "success")
        completed = await server.wait_state("completed")
        self.assertEqual(completed["cycle_number"], 2)
        self.assertTrue(completed["snapshot"]["valid"])

    async def test_full_dag_completes_two_cycles_after_its_first_cli_disconnects(self):
        """Three stages, two cycles, socket/commands services, real HTTP/CLI/SQLite/processes."""
        gate = self.w.root / "release-full-dag"
        self.w.gates.append(gate)
        template = self.w.template(services=True, versions=True, gate=gate)
        server = await self.start_server()
        initial = await server.launch(template)
        first = await self.start_cli(server, ["resume"])
        code, output, error = await first.finish()
        self.assertEqual(code, 0, error)
        self.assertEqual(json.loads(output)["result"], "success")
        running = await server.wait_state("stage_running")
        self.assertEqual(running["experiment_id"], initial["experiment_id"])
        socket = next(
            row for row in running["services"] if row["interface"] == "socket"
        )
        socket_pid = socket["process"]["pid"]
        self.assertTrue(process_running(socket_pid))
        second = await self.start_cli(server, ["status"])
        code, output, error = await second.finish()
        self.assertEqual(code, 0, error)
        self.assertEqual(json.loads(output)["phase"], "stage_running")
        self.assertTrue((await server.get("/health"))["controller_alive"])
        gate.touch()
        final = await server.wait_state("completed", timeout=80)
        self.assertEqual(final["cycle_number"], 2)
        self.assertIsNone(final["error"])
        self.assertFalse(process_running(socket_pid))
        self.assertTrue(all(row["stopped"] for row in final["services"]))
        root = find_experiment(self.w.source, final["experiment_id"])
        artifacts = sorted(
            root.glob("shared_artifacts/epoch_*/*/*/attempt_*/output.json")
        )
        self.assertEqual(len(artifacts), 6)
        observed = {
            (read_json(path)["cycle"], read_json(path)["label"]) for path in artifacts
        }
        self.assertEqual(
            observed, {(cycle, stage) for cycle in (1, 2) for stage in ("A", "B", "C")}
        )
        self.assertTrue(
            all(read_json(path)["text"] == "portable input\n" for path in artifacts)
        )
        commands = next(
            row for row in final["services"] if row["interface"] == "commands"
        )
        actions = (
            root / "module_data" / commands["service_id"] / "controls/actions.jsonl"
        )
        self.assertEqual(
            [json.loads(line)["action"] for line in actions.read_text().splitlines()],
            ["start", "stop"],
        )
        snapshot = final["snapshot"]
        self.assertTrue(snapshot["valid"])
        logs = await self.start_cli(server, ["logs", "--limit", "1000"])
        code, output, error = await logs.finish()
        self.assertEqual(code, 0, error)
        self.assertTrue(json.loads(output)["events"])
        self.assertEqual(
            (await server.get("/state"))["experiment_id"], final["experiment_id"]
        )
        (server.control / "stop").touch()
        await asyncio.wait_for(server.process.wait(), 30)
        self.assertEqual(server.process.returncode, 0)
