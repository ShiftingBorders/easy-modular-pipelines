"""Approved webserver_cli.md F/H: actual HTTP CLI, replacing local runner ownership."""

import json
from pathlib import Path

from tests.helpers.http_runtime import ServerTestCase


class CliTests(ServerTestCase):
    async def test_arguments_and_invalid_json_report_errors_without_affecting_server(
        self,
    ):
        server = await self.start_server()
        for args, text in (
            (["run"], "required"),
            (["command", "run", "--args", "{invalid"], "invalid_input"),
        ):
            with self.subTest(args=args):
                client = await self.start_cli(server, args)
                code, _stdout, stderr = await client.finish()
                self.assertEqual(code, 2, stderr)
                self.assertIn(text, stderr)
                self.assertEqual((await server.get("/state"))["phase"], "idle")

    async def test_shell_reads_and_explicit_stop_work_while_real_stage_is_pending(self):
        gate = self.w.root / "release-stage"
        self.w.gates.append(gate)
        template = self.w.template(gate=gate)
        server = await self.start_server()
        await server.launch(template)
        client = await self.start_cli(server, ["shell"])
        receipt = await client.send("step")
        self.assertEqual(receipt["state"], "pending")
        await server.wait_state("stage_running")
        state = await client.send("status")
        self.assertEqual(state["phase"], "stage_running")
        logs = await client.send("logs")
        self.assertTrue(logs["events"])
        stopped = await client.send("stop --wait")
        self.assertEqual(stopped["result"], "success", stopped)
        result = await client.send(f"result {receipt['command_id']}")
        self.assertEqual(result["state"], "cancelled")
        self.assertTrue((await server.get("/health"))["controller_alive"])

    async def test_json_target_snapshot_reload_noop_and_invalid_commands_are_explicit(
        self,
    ):
        template = self.w.template()
        server = await self.start_server()
        await server.launch(template)
        client = await self.start_cli(server, ["shell"])
        reset = await client.send("reset-retries stage 1 --wait")
        self.assertEqual(reset["result"], "success", reset)
        snapshot = await client.send('snapshot --label "CLI point" --wait')
        self.assertTrue(snapshot["data"]["valid"], snapshot)
        before = await server.get("/state")
        reloaded = await client.send("command reload_template --wait")
        self.assertEqual(reloaded["result"], "success", reloaded)
        self.assertFalse(reloaded["data"]["changed"])
        self.assertIsNone(reloaded["data"]["snapshot_id"])
        after = await server.get("/state")
        self.assertEqual(after["template_revision_id"], before["template_revision_id"])
        self.assertEqual(after["stable_snapshot_id"], before["stable_snapshot_id"])
        for name, args, expected in (
            ("rollback", {"snapshot_id": "absent"}, "invalid_request"),
            ("retry", {"position": 1}, "invalid_request"),
        ):
            file = self.w.root / f"{name}-args.json"
            file.write_text(json.dumps(args), encoding="utf-8")
            response = await client.send(f'command {name} --args "@{file}" --wait')
            self.assertEqual(response["result"], "fail", response)
            self.assertEqual(response["error"]["code"], expected)

    async def test_eof_disconnects_client_and_preserves_prepared_experiment(self):
        server = await self.start_server()
        initial = await server.launch(self.w.template())
        client = await self.start_cli(server, ["shell"])
        self.assertEqual(
            (await client.send("status"))["experiment_id"], initial["experiment_id"]
        )
        client.process.stdin.close()
        await client.process.stdin.wait_closed()
        code, _out, err = await client.finish()
        self.assertEqual(code, 0, err)
        state = await server.get("/state")
        self.assertEqual(
            (state["phase"], state["experiment_id"]),
            ("waiting", initial["experiment_id"]),
        )
        self.assertTrue((await server.get("/health"))["controller_alive"])

    async def test_unescaped_unicode_server_paths_and_one_shot_json(self):
        server = await self.start_server()
        template = self.w.template()
        path = self.w.source / "эксперимент с пробелом.yaml"
        import yaml

        path.write_text(yaml.safe_dump(template), encoding="utf-8")
        client = await self.start_cli(
            server, ["run", "--template", str(path), "--delayed-start"]
        )
        code, output, errors = await client.finish()
        self.assertEqual(code, 0, errors)
        result = json.loads(output)
        await server.wait_state("waiting")
        self.assertTrue(result["data"]["signaled"])
        self.assertIn(result["command_id"], errors)
        second = await self.start_cli(server, ["status"], cwd=self.w.target)
        self.assertEqual(
            json.loads((await second.finish())[1])["experiment_id"],
            result["experiment_id"],
        )
        self.assertTrue(Path(path).is_file())

    async def test_client_wait_deadline_does_not_cancel_real_stage(self):
        gate = self.w.root / "release-deadline"
        self.w.gates.append(gate)
        server = await self.start_server()
        await server.launch(self.w.template(gate=gate))
        client = await self.start_cli(server, ["step", "--wait-timeout", "0.2"])
        code, _output, errors = await client.finish()
        self.assertEqual(code, 4, errors)
        identifier = errors.split("command_id=", 1)[1].splitlines()[0]
        self.assertEqual(
            (await server.wait_state("stage_running"))["phase"], "stage_running"
        )
        self.assertEqual(
            (await server.get("/commands/" + identifier))["state"], "pending"
        )
        gate.touch()
        result = await server.result(identifier)
        self.assertEqual(result["state"], "succeeded", result)
        await server.wait_state("completed")
