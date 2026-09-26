"""Approved RT01, RT04, RT19: real HTTP/CLI reload commands and receipt ordering."""

import copy
from uuid import uuid4

import yaml

from tests.helpers.dag import wait_until
from tests.helpers.http_runtime import ServerTestCase


class TemplateReloadCliTests(ServerTestCase):
    async def test_wait_timeout_does_not_cancel_reload_and_pending_receipt_is_observable_rt19(
        self,
    ):
        template = self.w.template()
        server = await self.start_server(fault_mode="controlled")
        before = await server.launch(template)
        template["stages"][0]["settings"]["label"] = "changed"
        path = self.w.source / "reload.yaml"
        path.write_text(yaml.safe_dump(template), encoding="utf-8")
        identifier = str(uuid4())
        (server.control / "hold-snapshot").touch()
        client = await self.start_cli(
            server,
            [
                "template",
                "reload",
                "--template",
                str(path),
                "--command-id",
                identifier,
                "--wait",
                "--wait-timeout",
                "0.3",
            ],
        )
        try:
            await wait_until((server.control / "snapshot.entered").exists, timeout=30)
            code, _output, error = await client.finish()
            self.assertEqual(code, 4, error)
            self.assertEqual(
                (await server.get("/commands/" + identifier))["state"], "pending"
            )
            self.assertEqual(
                (await server.get("/state"))["template_revision_id"],
                before["template_revision_id"],
            )
        finally:
            (server.control / "snapshot.release").touch()
        completed = await server.result(identifier)
        self.assertEqual(completed["result"], "success", completed)
        self.assertEqual((await server.get("/state"))["phase"], "waiting")

    async def test_cli_reload_and_generic_command_share_contract_and_receipt_rt19(self):
        template = self.w.template()
        server = await self.start_server()
        before = await server.launch(template)
        client = await self.start_cli(server, ["shell"])
        noop = await client.send("template reload --wait")
        self.assertEqual(noop["result"], "success", noop)
        self.assertFalse(noop["data"]["changed"])
        self.assertEqual(
            noop["data"]["template_revision_id"], before["template_revision_id"]
        )
        template["stages"][0]["settings"]["label"] = "reloaded"
        path = self.w.source / "updated template.yaml"
        path.write_text(yaml.safe_dump(template), encoding="utf-8")
        changed = await client.send(f'template reload --template "{path}" --wait')
        self.assertEqual(changed["result"], "success", changed)
        self.assertTrue(changed["data"]["changed"])
        self.assertEqual((await server.get("/state"))["phase"], "waiting")
        generic = await server.command("reload_template", {"template_path": str(path)})
        self.assertFalse(generic["data"]["changed"])
        identifier = str(uuid4())
        template["stages"][0]["settings"]["label"] = "second"
        path.write_text(yaml.safe_dump(template), encoding="utf-8")
        receipt = await server.submit(
            "reload_template", {"template_path": str(path)}, command_id=identifier
        )
        result = await server.result(receipt["command_id"])
        self.assertEqual(result["result"], "success", result)
        snapshots = await server.get("/snapshots")
        repeated = await server.submit(
            "reload_template", {"template_path": str(path)}, command_id=identifier
        )
        self.assertEqual(await server.result(repeated["command_id"]), result)
        self.assertEqual(await server.get("/snapshots"), snapshots)
        conflict = await server.client.post(
            "/commands",
            json={"command_id": identifier, "command": "reload_template", "args": {}},
        )
        self.assertEqual(conflict.status_code, 409)

    async def test_admission_invalid_arguments_and_maintenance_mode_rt01(self):
        template = self.w.template()
        server = await self.start_server()
        absent = await server.command("reload_template")
        self.assertEqual(absent["error"]["code"], "invalid_state")
        await server.launch(template)
        for args, target in (
            ({"unknown": 1}, None),
            ({}, {"kind": "stage", "position": 1}),
            ({"template_path": "relative.yaml"}, None),
        ):
            result = await server.command("reload_template", args, target=target)
            self.assertEqual(result["error"]["code"], "invalid_request", result)
        self.assertEqual((await server.get("/state"))["phase"], "waiting")
        await server.command("stop")
        mode = await server.command("server.mode", {"mode": "maintenance"})
        self.assertEqual(mode["result"], "success", mode)
        rejected = await server.client.post(
            "/commands", json={"command": "reload_template", "args": {}}
        )
        self.assertEqual(rejected.status_code, 409)
        self.assertIn("invalid_mode", rejected.text)

    async def test_chain_applies_revision_before_resume_and_failure_cancels_tail_rt19(
        self,
    ):
        gate = self.w.root / "release-reloaded-stage"
        self.w.gates.append(gate)
        template = self.w.template(gate=gate)
        server = await self.start_server()
        before = await server.launch(template)
        path = self.w.source / "updated.yaml"
        changed = copy.deepcopy(template)
        changed["stages"][0]["settings"]["label"] = "new"
        path.write_text(yaml.safe_dump(changed), encoding="utf-8")
        response = await server.client.post(
            "/chains",
            json={
                "commands": [
                    {"command": "pause"},
                    {
                        "command": "reload_template",
                        "args": {"template_path": str(path)},
                    },
                    {"command": "resume"},
                ]
            },
        )
        self.assertEqual(response.status_code, 202, response.text)
        results = [
            await server.result(item["command_id"])
            for item in response.json()["commands"]
        ]
        self.assertEqual([item["result"] for item in results], ["success"] * 3)
        state = await server.wait_state("stage_running")
        self.assertNotEqual(
            state["template_revision_id"], before["template_revision_id"]
        )
        self.assertEqual(
            state["template_revision_id"], results[1]["data"]["template_revision_id"]
        )
        gate.touch()
        await server.wait_state("completed")
        await server.launch(template)
        path.write_text("invalid: template", encoding="utf-8")
        response = await server.client.post(
            "/chains",
            json={
                "commands": [
                    {"command": "pause"},
                    {
                        "command": "reload_template",
                        "args": {"template_path": str(path)},
                    },
                    {"command": "resume"},
                ]
            },
        )
        results = [
            await server.result(item["command_id"])
            for item in response.json()["commands"]
        ]
        self.assertEqual(
            [item["state"] for item in results], ["succeeded", "failed", "cancelled"]
        )
        self.assertEqual((await server.get("/state"))["mode"], "paused")
