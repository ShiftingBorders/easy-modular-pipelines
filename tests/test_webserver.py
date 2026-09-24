"""Approved webserver_cli.md A/C/D: real HTTP to a spawned controller."""

import asyncio
import json
import os
import sqlite3
from urllib.parse import urlsplit
from uuid import uuid4

from tests.helpers.http_runtime import ServerTestCase


class HTTPContractTests(ServerTestCase):
    async def test_openapi_and_authentication_cover_real_http_routes(self):
        token = "owned-test-secret"
        server = await self.start_server(
            settings={"token_env": "EMP_TEST_TOKEN"}, env={"EMP_TEST_TOKEN": token}
        )
        for path in ("/health", "/state", "/resources"):
            response = await server.client.get(
                path, headers={"Authorization": "Bearer incorrect"}
            )
            self.assertEqual(response.status_code, 401)
            self.assertEqual(response.headers["www-authenticate"], "Bearer")
            self.assertNotIn(token, response.text)
        self.assertEqual((await server.get("/state"))["phase"], "idle")
        schema = await server.client.get(
            server.url.removesuffix("/api") + "/openapi.json"
        )
        self.assertEqual(schema.status_code, 200)
        self.assertIn("requestBody", schema.json()["paths"]["/api/commands"]["post"])

    async def test_invalid_envelopes_targets_ids_and_unknown_commands_are_rejected(
        self,
    ):
        server = await self.start_server()
        for document in (
            {},
            [],
            {"command": "pause", "extra": 1},
            {"command": "pause", "api_version": True},
            {"command": "pause", "api_version": 2},
            {"command": "pause", "command_id": "bad"},
            {"command": "pause", "args": []},
            {"command": "retry", "target": {"kind": "stage", "position": True}},
            {"command": "retry", "target": {"kind": "other", "position": 1}},
            {"command": "server.unknown"},
        ):
            with self.subTest(document=document):
                response = await server.client.post("/commands", json=document)
                self.assertEqual(response.status_code, 400, response.text)
        for body in (
            {"commands": []},
            {"commands": [{"command": "server.shutdown"}]},
            {"commands": [{"commands": []}]},
            {"commands": [{"command": "pause"}], "extra": 1},
        ):
            with self.subTest(body=body):
                self.assertEqual(
                    (await server.client.post("/chains", json=body)).status_code, 400
                )
        self.assertEqual((await server.get("/state"))["phase"], "idle")
        self.assertTrue((await server.get("/health"))["controller_alive"])

    async def test_http_body_size_content_type_syntax_and_query_boundaries(self):
        server = await self.start_server(
            settings={"max_request_bytes": 512, "read_timeout_seconds": 0.2}
        )
        response = await server.client.post(
            "/commands", content=b"{}", headers={"Content-Type": "text/plain"}
        )
        self.assertEqual(response.status_code, 415)
        response = await server.client.post(
            "/commands", content=b"{", headers={"Content-Type": "application/json"}
        )
        self.assertEqual(response.status_code, 400)
        response = await server.client.post(
            "/commands", json={"command": "pause", "args": {"large": "x" * 1024}}
        )
        self.assertEqual(response.status_code, 413)
        for path, params in (
            ("/state", {"extra": 1}),
            ("/resources", {"limit": 1}),
            ("/resources/history", {"after": -1}),
            ("/resources/history", {"limit": 1001}),
            ("/resources/history", [("limit", 1), ("limit", 2)]),
        ):
            with self.subTest(path=path, params=params):
                response = await server.client.get(path, params=params)
                self.assertEqual(response.status_code, 400, response.text)
        address = urlsplit(server.url)
        reader, writer = await asyncio.open_connection(address.hostname, address.port)
        try:
            writer.write(
                b"POST /api/commands HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{"
            )
            await writer.drain()
            status = await asyncio.wait_for(reader.readline(), 3)
            self.assertIn(b"408", status)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_retained_ids_chains_conflicts_and_expired_results(self):
        server = await self.start_server(settings={"result_ttl_seconds": 0.5})
        identifier = str(uuid4())
        body = {"command_id": identifier, "command": "stats.state"}
        first = await server.client.post("/commands", json=body)
        self.assertEqual(first.status_code, 202)
        self.assertTrue(first.headers["location"].endswith(identifier))
        original = await server.result(identifier)
        replay = await server.client.post("/commands", json=body)
        self.assertEqual(replay.json(), original)
        conflict = await server.client.post(
            "/commands", json={**body, "command": "pause"}
        )
        self.assertEqual(conflict.status_code, 409)
        chain_id = str(uuid4())
        commands = [
            {"command_id": str(uuid4()), "command": name}
            for name in ("stats.state", "pause", "resume")
        ]
        chain = {"chain_id": chain_id, "commands": commands}
        response = await server.client.post("/chains", json=chain)
        self.assertEqual(response.status_code, 202, response.text)
        results = [await server.result(item["command_id"]) for item in commands]
        self.assertEqual(
            [r["state"] for r in results], ["succeeded", "failed", "cancelled"]
        )
        self.assertEqual(
            results[0]["data"]["current_command"]["command_id"],
            commands[0]["command_id"],
        )
        self.assertEqual(
            (
                await server.client.post(
                    "/chains", json={**chain, "commands": list(reversed(commands))}
                )
            ).status_code,
            409,
        )
        await asyncio.sleep(0.6)
        response = await server.client.get("/commands/" + identifier)
        self.assertEqual(response.status_code, 404)
        self.assertIn("never executed", response.json()["error"]["message"])
        self.assertEqual(
            (await server.client.get("/commands/not-uuid")).status_code, 400
        )

    async def test_health_remains_responsive_when_real_hash_database_blocks_controller(
        self,
    ):
        template = self.w.template()
        server = await self.start_server(settings={"read_timeout_seconds": 0.15})
        path = self.w.source / "blocked-template.yaml"
        import yaml

        path.write_text(yaml.safe_dump(template), encoding="utf-8")
        lock = sqlite3.connect(self.w.source / "hashes.sqlite")
        try:
            lock.execute("BEGIN EXCLUSIVE")
            receipt = await server.submit(
                "run", {"template_path": str(path), "delayed_start": True}
            )
            async with asyncio.timeout(3):
                while True:
                    response = await server.client.get("/state")
                    if response.status_code == 504:
                        break
                    await asyncio.sleep(0.025)
            health = await asyncio.wait_for(server.get("/health"), 1)
            self.assertTrue(health["controller_alive"])
            self.assertNotIn("phase", response.json())
        finally:
            lock.rollback()
            lock.close()
        self.assertEqual(
            (await server.result(receipt["command_id"]))["result"], "success"
        )
        await server.wait_state("waiting")

    async def test_journal_checkpoint_and_read_failures_survive_http_translation(self):
        server = await self.start_server()
        state = await server.launch(self.w.template())
        path = "/experiments/" + state["experiment_id"] + "/events"
        first = await server.get(path, limit=1)
        self.assertEqual(len(first["events"]), 1)
        following = await server.get(
            path, limit=1000, cursor=json.dumps(first["checkpoint"])
        )
        self.assertNotIn(
            first["events"][0]["event"]["event_id"],
            [row["event"]["event_id"] for row in following["events"]],
        )
        for params in (
            {"limit": 0},
            {"cursor": "bad"},
            {"cursor": "x" * 4097},
            {"extra": "field"},
        ):
            self.assertEqual(
                (await server.client.get(path, params=params)).status_code, 400
            )
        self.assertEqual(
            (await server.client.get("/experiments/absent/events")).status_code, 404
        )
        snapshot = await server.command("snapshot")
        self.assertEqual(snapshot["result"], "success", snapshot)
        self.assertEqual(
            (
                await server.command(
                    "rollback", {"snapshot_id": snapshot["data"]["snapshot_id"]}
                )
            )["result"],
            "success",
        )
        old = await server.client.get(
            path, params={"cursor": json.dumps(first["checkpoint"])}
        )
        self.assertNotEqual(old.status_code, 200)
        self.assertEqual((await server.get("/state"))["phase"], "waiting")

    async def test_real_server_health_state_and_command_result(self):
        """B/C/D: the HTTP server, test caller and controller are separate real processes."""
        server = await self.start_server()
        health = await server.get("/health")
        self.assertTrue(health["controller_alive"])
        self.assertNotEqual(health["controller"]["pid"], os.getpid())
        self.assertEqual((await server.get("/state"))["phase"], "idle")
        receipt = await server.submit("pause")
        result = await server.result(receipt["command_id"])
        self.assertEqual(result["state"], "failed")
        self.assertEqual(result["error"]["code"], "invalid_state")
        self.assertEqual(result["server_instance_id"], health["server_instance_id"])
