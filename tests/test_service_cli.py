"""Approved G/H migration: disconnecting a CLI preserves server-owned services."""

import asyncio

from tests.helpers.dag import process_running
from tests.helpers.http_runtime import ServerTestCase


class ServiceCliTests(ServerTestCase):
    async def check_disconnect(self, ending):
        template = self.w.template(services=True)
        server = await self.start_server()
        state = await server.launch(template)
        socket = next(
            item for item in state["services"] if item["implementation"] == "full"
        )
        self.assertTrue(process_running(socket["process"]["pid"]))
        client = await self.start_cli(server, ["shell"])
        await client.send("status")
        if ending == "quit":
            client.process.stdin.write(b"quit\n")
            await client.process.stdin.drain()
        else:
            client.process.stdin.close()
            await client.process.stdin.wait_closed()
        code, _output, error = await client.finish()
        self.assertEqual(code, 0, error)
        self.assertTrue(process_running(socket["process"]["pid"]))
        self.assertTrue(
            all(item["ready"] for item in (await server.get("/state"))["services"])
        )
        second = await self.start_cli(server, ["stop"])
        code, _output, error = await second.finish()
        self.assertEqual(code, 0, error)
        self.assertFalse(process_running(socket["process"]["pid"]))
        self.assertTrue(
            all(item["stopped"] for item in (await server.get("/state"))["services"])
        )
        self.assertTrue((await server.get("/health"))["controller_alive"])

    async def test_quit_preserves_services_until_another_client_sends_stop(self):
        async with asyncio.timeout(90):
            await self.check_disconnect("quit")

    async def test_eof_preserves_services_until_another_client_sends_stop(self):
        async with asyncio.timeout(90):
            await self.check_disconnect("eof")
