"""Approved D/F/H migration: monitoring belongs to the server, never to its CLI."""

import asyncio
import json

from core.runner_utils.runtimeio import read_json, write_json
from tests.helpers.dag import process_running
from tests.helpers.http_runtime import HTTPServer, ServerTestCase
from tests.helpers.resources import write_settings


class ResourceCliTests(ServerTestCase):
    async def test_idle_samples_stay_in_ram_and_stopped_journal_does_not_grow(self):
        config = write_settings(self.w.root)
        server = await self.start_server(settings={"resource_config_path": str(config)})
        async with asyncio.timeout(15):
            while not (await server.get("/resources/history"))["samples"]:
                await asyncio.sleep(0.05)
        self.assertFalse((self.w.source / "experiments.json").exists())
        state = await server.launch(self.w.template())
        path = "/experiments/" + state["experiment_id"] + "/events"
        async with asyncio.timeout(15):
            while True:
                page = await server.get(path, limit=1000)
                records = [
                    row
                    for row in page["events"]
                    if row["event"]["event_type"] == "resources.recorded"
                ]
                if records:
                    break
                await asyncio.sleep(0.05)
        self.assertEqual((await server.command("stop"))["result"], "success")
        before = await server.get(path, limit=1000)
        history = await server.get("/resources/history")
        last = history["cursor"]
        async with asyncio.timeout(15):
            while True:
                later = await server.get("/resources/history", after=last)
                if len(later["samples"]) >= 3:
                    break
                await asyncio.sleep(0.05)
        after = await server.get(path, limit=1000)
        before_ids = [
            row["event"]["event_id"]
            for row in before["events"]
            if row["event"]["event_type"] == "resources.recorded"
        ]
        after_ids = [
            row["event"]["event_id"]
            for row in after["events"]
            if row["event"]["event_type"] == "resources.recorded"
        ]
        self.assertEqual(after_ids, before_ids)

    async def test_relative_server_collector_config_and_real_cli_resource_reads(self):
        server = HTTPServer(self.w)
        self.addAsyncCleanup(server.close)
        config = write_settings(server.control, history_seconds=123)
        settings = read_json(server.config)
        write_json(server.config, {**settings, "resource_config_path": config.name})
        await server.start(cwd=self.w.target)
        async with asyncio.timeout(20):
            while not (status := await server.get("/resources"))["latest"]:
                await asyncio.sleep(0.05)
        client = await self.start_cli(server, ["resources"])
        code, output, error = await client.finish()
        self.assertEqual(code, 0, error)
        status = json.loads(output)
        self.assertEqual(status["config_path"], str(config))
        collector = status["pid"]
        self.assertTrue(process_running(collector))
        history = await self.start_cli(server, ["resource-history", "--limit", "2"])
        code, output, error = await history.finish()
        self.assertEqual(code, 0, error)
        self.assertTrue(json.loads(output)["samples"])
        self.assertLessEqual(len(json.loads(output)["samples"]), 2)
        self.assertTrue(process_running(collector))
        (server.control / "stop").touch()
        await asyncio.wait_for(server.process.wait(), 30)
        self.assertFalse(process_running(collector))

    async def test_invalid_collector_config_does_not_prevent_cli_dag_completion(self):
        config = self.w.root / "invalid-collector.json"
        config.write_text("{", encoding="utf-8")
        server = await self.start_server(settings={"resource_config_path": str(config)})
        await server.launch(self.w.template())
        async with asyncio.timeout(15):
            while (status := await server.get("/resources"))[
                "state"
            ] != "configuration_error":
                await asyncio.sleep(0.05)
        self.assertTrue(status["error"])
        client = await self.start_cli(server, ["step"])
        code, output, error = await client.finish()
        self.assertEqual(code, 0, error)
        self.assertEqual(json.loads(output)["result"], "success")
        self.assertEqual((await server.get("/state"))["phase"], "completed")
        self.assertTrue((await server.get("/health"))["controller_alive"])
