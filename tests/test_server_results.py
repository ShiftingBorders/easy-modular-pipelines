"""Approved C/D/E: deterministic RAM limits plus real HTTP priority and archive work."""

import asyncio
import unittest
from queue import Queue
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from core.serverruntime import ServerError, ServerRuntime, load_server_settings
from tests.helpers.dag import REPOSITORY, wait_until
from tests.helpers.http_runtime import ServerTestCase


class ResultCacheTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        settings = load_server_settings(
            overrides={
                "project_root": str(REPOSITORY),
                "hash_config_path": str(REPOSITORY / "unused.json"),
                "max_pending_commands": 2,
                "max_command_records": 3,
                "read_timeout_seconds": 0.05,
                "max_read_requests": 1,
            }
        )
        # These tests exercise only RAM bookkeeping; native IPC is covered separately.
        self.runtime = ServerRuntime(settings)
        self.runtime._state = "ready"
        self.runtime._process = SimpleNamespace(is_alive=lambda: True)
        self.runtime._requests = Queue()

    def reply(self, identifier, *, state="succeeded", blob=""):
        self.runtime._accept_response(
            {
                "command_id": identifier,
                "chain_id": None,
                "state": state,
                "result": "success" if state == "succeeded" else "fail",
                "data": {"blob": blob},
                "error": None,
            }
        )

    def submit(self, name="pause"):
        return self.runtime.submit({"command": name})["command_id"]

    async def test_eviction_uses_completion_order_and_preserves_pending(self):
        first, second = self.submit(), self.submit()
        self.reply(second)
        pending = self.submit()
        # Windows can give both responses the same monotonic clock tick.
        await asyncio.sleep(0.025)
        self.reply(first)
        fourth = self.submit()
        with self.assertRaises(ServerError) as error:
            self.runtime.result(second)
        self.assertEqual(error.exception.status, 404)
        self.assertEqual(self.runtime.result(first)["state"], "succeeded")
        self.assertEqual(self.runtime.result(pending)["state"], "pending")
        self.assertEqual(self.runtime.result(fourth)["state"], "pending")

    async def test_byte_limit_ttl_and_chain_index_are_pruned(self):
        self.runtime.settings.max_cache_bytes = 400
        self.runtime.settings.max_response_bytes = 400
        first = self.submit()
        self.reply(first, blob="a" * 120)
        second = self.submit()
        self.reply(second, blob="b" * 120)
        self.assertNotIn(first, self.runtime._records)
        self.assertLessEqual(self.runtime._cache_bytes, 400)
        self.assertEqual(self.runtime.result(second)["state"], "succeeded")
        record = self.runtime._records[second]
        with (
            patch(
                "core.serverruntime.time.monotonic",
                return_value=record.finished_at + self.runtime.settings.result_ttl,
            ),
            self.assertRaises(ServerError),
        ):
            self.runtime.result(second)
        self.assertEqual(self.runtime._cache_bytes, 0)
        chain = self.runtime.submit({"commands": [{"command": "stop"}]}, chain=True)
        identifier = chain["commands"][0]["command_id"]
        self.reply(identifier)
        self.runtime.settings.result_ttl = 0
        with self.assertRaises(ServerError):
            self.runtime.result(identifier)
        self.assertEqual(self.runtime._chains, {})

    async def test_late_complete_response_replaces_unknown_but_never_known_outcome(
        self,
    ):
        identifier = self.submit()
        self.runtime._unavailable("controller exited")
        self.assertEqual(self.runtime.result(identifier)["state"], "unknown")
        self.reply(identifier)
        self.reply(identifier, state="failed")
        self.assertEqual(self.runtime.result(identifier)["state"], "succeeded")
        self.runtime._accept_response(
            {"command_id": identifier, "state": "succeeded", "result": "fail"}
        )
        self.assertEqual(self.runtime.health()["state"], "unavailable")
        self.assertEqual(self.runtime.result(identifier)["state"], "succeeded")

    async def test_active_read_collision_limit_timeout_and_cancellation_release_slot(
        self,
    ):
        identifier = str(uuid4())
        with patch("core.serverruntime.uuid4", return_value=identifier):
            task = asyncio.create_task(self.runtime.read("stats.state"))
            await asyncio.sleep(0)
        with self.assertRaises(ServerError) as collision:
            self.runtime.submit({"command": "pause", "command_id": identifier})
        self.assertEqual(collision.exception.status, 409)
        with self.assertRaises(ServerError) as full:
            await self.runtime.read("stats.state")
        self.assertEqual(full.exception.status, 429)
        with self.assertRaises(ServerError) as timed_out:
            await task
        self.assertEqual(timed_out.exception.status, 504)
        self.assertEqual(self.runtime._reads, {})
        task = asyncio.create_task(self.runtime.read("stats.state"))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.runtime._reads, {})

    async def test_transport_queue_full_does_not_leave_phantom_receipt(self):
        self.runtime._requests = Queue(maxsize=1)
        first = self.submit()
        with self.assertRaises(ServerError) as full:
            self.submit()
        self.assertEqual(full.exception.status, 429)
        self.assertEqual(list(self.runtime._records), [first])

    async def test_only_one_standalone_stop_uses_the_reserved_slot(self):
        self.submit()
        self.submit()
        stop = self.submit("stop")
        self.assertEqual(self.runtime.result(stop)["state"], "pending")
        with self.assertRaises(ServerError) as duplicate:
            self.submit("stop")
        self.assertEqual(duplicate.exception.code, "stop_pending")
        self.assertEqual(len(self.runtime._records), 3)

    async def test_unknown_outcomes_also_obey_the_result_memory_limit(self):
        self.runtime.settings.max_cache_bytes = 400
        first, second = self.submit(), self.submit()
        self.runtime._unavailable("a bounded test transport failure")
        self.assertLessEqual(self.runtime._cache_bytes, 400)
        for identifier in (first, second):
            try:
                response = self.runtime.result(identifier)
            except ServerError as error:
                self.assertEqual(error.status, 404)
            else:
                self.assertEqual(response["state"], "unknown")


class CommandFlowTests(ServerTestCase):
    async def test_server_shutdown_waits_for_archive_publication(self):
        await self.source_archive()
        server = await self.start_server(project=self.w.target)
        self.w.filer.release.clear()
        await server.submit(
            "archive.install",
            {
                "archive_path": str(self.w.archive),
                "destination": str(self.w.destination),
            },
        )
        try:
            await wait_until(self.w.filer.entered.is_set)
            (server.control / "stop").touch()
            await asyncio.sleep(0.3)
            self.assertIsNone(server.process.returncode)
            self.assertFalse((self.w.destination / "installation.json").exists())
        finally:
            self.w.filer.release.set()
        self.assertEqual(await asyncio.wait_for(server.process.wait(), 20), 0)
        self.assertTrue((self.w.destination / "installation.json").is_file())

    async def test_pending_limit_reserves_standalone_stop_even_with_chain_stop(self):
        server = await self.start_server(settings={"max_pending_commands": 3})
        gate = self.w.root / "stage.release"
        self.w.gates.append(gate)
        await server.launch(self.w.template(gate=gate))
        response = await server.client.post(
            "/chains",
            json={
                "commands": [
                    {"command": "step"},
                    {"command": "stop"},
                    {"command": "resume"},
                ]
            },
        )
        self.assertEqual(response.status_code, 202, response.text)
        ids = [item["command_id"] for item in response.json()["commands"]]
        await server.wait_state("stage_running")
        refused = await server.client.post("/commands", json={"command": "pause"})
        self.assertEqual(refused.status_code, 429, refused.text)
        stop = await server.command("stop")
        self.assertEqual(stop["state"], "succeeded", stop)
        for identifier in ids:
            self.assertEqual((await server.result(identifier))["state"], "cancelled")
        await server.wait_state("stopped")

    async def test_oversized_response_preserves_actual_outcome_and_other_results(self):
        server = await self.start_server(
            settings={"max_response_bytes": 1024}, fault_mode="controlled"
        )
        response = await server.command("fixture.echo", {"bytes": 2048})
        self.assertEqual(response["state"], "unavailable")
        self.assertIsNone(response["result"])
        self.assertEqual(
            response["error"]["details"],
            {"command_state": "succeeded", "command_result": "success"},
        )
        self.assertEqual((await server.command("fixture.echo"))["state"], "succeeded")

    async def test_archive_commit_survives_stop_and_other_clients_can_read(self):
        await self.source_archive()
        server = await self.start_server(project=self.w.target)
        self.w.filer.release.clear()
        response = await server.client.post(
            "/chains",
            json={
                "commands": [
                    {
                        "command": "archive.install",
                        "args": {
                            "archive_path": str(self.w.archive),
                            "destination": str(self.w.destination),
                        },
                    },
                    {
                        "command": "archive.inspect",
                        "args": {"archive_path": str(self.w.archive)},
                    },
                ]
            },
        )
        self.assertEqual(response.status_code, 202, response.text)
        operation, tail = [item["command_id"] for item in response.json()["commands"]]
        try:
            await wait_until(self.w.filer.entered.is_set)
            state = await server.get("/state")
            self.assertEqual(state["current_command"]["command"], "archive.install")
            stop = await server.submit("stop")
            self.assertEqual((await server.result(tail))["state"], "cancelled")
            self.assertEqual(
                (await server.get("/commands/" + operation))["state"], "pending"
            )
            self.assertEqual(
                (await server.get("/commands/" + stop["command_id"]))["state"],
                "pending",
            )
            second = await self.start_cli(server, ["status"])
            self.assertEqual((await second.finish())[0], 0)
        finally:
            self.w.filer.release.set()
        self.assertEqual((await server.result(operation))["state"], "succeeded")
        self.assertEqual(
            (await server.result(stop["command_id"]))["state"], "succeeded"
        )
        self.assertTrue((self.w.destination / "installation.json").is_file())

    async def test_server_restart_loses_receipts_without_replaying_commands(self):
        server = await self.start_server()
        failed = await server.command("pause")
        old_instance = failed["server_instance_id"]
        await server.close()
        fresh = await self.start_server()
        self.assertNotEqual(
            (await fresh.get("/health"))["server_instance_id"], old_instance
        )
        self.assertEqual(
            (await fresh.client.get("/commands/" + failed["command_id"])).status_code,
            404,
        )
        self.assertEqual((await fresh.get("/state"))["phase"], "idle")
