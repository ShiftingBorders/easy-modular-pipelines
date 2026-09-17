"""Approved A/G: common server against actual TCP and a durable SQLite journal."""

import asyncio
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from uuid import uuid4

from core.logger import OperationLogger
from core.runner_utils.connection import ParticipantConnection
from core.runner_utils.participant_server import ParticipantServer
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from tests.helpers.dag import TEMP_ROOT, wait_until
from tests.helpers.logging_process import write_settings


class ParticipantProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.folder = tempfile.TemporaryDirectory(dir=TEMP_ROOT)
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name).resolve()
        self.identity = {
            "experiment_id": "protocol-test",
            "participant_id": str(uuid4()),
            "participant_instance_id": str(uuid4()),
        }
        self.logger = OperationLogger(write_settings(self.root, context=self.identity))
        self.logger.open()
        self.addCleanup(self.logger.close)
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.calls = []
        self.controls = []
        self.server = ParticipantServer(
            self.root / "endpoint.json",
            self.identity,
            self.logger,
            self.handle,
            control_timeout_seconds=1,
        )
        await self.server.start()
        self.addAsyncCleanup(self.server.close)
        self.connection = ParticipantConnection(
            self.server.endpoint_path, self.identity
        )
        await self.connection.connect(timeout_seconds=2)
        self.addAsyncCleanup(self.connection.close)

    async def handle(self, request):
        if request["command"] in ("heartbeat", "interrupt", "shutdown"):
            self.controls.append(request["command"])
            return {"result": "success", "data": {}}
        self.calls.append(request["request_id"])
        if request["args"].get("hold"):
            self.entered.set()
            await self.release.wait()
        return {"result": "success", "data": request["args"]}

    async def test_long_work_keeps_controls_responsive_and_late_result_is_durable(self):
        identifier = str(uuid4())
        work = asyncio.create_task(
            self.connection.request(
                identifier, "echo", {"hold": True}, timeout_seconds=0.2
            )
        )
        await self.entered.wait()
        heartbeat = await self.connection.request(
            str(uuid4()), "heartbeat", {}, timeout_seconds=1
        )
        self.assertEqual(heartbeat["result"], "success")
        state = await self.connection.query_command_state(
            str(uuid4()), timeout_seconds=1
        )
        self.assertEqual(state["data"]["current"]["request_id"], identifier)
        with self.assertRaises(TimeoutError):
            await work
        self.release.set()
        await wait_until(lambda: self.logger.read_command_result(identifier))
        reply = await self.connection.request(
            str(uuid4()), "echo", {"n": 2}, timeout_seconds=1
        )
        self.assertEqual(reply["data"], {"n": 2})
        self.assertEqual(
            self.logger.read_command_result(identifier)["response"]["data"],
            {"hold": True},
        )
        with self.assertRaises(ValueError):
            await self.connection.request(identifier, "echo", {}, timeout_seconds=1)

    async def test_queue_expiry_never_enters_handler_and_interrupt_targets_one_call(
        self,
    ):
        first, second = str(uuid4()), str(uuid4())
        work = asyncio.create_task(
            self.connection.request(first, "echo", {"hold": True}, timeout_seconds=3)
        )
        await self.entered.wait()
        expired = await self.connection.request(
            second,
            "echo",
            {"n": 2},
            timeout_seconds=1,
            deadline_monotonic=time.monotonic() + 0.1,
        )
        self.assertEqual(expired["data"]["reason"], "queue_timeout")
        self.assertNotIn(second, self.calls)
        await self.connection.request(
            str(uuid4()), "interrupt", {"request_id": first}, timeout_seconds=1
        )
        self.assertEqual((await work)["data"]["reason"], "interrupted")
        self.assertEqual(
            (
                await self.connection.query_command_state(
                    str(uuid4()), timeout_seconds=1
                )
            )["data"]["pending"],
            [],
        )

    async def test_business_context_is_data_and_result_precedes_reply(self):
        identifier = str(uuid4())
        reply = await self.connection.request(
            identifier, "echo", {"context": {"business": 42}}, timeout_seconds=1
        )
        record = self.logger.read_command_result(identifier)
        self.assertEqual(record["author"], "participant")
        self.assertEqual(record["response"]["data"], reply["data"])
        self.assertNotIn("business", record["event"]["context"])
        self.assertEqual(
            record["event"]["context"]["participant_id"],
            self.identity["participant_id"],
        )

    async def test_disconnect_does_not_cancel_work_and_reconnect_rejects_reused_id(
        self,
    ):
        identifier = str(uuid4())
        work = asyncio.create_task(
            self.connection.request(
                identifier, "echo", {"hold": True}, timeout_seconds=5
            )
        )
        await self.entered.wait()
        await self.connection.close()
        with self.assertRaises(ConnectionError):
            await work
        self.release.set()
        await wait_until(lambda: self.logger.read_command_result(identifier))
        connection = ParticipantConnection(self.server.endpoint_path, self.identity)
        await connection.connect(timeout_seconds=1)
        try:
            with self.assertRaises(ConnectionError):
                await connection.request(identifier, "echo", {}, timeout_seconds=1)
        finally:
            await connection.close()
        self.assertEqual(self.calls, [identifier])

    async def test_journal_failure_blocks_effects_but_shutdown_handler_remains_available(
        self,
    ):
        settings = read_json(self.root / "settings.json")
        path = Path(settings["logging"]["db_path"])
        if not path.is_absolute():
            path = self.root / path
        lock = sqlite3.connect(path)
        lock.execute("BEGIN IMMEDIATE")
        try:
            reply = await self.connection.request(
                str(uuid4()), "echo", {}, timeout_seconds=10
            )
            self.assertEqual(reply["result"], "fail")
            self.assertEqual(self.calls, [])
        finally:
            lock.rollback()
            lock.close()
        heartbeat = await self.connection.request(
            str(uuid4()), "heartbeat", {}, timeout_seconds=1
        )
        self.assertEqual(heartbeat["result"], "fail")
        await self.connection.request(str(uuid4()), "shutdown", {}, timeout_seconds=1)
        self.assertIn("shutdown", self.controls)

    async def test_live_endpoint_and_wrong_role_are_rejected_without_overwriting_token(
        self,
    ):
        endpoint = self.server.endpoint_path.read_bytes()
        second = ParticipantServer(
            self.server.endpoint_path, self.identity, self.logger, self.handle
        )
        with self.assertRaises(RuntimeError):
            await second.start()
        self.assertEqual(self.server.endpoint_path.read_bytes(), endpoint)
        module = ParticipantConnection(
            self.server.endpoint_path, self.identity, role="module"
        )
        with self.assertRaises((EOFError, OSError)):
            await module.connect(timeout_seconds=1)
        await module.close()

    async def test_terminated_process_handle_does_not_reserve_endpoint(self):
        """A7/D12: Windows may retain the creation identity after process exit."""
        process = await asyncio.to_thread(
            subprocess.Popen,
            [sys.executable, "-c", "import time; time.sleep(0.2)"],
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        try:
            previous = process_identity(process.pid)
            await asyncio.to_thread(process.wait, 5)
            endpoint = self.root / "restarted.json"
            write_json(endpoint, {"process": previous})
            server = ParticipantServer(
                endpoint, self.identity, self.logger, self.handle
            )
            try:
                await server.start()
                self.assertNotEqual(read_json(endpoint)["process"], previous)
            finally:
                await server.close()
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
