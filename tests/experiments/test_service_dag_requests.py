"""Approved D/E12-13: caller-owned requests use real service processes and TCP."""

import asyncio
import time
import unittest
from uuid import uuid4

from tests.helpers.dag import terminate_owned
from tests.helpers.services import ServiceWorkspace, wait_for


class ServiceDagRequestTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ServiceWorkspace()
        self.addAsyncCleanup(self.w.close)
        self.definition = self.w.service()
        self.sid = self.definition["service_id"]
        await self.w.manager.start_all(self.w.state)

    async def test_expired_unsent_request_has_no_effect_and_no_manager_accepted_result(
        self,
    ):
        identifier = str(uuid4())
        reply = await self.w.manager.enqueue(
            self.w.state,
            self.sid,
            identifier,
            "echo",
            {},
            deadline=time.monotonic() - 1,
        )
        self.assertEqual(reply["data"]["reason"], "queue_timeout")
        self.assertIsNone(self.w.journal.client.read_command_result(identifier))
        self.assertFalse(
            any(
                row.get("request_id") == identifier
                for row in self.w.trace(self.definition)
            )
        )
        with self.assertRaises(ValueError):
            self.w.manager.enqueue(self.w.state, self.sid, identifier, "echo", {})

    async def test_caller_timeout_removes_queued_request_without_restarting_service(
        self,
    ):
        w = self.w
        gate = w.root / "release-busy"
        busy = asyncio.create_task(
            w.manager.request(w.state, self.sid, "echo", {"gate": str(gate)})
        )
        await wait_for(lambda: w.state.services[self.sid].active_request is not None)
        instance = w.state.services[self.sid]
        identifier = str(uuid4())
        queued = w.manager.enqueue(
            w.state, self.sid, identifier, "echo", {}, deadline=time.monotonic() + 0.05
        )
        await asyncio.sleep(0.1)
        self.assertTrue(await w.manager.cancel_request(w.state, self.sid, identifier))
        self.assertEqual((await queued)["data"]["reason"], "cancelled_before_send")
        gate.touch()
        await busy
        await w.manager.request(w.state, self.sid, "echo", {"barrier": True})
        self.assertFalse(
            any(row.get("request_id") == identifier for row in w.trace(self.definition))
        )
        self.assertIs(w.state.services[self.sid], instance)
        self.assertEqual(instance.restart_count, 0)

    async def test_service_restart_never_replays_sent_or_pending_caller_request(self):
        w = self.w
        first, second = str(uuid4()), str(uuid4())
        sent = w.manager.enqueue(
            w.state, self.sid, first, "echo", {"gate": str(w.root / "never")}
        )
        await wait_for(
            lambda: any(
                row["event"] == "work_started" and row["request_id"] == first
                for row in w.trace(self.definition)
            )
        )
        queued = w.manager.enqueue(w.state, self.sid, second, "echo", {})
        old = w.state.services[self.sid]
        terminate_owned(old.process_identity)
        results = await asyncio.wait_for(asyncio.gather(sent, queued), 20)
        self.assertEqual([reply["result"] for reply in results], ["fail", "fail"])
        await wait_for(
            lambda: (
                w.state.services[self.sid] is not old
                and w.state.services[self.sid].ready
            )
        )
        trace = w.trace(self.definition)
        self.assertEqual(
            sum(
                row["event"] == "work_started" and row["request_id"] == first
                for row in trace
            ),
            1,
        )
        self.assertFalse(any(row.get("request_id") == second for row in trace))
        self.assertIsNone(w.journal.client.read_command_result(first))
        self.assertIsNone(w.journal.client.read_command_result(second))
