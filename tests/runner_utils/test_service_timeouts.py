"""Approved services.md B/C/D: real elapsed default timeouts, no accelerated clock."""

import asyncio
import time
import unittest

from tests.helpers.dag import process_running
from tests.helpers.services import ServiceWorkspace, wait_for


class ServiceTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ServiceWorkspace()
        self.addAsyncCleanup(self.w.close)

    async def test_startup_can_outlast_heartbeat_grace_but_not_start_timeout(self):
        """B: real 12-second preparation is valid under start_timeout=30 despite grace=10."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            controls = w.controls[definition["service_id"]]
            (controls / "hold-start").touch()
            startup = asyncio.create_task(w.manager.start_all(w.state))
            received = await wait_for(
                lambda: next(
                    (row for row in w.trace(definition) if row["event"] == "received"),
                    None,
                )
            )
            await wait_for(lambda: time.monotonic() - received["at"] >= 12, timeout=15)
            self.assertFalse(startup.done())
            self.assertEqual(
                len([row for row in w.trace(definition) if row["event"] == "started"]),
                1,
            )
            (controls / "hold-start").unlink()
            self.assertEqual(await startup, "ready")

    async def test_real_startup_deadline_and_zero_retry_budget(self):
        """B/D: no ready reply consumes the real 30-second startup deadline, then stops."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service(retries=0)
            definition["errors"]["on_exhausted"] = "stop"
            (w.controls[definition["service_id"]] / "hold-start").touch()
            before = time.monotonic()
            self.assertEqual(await w.manager.start_all(w.state), "stop")
            self.assertGreaterEqual(time.monotonic() - before, 30)
            self.assertEqual(
                len([row for row in w.trace(definition) if row["event"] == "started"]),
                1,
            )
            instance = w.state.services[definition["service_id"]]
            self.assertTrue(instance.stopped)
            self.assertEqual(instance.restart_count, 0)

    async def test_missing_heartbeat_waits_for_real_grace_then_restarts(self):
        """B/D: silence affects only the original instance; replacement is freshly ready."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            original = w.state.services[sid]
            controls = w.controls[sid]
            before = time.monotonic()
            (controls / "silent-heartbeat").write_text(original.service_instance_id)
            await wait_for(
                lambda: (
                    w.state.services[sid].service_instance_id
                    != original.service_instance_id
                    and w.state.services[sid].ready
                ),
                timeout=50,
            )
            self.assertGreaterEqual(time.monotonic() - before, 10)
            self.assertFalse(process_running(original.process_identity["pid"]))
            self.assertEqual(w.state.services[sid].restart_count, 1)

    async def check_work_timeout(self, policy):
        w = ServiceWorkspace()
        try:
            definition = w.service(policy=policy)
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            original = w.state.services[sid]
            gate = w.root / "release-work"
            first = asyncio.create_task(
                w.manager.request(w.state, sid, "echo", {"gate": str(gate), "n": 1})
            )
            second = asyncio.create_task(
                w.manager.request(w.state, sid, "echo", {"n": 2})
            )
            await wait_for(lambda: original.active_request is not None)
            request_id = original.active_request["request_id"]
            sent = original.active_request["sent_monotonic"]
            await wait_for(
                lambda: w.journal.client.read_command_result(request_id), timeout=40
            )
            self.assertGreaterEqual(time.monotonic() - sent, 30, policy)
            self.assertEqual(
                w.journal.client.read_command_result(request_id)["outcome"], "timed_out"
            )
            if policy == "restart":
                await wait_for(
                    lambda: (
                        w.state.services[sid].service_instance_id
                        != original.service_instance_id
                        and w.state.services[sid].ready
                    ),
                    timeout=45,
                )
                gate.touch()
                reply = await first
                self.assertEqual(reply["result"], "success")
                self.assertNotEqual(reply["request_id"], request_id)
                self.assertFalse(process_running(original.process_identity["pid"]))
                self.assertEqual((await second)["data"], {"n": 2})
            elif policy == "pause":
                self.assertEqual((await first)["result"], "fail")
                self.assertEqual(original.blocked_action, "pause")
                self.assertFalse(second.done())
                self.assertTrue(process_running(original.process_identity["pid"]))
                observer = asyncio.create_task(w.manager.monitor(w.state))
                gate.touch()
                self.assertEqual((await second)["result"], "success")
                self.assertEqual(
                    w.journal.client.read_command_result(request_id)["outcome"],
                    "timed_out",
                )
                self.assertTrue(
                    any(
                        row["data"].get("ignored") == "command_timeout"
                        for row in w.events("service.message_ignored")
                    )
                )
                observer.cancel()
                await asyncio.gather(observer, return_exceptions=True)
            else:
                self.assertEqual((await first)["result"], "fail")
                self.assertEqual(original.blocked_action, "stop")
                result = await w.manager.stop_all(w.state)
                self.assertTrue(result[sid]["stopped"], result)
                self.assertEqual((await second)["result"], "fail")
                self.assertFalse(process_running(original.process_identity["pid"]))
            self.assertFalse(
                any(row["event"] == "duplicate" for row in w.trace(definition))
            )
        finally:
            await w.close()

    async def test_all_command_timeout_policies_use_real_thirty_second_deadlines(self):
        """C: pause/restart/stop use actual 30-second commands with ongoing heartbeats."""
        async with asyncio.timeout(180):
            await asyncio.gather(
                *(
                    self.check_work_timeout(policy)
                    for policy in ("pause", "restart", "stop")
                )
            )

    async def test_malformed_reply_retries_once_and_second_error_restarts(self):
        """B: actual malformed frames exercise reconnect/new UUID and full replacement."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            instance = w.state.services[sid]
            controls = w.controls[sid]
            (controls / "split-frames").touch()
            previous = instance.last_status["request_id"]
            (controls / "malformed").write_text("1")
            await wait_for(
                lambda: (
                    not (controls / "malformed").exists()
                    and instance.last_status["request_id"] != previous
                )
            )
            self.assertIs(w.state.services[sid], instance)
            (controls / "malformed").write_text("2")
            await wait_for(
                lambda: (
                    w.state.services[sid].service_instance_id
                    != instance.service_instance_id
                    and w.state.services[sid].ready
                ),
                timeout=60,
            )
            self.assertFalse(process_running(instance.process_identity["pid"]))
            self.assertEqual(w.state.services[sid].restart_count, 1)
