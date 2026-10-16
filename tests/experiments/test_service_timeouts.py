"""Service deadline boundaries and a small set of real watchdog integrations."""

import asyncio
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from core.experiments import services as service_runtime
from core.experiments.state import ServiceInstance, _process_identity_pid
from core.models.participant_observations import ServiceObservation
from core.models.runner_state import WorkingServiceRequest
from core.models.updates import _update_model
from tests.helpers.dag import process_running
from tests.helpers.services import ServiceWorkspace, wait_for


class ServiceTimeoutTests(unittest.IsolatedAsyncioTestCase):
    def _workspace(self) -> ServiceWorkspace:
        work = ServiceWorkspace()
        self.addAsyncCleanup(work.close)
        return work

    async def test_startup_and_heartbeat_deadline_boundaries(self):
        """Startup uses its own budget; established health uses heartbeat grace."""
        w = self._workspace()
        definition = w.service()
        sid = definition["service_id"]
        manager = w.manager
        for ever_ready, observed_at, expired in (
            (False, 109.999, False),
            (False, 110.0, False),
            (False, 110.001, False),
            (False, 129.999, False),
            (False, 130.0, True),
            (False, 130.001, True),
            (True, 109.999, False),
            (True, 110.0, True),
            (True, 110.001, True),
        ):
            with self.subTest(ever_ready=ever_ready, observed_at=observed_at):
                instance = ServiceInstance(sid, str(uuid4()), definition)
                instance.ever_ready = ever_ready
                instance.ready = ever_ready
                instance.start_deadline = 130.0
                request_id = str(uuid4())
                response = ServiceObservation(
                    protocol_version=2,
                    request_id=request_id,
                    command="heartbeat",
                    result="success",
                    data={},
                )
                manager._probes[sid] = {
                    "request_id": request_id,
                    "sent_monotonic": 100.0,
                }
                # This synchronous operation cannot advance any background task.
                with patch.object(
                    service_runtime,
                    "time",
                    SimpleNamespace(monotonic=lambda now=observed_at: now),
                ):
                    manager._check_heartbeat_deadline(sid, instance)
                    self.assertEqual(
                        instance.failure is not None, ever_ready and expired
                    )
                    self.assertTrue(
                        manager._handle_heartbeat(
                            sid, instance, response, request_id, {}
                        )
                    )
                self.assertEqual(instance.ready, not expired)
                if expired:
                    self.assertEqual(
                        instance.failure.code,
                        "heartbeat_timeout" if ever_ready else "startup_timeout",
                    )
                else:
                    self.assertIsNone(instance.failure)
                    self.assertEqual(instance.last_status.request_id, request_id)

    async def test_command_response_deadline_boundaries(self):
        """A success received at or after the command deadline is accepted as timeout."""
        w = self._workspace()
        definition = w.service()
        sid = definition["service_id"]
        for observed_at, outcome in (
            (129.999, "succeeded"),
            (130.0, "timed_out"),
            (130.001, "timed_out"),
        ):
            with self.subTest(observed_at=observed_at):
                instance = ServiceInstance(sid, str(uuid4()), definition)
                request_id = str(uuid4())
                instance.active_request = WorkingServiceRequest(
                    owner="service",
                    request_id=request_id,
                    command="echo",
                    args={},
                    queued_monotonic=100.0,
                    sent_monotonic=100.0,
                    service_instance_id=instance.service_instance_id,
                    timed_out=False,
                )
                w.state.services[sid] = instance
                w.state.used_request_ids.add(request_id)
                with patch.object(
                    service_runtime,
                    "time",
                    SimpleNamespace(monotonic=lambda now=observed_at: now),
                ):
                    await w.manager._handle_message(
                        w.state,
                        sid,
                        {
                            "protocol_version": 2,
                            "message_type": "response",
                            "request_id": request_id,
                            "command": "echo",
                            "result": "success",
                            "data": {},
                        },
                    )
                self.assertIsNone(instance.active_request)
                self.assertEqual(
                    w.journal.client.read_command_result(request_id)["outcome"], outcome
                )

    async def test_journal_recovery_extends_deadlines_once(self):
        """Repeated reads exclude only recovery elapsed since this operation's baseline."""
        w = self._workspace()
        definition = w.service()
        instance = ServiceInstance(definition["service_id"], str(uuid4()), definition)
        instance.start_deadline = 130.0
        request = WorkingServiceRequest(
            owner="caller",
            request_id=str(uuid4()),
            command="echo",
            args={},
            queued_monotonic=100.0,
            deadline_monotonic=130.0,
            sent_monotonic=None,
            timed_out=False,
        )
        with patch.object(
            w.journal.client, "_recovery_seconds", return_value=3.0
        ) as elapsed:
            self.assertEqual(w.manager._startup_deadline(instance), 130.0)
            self.assertEqual(w.manager._request_deadline(request), 130.0)
            elapsed.return_value = 8.0
            for _ in range(2):
                self.assertEqual(w.manager._startup_deadline(instance), 135.0)
                self.assertEqual(w.manager._request_deadline(request), 135.0)
        self.assertEqual(instance.start_deadline, 130.0)
        self.assertEqual(request.deadline_monotonic, 130.0)

    async def test_startup_can_outlast_heartbeat_grace_but_not_start_timeout(self):
        """B: real 12-second preparation is valid under start_timeout=30 despite grace=10."""
        async with asyncio.timeout(120):
            w = self._workspace()
            definition = w.service()
            controls = w.controls[definition["service_id"]]
            (controls / "hold-start").touch()
            w.publish_service_definitions()
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
            w = self._workspace()
            definition = w.service(retries=0)
            definition["errors"]["on_exhausted"] = "stop"
            (w.controls[definition["service_id"]] / "hold-start").touch()
            before = time.monotonic()
            w.publish_service_definitions()
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
            w = self._workspace()
            definition = w.service()
            sid = definition["service_id"]
            w.publish_service_definitions()
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
            self.assertFalse(
                process_running(_process_identity_pid(original.process_identity))
            )
            self.assertEqual(w.state.services[sid].restart_count, 1)

    async def check_work_timeout(self, policy, *, real_deadline=False):
        w = ServiceWorkspace()
        tasks = []
        try:
            definition = w.service(policy=policy)
            sid = definition["service_id"]
            w.publish_service_definitions()
            await w.manager.start_all(w.state)
            original = w.state.services[sid]
            gate = w.root / "release-work"
            first = asyncio.create_task(
                w.manager.request(w.state, sid, "echo", {"gate": str(gate), "n": 1})
            )
            second = asyncio.create_task(
                w.manager.request(w.state, sid, "echo", {"n": 2})
            )
            tasks.extend((first, second))
            await wait_for(
                lambda: any(
                    row["event"] == "work_started" for row in w.trace(definition)
                )
            )
            request_id = original.active_request.request_id
            sent = original.active_request.sent_monotonic
            if not real_deadline:
                self.assertIsNone(w.journal.client.read_command_result(request_id))
                # The policy matrix starts from a valid expired state; the real
                # watchdog and exact arithmetic are exercised separately.
                original.active_request = _update_model(
                    original.active_request,
                    queued_monotonic=original.active_request.queued_monotonic
                    - definition["command_timeout_seconds"],
                    sent_monotonic=original.active_request.sent_monotonic
                    - definition["command_timeout_seconds"],
                )
            await wait_for(
                lambda: w.journal.client.read_command_result(request_id), timeout=40
            )
            if real_deadline:
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
                self.assertFalse(
                    process_running(_process_identity_pid(original.process_identity))
                )
                self.assertEqual((await second)["data"], {"n": 2})
            elif policy == "pause":
                self.assertEqual((await first)["result"], "fail")
                self.assertEqual(original.blocked_action, "pause")
                self.assertFalse(second.done())
                self.assertTrue(
                    process_running(_process_identity_pid(original.process_identity))
                )
                observer = asyncio.create_task(w.manager.monitor(w.state))
                tasks.append(observer)
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
                self.assertFalse(
                    process_running(_process_identity_pid(original.process_identity))
                )
            self.assertFalse(
                any(row["event"] == "duplicate" for row in w.trace(definition))
            )
        finally:
            failure = sys.exception()
            try:
                await w.close()
            except BaseException as cleanup_error:
                if failure is None:
                    raise
                failure.add_note(
                    f"Service workspace cleanup also failed: {cleanup_error!r}"
                )
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def test_expired_commands_apply_pause_restart_and_stop_policies(self):
        """All policies retain real transport, process ownership, queues and late results."""
        async with asyncio.timeout(180):
            await asyncio.gather(
                *(
                    self.check_work_timeout(policy)
                    for policy in ("pause", "restart", "stop")
                )
            )

    async def test_real_command_deadline_expires_while_heartbeats_continue(self):
        """One real default command watchdog connects the deadline rule to elapsed time."""
        async with asyncio.timeout(120):
            await self.check_work_timeout("pause", real_deadline=True)

    async def test_malformed_reply_retries_once_and_second_error_restarts(self):
        """B: actual malformed frames exercise reconnect/new UUID and full replacement."""
        async with asyncio.timeout(120):
            w = self._workspace()
            definition = w.service()
            sid = definition["service_id"]
            w.publish_service_definitions()
            await w.manager.start_all(w.state)
            instance = w.state.services[sid]
            controls = w.controls[sid]
            (controls / "split-frames").touch()
            previous = instance.last_status.request_id
            (controls / "malformed").write_text("1")
            await wait_for(
                lambda: (
                    not (controls / "malformed").exists()
                    and instance.last_status.request_id != previous
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
            self.assertFalse(
                process_running(_process_identity_pid(instance.process_identity))
            )
            self.assertEqual(w.state.services[sid].restart_count, 1)
