"""Approved services.md C/D/E/F: reconnection, replay prevention and rebuild boundaries."""

import asyncio
import copy
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

from core.runner_utils.connection import ParticipantConnection
from core.runner_utils.runtimeio import read_json, write_json
from tests.helpers.dag import process_running
from tests.helpers.services import ServiceWorkspace, wait_for


class ServiceRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ServiceWorkspace()
        self.addAsyncCleanup(self.w.close)

    async def test_untracked_peer_work_prevents_recovery_and_replay(self):
        """E: actual work submitted by a lost owner cannot be inferred safe to repeat."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            instance = w.state.services[sid]
            await w.manager.close()
            endpoint = read_json(instance.endpoint_path)
            identity = {
                key: endpoint[key]
                for key in (
                    "experiment_id",
                    "participant_id",
                    "participant_instance_id",
                )
            }
            connection = ParticipantConnection(instance.endpoint_path, identity)
            try:
                await connection.connect(timeout_seconds=30)
                await connection.send_message(
                    {
                        "protocol_version": 2,
                        "message_type": "request",
                        **identity,
                        "request_id": str(uuid4()),
                        "command": "echo",
                        "args": {"gate": str(w.root / "never-release")},
                    }
                )
                await wait_for(
                    lambda: any(
                        row["event"] == "work_started" for row in w.trace(definition)
                    )
                )
            finally:
                await connection.close()
            self.assertEqual(await w.replacement_manager().recover(w.state), "stop")
            await asyncio.sleep(definition["heartbeat"]["interval_seconds"] + 0.2)
            self.assertIs(w.state.services[sid], instance)
            self.assertTrue(process_running(instance.process_identity["pid"]))
            self.assertEqual(
                len(
                    [
                        row
                        for row in w.trace(definition)
                        if row["event"] == "work_started"
                    ]
                ),
                1,
            )

    async def test_recovered_service_without_owned_handle_is_not_forcibly_killed(self):
        """D/E: a failed RPC after reconnection does not confer an OS kill capability."""
        async with asyncio.timeout(120):
            w = self.w
            first, second = w.service(), w.service()
            await w.manager.start_all(w.state)
            sid = first["service_id"]
            original = w.state.services[sid]
            await w.manager.close()
            self.assertEqual(await w.replacement_manager().recover(w.state), "ready")
            self.assertEqual(w.manager._processes, {})
            (w.controls[sid] / "ignore-shutdown").touch()
            began = time.monotonic()
            result = await w.manager.stop_all(w.state)
            self.assertGreaterEqual(time.monotonic() - began, 30)
            self.assertFalse(result[sid]["stopped"])
            self.assertTrue(result[second["service_id"]]["stopped"])
            self.assertTrue(process_running(original.process_identity["pid"]))
            self.assertEqual(
                len([row for row in w.trace(first) if row["event"] == "started"]), 1
            )
            (w.controls[sid] / "ignore-shutdown").unlink()
            self.assertTrue((await w.manager.stop_all(w.state))[sid]["stopped"])

    async def test_old_probe_and_successful_work_do_not_restore_health(self):
        """A/D: late responses are dropped by correlation; work cannot replace heartbeat."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            original = w.state.services[sid]
            previous = original.last_status["request_id"]
            (w.controls[sid] / "silent-heartbeat").write_text(
                original.service_instance_id
            )
            base = {
                "protocol_version": 2,
                "message_type": "response",
                "result": "success",
                "data": {},
            }
            write_json(
                w.controls[sid] / "heartbeat-replies.json",
                {
                    "messages": [
                        {**base, "request_id": previous},
                    ]
                },
            )
            await wait_for(
                lambda: not (w.controls[sid] / "heartbeat-replies.json").exists()
            )
            reply = await w.manager.request(w.state, sid, "echo", {})
            self.assertEqual(reply["result"], "success")
            self.assertEqual(original.last_status["request_id"], previous)
            await wait_for(
                lambda: (
                    w.state.services[sid] is not original
                    and w.state.services[sid].ready
                ),
                timeout=50,
            )

    async def test_shared_code_change_stops_every_live_owner(self):
        """F: replacing one shared module version stops all services using its code directory."""
        async with asyncio.timeout(120):
            w = self.w
            first, second, survivor = w.service(), w.service(), w.service()
            second["module"] = copy.deepcopy(first["module"])
            await w.manager.start_all(w.state)
            template = copy.deepcopy(w.state.template)
            for definition in template["services"][:2]:
                definition["module"]["hash"] = "f" * 64
            await w.manager.prepare_rebuild(w.state, template)
            for definition in (first, second):
                instance = w.state.services[definition["service_id"]]
                self.assertTrue(instance.stopped)
                self.assertFalse(process_running(instance.process_identity["pid"]))
            self.assertTrue(w.state.services[survivor["service_id"]].ready)

    async def test_close_detaches_and_recovery_preserves_live_work_and_pending_ids(
        self,
    ):
        """C/E: the real service survives close, then resumes its original sent request and queue."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            identity = w.state.services[sid].process_identity
            gate = w.root / "release"
            calls = [
                asyncio.create_task(w.manager.request(w.state, sid, "echo", args))
                for args in ({"gate": str(gate)}, {"second": True})
            ]
            await wait_for(
                lambda: any(
                    row["event"] == "work_started" for row in w.trace(definition)
                )
            )
            active_id = w.state.services[sid].active_request["request_id"]
            pending_id = w.state.services[sid].pending_requests[0]["request_id"]
            w.store.save(w.state)
            await w.manager.close()
            await asyncio.gather(*calls, return_exceptions=True)
            self.assertTrue(process_running(identity["pid"]))
            w.state = w.store.load(w.experiment)
            manager = w.replacement_manager()
            self.assertEqual(await manager.recover(w.state), "ready")
            self.assertEqual(w.state.services[sid].process_identity, identity)
            gate.touch()
            await wait_for(lambda: w.journal.client.read_command_result(pending_id))
            for request_id in (active_id, pending_id):
                self.assertEqual(
                    w.journal.client.read_command_result(request_id)["outcome"],
                    "succeeded",
                )
                self.assertEqual(
                    len(
                        [
                            row
                            for row in w.trace(definition)
                            if row["event"] == "received"
                            and row.get("request_id") == request_id
                        ]
                    ),
                    1,
                )
            stopped = await manager.stop_all(w.state)
            self.assertTrue(stopped[sid]["stopped"], stopped)
            self.assertFalse(process_running(identity["pid"]))

    async def test_unreported_result_does_not_become_a_replayed_request(self):
        """E: absence from current/pending is not proof that the old request was never executed."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            call = asyncio.create_task(
                w.manager.request(w.state, sid, "echo", {"lose_result": True})
            )
            await wait_for(
                lambda: any(
                    row["event"] == "unreported_work_finished"
                    for row in w.trace(definition)
                )
            )
            request_id = w.state.services[sid].active_request["request_id"]
            w.store.save(w.state)
            await w.manager.close()
            await asyncio.gather(call, return_exceptions=True)
            w.state = w.store.load(w.experiment)
            self.assertEqual(await w.replacement_manager().recover(w.state), "ready")
            self.assertEqual(
                w.state.services[sid].active_request["request_id"], request_id
            )
            self.assertIsNone(w.journal.client.read_command_result(request_id))
            self.assertEqual(
                len(
                    [
                        row
                        for row in w.trace(definition)
                        if row["event"] == "received"
                        and row.get("request_id") == request_id
                    ]
                ),
                1,
            )

    async def test_recovery_uses_remaining_real_command_deadline(self):
        """E: eight seconds of downtime/prehistory are not added to the 30-second request budget."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            call = asyncio.create_task(
                w.manager.request(
                    w.state, sid, "echo", {"gate": str(w.root / "unreleased")}
                )
            )
            await wait_for(lambda: w.state.services[sid].active_request is not None)
            entry = dict(w.state.services[sid].active_request)
            await wait_for(
                lambda: time.monotonic() - entry["sent_monotonic"] >= 8, timeout=12
            )
            w.store.save(w.state)
            await w.manager.close()
            await asyncio.gather(call, return_exceptions=True)
            w.state = w.store.load(w.experiment)
            manager = w.replacement_manager()
            recovered_at = time.monotonic()
            self.assertEqual(await manager.recover(w.state), "ready")
            self.assertEqual(
                w.state.services[sid].active_request["sent_monotonic"],
                entry["sent_monotonic"],
            )
            self.assertEqual(await manager.monitor(w.state), "pause")
            self.assertGreaterEqual(time.monotonic() - entry["sent_monotonic"], 30)
            self.assertLess(time.monotonic() - recovered_at, 28)
            self.assertEqual(
                w.journal.client.read_command_result(entry["request_id"])["outcome"],
                "timed_out",
            )

    async def test_journal_accepted_result_is_not_reclassified_after_deadline(self):
        """E: actual accepted outcome survives stale state.json and more than 30 seconds elapsed."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            gate = w.root / "release"
            call = asyncio.create_task(
                w.manager.request(w.state, sid, "echo", {"gate": str(gate)})
            )
            await wait_for(lambda: w.state.services[sid].active_request is not None)
            entry = dict(w.state.services[sid].active_request)
            w.store.save(w.state)
            with patch.object(
                w.store,
                "save",
                side_effect=OSError("publication unavailable after result"),
            ):
                gate.touch()
                reply = await call
                self.assertEqual(reply["result"], "success")
                await w.manager.close()
            await wait_for(
                lambda: time.monotonic() - entry["sent_monotonic"] > 30, timeout=35
            )
            w.state = w.store.load(w.experiment)
            self.assertIsNotNone(w.state.services[sid].active_request)
            self.assertEqual(await w.replacement_manager().recover(w.state), "ready")
            self.assertIsNone(w.state.services[sid].active_request)
            self.assertEqual(
                w.journal.client.read_command_result(entry["request_id"])["outcome"],
                "succeeded",
            )

    async def test_changed_service_restarts_while_unchanged_instance_survives(self):
        """F: prepare/reconcile stop only affected services and retain the owner's pause."""
        async with asyncio.timeout(120):
            w = self.w
            first, second = w.service(), w.service()
            await w.manager.start_all(w.state)
            old = {sid: instance for sid, instance in w.state.services.items()}
            template = copy.deepcopy(w.state.template)
            template["services"][0]["settings"]["new_setting"] = True
            await w.manager.prepare_rebuild(w.state, template)
            self.assertTrue(old[first["service_id"]].stopped)
            self.assertTrue(
                process_running(old[second["service_id"]].process_identity["pid"])
            )
            w.state.template = template
            self.assertEqual(await w.manager.reconcile(w.state, template), "ready")
            self.assertIs(
                w.state.services[second["service_id"]], old[second["service_id"]]
            )
            self.assertNotEqual(
                w.state.services[first["service_id"]].service_instance_id,
                old[first["service_id"]].service_instance_id,
            )
            self.assertEqual(w.state.mode, "paused")

    async def test_removed_and_added_services_reconcile_without_touching_survivor(self):
        """F: removed definitions stop; new definitions start; unaffected definitions keep identity."""
        async with asyncio.timeout(120):
            w = self.w
            removed, survivor, added = w.service(), w.service(), w.service()
            template = copy.deepcopy(w.state.template)
            w.state.template["services"] = [removed, survivor]
            await w.manager.start_all(w.state)
            survivor_instance = w.state.services[survivor["service_id"]]
            removed_instance = w.state.services[removed["service_id"]]
            template["services"] = [survivor, added]
            await w.manager.prepare_rebuild(w.state, template)
            w.state.template = template
            self.assertEqual(await w.manager.reconcile(w.state, template), "ready")
            self.assertNotIn(removed["service_id"], w.state.services)
            self.assertFalse(process_running(removed_instance.process_identity["pid"]))
            self.assertIs(w.state.services[survivor["service_id"]], survivor_instance)
            self.assertTrue(w.state.services[added["service_id"]].ready)

    async def test_backpressure_on_one_real_socket_does_not_block_other_service_or_stop(
        self,
    ):
        """B/D: a peer stops reading a large frame; independent work and fresh-channel stop still succeed."""
        async with asyncio.timeout(120):
            w = self.w
            blocked, healthy = w.service(), w.service()
            await w.manager.start_all(w.state)
            sid = blocked["service_id"]
            (w.controls[sid] / "pause-reading").touch()
            working = asyncio.create_task(
                w.manager.request(
                    w.state, sid, "echo", {"bulk": "x" * (2 * 1024 * 1024)}
                )
            )
            await wait_for(lambda: (w.controls[sid] / "reading-paused.json").exists())
            reply = await w.manager.request(
                w.state, healthy["service_id"], "echo", {"available": True}
            )
            self.assertEqual(reply["result"], "success")
            result = await w.manager.stop_all(w.state)
            self.assertTrue(all(value["stopped"] for value in result.values()), result)
            self.assertEqual((await working)["result"], "fail")

    async def test_unknown_process_identity_and_unmatched_peer_work_fail_closed(self):
        """E: invalid saved identity cannot authorize an unverified replacement or replay."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            await w.manager.start_all(w.state)
            sid = definition["service_id"]
            identity = w.state.services[sid].process_identity
            await w.manager.close()
            w.state.services[sid].process_identity = None
            self.assertEqual(await w.replacement_manager().recover(w.state), "stop")
            self.assertTrue(process_running(identity["pid"]))
            self.assertEqual(
                len([row for row in w.trace(definition) if row["event"] == "started"]),
                1,
            )
