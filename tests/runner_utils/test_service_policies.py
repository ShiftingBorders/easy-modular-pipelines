"""Approved services.md A/C/D: real failure policies and safe shutdown ownership."""

import asyncio
import time
import unittest
from unittest.mock import patch

from tests.helpers.dag import process_running
from tests.helpers.services import ServiceWorkspace, wait_for


class ServicePolicyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ServiceWorkspace()
        self.addAsyncCleanup(self.w.close)

    async def test_retry_budgets_pause_stop_and_manual_restart(self):
        """D: one initial launch plus three automatic attempts, preserving counts on manual recovery."""
        async with asyncio.timeout(180):
            for policy in ("pause", "stop"):
                w = ServiceWorkspace()
                try:
                    definition = w.service()
                    definition["errors"]["on_exhausted"] = policy
                    sid = definition["service_id"]
                    (w.controls[sid] / "fail-health").touch()
                    self.assertEqual(await w.manager.start_all(w.state), policy)
                    self.assertEqual(
                        len(
                            [
                                row
                                for row in w.trace(definition)
                                if row["event"] == "started"
                            ]
                        ),
                        4,
                    )
                    self.assertEqual(w.state.services[sid].restart_count, 3)
                    self.assertTrue(w.state.services[sid].stopped)
                    if policy == "pause":
                        (w.controls[sid] / "fail-health").unlink()
                        self.assertEqual(
                            await w.manager.restart(w.state, sid, automatic=False),
                            "ready",
                        )
                        self.assertEqual(w.state.services[sid].restart_count, 3)
                        self.assertEqual(w.state.mode, "paused")
                finally:
                    await w.close()

    async def test_skip_continues_recovery_beyond_the_budget(self):
        """D: service skip means unlimited recovery, never advancing with an unavailable service."""
        async with asyncio.timeout(180):
            w = self.w
            definition = w.service()
            definition["errors"]["on_exhausted"] = "skip"
            sid = definition["service_id"]
            (w.controls[sid] / "fail-health").touch()
            starting = asyncio.create_task(w.manager.start_all(w.state))
            await wait_for(
                lambda: (
                    len(
                        [
                            row
                            for row in w.trace(definition)
                            if row["event"] == "started"
                        ]
                    )
                    >= 5
                ),
                timeout=40,
            )
            self.assertFalse(starting.done())
            (w.controls[sid] / "fail-health").unlink()
            self.assertEqual(await starting, "ready")
            self.assertGreaterEqual(w.state.services[sid].restart_count, 4)

    async def test_cancelling_startup_stops_the_already_created_process(self):
        """A: cancelling readiness cannot leave a half-started owned process running."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            (w.controls[sid] / "hold-start").touch()
            starting = asyncio.create_task(w.manager.start_all(w.state))
            started = await wait_for(
                lambda: next(
                    (row for row in w.trace(definition) if row["event"] == "started"),
                    None,
                )
            )
            starting.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await starting
            self.assertTrue(w.state.services[sid].stopped)
            self.assertFalse(process_running(started["process"]["pid"]))

    async def test_cancelling_a_work_waiter_does_not_replay_or_cancel_sent_work(self):
        """C: abandoning an await leaves exactly one actual command and a journaled outcome."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            gate = w.root / "release"
            working = asyncio.create_task(
                w.manager.request(w.state, sid, "echo", {"gate": str(gate)})
            )
            await wait_for(
                lambda: any(
                    row["event"] == "work_started" for row in w.trace(definition)
                )
            )
            request_id = w.state.services[sid].active_request["request_id"]
            working.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await working
            gate.touch()
            await wait_for(
                lambda: (
                    w.journal.client.read_command_result(request_id) is not None
                    and not w.journal.client.read_command_result(request_id)[
                        "provisional"
                    ]
                )
            )
            self.assertEqual(
                w.journal.client.read_command_result(request_id)["outcome"], "succeeded"
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

    async def test_unconfirmed_stop_cannot_launch_another_instance(self):
        """D: a real unresponsive peer plus denied owned kill cannot authorize a second copy."""
        async with asyncio.timeout(180):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            instance = w.state.services[sid]
            (w.controls[sid] / "ignore-shutdown").touch()
            process = w.manager._processes[sid]
            with patch.object(
                process,
                "kill",
                side_effect=PermissionError("test: process kill denied"),
            ):
                began = time.monotonic()
                result = await w.manager.stop_all(w.state)
                self.assertGreaterEqual(time.monotonic() - began, 30)
                self.assertFalse(result[sid]["stopped"])
                self.assertTrue(process_running(instance.process_identity["pid"]))
                self.assertEqual(
                    await w.manager.restart(w.state, sid, automatic=False), "stop"
                )
                self.assertEqual(
                    len(
                        [
                            row
                            for row in w.trace(definition)
                            if row["event"] == "started"
                        ]
                    ),
                    1,
                )
            (w.controls[sid] / "ignore-shutdown").unlink()
            result = await w.manager.stop_all(w.state)
            self.assertTrue(result[sid]["stopped"], result)
            if w.manager._monitor_task.done():
                self.assertIn(w.manager._monitor_task.result(), ("pause", "stop"))

    async def test_bad_stop_command_does_not_claim_commands_only_service_was_disabled(
        self,
    ):
        """D: actual command spawn failure remains a failed stop despite exited start command."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service(interface="commands", implementation="action")
            await w.manager.start_all(w.state)
            module = definition["module"]
            code = w.experiment / "modules" / module["name"] / module["version"]
            import yaml

            config = yaml.safe_load((code / "module.yaml").read_text())
            config["commands"]["stop"] = [str(w.root / "missing-stop-command.exe")]
            (code / "module.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")
            module["hash"] = w.modules.module_hash(module["name"], target_folder=code)
            w.hashes.remove_module_hash(module["name"], module["version"])
            w.hashes.add_module_hash(module["name"], module["version"], module["hash"])
            w.state.services[definition["service_id"]].definition["module"] = module
            result = await w.manager.stop_all(w.state)
            self.assertFalse(result[definition["service_id"]]["stopped"])
            self.assertTrue(result[definition["service_id"]]["error"])
