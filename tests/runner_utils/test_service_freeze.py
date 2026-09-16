"""Approved services.md F: real freezes and both participants' crash/hang matrix."""

import asyncio
import os
import subprocess
import time
import unittest
from uuid import uuid4

from core.runner_utils.runtimeio import read_json, write_json
from tests.helpers.dag import REPOSITORY, process_running, terminate_owned
from tests.helpers.services import ServiceWorkspace, wait_for


class ServiceFreezeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ServiceWorkspace()
        self.addAsyncCleanup(self.w.close)
        self.owners = []
        self.addAsyncCleanup(self.close_owners)

    async def owner(self, snapshot, *, mode="freeze", phase="frozen", fault="none"):
        controls = self.w.root / f"owner-{uuid4()}"
        controls.mkdir()
        process = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--project",
            str(REPOSITORY),
            "--no-sync",
            "python",
            "-B",
            "-m",
            "tests.helpers.service_owner",
            "--workspace",
            str(self.w.root),
            "--controls",
            str(controls),
            "--snapshot",
            snapshot,
            "--mode",
            mode,
            "--phase",
            phase,
            "--fault",
            fault,
            cwd=REPOSITORY,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        output = asyncio.create_task(process.communicate())
        self.owners.append((process, controls, output))
        await wait_for(lambda: (controls / "owner.json").exists())
        return process, controls, output

    async def close_owners(self):
        for process, controls, output in self.owners:
            if process.returncode is None:
                terminate_owned(read_json(controls / "owner.json"))
            await asyncio.wait_for(process.wait(), 10)
            await output

    async def test_owner_crash_during_startup_preserves_original_deadline(self):
        """E: reconnecting after an owner crash cannot grant another 30-second startup budget."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service(retries=0)
            definition["errors"]["on_exhausted"] = "stop"
            w.store.save(w.state)
            sid = definition["service_id"]
            (w.controls[sid] / "hold-start").touch()
            process, controls, output = await self.owner(
                str(uuid4()), mode="start-crash"
            )
            await asyncio.wait_for(process.wait(), 15)
            await output
            self.assertTrue((controls / "checkpoint.json").exists())
            w.state = w.store.load(w.experiment)
            instance = w.state.services[sid]
            deadline = instance.start_deadline
            self.assertFalse(instance.ever_ready)
            await wait_for(lambda: time.monotonic() >= deadline - 22, timeout=12)
            began = time.monotonic()
            self.assertEqual(await w.manager.recover(w.state), "stop")
            self.assertGreaterEqual(time.monotonic(), deadline)
            self.assertLess(time.monotonic() - began, 28)
            self.assertTrue(instance.stopped)
            self.assertEqual(
                len([row for row in w.trace(definition) if row["event"] == "started"]),
                1,
            )

    async def test_freeze_export_unfreeze_and_restore_actual_counter(self):
        """F: physical writes stop, an allocated export is usable, and writes resume."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            counter = w.experiment / "module_data" / sid / "counter.json"
            await wait_for(counter.exists)
            snapshot = str(uuid4())
            paths = await w.manager.save_states(w.state, snapshot)
            frozen = read_json(counter)
            self.assertTrue(frozen["frozen"])
            heartbeat = w.state.services[sid].last_status["request_id"]
            await wait_for(
                lambda: w.state.services[sid].last_status["request_id"] != heartbeat
            )
            self.assertEqual(read_json(counter), frozen)
            self.assertEqual(read_json(paths[sid])["counter"], frozen["counter"])
            with self.assertRaises(RuntimeError):
                await w.manager.request(w.state, sid, "echo", {})
            await w.manager.unfreeze(w.state, snapshot)
            await wait_for(lambda: read_json(counter)["counter"] > frozen["counter"])
            await w.manager.load_states(w.state, paths)
            self.assertTrue(w.state.services[sid].ready)
            self.assertEqual(w.state.mode, "paused")

    async def test_optional_state_and_failed_or_escaping_exports(self):
        """F: optional absence is explicit; failed exports unwind freeze and cannot escape."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service(required=False)
            sid = definition["service_id"]
            controls = w.controls[sid]
            await w.manager.start_all(w.state)
            (controls / "null-state").touch()
            snapshot = str(uuid4())
            self.assertEqual(await w.manager.save_states(w.state, snapshot), {})
            await w.manager.unfreeze(w.state, snapshot)
            (controls / "null-state").unlink()
            for flag in ("fail-save", "escape-state"):
                (controls / flag).touch()
                with (
                    self.subTest(flag=flag),
                    self.assertRaises((ValueError, RuntimeError)),
                ):
                    await w.manager.save_states(w.state, str(uuid4()))
                self.assertIsNone(w.manager._snapshot_id)
                self.assertIsNone(w.state.services[sid].freeze_id)
                (controls / flag).unlink()

    async def test_missing_required_restore_is_rejected_before_any_load(self):
        """F: validate the full restore set before issuing the first external load."""
        async with asyncio.timeout(120):
            w = self.w
            first, second = w.service(), w.service()
            await w.manager.start_all(w.state)
            path = w.experiment / "partial.json"
            write_json(path, {"counter": 5})
            with self.assertRaises(ValueError):
                await w.manager.load_states(w.state, {first["service_id"]: path})
            for definition in (first, second):
                self.assertFalse(
                    any(
                        row.get("command") == "load_state"
                        for row in w.trace(definition)
                    )
                )

    async def test_failed_unfreeze_is_not_reported_as_write_resumption(self):
        """F: a rejected unfreeze retains the barrier and requires owner intervention."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            snapshot = str(uuid4())
            await w.manager.save_states(w.state, snapshot)
            (w.controls[sid] / "fail-unfreeze").touch()
            with self.assertRaises(RuntimeError):
                await w.manager.unfreeze(w.state, snapshot)
            self.assertEqual(w.manager._snapshot_id, snapshot)
            self.assertTrue(
                read_json(w.experiment / "module_data" / sid / "counter.json")["frozen"]
            )
            (w.controls[sid] / "fail-unfreeze").unlink()
            await w.manager.unfreeze(w.state, snapshot)

    async def test_cancelled_freeze_drains_inflight_work_and_confirms_unfreeze(self):
        """F: cancellation after send does not leave a successfully released peer frozen."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            hold = w.controls[sid] / "hold-freeze"
            hold.touch()
            saving = asyncio.create_task(w.manager.save_states(w.state, str(uuid4())))
            await wait_for(
                lambda: any(
                    row["event"] == "freeze_entered" for row in w.trace(definition)
                )
            )
            saving.cancel()
            hold.unlink()
            with self.assertRaises(asyncio.CancelledError):
                await saving
            self.assertIsNone(w.manager._snapshot_id)
            self.assertIsNone(w.state.services[sid].prepared_freeze_id)
            counter = w.experiment / "module_data" / sid / "counter.json"
            await wait_for(lambda: not read_json(counter)["frozen"])

    async def test_partial_export_and_partial_load_are_reported_as_failures(self):
        """F: a second participant's failure unwinds all freezes and cannot certify a partial restore."""
        async with asyncio.timeout(120):
            w = self.w
            first, second = w.service(), w.service()
            await w.manager.start_all(w.state)
            (w.controls[second["service_id"]] / "fail-save").touch()
            with self.assertRaises(RuntimeError):
                await w.manager.save_states(w.state, str(uuid4()))
            self.assertIsNone(w.manager._snapshot_id)
            for instance in w.state.services.values():
                self.assertIsNone(instance.freeze_id)
            (w.controls[second["service_id"]] / "fail-save").unlink()
            snapshot = str(uuid4())
            paths = await w.manager.save_states(w.state, snapshot)
            await w.manager.unfreeze(w.state, snapshot)
            (w.controls[second["service_id"]] / "fail-load").touch()
            with self.assertRaises(RuntimeError):
                await w.manager.load_states(w.state, paths)
            self.assertTrue(
                any(row.get("command") == "load_state" for row in w.trace(first))
            )

    @unittest.skipUnless(os.name == "nt", "Windows junction escape")
    async def test_export_cannot_escape_through_a_real_directory_junction(self):
        """F: resolve an actual Windows reparse point before accepting a returned state file."""
        async with asyncio.timeout(120):
            w = self.w
            definition = w.service()
            sid = definition["service_id"]
            await w.manager.start_all(w.state)
            hold = w.controls[sid] / "hold-save"
            hold.touch()
            snapshot = str(uuid4())
            saving = asyncio.create_task(w.manager.save_states(w.state, snapshot))
            await wait_for(
                lambda: any(
                    row.get("command") == "save_state" for row in w.trace(definition)
                )
            )
            directory = w.experiment / "shared_data/service_state" / sid / snapshot
            target = w.root / "outside-export"
            target.mkdir()
            write_json(target / "state.json", {"counter": 10})
            link = directory / "redirect"
            created = await asyncio.to_thread(
                subprocess.run,
                ["cmd", "/c", "mklink", "/J", str(link), str(target)],
                capture_output=True,
                check=False,
            )
            self.assertEqual(created.returncode, 0, created.stderr)
            try:
                write_json(
                    w.controls[sid] / "export-path.json",
                    {
                        "state_path": (link / "state.json")
                        .relative_to(w.experiment)
                        .as_posix()
                    },
                )
                hold.unlink()
                with self.assertRaises(ValueError):
                    await saving
                self.assertIsNone(w.manager._snapshot_id)
                self.assertTrue((target / "state.json").exists())
            finally:
                # Remove only the owned junction, never recursively traverse its target.
                link.rmdir()

    async def check_owner_fault(self, fault, phase):
        w = self.w
        definition = w.service()
        sid = definition["service_id"]
        service_controls = w.controls[sid]
        if phase == "before_freeze":
            (service_controls / "hold-freeze").touch()
        snapshot = str(uuid4())
        process, controls, output = await self.owner(snapshot, phase=phase, fault=fault)
        await wait_for(lambda: (controls / "checkpoint.json").exists(), timeout=40)
        identity = read_json(controls / "ready.json")[sid]
        self.assertTrue(process_running(identity["pid"]))
        if fault == "hang":
            self.assertIsNone(process.returncode)
            terminate_owned(read_json(controls / "owner.json"))
        await asyncio.wait_for(process.wait(), 10)
        await output
        self.assertTrue(process_running(identity["pid"]))
        self.assertFalse((controls / "done.json").exists())
        (service_controls / "hold-freeze").unlink(missing_ok=True)
        counter = w.experiment / "module_data" / sid / "counter.json"
        try:
            await wait_for(lambda: counter.exists() and read_json(counter)["frozen"])
        except TimeoutError as error:
            error.add_note(
                "\n".join(
                    path.read_text(encoding="utf-8")
                    for path in (w.experiment / "shared_artifacts/services").rglob(
                        "stderr.log"
                    )
                )
            )
            raise
        recovered, recovery, result = await self.owner(snapshot, mode="recover")
        await asyncio.wait_for(recovered.wait(), 60)
        _, stderr = await result
        failure = recovery / "error.json"
        diagnostic = failure.read_text() if failure.exists() else stderr.decode()
        if failure.exists():
            diagnostic += "\n" + "\n".join(
                path.read_text(encoding="utf-8")
                for path in (w.experiment / "shared_artifacts/services").rglob(
                    "stderr.log"
                )
            )
        self.assertFalse(failure.exists(), diagnostic)
        self.assertEqual(read_json(recovery / "recovered.json")["action"], "pause")
        self.assertEqual(read_json(recovery / "done.json")["mode"], "paused")
        self.assertFalse(read_json(counter)["frozen"])
        self.assertFalse(process_running(identity["pid"]))

    async def test_controller_crash_before_freeze_ack_can_recover_and_unfreeze(self):
        """E/F: actual owner death leaves service alive and an uncertain freeze recoverable."""
        async with asyncio.timeout(180):
            await self.check_owner_fault("crash", "before_freeze")

    async def test_controller_hang_before_freeze_ack_can_recover_and_unfreeze(self):
        """E/F: a blocked owner is replaced only after its termination is confirmed."""
        async with asyncio.timeout(180):
            await self.check_owner_fault("hang", "before_freeze")

    async def test_controller_crash_after_freeze_can_recover_and_unfreeze(self):
        """E/F: persisted freeze survives actual owner loss without false automatic resume."""
        async with asyncio.timeout(180):
            await self.check_owner_fault("crash", "frozen")

    async def test_controller_hang_after_freeze_can_recover_and_unfreeze(self):
        """E/F: frozen service stays independent while its controlling Python process hangs."""
        async with asyncio.timeout(180):
            await self.check_owner_fault("hang", "frozen")

    async def check_service_fault(self, fault, phase):
        w = self.w
        definition = w.service()
        sid = definition["service_id"]
        controls = w.controls[sid]
        await w.manager.start_all(w.state)
        instance = w.state.services[sid]
        snapshot = str(uuid4())
        if phase == "before_freeze":
            write_json(
                controls / "fault.json", {"phase": "before_freeze", "action": fault}
            )
            operation = asyncio.create_task(w.manager.save_states(w.state, snapshot))
        else:
            await w.manager.save_states(w.state, snapshot)
            write_json(controls / "fault.json", {"phase": "idle", "action": fault})
            await wait_for(lambda: (controls / "fault-entered.json").exists())
            operation = asyncio.create_task(w.manager.unfreeze(w.state, snapshot))
        await wait_for(lambda: (controls / "fault-entered.json").exists())
        if fault == "hang":
            self.assertTrue(process_running(instance.process_identity["pid"]))
        with self.assertRaises(RuntimeError):
            await operation
        self.assertEqual(w.state.mode, "paused")
        self.assertEqual(w.state.stage_position, 1)
        self.assertFalse(
            any(
                row["event"] == "work_finished"
                and row.get("command") == "unfreeze_writes"
                for row in w.trace(definition)
            )
        )
        (controls / "fault.json").unlink()

    async def test_service_crash_before_freeze_ack_cannot_produce_success(self):
        """F: failed participant before acknowledgement does not become a valid export."""
        async with asyncio.timeout(180):
            await self.check_service_fault("crash", "before_freeze")

    async def test_service_hang_before_freeze_ack_cannot_produce_success(self):
        """F: actual participant hang exercises heartbeat and command/stop deadlines."""
        async with asyncio.timeout(180):
            await self.check_service_fault("hang", "before_freeze")

    async def test_service_crash_after_freeze_cannot_confirm_unfreeze(self):
        """F: replacement cannot confirm the original participant's write resumption."""
        async with asyncio.timeout(180):
            await self.check_service_fault("crash", "frozen")

    async def test_service_hang_after_freeze_cannot_confirm_unfreeze(self):
        """F: frozen but unresponsive participant never yields a successful unfreeze."""
        async with asyncio.timeout(180):
            await self.check_service_fault("hang", "frozen")
