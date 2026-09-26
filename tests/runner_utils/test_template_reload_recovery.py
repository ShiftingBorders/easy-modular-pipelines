"""Approved RT18: actual owner death across reload and automatic rollback boundaries."""

import asyncio
import copy
import ctypes
import json
import os
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import psutil

from core.runner_utils.runtimeio import process_identity, read_json, write_json
from core.runner_utils.state import state_to_document
from core.serverruntime import recovery_candidates
from tests.helpers.dag import REPOSITORY, process_running, terminate_owned
from tests.helpers.reload import candidate, events, version, workspace
from tests.helpers.services import wait_for
from tests.helpers.snapshots import file_inventory


class TemplateReloadRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_recovery_waits_for_launcher_cleanup_across_crashes_rr12(self):
        for phase, second_crash, legacy in (
            ("child_endpoint", None, False),
            ("child_endpoint", "child_reconciled", False),
            ("child_endpoint", "child_stopped", False),
            ("service_started", None, False),
            ("child_endpoint", None, True),
        ):
            with self.subTest(phase=phase, second_crash=second_crash, legacy=legacy):
                work, old, identifier, path, launcher, child = await self.child_crash(
                    phase=phase, launcher_cleanup=True
                )
                sid = work.socket["service_id"]
                controls = work.files.files.controls[sid]
                gate = controls / "hold-launcher-cleanup"
                root = work.runner._state.experiment_directory
                template_path = work.runner._state.template_path
                record = read_json(path)
                self.assertEqual(record["launcher_process"], launcher)
                self.assertEqual(
                    record["process"], child if phase == "service_started" else launcher
                )
                if legacy:
                    del record["launcher_process"]
                    write_json(path, record)
                if second_crash is not None:
                    owner = await work.start_owner(
                        "recover", second_crash, experiment_id=identifier
                    )
                    self.assertEqual(
                        await asyncio.wait_for(owner.wait(), 90),
                        23,
                        (work.root / "owner-1.log").read_text(encoding="utf-8"),
                    )
                    self.assertEqual(read_json(path)["process"], child)
                    self.assertEqual(read_json(path)["launcher_process"], launcher)
                    self.assertTrue(process_running(launcher["pid"]))
                    if second_crash == "child_stopped":
                        self.assertFalse(process_running(child["pid"]))
                marker = root / "module_data" / sid / "after-crash.txt"
                marker.write_text("not in protective snapshot", encoding="utf-8")
                before_data = file_inventory(root / "module_data")
                before_snapshots = file_inventory(work.root / "snapshots")
                template_bytes = template_path.read_bytes()
                starts = [
                    row
                    for row in work.files.trace(work.socket)
                    if row["event"] == "started"
                ]
                runner = work.replacement()
                self.assertFalse(runner._services._launch_processes)
                waiting = asyncio.Event()

                def observe(pid, launcher=launcher, waiting=waiting):
                    if pid == launcher["pid"]:
                        waiting.set()
                    return process_identity(pid)

                task = None
                try:
                    with patch("core.runner_utils.services.process_identity", observe):
                        task = asyncio.create_task(runner.recover(identifier))
                        await asyncio.wait_for(waiting.wait(), 30)
                        await wait_for(
                            (controls / "launcher-cleanup-started.json").is_file, 15
                        )
                        self.assertFalse(task.done())
                        self.assertFalse(process_running(child["pid"]))
                        self.assertTrue(process_running(launcher["pid"]))
                        self.assertTrue(runner._state.services[sid].stopped)
                        self.assertEqual(read_json(path)["process"], child)
                        self.assertEqual(read_json(path)["launcher_process"], launcher)
                        self.assertEqual(
                            file_inventory(root / "module_data"), before_data
                        )
                        self.assertEqual(template_path.read_bytes(), template_bytes)
                        self.assertEqual(
                            file_inventory(work.root / "snapshots"), before_snapshots
                        )
                        self.assertFalse(
                            (
                                work.root
                                / "controller/restore_transactions"
                                / f"{root.name}.json"
                            ).exists()
                        )
                        self.assertEqual(
                            [
                                row
                                for row in work.files.trace(work.socket)
                                if row["event"] == "started"
                            ],
                            starts,
                        )
                        gate.unlink()
                        await asyncio.wait_for(task, 60)
                    self.assertFalse(process_running(launcher["pid"]))
                    self.assertTrue((controls / "launcher-cleanup-finished").is_file())
                    self.assertFalse(marker.exists())
                    self.assertFalse(marker.with_name("launcher-cleanup.txt").exists())
                    self.assertEqual(runner._state.template, old)
                    self.assertEqual(await work.value(), 39)
                    self.assertIsNone(runner._state.pending_rebuild)
                    self.assertEqual(runner.get_state()["phase"], "waiting")
                    self.assertEqual(runner._state.mode, "paused")
                    observations = events(runner, "control.reconciled")
                    self.assertTrue(
                        any(
                            event["data"].get("state") == "rolled_back"
                            for event in observations
                        )
                    )
                    if phase == "child_endpoint" and second_crash != "child_reconciled":
                        reconciled = [
                            event
                            for event in observations
                            if event["data"].get("action") == "rebuild_service_process"
                        ]
                        self.assertEqual(len(reconciled), 1)
                        self.assertEqual(
                            reconciled[0]["data"]["launched_process"], launcher
                        )
                        self.assertEqual(
                            reconciled[0]["data"]["participant_process"], child
                        )
                    if phase == "service_started":
                        self.assertTrue(
                            any(
                                event["data"].get("launcher_process") == launcher
                                and event["data"]["process"] == child
                                for event in events(runner, "service.started")
                            )
                        )
                finally:
                    gate.unlink(missing_ok=True)
                    if task is not None:
                        if not task.done():
                            task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                await work.close()

    async def test_launcher_wait_failure_preserves_recovery_inputs_rr12(self):
        cases = ["timeout", "cancel", "access_denied", "os_error"]
        if os.name == "nt":
            cases.append("winerror_5")
        for failure in cases:
            with self.subTest(failure=failure):
                work, old, identifier, path, launcher, child = await self.child_crash(
                    launcher_cleanup=True, start_timeout=5
                )
                root = work.runner._state.experiment_directory
                sid = work.socket["service_id"]
                controls = work.files.files.controls[sid]
                gate = controls / "hold-launcher-cleanup"
                before_data = file_inventory(root / "module_data")
                before_snapshots = file_inventory(work.root / "snapshots")
                runner = work.replacement()
                waiting = asyncio.Event()

                def observe(pid, launcher=launcher, waiting=waiting, failure=failure):
                    if pid == launcher["pid"]:
                        waiting.set()
                        if failure == "access_denied":
                            raise psutil.AccessDenied(pid)
                        if failure == "winerror_5":
                            raise ctypes.WinError(5)
                        if failure == "os_error":
                            raise OSError(f"Cannot inspect launcher {pid}")
                    return process_identity(pid)

                task = None
                try:
                    with patch("core.runner_utils.services.process_identity", observe):
                        task = asyncio.create_task(runner.recover(identifier))
                        await asyncio.wait_for(waiting.wait(), 30)
                        if failure == "cancel":
                            task.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await task
                        elif failure == "timeout":
                            with self.assertRaisesRegex(
                                RuntimeError,
                                f"{sid} launcher {launcher['pid']} still owns",
                            ):
                                await asyncio.wait_for(task, 30)
                        else:
                            with self.assertRaises(
                                psutil.AccessDenied
                                if failure == "access_denied"
                                else OSError
                            ):
                                await asyncio.wait_for(task, 30)
                    self.assertTrue(process_running(launcher["pid"]))
                    self.assertFalse(process_running(child["pid"]))
                    self.assertEqual(read_json(path)["launcher_process"], launcher)
                    self.assertEqual(read_json(path)["process"], child)
                    self.assertIsNotNone(runner._state.pending_rebuild)
                    self.assertEqual(file_inventory(root / "module_data"), before_data)
                    self.assertEqual(
                        file_inventory(work.root / "snapshots"), before_snapshots
                    )
                    self.assertFalse(
                        (
                            work.root
                            / "controller/restore_transactions"
                            / f"{root.name}.json"
                        ).exists()
                    )
                    if failure != "cancel":
                        audit = json.dumps(events(runner, "error.recorded"))
                        self.assertIn(
                            str(launcher["pid"]) if failure != "winerror_5" else "5",
                            audit,
                        )
                    gate.unlink()
                    await wait_for(
                        lambda pid=launcher["pid"]: not process_running(pid), 15
                    )
                    await runner.close()
                    fresh = work.replacement()
                    await fresh.recover(identifier)
                    self.assertEqual(fresh._state.template, old)
                    self.assertEqual(await work.value(), 39)
                    self.assertIsNone(fresh._state.pending_rebuild)
                    self.assertEqual(fresh._state.mode, "paused")
                finally:
                    gate.unlink(missing_ok=True)
                    if task is not None:
                        if not task.done():
                            task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                await work.close()

    async def test_launcher_reset_identity_and_metadata_boundaries_rr12(self):
        work = workspace(services=True)
        self.addAsyncCleanup(work.close)
        runner = await work.launch()
        state = runner._state
        sid = work.socket["service_id"]
        instance = state.services[sid]
        path = instance.artifacts_directory / "process.json"
        record = read_json(path)
        launcher = record["launcher_process"]
        self.assertEqual(launcher, instance.process_identity)
        stopped = await runner._services.stop_all(state)
        self.assertTrue(
            all(item["stopped"] and not item["error"] for item in stopped.values())
        )
        await runner._services.reset(state)
        self.assertFalse(runner._services._launch_processes)
        cases = [
            "native",
            "reused",
            "file_missing",
            "process_missing",
            "psutil_missing",
        ]
        if os.name == "nt":
            cases.extend([87, 1168])
        for absence in cases:
            with self.subTest(absence=absence):
                observed = []

                def observe(pid, observed=observed, absence=absence):
                    if pid == launcher["pid"]:
                        observed.append(pid)
                        if absence == "reused":
                            return {
                                **launcher,
                                "created_at_os": launcher["created_at_os"] + 1,
                            }
                        if absence == "file_missing":
                            raise FileNotFoundError("departed launcher")
                        if absence == "process_missing":
                            raise ProcessLookupError("departed launcher")
                        if absence == "psutil_missing":
                            raise psutil.NoSuchProcess(pid)
                        if isinstance(absence, int):
                            raise ctypes.WinError(absence)
                    return process_identity(pid)

                with patch("core.runner_utils.services.process_identity", observe):
                    await runner._services.reset(state)
                self.assertEqual(observed, [launcher["pid"]])
        for key in ("experiment_id", "participant_id", "participant_instance_id"):
            with self.subTest(mismatched_field=key):
                write_json(path, {**record, key: str(uuid4())})
                with self.assertRaisesRegex(
                    RuntimeError, f"{sid} launcher ownership cannot be verified"
                ):
                    await runner._services.reset(state)
        write_json(
            path,
            {key: value for key, value in record.items() if key != "launcher_process"},
        )
        await runner._services.reset(state)
        write_json(path, record)

    @unittest.skipUnless(os.name == "posix", "Linux zombie launcher observations")
    async def test_zombie_launcher_does_not_block_reset_rr12(self):
        work = workspace(services=True)
        self.addAsyncCleanup(work.close)
        runner = await work.launch()
        state = runner._state
        path = (
            state.services[work.socket["service_id"]].artifacts_directory
            / "process.json"
        )
        stopped = await runner._services.stop_all(state)
        self.assertTrue(
            all(item["stopped"] and not item["error"] for item in stopped.values())
        )
        await runner._services.reset(state)
        original = read_json(path)
        ready = work.root / "zombie-launcher.json"
        release = work.root / "reap-launcher"
        source = """
import os
import sys
import time
from pathlib import Path
from core.runner_utils.runtimeio import process_identity, write_json

pid = os.fork()
if pid == 0:
    time.sleep(0.5)
    os._exit(0)
write_json(Path(sys.argv[1]), process_identity(pid))
while not Path(sys.argv[2]).exists():
    time.sleep(0.05)
os.waitpid(pid, 0)
"""
        supervisor = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "python",
            "-c",
            source,
            str(ready),
            str(release),
            cwd=REPOSITORY,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await wait_for(ready.is_file, 15)
            identity = read_json(ready)
            await wait_for(
                lambda: (
                    psutil.Process(identity["pid"]).status() == psutil.STATUS_ZOMBIE
                ),
                15,
            )
            self.assertNotEqual(psutil.Process(identity["pid"]).ppid(), os.getpid())
            write_json(path, {**original, "launcher_process": identity})
            await runner._services.reset(state)
            self.assertEqual(
                psutil.Process(identity["pid"]).status(), psutil.STATUS_ZOMBIE
            )
        finally:
            release.touch()
            await asyncio.wait_for(supervisor.wait(), 15)
            write_json(path, original)
        self.assertEqual(supervisor.returncode, 0)

    async def test_pending_rebuild_rejects_other_recovery_targets_without_detaching_rr11(
        self,
    ):
        work = workspace(services=True)
        self.addAsyncCleanup(work.close)
        runner = work.runner
        other_template = copy.deepcopy(work.template)
        other_template["services"] = []
        await runner.run(
            work.files.write_template(other_template, "other.yaml"), delayed_start=True
        )
        await asyncio.wait_for(runner._ready.wait(), 90)
        other_id = runner._state.experiment_id
        other_root = runner._state.experiment_directory
        await runner.stop()
        runner = await work.launch()
        other_files = file_inventory(other_root)
        original = candidate(work)
        identifier = runner._state.experiment_id
        await work.value(47)
        document = candidate(work)
        document["services"][0]["settings"]["hold_load"] = True
        controls = work.files.files.controls[work.socket["service_id"]]
        reload_task = asyncio.create_task(
            runner.reload_template(work.files.write_template(document))
        )
        try:
            await wait_for(
                lambda: any(
                    row["event"] == "work_started"
                    and row.get("command") == "load_state"
                    for row in work.files.trace(work.socket)
                ),
                60,
            )
            reload_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await reload_task
            state = runner._state
            pending = copy.deepcopy(state.pending_rebuild)
            self.assertIsNotNone(pending)
            finished_task = runner._task
            self.assertTrue(finished_task.done())
            identities = {
                sid: copy.deepcopy(service.process_identity)
                for sid, service in state.services.items()
            }
            self.assertTrue(
                all(
                    process_running(identity["pid"]) for identity in identities.values()
                )
            )
            connections = dict(runner._services._connections)
            processes = dict(runner._services._processes)
            client = runner._journal.client
            active_task = asyncio.create_task(asyncio.Event().wait())
            try:
                for scheduler in (None, finished_task, active_task):
                    runner._task = scheduler
                    for target in (other_id, str(uuid4())):
                        with self.subTest(
                            scheduler="absent"
                            if scheduler is None
                            else "finished"
                            if scheduler.done()
                            else "active",
                            target=target,
                        ):
                            before = state_to_document(state)
                            with (
                                patch(
                                    "core.runner_utils.experimentrunner.read_json",
                                    side_effect=AssertionError(
                                        "Rejected recovery must not read its target"
                                    ),
                                ),
                                patch.object(
                                    runner._services, "close", new_callable=AsyncMock
                                ) as close_services,
                                patch.object(
                                    runner._stages, "close", new_callable=AsyncMock
                                ) as close_stages,
                                patch.object(runner._journal, "close") as close_journal,
                                self.assertRaisesRegex(
                                    RuntimeError, "pending rebuild before switching"
                                ),
                            ):
                                await runner.recover(target)
                            close_services.assert_not_awaited()
                            close_stages.assert_not_awaited()
                            close_journal.assert_not_called()
                            self.assertIs(runner._state, state)
                            self.assertEqual(state_to_document(state), before)
                            self.assertEqual(runner._requested_id, identifier)
                            self.assertIs(runner._journal.client, client)
                            self.assertEqual(runner._services._connections, connections)
                            self.assertEqual(runner._services._processes, processes)
                            self.assertIs(runner._task, scheduler)
                            self.assertFalse(active_task.done())
            finally:
                runner._task = finished_task
                active_task.cancel()
                await asyncio.gather(active_task, return_exceptions=True)
            self.assertEqual(file_inventory(other_root), other_files)
            with self.assertRaisesRegex(RuntimeError, "unfinished template reload"):
                await runner.run(work.template_path)
            self.assertEqual(state.pending_rebuild, pending)
            (controls / "release-load").touch()
            await runner.stop()
            self.assertEqual(runner._state.experiment_id, identifier)
            self.assertEqual(runner._state.pending_rebuild, pending)
            self.assertTrue(
                all(
                    not process_running(identity["pid"])
                    for identity in identities.values()
                )
            )
            await runner.recover(identifier)
            self.assertEqual(runner._state.template, original)
            self.assertIsNone(runner._state.pending_rebuild)
            self.assertEqual(await work.value(), 47)
            self.assertEqual(runner.get_state()["phase"], "waiting")
            await runner.stop()
            await runner.recover(other_id)
            self.assertEqual(runner._state.experiment_id, other_id)
            self.assertIsNone(runner._state.pending_rebuild)
            await runner.stop()
            fresh = work.replacement()
            self.assertIsNone(fresh._state)
            await fresh.recover(identifier)
            self.assertEqual(fresh._state.experiment_id, identifier)
            self.assertIsNone(fresh._state.pending_rebuild)
        finally:
            (controls / "release-load").touch()
            if not reload_task.done():
                reload_task.cancel()
            await asyncio.gather(reload_task, return_exceptions=True)

    async def child_crash(
        self, *, phase="child_endpoint", launcher_cleanup=False, start_timeout=30
    ):
        work, old, _new, identifier = await self.crash(
            phase,
            services=True,
            launcher_cleanup=launcher_cleanup,
            start_timeout=start_timeout,
        )
        root = work.runner._state.experiment_directory
        saved = read_json(root / "runner/state.json")
        service = saved["services"][work.socket["service_id"]]
        process_file = (
            root
            / "shared_artifacts/services"
            / work.socket["service_id"]
            / service["service_instance_id"]
            / "process.json"
        )
        record = read_json(process_file)
        launched = record.get("launcher_process", record["process"])
        child = read_json(root / service["endpoint_path"])["process"]
        self.assertNotEqual(launched, child)
        self.assertTrue(process_running(launched["pid"]))
        self.assertTrue(process_running(child["pid"]))
        return work, old, identifier, process_file, launched, child

    async def test_departed_child_and_launcher_restore_snapshot_rr08(self):
        cases = ["native", "file_missing", "process_missing", "psutil_missing"]
        if os.name == "nt":
            cases.extend([87, 1168])
        for absence in cases:
            with self.subTest(absence=absence):
                work, old, identifier, _path, launched, child = await self.child_crash()
                terminate_owned(child)
                await wait_for(lambda pid=child["pid"]: not process_running(pid), 15)
                await wait_for(lambda pid=launched["pid"]: not process_running(pid), 15)
                runner = work.replacement()

                def observe(pid, child=child, absence=absence):
                    if pid == child["pid"]:
                        if absence == "file_missing":
                            raise FileNotFoundError("departed child")
                        if absence == "process_missing":
                            raise ProcessLookupError("departed child")
                        if absence == "psutil_missing":
                            raise psutil.NoSuchProcess(pid)
                        if isinstance(absence, int):
                            raise ctypes.WinError(absence)
                    return process_identity(pid)

                with patch(
                    "core.runner_utils.experimentrunner.process_identity", observe
                ):
                    await runner.recover(identifier)
                self.assertEqual(runner._state.template, old)
                self.assertEqual(await work.value(), 39)
                self.assertIsNone(runner._state.pending_rebuild)
                self.assertEqual(runner.get_state()["phase"], "waiting")
                self.assertEqual(runner.get_state()["mode"], "paused")
                self.assertTrue(events(runner, "reload.candidate"))
                self.assertTrue(
                    any(
                        event["data"].get("state") == "rolled_back"
                        for event in events(runner, "control.reconciled")
                    )
                )
                await work.close()

    async def test_child_exits_during_ancestry_inspection_rr08(self):
        work, old, identifier, _path, launched, child = await self.child_crash()
        runner = work.replacement()
        parents = psutil.Process.parents
        observed = []

        def exit_during_inspection(process):
            if process.pid == child["pid"]:
                observed.append(process.pid)
                child_process = psutil.Process(child["pid"])
                launcher_process = psutil.Process(launched["pid"])
                terminate_owned(child)
                child_process.wait(timeout=15)
                launcher_process.wait(timeout=15)
                raise psutil.NoSuchProcess(child["pid"])
            return parents(process)

        with patch.object(psutil.Process, "parents", exit_during_inspection):
            await runner.recover(identifier)
        self.assertEqual(observed, [child["pid"]])
        self.assertEqual(runner._state.template, old)
        self.assertEqual(await work.value(), 39)
        self.assertIsNone(runner._state.pending_rebuild)
        self.assertEqual(runner._state.mode, "paused")

    async def test_departed_child_does_not_release_live_launcher_rr08(self):
        work, old, identifier, _path, launched, child = await self.child_crash()
        parent = psutil.Process(launched["pid"])
        parent.suspend()
        self.addCleanup(terminate_owned, launched)
        terminate_owned(child)
        await wait_for(lambda: not process_running(child["pid"]), 15)
        if os.name == "posix":
            self.assertEqual(
                psutil.Process(child["pid"]).status(), psutil.STATUS_ZOMBIE
            )
        runner = work.replacement()
        observed = []

        async def stop_launcher(state, **kwargs):
            instance = state.services[work.socket["service_id"]]
            observed.append(instance.process_identity)
            self.assertEqual(instance.process_identity, launched)
            self.assertFalse(instance.stopped)
            self.assertTrue(process_running(launched["pid"]))
            return {
                work.socket["service_id"]: {
                    "stopped": False,
                    "error": "launcher is still alive",
                }
            }

        with (
            patch.object(runner._services, "stop_all", stop_launcher),
            patch.object(runner, "_fail", new_callable=AsyncMock),
            self.assertRaisesRegex(RuntimeError, "not stopped cleanly"),
        ):
            await runner.recover(identifier)
        self.assertEqual(observed, [launched])
        self.assertIsNotNone(runner._state.pending_rebuild)
        self.assertTrue(process_running(launched["pid"]))
        terminate_owned(launched)
        await wait_for(lambda: not process_running(launched["pid"]), 15)
        await runner.close()
        runner = work.replacement()
        await runner.recover(identifier)
        self.assertEqual(runner._state.template, old)
        self.assertEqual(await work.value(), 39)
        self.assertIsNone(runner._state.pending_rebuild)

    async def test_uncertain_child_ownership_blocks_restoration_rr08(self):
        cases = ["access_denied", "missing_ancestor", "ancestry_io"]
        if os.name == "nt":
            cases.append("winerror_5")
        for problem in cases:
            with self.subTest(problem=problem):
                work, _old, identifier, path, launched, child = await self.child_crash()
                runner = work.replacement()
                parents = psutil.Process.parents

                def observe(pid, child=child, problem=problem):
                    if pid == child["pid"]:
                        if problem == "access_denied":
                            raise psutil.AccessDenied(pid)
                        if problem == "winerror_5":
                            raise ctypes.WinError(5)
                    return process_identity(pid)

                def inspect(
                    process,
                    child=child,
                    problem=problem,
                    launched=launched,
                    parents=parents,
                ):
                    if process.pid == child["pid"]:
                        if problem == "missing_ancestor":
                            raise psutil.NoSuchProcess(launched["pid"])
                        if problem == "ancestry_io":
                            raise OSError("cannot inspect ancestry")
                    return parents(process)

                with (
                    patch(
                        "core.runner_utils.experimentrunner.process_identity", observe
                    ),
                    patch.object(psutil.Process, "parents", inspect),
                    patch.object(runner, "_fail", new_callable=AsyncMock),
                    patch.object(
                        runner._snapshots, "restore", new_callable=AsyncMock
                    ) as restore,
                ):
                    with self.assertRaises(
                        (psutil.AccessDenied, OSError, RuntimeError)
                    ):
                        await runner.recover(identifier)
                    restore.assert_not_awaited()
                self.assertIsNotNone(runner._state.pending_rebuild)
                self.assertEqual(read_json(path)["process"], launched)
                self.assertTrue(process_running(child["pid"]))
                self.assertTrue(process_running(launched["pid"]))
                await work.close()

    @unittest.skipUnless(os.name == "posix", "Linux zombie process observations")
    async def test_zombie_launcher_is_terminated_rr08(self):
        # Another process owns this zombie: recovery cannot reap it with wait().
        work, old, identifier, path, launched, child = await self.child_crash()
        terminate_owned(child)
        await wait_for(lambda: not process_running(launched["pid"]), 15)
        ready = work.root / "zombie-identity.json"
        release = work.root / "reap-zombie"
        source = """
import os
import sys
import time
from pathlib import Path
from core.runner_utils.runtimeio import process_identity, write_json

pid = os.fork()
if pid == 0:
    time.sleep(0.5)
    os._exit(0)
write_json(Path(sys.argv[1]), process_identity(pid))
while not Path(sys.argv[2]).exists():
    time.sleep(0.05)
os.waitpid(pid, 0)
"""
        supervisor = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "python",
            "-c",
            source,
            str(ready),
            str(release),
            cwd=REPOSITORY,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            await wait_for(ready.is_file, 15)
            identity = read_json(ready)
            await wait_for(
                lambda: (
                    psutil.Process(identity["pid"]).status() == psutil.STATUS_ZOMBIE
                ),
                15,
            )
            self.assertNotEqual(psutil.Process(identity["pid"]).ppid(), os.getpid())
            write_json(path, {**read_json(path), "process": identity})
            runner = work.replacement()
            restore = runner._snapshots.restore

            async def verify_launcher_stopped(state, *args, **kwargs):
                instance = state.services[work.socket["service_id"]]
                self.assertEqual(instance.process_identity, identity)
                self.assertTrue(instance.stopped)
                return await restore(state, *args, **kwargs)

            with patch.object(runner._snapshots, "restore", verify_launcher_stopped):
                await runner.recover(identifier)
            self.assertEqual(runner._state.template, old)
            self.assertEqual(await work.value(), 39)
            self.assertIsNone(runner._state.pending_rebuild)
        finally:
            release.touch()
            await asyncio.wait_for(supervisor.wait(), 15)
        self.assertEqual(supervisor.returncode, 0)

    async def test_committed_services_override_unfinished_file_with_same_run_rr04(self):
        work, _old, new, identifier = await self.crash(
            "committed_stale_file", services=True
        )
        root = work.runner._state.experiment_directory
        saved = read_json(root / "runner/state.json")
        sid = work.socket["service_id"]
        previous = saved["services"][sid]
        self.assertIsNotNone(saved["pending_rebuild"])
        self.assertTrue(previous["stopped"])
        endpoint = read_json(root / previous["endpoint_path"])
        current_id = endpoint["participant_instance_id"]
        current_process = endpoint["process"]
        self.assertNotEqual(previous["service_instance_id"], current_id)
        self.assertTrue(process_running(current_process["pid"]))
        runner = work.replacement()
        await runner.recover(identifier)
        self.assertEqual(runner._state.run_id, saved["run_id"])
        self.assertEqual(runner._state.template, new)
        self.assertIsNone(runner._state.pending_rebuild)
        self.assertEqual(runner._state.services[sid].service_instance_id, current_id)
        self.assertEqual(runner._state.services[sid].process_identity, current_process)
        self.assertEqual(await work.value(), 39)
        self.assertTrue(runner._state.stage_result_ids)
        started = [
            event
            for event in events(runner, "service.started")
            if event["context"].get("participant_id") == sid
        ]
        await runner.retry(1)
        self.assertFalse(process_running(current_process["pid"]))
        self.assertNotEqual(runner._state.services[sid].service_instance_id, current_id)
        self.assertEqual(
            len(
                [
                    event
                    for event in events(runner, "service.started")
                    if event["context"].get("participant_id") == sid
                ]
            ),
            len(started) + 1,
        )

    async def test_later_file_service_observations_without_rebuild_are_retained_rr04(
        self,
    ):
        work = workspace(services=True)
        self.addAsyncCleanup(work.close)
        runner = await work.launch()
        root = runner._state.experiment_directory
        identifier = runner._state.experiment_id
        sid = work.socket["service_id"]
        await runner.close()
        saved = read_json(root / "runner/state.json")
        self.assertIsNone(saved["pending_rebuild"])
        saved["checkpoint_id"] = str(uuid4())
        saved["services"][sid]["restart_count"] = 7
        write_json(root / "runner/state.json", saved)
        recovered = work.replacement()
        await recovered.recover(identifier)
        self.assertEqual(recovered._state.services[sid].restart_count, 7)
        self.assertEqual(
            recovered._state.services[sid].service_instance_id,
            saved["services"][sid]["service_instance_id"],
        )

    @unittest.skipUnless(os.name == "nt", "Windows missing-process error codes")
    async def test_missing_windows_process_with_stale_checkpoint_recovers_rr02(self):
        for code in (87, 1168):
            with self.subTest(winerror=code):
                work, old, _new, identifier = await self.crash(
                    "service_stopped_uncheckpointed", services=True
                )
                root = work.runner._state.experiment_directory
                saved = read_json(root / "runner/state.json")
                service = saved["services"][work.socket["service_id"]]
                self.assertFalse(service["stopped"])
                pid = service["process_identity"]["pid"]
                self.assertFalse(process_running(pid))
                runner = work.replacement()

                def observe(observed_pid, pid=pid, code=code):
                    if observed_pid == pid:
                        raise ctypes.WinError(code)
                    return process_identity(observed_pid)

                with patch(
                    "core.runner_utils.experimentrunner.process_identity",
                    side_effect=observe,
                ) as lookup:
                    await runner.recover(identifier)
                self.assertIn(((pid,), {}), lookup.call_args_list)
                self.assertEqual(runner._state.template, old)
                self.assertEqual(await work.value(), 39)
                self.assertIsNone(runner._state.pending_rebuild)
                self.assertEqual(runner._state.mode, "paused")
                self.assertTrue(events(runner, "reload.candidate"))
                await work.close()

    @unittest.skipUnless(os.name == "nt", "Windows process access error codes")
    async def test_process_access_error_does_not_authorize_restore_rr02(self):
        work, _old, _new, identifier = await self.crash(
            "service_stopped_uncheckpointed", services=True
        )
        root = work.runner._state.experiment_directory
        saved = read_json(root / "runner/state.json")
        pid = saved["services"][work.socket["service_id"]]["process_identity"]["pid"]
        runner = work.replacement()

        def observe(observed_pid):
            if observed_pid == pid:
                raise ctypes.WinError(5)
            return process_identity(observed_pid)

        with (
            patch(
                "core.runner_utils.experimentrunner.process_identity",
                side_effect=observe,
            ),
            patch.object(runner, "_fail", new_callable=AsyncMock),
            patch.object(
                runner._snapshots, "restore", new_callable=AsyncMock
            ) as restore,
        ):
            with self.assertRaises(OSError) as caught:
                await runner.recover(identifier)
            self.assertEqual(caught.exception.winerror, 5)
            restore.assert_not_awaited()
        self.assertIsNotNone(runner._state.pending_rebuild)

    async def test_child_endpoint_recovers_before_readiness_and_after_second_crash_rr03(
        self,
    ):
        for repeat in (False, True):
            with self.subTest(repeated_recovery=repeat):
                work, old, _new, identifier = await self.crash(
                    "child_endpoint", services=True
                )
                root = work.runner._state.experiment_directory
                saved = read_json(root / "runner/state.json")
                service = saved["services"][work.socket["service_id"]]
                process_file = (
                    root
                    / "shared_artifacts/services"
                    / work.socket["service_id"]
                    / service["service_instance_id"]
                    / "process.json"
                )
                endpoint = root / service["endpoint_path"]
                launched = read_json(process_file)["process"]
                child = read_json(endpoint)["process"]
                self.assertNotEqual(launched, child)
                self.assertTrue(process_running(launched["pid"]))
                self.assertTrue(process_running(child["pid"]))
                if repeat:
                    owner = await work.start_owner(
                        "recover", "child_reconciled", experiment_id=identifier
                    )
                    self.assertEqual(await asyncio.wait_for(owner.wait(), 90), 23)
                    self.assertEqual(read_json(process_file)["process"], child)
                    terminate_owned(launched)
                    await wait_for(
                        lambda pid=launched["pid"]: not process_running(pid), 15
                    )
                    self.assertTrue(process_running(child["pid"]))
                runner = work.replacement()
                await runner.recover(identifier)
                self.assertEqual(runner._state.template, old)
                self.assertEqual(await work.value(), 39)
                self.assertIsNone(runner._state.pending_rebuild)
                self.assertEqual(runner._state.mode, "paused")
                self.assertFalse(process_running(child["pid"]))
                await wait_for(lambda pid=launched["pid"]: not process_running(pid), 15)
                if not repeat:
                    audit = [
                        event
                        for event in events(runner, "control.reconciled")
                        if event["data"].get("action") == "rebuild_service_process"
                    ]
                    self.assertEqual(len(audit), 1)
                    self.assertEqual(audit[0]["data"]["launched_process"], launched)
                    self.assertEqual(audit[0]["data"]["participant_process"], child)
                await work.close()

    async def test_unrelated_or_mismatched_child_endpoint_blocks_restore_rr03(self):
        for mismatch in (
            "unrelated_process",
            "reused_pid",
            "experiment_id",
            "participant_id",
        ):
            with self.subTest(mismatch=mismatch):
                work, _old, _new, identifier = await self.crash(
                    "child_endpoint", services=True
                )
                root = work.runner._state.experiment_directory
                saved = read_json(root / "runner/state.json")
                service = saved["services"][work.socket["service_id"]]
                endpoint = root / service["endpoint_path"]
                original = read_json(endpoint)
                announced = copy.deepcopy(original)
                if mismatch == "unrelated_process":
                    announced["process"] = process_identity(os.getpid())
                elif mismatch == "reused_pid":
                    announced["process"]["created_at_os"] += 1
                else:
                    announced[mismatch] = "another-identity"
                write_json(endpoint, announced)
                runner = work.replacement()
                try:
                    with (
                        patch.object(runner, "_fail", new_callable=AsyncMock),
                        patch.object(
                            runner._snapshots, "restore", new_callable=AsyncMock
                        ) as restore,
                    ):
                        with self.assertRaises(RuntimeError):
                            await runner.recover(identifier)
                        restore.assert_not_awaited()
                    self.assertIsNotNone(runner._state.pending_rebuild)
                    self.assertTrue(process_running(original["process"]["pid"]))
                finally:
                    write_json(endpoint, original)
                await work.close()

    async def crash(
        self,
        phase,
        *,
        services=False,
        state_file="normal",
        launcher_cleanup=False,
        start_timeout=30,
    ):
        work = workspace(services=services)
        self.addAsyncCleanup(work.close)
        work.template["start_timeout"] = start_timeout
        if launcher_cleanup:
            # Freeze fixture counters so byte comparisons isolate restoration
            # from the unrelated peer's legitimate background increments.
            for definition in work.template["services"]:
                definition["settings"]["auto_increment"] = False
        if services:
            changed_service = version(
                work,
                work.socket,
                "2",
                name="renamed" if phase == "module_data_moved" else None,
            )
            changed_service["settings"]["auto_increment"] = False
            if phase == "child_endpoint" or launcher_cleanup:
                changed_service["settings"]["launch_child"] = True
            if launcher_cleanup:
                changed_service["settings"]["launcher_cleanup"] = True
                controls = work.files.files.controls[work.socket["service_id"]]
                (controls / "hold-launcher-cleanup").touch()
        runner = await work.launch()
        await runner.step()
        old = candidate(work)
        if services:
            await work.value(39)
        document = candidate(work)
        document["stages"][1]["settings"]["echo"] = "candidate"
        document["stages"].append(copy.deepcopy(work.extra_stage))
        if services:
            document["services"][0] = changed_service
        work.files.write_template(document, "reload.yaml")
        experiment_id = runner._state.experiment_id
        root = runner._state.experiment_directory
        stale = read_json(root / "runner/state.json")
        await runner.close()
        owner = await work.start_owner("reload", phase, experiment_id=experiment_id)
        code = await asyncio.wait_for(owner.wait(), 120)
        self.assertEqual(
            code, 23, (work.root / "owner-0.log").read_text(encoding="utf-8")
        )
        self.assertEqual(read_json(work.root / "owner-fault.json")["phase"], phase)
        if state_file == "missing":
            (root / "runner/state.json").unlink(missing_ok=True)
        elif state_file == "stale":
            stale["owner_identity"] = None
            write_json(root / "runner/state.json", stale)
        self.assertIn(experiment_id, recovery_candidates(work.root))
        return work, old, document, experiment_id

    async def test_real_crashes_before_and_after_commit_rt18(self):
        for phase, state_file in (
            ("rebuild_prepared", "normal"),
            ("rebuild_intent", "missing"),
            ("module_published", "normal"),
            ("template_published", "stale"),
            ("template_applied", "missing"),
            ("rebuild_committed", "stale"),
        ):
            with self.subTest(phase=phase, state_file=state_file):
                work, old, new, identifier = await self.crash(
                    phase, state_file=state_file
                )
                runner = work.replacement()
                await runner.recover(identifier)
                self.assertEqual(
                    runner._state.template, new if phase == "rebuild_committed" else old
                )
                self.assertIsNone(runner._state.pending_rebuild)
                self.assertEqual(runner._state.mode, "paused")
                self.assertEqual(runner.get_state()["phase"], "waiting")
                if phase not in ("rebuild_prepared", "rebuild_committed"):
                    self.assertTrue(events(runner, "reload.candidate"))
                    self.assertEqual(len(events(runner, "template.applied")), 1)
                await work.close()

    async def test_crashes_preserve_new_and_old_service_ownership_rt18(self):
        for phase in (
            "service_stopped",
            "module_data_moved",
            "service_started",
            "load_applied",
        ):
            with self.subTest(phase=phase):
                work, old, _new, identifier = await self.crash(phase, services=True)
                runner = work.replacement()
                await runner.recover(identifier)
                self.assertEqual(runner._state.template, old)
                self.assertEqual(await work.value(), 39)
                self.assertIsNone(runner._state.pending_rebuild)
                self.assertTrue(all(s.ready for s in runner._state.services.values()))
                await work.close()

    async def test_real_crashes_during_automatic_rollback_preserve_audit_rt15_rt18(
        self,
    ):
        for phase in ("staging", "old_moved", "new_installed", "journal_committed"):
            with self.subTest(phase=phase):
                work, old, _new, identifier = await self.crash(phase)
                runner = work.replacement()
                await runner.recover(identifier)
                self.assertEqual(runner._state.template, old)
                self.assertEqual(len(events(runner, "template.applied")), 1)
                candidate_ids = [
                    e["event_id"] for e in events(runner, "reload.candidate")
                ]
                self.assertTrue(candidate_ids)
                identity = runner._journal.client.get_journal_info()
                await runner.close()
                recovered = work.replacement()
                await recovered.recover(identifier)
                self.assertEqual(recovered._journal.client.get_journal_info(), identity)
                self.assertEqual(
                    [e["event_id"] for e in events(recovered, "reload.candidate")],
                    candidate_ids,
                )
                await work.close()

    async def test_unverifiable_spawn_intent_keeps_recovery_blocked_rt18(self):
        work, _old, _new, identifier = await self.crash(
            "service_spawn_intent", services=True
        )
        runner = work.replacement()
        with self.assertRaises(RuntimeError):
            await runner.recover(identifier)
        self.assertIsNotNone(runner._state.pending_rebuild)
        self.assertIn(identifier, recovery_candidates(work.root))
