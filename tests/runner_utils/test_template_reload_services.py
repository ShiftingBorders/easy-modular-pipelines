"""Approved RT01, RT03, RT09-RT12, RT14-RT15, RT17: real service reloads."""

import asyncio
import copy
import hashlib
import json
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from core.runner_utils.runtimeio import read_json, write_json
from tests.helpers.dag import process_running
from tests.helpers.reload import candidate, events, version, workspace
from tests.helpers.services import wait_for


class TemplateReloadServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_recovery_rejects_affected_services_before_effects_rr15(self):
        work = await self.prepare()
        work.template["services"][1]["settings"].update(
            launch_child=True, launcher_cleanup=True
        )
        runner = await work.launch()
        identifier = runner._state.experiment_id
        sid = work.commands["service_id"]
        root = runner._state.experiment_directory
        instance = runner._state.services[sid]
        process_file = (
            root
            / "shared_artifacts/services"
            / sid
            / instance.service_instance_id
            / "process.json"
        )
        record = read_json(process_file)
        launcher = runner._services._processes[sid]
        self.assertNotEqual(launcher.pid, instance.process_identity["pid"])
        await runner.close()
        legacy = copy.deepcopy(record)
        legacy.pop("launcher_process")
        write_json(process_file, legacy)
        runner = work.replacement()
        await runner.recover(identifier)
        self.assertFalse(runner._services._processes)
        await runner.step()
        state = runner._state
        original = candidate(work)
        scheduler = runner._task
        attempt, response = runner._last_attempt, copy.deepcopy(runner._last_response)
        progress = (state.stage_position, copy.deepcopy(state.stage_result_ids))
        identities = {
            key: item.service_instance_id for key, item in state.services.items()
        }
        applied = state.template_path.read_bytes()
        marker = root / "module_data" / sid / "original.txt"
        marker.write_text("original", encoding="utf-8")
        try:
            for metadata in ("legacy", "null", "missing"):
                for change in ("replace", "remove", "both", "hash"):
                    with self.subTest(metadata=metadata, change=change):
                        if metadata == "missing":
                            process_file.unlink(missing_ok=True)
                        else:
                            write_json(
                                process_file,
                                legacy
                                if metadata == "legacy"
                                else {**legacy, "launcher_process": None},
                            )
                        document = candidate(work)
                        if change == "remove":
                            document["services"].pop(1)
                        elif change == "hash":
                            document["services"][1]["module"]["hash"] = "0" * 64
                        else:
                            document["services"][1]["settings"]["nullable"] = "changed"
                            if change == "both":
                                document["services"][0]["settings"]["nullable"] = (
                                    "changed"
                                )
                        with (
                            patch.object(
                                runner._snapshots,
                                "create",
                                wraps=runner._snapshots.create,
                            ) as create,
                            patch.object(
                                runner._services,
                                "stop_all",
                                wraps=runner._services.stop_all,
                            ) as stop,
                            patch.object(
                                runner._assembler,
                                "rebuild",
                                wraps=runner._assembler.rebuild,
                            ) as rebuild,
                        ):
                            with self.assertRaisesRegex(
                                RuntimeError, "launcher identity is unavailable"
                            ):
                                await runner.reload_template(
                                    work.files.write_template(document)
                                )
                            create.assert_not_awaited()
                            stop.assert_not_awaited()
                            rebuild.assert_not_awaited()
                        self.assertEqual(state.template, original)
                        self.assertEqual(state.template_path.read_bytes(), applied)
                        self.assertEqual(
                            (state.stage_position, state.stage_result_ids), progress
                        )
                        self.assertIs(runner._task, scheduler)
                        self.assertFalse(scheduler.done())
                        self.assertIs(runner._last_attempt, attempt)
                        self.assertEqual(runner._last_response, response)
                        self.assertIsNone(state.pending_rebuild)
                        self.assertEqual(runner.get_state()["phase"], "waiting")
                        self.assertEqual(
                            {
                                key: item.service_instance_id
                                for key, item in state.services.items()
                            },
                            identities,
                        )
                        self.assertTrue(
                            all(
                                item.ready and not item.stopped
                                for item in state.services.values()
                            )
                        )
                        self.assertEqual(marker.read_text(encoding="utf-8"), "original")
                        self.assertIsNone(launcher.poll())
                        self.assertEqual(work.manifests(), [])
                        finished = events(runner, "operation.finished")[-1]
                        self.assertEqual(finished["data"]["status"], "failed")
                        self.assertTrue(
                            any(
                                event["operation_id"] == finished["operation_id"]
                                and "launcher identity is unavailable"
                                in str(event["data"])
                                for event in events(runner, "error.recorded")
                            )
                        )
            write_json(process_file, legacy)
            unchanged = await runner.reload_template(
                work.files.write_template(original)
            )
            self.assertFalse(unchanged["changed"])
            document = candidate(work)
            document["stages"][2]["settings"]["echo"] = "stage-only"
            await runner.reload_template(work.files.write_template(document))
            self.assertEqual(
                {key: item.service_instance_id for key, item in state.services.items()},
                identities,
            )
            self.assertEqual(await work.value(37), 37)
            await runner.step()
        finally:
            write_json(process_file, record)

    async def test_launcher_evidence_and_wait_boundaries_rr15(self):
        for recovered in (False, True):
            for outcome in ("complete", "timeout", "cancel"):
                with self.subTest(recovered=recovered, outcome=outcome):
                    work = await self.prepare()
                    work.template["start_timeout"] = 2
                    work.template["services"][0]["settings"].update(
                        launch_child=True, launcher_cleanup=True
                    )
                    runner = await work.launch()
                    state = runner._state
                    sid = work.socket["service_id"]
                    launcher = runner._services._processes[sid]
                    process_file = (
                        state.experiment_directory
                        / "shared_artifacts/services"
                        / sid
                        / state.services[sid].service_instance_id
                        / "process.json"
                    )
                    record = read_json(process_file)
                    if recovered:
                        runner._services._processes.pop(sid)
                    else:
                        legacy = copy.deepcopy(record)
                        legacy.pop("launcher_process")
                        write_json(process_file, legacy)
                    document = candidate(work)
                    document["services"][0]["settings"]["nullable"] = "changed"
                    controls = work.files.files.controls[sid]
                    gate = controls / "hold-launcher-cleanup"
                    gate.touch()
                    task = asyncio.create_task(
                        runner._services.prepare_rebuild(state, document)
                    )
                    try:
                        await wait_for(
                            (controls / "launcher-cleanup-started.json").exists
                        )
                        self.assertIsNone(launcher.poll())
                        self.assertFalse(task.done())
                        if outcome == "complete":
                            gate.unlink()
                            await asyncio.wait_for(task, 15)
                            self.assertIsNotNone(launcher.poll())
                            self.assertTrue(
                                (controls / "launcher-cleanup-finished").exists()
                            )
                        elif outcome == "timeout":
                            with self.assertRaisesRegex(
                                RuntimeError, "still owns runtime files"
                            ):
                                await asyncio.wait_for(task, 15)
                        else:
                            task.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await task
                    finally:
                        gate.unlink(missing_ok=True)
                        await asyncio.gather(task, return_exceptions=True)
                        await asyncio.to_thread(launcher.wait, 15)
                        write_json(process_file, record)
                        runner._services._processes[sid] = launcher
                    await work.close()

    async def test_mismatched_launcher_metadata_rejects_preparation_rr15(self):
        work = await self.prepare()
        runner = await work.launch()
        state = runner._state
        sid = work.socket["service_id"]
        process_file = (
            state.experiment_directory
            / "shared_artifacts/services"
            / sid
            / state.services[sid].service_instance_id
            / "process.json"
        )
        record = read_json(process_file)
        launcher = runner._services._processes.pop(sid)
        document = candidate(work)
        document["services"][0]["settings"]["nullable"] = "changed"
        try:
            for field in ("experiment_id", "participant_id", "participant_instance_id"):
                with self.subTest(field=field):
                    write_json(process_file, {**record, field: str(uuid4())})
                    with (
                        patch.object(
                            runner._snapshots, "create", wraps=runner._snapshots.create
                        ) as create,
                        patch.object(
                            runner._services,
                            "stop_all",
                            wraps=runner._services.stop_all,
                        ) as stop,
                        self.assertRaisesRegex(
                            RuntimeError, "launcher ownership cannot be verified"
                        ),
                    ):
                        await runner.reload_template(
                            work.files.write_template(document)
                        )
                    create.assert_not_awaited()
                    stop.assert_not_awaited()
                    self.assertIsNone(launcher.poll())
        finally:
            write_json(process_file, record)
            runner._services._processes[sid] = launcher

    async def test_launcher_cleanup_precedes_replacement_or_removal_rr10(self):
        for remove in (False, True):
            with self.subTest(remove=remove):
                work = await self.prepare()
                work.template["services"][0]["settings"].update(
                    launch_child=True, launcher_cleanup=True
                )
                work.template["services"][1]["heartbeat"]["interval_seconds"] = 0.1
                replacement = version(work, work.socket, "1", name="replacement")
                runner = await work.launch()
                state = runner._state
                sid, peer = (item["service_id"] for item in state.template["services"])
                launcher = runner._services._processes[sid]
                child = state.services[sid].process_identity
                self.assertNotEqual(launcher.pid, child["pid"])
                peer_identity = copy.deepcopy(state.services[peer].process_identity)
                peer_instance = state.services[peer].service_instance_id
                controls = work.files.files.controls[sid]
                gate = controls / "hold-launcher-cleanup"
                gate.touch()
                marker = state.experiment_directory / "module_data" / sid / "old.txt"
                marker.write_text("old data", encoding="utf-8")
                document = candidate(work)
                if remove:
                    document["services"].pop(0)
                else:
                    document["services"][0] = replacement
                original = candidate(work)
                applied = events(runner, "template.applied")
                waiting = threading.Event()
                original_wait = launcher.wait
                loop_thread = threading.get_ident()

                def wait_for_launcher(
                    timeout=None,
                    *,
                    loop_thread=loop_thread,
                    waiting=waiting,
                    original_wait=original_wait,
                ):
                    self.assertNotEqual(threading.get_ident(), loop_thread)
                    waiting.set()
                    return original_wait(timeout)

                task = None
                try:
                    with (
                        patch.object(launcher, "wait", wait_for_launcher),
                        patch.object(
                            runner._assembler,
                            "rebuild",
                            wraps=runner._assembler.rebuild,
                        ) as rebuild,
                        patch.object(
                            runner._services._processes[peer],
                            "wait",
                            side_effect=AssertionError(
                                "Unchanged launcher must not be awaited"
                            ),
                        ),
                    ):
                        task = asyncio.create_task(
                            runner.reload_template(work.files.write_template(document))
                        )
                        await wait_for(waiting.is_set, 60)
                        await wait_for(
                            (controls / "launcher-cleanup-started.json").exists
                        )
                        self.assertFalse(process_running(child["pid"]))
                        self.assertIsNone(launcher.poll())
                        start = time.monotonic()
                        await wait_for(
                            lambda work=work, start=start: (
                                len(
                                    [
                                        row
                                        for row in work.files.trace(work.commands)
                                        if row.get("command") == "heartbeat"
                                        and row["event"] == "received"
                                        and row["at"] > start
                                    ]
                                )
                                >= 2
                            )
                        )
                        self.assertFalse(task.done())
                        self.assertEqual(rebuild.await_count, 1)
                        self.assertTrue(rebuild.await_args.kwargs["prepare_only"])
                        self.assertEqual(marker.read_text(encoding="utf-8"), "old data")
                        self.assertEqual(state.template, original)
                        self.assertEqual(events(runner, "template.applied"), applied)
                        self.assertEqual(state.services[sid].process_identity, child)
                        self.assertTrue(state.services[peer].ready)
                        gate.unlink()
                        await asyncio.wait_for(task, 60)
                        self.assertEqual(rebuild.await_count, 2)
                    self.assertIsNotNone(launcher.poll())
                    self.assertTrue((controls / "launcher-cleanup-finished").exists())
                    cleanup = marker.with_name("launcher-cleanup.txt")
                    if remove:
                        self.assertNotIn(sid, state.services)
                        self.assertEqual(
                            cleanup.read_text(encoding="utf-8"), "old launcher cleanup"
                        )
                    else:
                        self.assertFalse(marker.exists())
                        self.assertFalse(cleanup.exists())
                        self.assertNotEqual(state.services[sid].process_identity, child)
                        self.assertEqual(await work.value(), 0)
                    self.assertEqual(
                        state.services[peer].process_identity, peer_identity
                    )
                    self.assertEqual(
                        state.services[peer].service_instance_id, peer_instance
                    )
                    self.assertTrue(state.services[peer].ready)
                    self.assertEqual(state.services[peer].restart_count, 0)
                    self.assertIsNone(state.services[peer].failure)
                    self.assertEqual(state.template, document)
                finally:
                    gate.unlink(missing_ok=True)
                    if task is not None:
                        if not task.done():
                            task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                await work.close()

    async def test_launcher_wait_timeout_or_cancel_preserves_recovery_rr10(self):
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                work = await self.prepare()
                work.template["start_timeout"] = 5
                work.template["services"][0]["settings"].update(
                    launch_child=True, launcher_cleanup=True
                )
                replacement = version(work, work.socket, "1", name="replacement")
                runner = await work.launch()
                state = runner._state
                sid = work.socket["service_id"]
                launcher = runner._services._processes[sid]
                controls = work.files.files.controls[sid]
                gate = controls / "hold-launcher-cleanup"
                gate.touch()
                marker = state.experiment_directory / "module_data" / sid / "old.txt"
                marker.write_text("old data", encoding="utf-8")
                original = candidate(work)
                document = candidate(work)
                document["services"][0] = replacement
                waiting = threading.Event()
                finished = threading.Event()
                original_wait = launcher.wait
                waits = []

                def wait_for_launcher(
                    timeout=None,
                    *,
                    waits=waits,
                    waiting=waiting,
                    original_wait=original_wait,
                    finished=finished,
                ):
                    waits.append(timeout)
                    waiting.set()
                    try:
                        return original_wait(timeout)
                    finally:
                        finished.set()

                task = None
                try:
                    with (
                        patch.object(launcher, "wait", wait_for_launcher),
                        patch.object(
                            runner._assembler,
                            "rebuild",
                            wraps=runner._assembler.rebuild,
                        ) as rebuild,
                        patch.object(
                            runner._services, "reset", wraps=runner._services.reset
                        ) as reset,
                    ):
                        task = asyncio.create_task(
                            runner.reload_template(work.files.write_template(document))
                        )
                        await wait_for(waiting.is_set, 60)
                        if cancel:
                            task.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await task
                            reset.assert_not_awaited()
                        else:
                            with self.assertRaisesRegex(
                                RuntimeError, f"{sid} launcher still owns"
                            ):
                                await asyncio.wait_for(task, 40)
                            self.assertGreaterEqual(reset.await_count, 1)
                            self.assertIn(
                                "launcher still owns",
                                json.dumps(events(runner, "error.recorded")),
                            )
                            self.assertIn(
                                "A service command still owns",
                                json.dumps(events(runner, "error.recorded")),
                            )
                        self.assertEqual(waits[0], original["start_timeout"])
                        self.assertEqual(rebuild.await_count, 1)
                        self.assertEqual(state.template, original)
                        self.assertIsNotNone(state.pending_rebuild)
                        self.assertEqual(marker.read_text(encoding="utf-8"), "old data")
                        self.assertIs(runner._services._processes[sid], launcher)
                        self.assertIsNone(launcher.poll())
                        self.assertEqual(len(work.manifests()), 1)
                        self.assertFalse(
                            any(
                                event["data"].get("state") == "files_published"
                                for event in events(runner, "control.observed")
                            )
                        )
                        gate.unlink()
                        await wait_for(
                            lambda launcher=launcher: launcher.poll() is not None
                        )
                        await wait_for(finished.is_set)
                    await runner.stop()
                    await runner.recover(state.experiment_id)
                    self.assertEqual(runner._state.template, original)
                    self.assertIsNone(runner._state.pending_rebuild)
                    self.assertEqual(runner.get_state()["phase"], "waiting")
                    self.assertEqual(marker.read_text(encoding="utf-8"), "old data")
                finally:
                    gate.unlink(missing_ok=True)
                    if task is not None:
                        if not task.done():
                            task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                await work.close()

    async def test_exited_launchers_need_no_extra_wait_rr10(self):
        for child in (False, True):
            with self.subTest(child=child):
                work = await self.prepare()
                work.template["services"][0]["settings"]["launch_child"] = child
                runner = await work.launch()
                sid = work.socket["service_id"]
                launcher = runner._services._processes[sid]
                result = await runner._services.stop_all(
                    runner._state, service_ids={sid}
                )
                self.assertTrue(result[sid]["stopped"])
                self.assertIsNone(result[sid]["error"])
                await asyncio.to_thread(launcher.wait, 15)
                document = candidate(work)
                document["services"][0]["settings"]["nullable"] = "changed"
                with patch.object(
                    launcher,
                    "wait",
                    side_effect=AssertionError("Launcher already exited"),
                ):
                    await runner._services.prepare_rebuild(runner._state, document)
                await work.close()

    async def test_unconfirmed_participant_stop_rejects_before_launcher_wait_rr10(self):
        work = await self.prepare()
        work.template["services"][0]["settings"]["launch_child"] = True
        runner = await work.launch()
        sid = work.socket["service_id"]
        launcher = runner._services._processes[sid]
        document = candidate(work)
        document["services"][0]["settings"]["nullable"] = "changed"
        for result in (
            {"stopped": False, "error": None},
            {"stopped": True, "error": "shutdown failed"},
        ):
            with (
                self.subTest(result=result),
                patch.object(
                    runner._services,
                    "stop_all",
                    new_callable=AsyncMock,
                    return_value={sid: result},
                ),
                patch.object(
                    launcher,
                    "wait",
                    side_effect=AssertionError("Stop is unconfirmed"),
                ),
                self.assertRaisesRegex(RuntimeError, "did not stop cleanly"),
            ):
                await runner._services.prepare_rebuild(runner._state, document)
        self.assertIsNone(launcher.poll())

    async def test_slow_resource_checks_keep_unchanged_service_heartbeats_rr06(self):
        work = await self.prepare()
        # Use one real TCP service; the command proxy is unrelated to this boundary.
        work.template["services"] = [work.socket]
        work.socket["heartbeat"] = {"interval_seconds": 0.1, "grace_seconds": 2}
        resource = work.root / "resource.bin"
        resource.write_bytes(b"checked resource" * 4096)
        work.template["resources"] = [
            {
                "name": "data",
                "path": str(resource),
                "hash": hashlib.sha256(resource.read_bytes()).hexdigest(),
            }
        ]
        runner = await work.launch()
        sid = work.socket["service_id"]
        instance = runner._state.services[sid].service_instance_id
        process = runner._state.services[sid].process_identity
        document = candidate(work)
        document["stages"][0]["settings"]["echo"] = "candidate"
        check = runner._assembler.check_resources
        loop_thread = threading.get_ident()
        intervals = []

        def slow_check(state):
            self.assertNotEqual(threading.get_ident(), loop_thread)
            start = time.monotonic()
            time.sleep(3)
            check(state)
            intervals.append((start, time.monotonic()))

        with patch.object(runner._assembler, "check_resources", slow_check):
            await runner.reload_template(work.files.write_template(document))
        self.assertEqual(len(intervals), 2)
        for start, end in intervals:
            self.assertGreater(end - start, work.socket["heartbeat"]["grace_seconds"])
            heartbeats = [
                row
                for row in work.files.trace(work.socket)
                if row["event"] == "received"
                and row.get("command") == "heartbeat"
                and start < row["at"] < end
            ]
            self.assertGreaterEqual(len(heartbeats), 2)
        current = runner._state.services[sid]
        self.assertEqual(current.service_instance_id, instance)
        self.assertEqual(current.process_identity, process)
        self.assertEqual(current.restart_count, 0)
        self.assertIsNone(current.failure)
        self.assertTrue(current.ready)
        self.assertEqual(runner._state.template, document)

    async def test_readded_service_isolates_orphan_data_and_rollback_restores_it_rr01(
        self,
    ):
        for same_name in (False, True):
            with self.subTest(same_name=same_name):
                work = await self.prepare()
                replacement = (
                    copy.deepcopy(work.socket)
                    if same_name
                    else version(work, work.socket, "1", name="returned-service")
                )
                runner = await work.launch()
                await work.value(42)
                sid = work.socket["service_id"]
                peer = work.commands["service_id"]
                peer_instance = runner._state.services[peer].service_instance_id
                root = runner._state.experiment_directory
                marker = root / "module_data" / sid / "old.txt"
                marker.write_text("removed module data", encoding="utf-8")
                peer_marker = root / "module_data" / peer / "peer.txt"
                peer_marker.write_text("peer data", encoding="utf-8")
                document = candidate(work)
                document["services"] = [
                    s for s in document["services"] if s["service_id"] != sid
                ]
                await runner.reload_template(work.files.write_template(document))
                self.assertTrue(marker.is_file())
                before_add = candidate(work)
                document = candidate(work)
                document["services"].append(replacement)
                result = await runner.reload_template(
                    work.files.write_template(document)
                )
                self.assertFalse(marker.exists())
                self.assertEqual(await work.value(), 0)
                self.assertFalse(
                    any(
                        row.get("command") == "load_state"
                        for row in work.files.trace(work.socket)
                    )
                )
                self.assertEqual(
                    runner._state.services[peer].service_instance_id, peer_instance
                )
                self.assertEqual(peer_marker.read_text(encoding="utf-8"), "peer data")
                audit = events(runner, "reload.service_data")[-1]
                self.assertEqual(audit["data"]["service_id"], sid)
                self.assertEqual(audit["data"]["reason"], "no_applied_service")
                await runner.rollback(result["snapshot_id"])
                await runner._ready.wait()
                self.assertEqual(runner._state.template, before_add)
                self.assertNotIn(sid, runner._state.services)
                self.assertEqual(
                    marker.read_text(encoding="utf-8"), "removed module data"
                )
                await work.close()

    async def prepare(self):
        work = workspace(services=True)
        self.addAsyncCleanup(work.close)
        for service in work.template["services"]:
            service["settings"]["auto_increment"] = False
        return work

    async def test_matching_name_loads_state_across_versions_and_preserves_peer_rt10(
        self,
    ):
        work = await self.prepare()
        second = version(work, work.socket, "2")
        runner = await work.launch()
        sid = work.socket["service_id"]
        peer = work.commands["service_id"]
        await work.value(42)
        peer_instance = runner._state.services[peer].service_instance_id
        root = runner._state.experiment_directory
        marker = root / "module_data" / sid / "retained.txt"
        marker.write_text("owned data", encoding="utf-8")
        for label in ("settings", "upgrade", "downgrade"):
            with self.subTest(change=label):
                old_instance = runner._state.services[sid].service_instance_id
                old_process = runner._state.services[sid].process_identity
                document = candidate(work)
                if label == "settings":
                    document["services"][0]["settings"]["nullable"] = "updated"
                else:
                    document["services"][0]["module"] = copy.deepcopy(
                        second["module"]
                        if label == "upgrade"
                        else work.socket["module"]
                    )
                result = await runner.reload_template(
                    work.files.write_template(document)
                )
                self.assertTrue(result["changed"])
                self.assertEqual(await work.value(), 42)
                self.assertEqual(marker.read_text(encoding="utf-8"), "owned data")
                self.assertNotEqual(
                    runner._state.services[sid].service_instance_id, old_instance
                )
                self.assertFalse(process_running(old_process["pid"]))
                self.assertEqual(
                    runner._state.services[peer].service_instance_id, peer_instance
                )
                self.assertTrue(runner._state.services[peer].ready)
        loads = [
            row
            for row in work.files.trace(work.socket)
            if row["event"] == "work_finished" and row.get("command") == "load_state"
        ]
        self.assertEqual(len(loads), 3)
        self.assertEqual([row["response"]["data"]["loaded"] for row in loads], [42] * 3)
        self.assertFalse(
            any(
                row.get("command") == "load_state"
                for row in work.files.trace(work.commands)
            )
        )

    async def test_different_name_has_fresh_data_and_rollback_restores_old_data_rt11(
        self,
    ):
        work = await self.prepare()
        changed = version(work, work.socket, "1", name="different-service")
        runner = await work.launch()
        await work.value(42)
        sid = work.socket["service_id"]
        marker = runner._state.experiment_directory / "module_data" / sid / "old.txt"
        marker.write_text("old module", encoding="utf-8")
        document = candidate(work)
        document["services"][0] = changed
        result = await runner.reload_template(work.files.write_template(document))
        self.assertFalse(marker.exists())
        self.assertEqual(await work.value(), 0)
        self.assertFalse(
            any(
                row.get("command") == "load_state"
                for row in work.files.trace(work.socket)
            )
        )
        await runner.rollback(result["snapshot_id"])
        await runner._ready.wait()
        self.assertEqual(await work.value(), 42)
        self.assertEqual(marker.read_text(encoding="utf-8"), "old module")

    async def test_new_id_does_not_inherit_same_named_state_and_reordering_keeps_instances_rt09_rt11(
        self,
    ):
        work = await self.prepare()
        runner = await work.launch()
        await work.value(31)
        old_instances = {
            sid: instance.service_instance_id
            for sid, instance in runner._state.services.items()
        }
        document = candidate(work)
        document["services"].reverse()
        await runner.reload_template(work.files.write_template(document))
        self.assertEqual(
            {sid: s.service_instance_id for sid, s in runner._state.services.items()},
            old_instances,
        )
        added = copy.deepcopy(work.socket)
        added["service_id"] = str(uuid4())
        controls = work.root / "controls" / added["service_id"]
        controls.mkdir()
        work.files.files.controls[added["service_id"]] = controls
        added["settings"]["controls"] = str(controls)
        document = candidate(work)
        document["services"].append(added)
        await runner.reload_template(work.files.write_template(document))
        response = await runner._services.request(
            runner._state, added["service_id"], "get_value", {}
        )
        self.assertEqual(response["data"]["value"], 0)
        self.assertEqual(await work.value(), 31)
        document = candidate(work)
        document["services"] = [
            s for s in document["services"] if s["service_id"] != added["service_id"]
        ]
        process = runner._state.services[added["service_id"]].process_identity
        await runner.reload_template(work.files.write_template(document))
        self.assertFalse(process_running(process["pid"]))
        self.assertEqual(
            {sid: s.service_instance_id for sid, s in runner._state.services.items()},
            old_instances,
        )

    async def test_incompatible_state_rolls_back_and_preserves_audit_rt12_rt15(self):
        work = await self.prepare()
        runner = await work.launch()
        await work.value(67)
        before = candidate(work)
        old_revision = runner._state.template_revision_id
        identity = runner._journal.client.get_journal_info()
        document = candidate(work)
        document["services"][0]["settings"]["reject_state"] = True
        with self.assertRaises(RuntimeError):
            await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner._state.template, before)
        self.assertEqual(runner._state.template_revision_id, old_revision)
        self.assertEqual(runner.get_state()["mode"], "paused")
        self.assertEqual(await work.value(), 67)
        self.assertIsNone(runner._state.pending_rebuild)
        self.assertNotEqual(
            runner._journal.client.get_journal_info()["generation"],
            identity["generation"],
        )
        applied = events(runner, "template.applied")
        self.assertEqual(
            [e["data"]["template_revision_id"] for e in applied], [old_revision]
        )
        self.assertEqual(
            events(runner, "reload.candidate")[-1]["data"]["template"], document
        )
        self.assertTrue(events(runner, "error.recorded"))
        self.assertEqual(
            events(runner, "control.reconciled")[-1]["data"]["state"], "rolled_back"
        )

    async def test_service_call_at_b_rewinds_b_but_unused_service_keeps_progress_rt06_rt09(
        self,
    ):
        work = await self.prepare()
        service_node = {
            "stage_id": work.stages[1]["stage_id"],
            "service_id": work.socket["service_id"],
            "settings": {},
            "timeout_seconds": 30,
            "errors": copy.deepcopy(work.stages[1]["errors"]),
        }
        work.template["stages"][1] = service_node
        runner = await work.launch()
        await runner.step()
        await runner.step()
        ids = dict(runner._state.stage_result_ids)
        document = candidate(work)
        document["services"][1]["settings"]["nullable"] = "unused"
        await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner._state.stage_result_ids, ids)
        self.assertEqual(runner._state.stage_position, 3)
        document = candidate(work)
        document["services"][0]["settings"]["nullable"] = "called by B"
        await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner._state.stage_position, 2)
        self.assertFalse(runner._pending_advance)
        self.assertNotIn(service_node["stage_id"], runner._state.stage_result_ids)

    async def test_stateless_export_is_optional_and_role_identity_cannot_change_rt03_rt10(
        self,
    ):
        work = await self.prepare()
        work.template["services"][0]["state_required"] = False
        controls = work.files.files.controls[work.socket["service_id"]]
        (controls / "null-state").touch()
        runner = await work.launch()
        document = candidate(work)
        document["services"][0]["settings"]["nullable"] = "changed"
        await runner.reload_template(work.files.write_template(document))
        self.assertFalse(
            any(
                row.get("command") == "load_state"
                for row in work.files.trace(work.socket)
            )
        )
        document = candidate(work)
        sid = document["services"][0]["service_id"]
        document["services"].pop(0)
        document["stages"][0]["stage_id"] = sid
        with self.assertRaisesRegex(ValueError, "role"):
            await runner.reload_template(work.files.write_template(document))

    async def test_cancel_state_load_keeps_recovery_requirement_rt17(self):
        work = await self.prepare()
        runner = await work.launch()
        original = candidate(work)
        document = candidate(work)
        document["services"][0]["settings"]["hold_load"] = True
        task = asyncio.create_task(
            runner.reload_template(work.files.write_template(document))
        )
        controls = work.files.files.controls[work.socket["service_id"]]
        try:
            await wait_for(
                lambda: any(
                    row["event"] == "work_started"
                    and row.get("command") == "load_state"
                    for row in work.files.trace(work.socket)
                ),
                timeout=60,
            )
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            pending = copy.deepcopy(runner._state.pending_rebuild)
            self.assertIsNotNone(pending)
            for action in (
                runner.resume,
                runner.step,
                lambda: runner.run(work.template_path),
                lambda: runner.rollback(pending["snapshot_id"]),
            ):
                with self.assertRaises(RuntimeError):
                    await action()
            (controls / "release-load").touch()
            await runner.stop()
            self.assertEqual(runner._state.pending_rebuild, pending)
            self.assertEqual(len(work.manifests()), 1)
            await runner.recover(runner._state.experiment_id)
            self.assertEqual(runner._state.template, original)
            self.assertIsNone(runner._state.pending_rebuild)
        finally:
            (controls / "release-load").touch()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_manual_stop_and_blocked_services_reject_reload_rt01(self):
        work = await self.prepare()
        runner = await work.launch()
        await runner.stop_service(1)
        with self.assertRaises(RuntimeError):
            await runner.reload_template()
        await runner.start_service(1)
        service = runner._state.services[work.socket["service_id"]]
        service.blocked_action = "pause"
        try:
            with self.assertRaises(RuntimeError):
                await runner.reload_template()
        finally:
            service.blocked_action = None

    async def test_controller_priority_stop_cancels_reload_and_blocks_chain_tail_rt17_rt19(
        self,
    ):
        work = await self.prepare()
        runner = await work.files.launch(work.template)
        work.runner = runner
        work.runners.append(runner)
        session = work.files.session
        document = candidate(work)
        document["services"][0]["settings"]["hold_load"] = True
        path = work.files.write_template(document, "reload.yaml")
        futures = session.chain(
            [
                {"command": "reload_template", "args": {"template_path": str(path)}},
                {"command": "resume"},
            ]
        )
        controls = work.files.files.controls[work.socket["service_id"]]
        try:
            await wait_for(
                lambda: any(
                    row["event"] == "work_started"
                    and row.get("command") == "load_state"
                    for row in work.files.trace(work.socket)
                ),
                timeout=60,
            )
            stop = session.post("stop")
            responses = await asyncio.wait_for(asyncio.gather(*futures, stop), 60)
            self.assertEqual(
                [r["state"] for r in responses[:2]], ["cancelled", "cancelled"]
            )
            self.assertTrue(responses[-1]["data"]["termination_confirmed"])
            self.assertIsNotNone(runner._state.pending_rebuild)
            blocked = await session.send("resume")
            self.assertEqual(blocked["error"]["code"], "invalid_state")
            (controls / "release-load").touch()
            recovered = await asyncio.wait_for(
                session.post("recover", {"experiment_id": runner._state.experiment_id}),
                90,
            )
            self.assertEqual(recovered["result"], "success", recovered)
            self.assertIsNone(runner._state.pending_rebuild)
        finally:
            (controls / "release-load").touch()

    async def test_unreferenced_service_without_id_gets_fresh_identity_and_role_mismatch_is_rejected_rt03_rt05(
        self,
    ):
        work = await self.prepare()
        runner = await work.launch()
        before = candidate(work)
        document = candidate(work)
        document["stages"][0]["module"] = copy.deepcopy(work.socket["module"])
        with self.assertRaisesRegex(ValueError, "role"):
            await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner._state.template, before)
        self.assertEqual(work.manifests(), [])
        document = candidate(work)
        extra = copy.deepcopy(document["services"][0])
        del extra["service_id"]
        document["services"].append(extra)
        await runner.reload_template(work.files.write_template(document))
        services = runner._state.template["services"]
        self.assertEqual(len({s["service_id"] for s in services}), 3)
        added_id = services[-1]["service_id"]
        self.assertNotIn(added_id, {s["service_id"] for s in before["services"]})
        self.assertFalse((await runner.reload_template())["changed"])

    async def test_loaded_service_must_confirm_fresh_readiness_rt12(self):
        work = await self.prepare()
        runner = await work.launch()
        await work.value(53)
        original = candidate(work)
        document = candidate(work)
        document["services"][0]["settings"]["fail_after_load"] = True
        document["services"][0]["heartbeat"]["interval_seconds"] = 0.05
        document["services"][0]["errors"]["retry_delay_seconds"] = 0
        with self.assertRaises(RuntimeError):
            await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner._state.template, original)
        self.assertEqual(await work.value(), 53)

    async def test_load_timeout_and_required_missing_state_fail_with_rollback_rt12(
        self,
    ):
        for missing in (False, True):
            with self.subTest(missing=missing):
                work = await self.prepare()
                controls = work.files.files.controls[work.socket["service_id"]]
                if missing:
                    work.template["services"][0]["state_required"] = False
                    (controls / "null-state").touch()
                runner = await work.launch()
                original = candidate(work)
                document = candidate(work)
                if missing:
                    document["services"][0]["state_required"] = True
                else:
                    document["services"][0]["command_timeout_seconds"] = 0.3
                    document["services"][0]["settings"]["hold_load"] = True
                try:
                    with self.assertRaises((ValueError, RuntimeError)):
                        await runner.reload_template(
                            work.files.write_template(document)
                        )
                    self.assertEqual(runner._state.template, original)
                    self.assertIsNone(runner._state.pending_rebuild)
                    self.assertEqual(runner.get_state()["mode"], "paused")
                finally:
                    (controls / "release-load").touch()
                await work.close()
