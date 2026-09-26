"""Approved RT08, RT15-RT18: protected publication, failures and cancellation."""

import asyncio
import copy
import hashlib
import shutil
import threading
import unittest
from unittest.mock import AsyncMock, patch

from core.logger_utils.events import JournalGenerationChanged, LoggingStorageError
from core.logger_utils.storage import SQLiteEventStore
from core.runner_utils.state import RunnerStateStore, state_to_document
from tests.helpers.reload import candidate, events, trace, workspace
from tests.helpers.services import wait_for


class TemplateReloadFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejected_preparation_preserves_last_attempt_and_execution_rr09(self):
        for problem, continuation in (
            ("hash", "step"),
            ("missing_module", "resume"),
            ("cancel_prepared", "step"),
            ("cancel_detached", "resume"),
        ):
            with self.subTest(problem=problem, continuation=continuation):
                work = workspace(cycles=1)
                self.addAsyncCleanup(work.close)
                runner = await work.launch()
                await runner.step()
                before = runner.get_state()
                attempt = runner._last_attempt
                response = copy.deepcopy(runner._last_response)
                state = state_to_document(runner._state)
                dag = runner._task
                self.assertIsNotNone(before["attempt_id"])
                self.assertIsNotNone(before["executor"])
                document = candidate(work)
                document["stages"][1]["settings"]["echo"] = "candidate"
                if problem == "hash":
                    document["stages"][1]["module"]["hash"] = "0" * 64
                elif problem == "missing_module":
                    document["stages"][1]["module"]["version"] = "missing"
                path = work.files.write_template(document)
                if problem.startswith("cancel"):
                    entered = asyncio.Event()
                    rebuild = runner._assembler.rebuild

                    async def prepare_then_wait(
                        *args, rebuild=rebuild, entered=entered, **kwargs
                    ):
                        await rebuild(*args, **kwargs)
                        entered.set()
                        await asyncio.Event().wait()

                    async def snapshot_wait(*args, entered=entered, **kwargs):
                        entered.set()
                        await asyncio.Event().wait()

                    target = (
                        runner._assembler
                        if problem == "cancel_prepared"
                        else runner._snapshots
                    )
                    method = "rebuild" if problem == "cancel_prepared" else "create"
                    replacement = (
                        prepare_then_wait
                        if problem == "cancel_prepared"
                        else snapshot_wait
                    )
                    with patch.object(target, method, replacement):
                        task = asyncio.create_task(runner.reload_template(path))
                        try:
                            await asyncio.wait_for(entered.wait(), 30)
                            task.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await task
                        finally:
                            if not task.done():
                                task.cancel()
                            await asyncio.gather(task, return_exceptions=True)
                else:
                    with self.assertRaises(
                        (ValueError, FileNotFoundError, NotADirectoryError)
                    ):
                        await runner.reload_template(path)

                after = runner.get_state()
                before.pop("observed_at")
                after.pop("observed_at")
                self.assertEqual(after, before)
                self.assertIs(runner._last_attempt, attempt)
                self.assertEqual(runner._last_response, response)
                after_state = state_to_document(runner._state)
                # Recreating the scheduler may write an equivalent checkpoint.
                after_state.pop("checkpoint_id")
                state.pop("checkpoint_id")
                self.assertEqual(after_state, state)
                self.assertEqual(work.manifests(), [])
                self.assertIsNone(runner._state.pending_rebuild)
                self.assertFalse(runner._task.done())
                if problem == "cancel_detached":
                    self.assertIsNot(runner._task, dag)
                else:
                    self.assertIs(runner._task, dag)
                operations = [
                    event
                    for event in events(runner, "operation.finished")
                    if event["data"]["status"]
                    == ("cancelled" if problem.startswith("cancel") else "failed")
                ]
                self.assertTrue(operations)
                if continuation == "step":
                    await asyncio.wait_for(runner.step(), 30)
                    expected = ["A", "B"]
                else:
                    await runner.resume()
                    await asyncio.wait_for(runner._task, 60)
                    self.assertEqual(runner.get_state()["phase"], "completed")
                    expected = ["A", "B", "C", "D"]
                self.assertEqual(
                    [row["label"] for row in trace(runner) if row["event"] == "start"],
                    expected,
                )
                await work.close()

    async def test_last_attempt_clears_only_on_apply_or_restoration_rr09(self):
        for outcome in ("applied", "rolled_back"):
            with self.subTest(outcome=outcome):
                work = workspace()
                self.addAsyncCleanup(work.close)
                runner = await work.launch()
                await runner.step()
                attempt = runner._last_attempt
                response = copy.deepcopy(runner._last_response)
                public = runner.get_state()
                old = candidate(work)
                result = await runner.reload_template()
                self.assertFalse(result["changed"])
                self.assertIs(runner._last_attempt, attempt)
                self.assertEqual(runner._last_response, response)
                self.assertEqual(runner.get_state()["attempt_id"], public["attempt_id"])
                self.assertEqual(runner.get_state()["executor"], public["executor"])
                document = candidate(work)
                document["stages"][1]["settings"]["echo"] = "candidate"
                path = work.files.write_template(document)
                if outcome == "applied":
                    await runner.reload_template(path)
                    self.assertEqual(runner._state.template, document)
                else:
                    rebuild = runner._assembler.rebuild

                    async def fail_publication(
                        *args, runner=runner, attempt=attempt, rebuild=rebuild, **kwargs
                    ):
                        if not kwargs.get("prepare_only", False):
                            # Fail before assigning the candidate template, so
                            # only successful restoration can clear the attempt.
                            self.assertIs(runner._last_attempt, attempt)
                            raise OSError("publication denied")
                        return await rebuild(*args, **kwargs)

                    with (
                        patch.object(runner._assembler, "rebuild", fail_publication),
                        self.assertRaisesRegex(OSError, "publication denied"),
                    ):
                        await runner.reload_template(path)
                    self.assertEqual(runner._state.template, old)
                    self.assertTrue(
                        any(
                            event["data"].get("state") == "rolled_back"
                            for event in events(runner, "control.reconciled")
                        )
                    )
                self.assertIsNone(runner._last_attempt)
                self.assertIsNone(runner._last_response)
                self.assertIsNone(runner.get_state()["attempt_id"])
                self.assertIsNone(runner.get_state()["executor"])
                self.assertIsNone(runner._state.pending_rebuild)
                self.assertEqual(runner._state.mode, "paused")
                self.assertEqual(runner.get_state()["result"], public["result"])
                self.assertEqual(len(work.manifests()), 1)
                await work.close()

    async def test_cancel_while_detaching_restores_scheduler_unless_stopping_rr05(self):
        for action in ("step", "resume", "stop", "close"):
            with self.subTest(action=action):
                work = workspace(services=True)
                self.addAsyncCleanup(work.close)
                runner = await work.launch()
                old = candidate(work)
                instances = {
                    sid: item.service_instance_id
                    for sid, item in runner._state.services.items()
                }
                document = candidate(work)
                document["stages"][0]["settings"]["echo"] = "candidate"
                dag = runner._task
                entered = asyncio.Event()
                release = asyncio.Event()
                gather = asyncio.gather

                async def wait_detached(
                    *tasks,
                    gather=gather,
                    dag=dag,
                    entered=entered,
                    release=release,
                    **kwargs,
                ):
                    result = await gather(*tasks, **kwargs)
                    if tasks == (dag,) and not entered.is_set():
                        entered.set()
                        await release.wait()
                    return result

                with patch(
                    "core.runner_utils.experimentrunner.asyncio.gather", wait_detached
                ):
                    reload_task = asyncio.create_task(
                        runner.reload_template(work.files.write_template(document))
                    )
                    try:
                        await asyncio.wait_for(entered.wait(), 30)
                        if action in ("stop", "close"):
                            await asyncio.wait_for(getattr(runner, action)(), 60)
                        else:
                            reload_task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await reload_task
                    finally:
                        release.set()
                        if not reload_task.done():
                            reload_task.cancel()
                        await gather(reload_task, return_exceptions=True)
                self.assertEqual(runner._state.template, old)
                self.assertIsNone(runner._state.pending_rebuild)
                if action in ("stop", "close"):
                    self.assertIs(runner._task, dag)
                    self.assertTrue(dag.done())
                else:
                    self.assertIsNot(runner._task, dag)
                    self.assertFalse(runner._task.done())
                    self.assertEqual(runner._state.mode, "paused")
                    self.assertEqual(runner._state.stage_result_ids, {})
                    self.assertEqual(
                        instances,
                        {
                            sid: item.service_instance_id
                            for sid, item in runner._state.services.items()
                        },
                    )
                    self.assertEqual(work.manifests(), [])
                    if action == "step":
                        await asyncio.wait_for(runner.step(), 30)
                        self.assertEqual(
                            [row["event"] for row in trace(runner)], ["start", "finish"]
                        )
                    else:
                        await runner.resume()
                        await wait_for(
                            lambda runner=runner: len(trace(runner)) >= 1, 30
                        )
                        await runner.pause()
                        labels = [
                            row["label"]
                            for row in trace(runner)
                            if row["event"] == "start"
                        ]
                        self.assertEqual(labels.count("A"), 1)
                await work.close()

    async def test_resource_integrity_failure_in_each_rebuild_phase_rr06(self):
        for phase in (1, 2):
            with self.subTest(check_number=phase):
                work = workspace()
                self.addAsyncCleanup(work.close)
                source = work.root / "resource.bin"
                source.write_bytes(b"original resource")
                work.template["resources"] = [
                    {
                        "name": "data",
                        "path": str(source),
                        "hash": hashlib.sha256(source.read_bytes()).hexdigest(),
                    }
                ]
                runner = await work.launch()
                old = candidate(work)
                document = candidate(work)
                document["stages"][0]["settings"]["echo"] = "candidate"
                check = runner._assembler.check_resources
                calls = 0

                def corrupt(state, phase=phase, check=check):
                    nonlocal calls
                    calls += 1
                    if calls == phase:
                        (
                            state.experiment_directory / "shared_data/resources/data"
                        ).write_bytes(b"corrupted")
                    check(state)

                with (
                    patch.object(runner._assembler, "check_resources", corrupt),
                    self.assertRaisesRegex(
                        ValueError, "Resource integrity check failed"
                    ),
                ):
                    await runner.reload_template(work.files.write_template(document))
                self.assertEqual(runner._state.template, old)
                self.assertIsNone(runner._state.pending_rebuild)
                self.assertEqual(len(work.manifests()), phase - 1)
                if phase == 2:
                    self.assertEqual(
                        (
                            runner._state.experiment_directory
                            / "shared_data/resources/data"
                        ).read_bytes(),
                        source.read_bytes(),
                    )
                await work.close()

    async def test_cancel_resource_check_waits_for_reader_before_cleanup_rr06(self):
        for phase in (1, 2):
            with self.subTest(check_number=phase):
                work = await self.make_run()
                runner = work.runner
                old = candidate(work)
                document = candidate(work)
                document["stages"][0]["settings"]["echo"] = "candidate"
                entered, release, finished = (threading.Event() for _ in range(3))
                check = runner._assembler.check_resources
                calls = 0

                def checking(
                    state,
                    phase=phase,
                    check=check,
                    entered=entered,
                    release=release,
                    finished=finished,
                ):
                    nonlocal calls
                    calls += 1
                    if calls == phase:
                        entered.set()
                        if not release.wait(30):
                            raise TimeoutError("resource check gate")
                        try:
                            check(state)
                        finally:
                            finished.set()
                    else:
                        check(state)

                with patch.object(runner._assembler, "check_resources", checking):
                    task = asyncio.create_task(
                        runner.reload_template(work.files.write_template(document))
                    )
                    try:
                        await wait_for(entered.is_set, 30)
                        task.cancel()
                        await asyncio.sleep(0.05)
                        self.assertFalse(task.done())
                        self.assertFalse(finished.is_set())
                        self.assertTrue(
                            list(
                                (
                                    runner._state.experiment_directory
                                    / "runner/rebuilds"
                                ).iterdir()
                            )
                        )
                        release.set()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                        self.assertTrue(finished.is_set())
                    finally:
                        release.set()
                        await asyncio.gather(task, return_exceptions=True)
                self.assertEqual(runner._state.template, old)
                if phase == 1:
                    self.assertIsNone(runner._state.pending_rebuild)
                    self.assertFalse(
                        list(
                            (
                                runner._state.experiment_directory / "runner/rebuilds"
                            ).iterdir()
                        )
                    )
                else:
                    self.assertIsNotNone(runner._state.pending_rebuild)
                    await runner.close()
                    recovered = work.replacement()
                    await recovered.recover(runner._state.experiment_id)
                    self.assertEqual(recovered._state.template, old)
                    self.assertIsNone(recovered._state.pending_rebuild)
                await work.close()

    async def make_run(self):
        work = workspace()
        self.addAsyncCleanup(work.close)
        await work.launch()
        return work

    async def test_insufficient_snapshot_space_does_not_apply_candidate_rt08(self):
        # RR14 extends RT08 to cover continued use of the original DAG.
        for services in (False, True):
            for continuation in ("step", "resume"):
                with self.subTest(services=services, continuation=continuation):
                    work = workspace(services=services, cycles=1)
                    self.addAsyncCleanup(work.close)
                    work.template["storage"]["min_snapshot_free_bytes"] = 1024
                    work.template["logging"]["min_free_bytes"] = 0
                    runner = await work.launch()
                    await runner.step()
                    before = runner.get_state()
                    state_before = state_to_document(runner._state)
                    attempt = runner._last_attempt
                    response = copy.deepcopy(runner._last_response)
                    dag = runner._task
                    template_file = runner._state.template_path
                    applied = template_file.read_bytes()
                    document = candidate(work)
                    document["stages"][2]["settings"]["echo"] = "candidate"
                    path = work.files.write_template(document)
                    usage = shutil.disk_usage(work.root)
                    with (
                        patch(
                            "core.runner_utils.snapshots.shutil.disk_usage",
                            return_value=type(usage)(usage.total, usage.used, 0),
                        ),
                        patch.object(
                            runner._services,
                            "save_states",
                            wraps=runner._services.save_states,
                        ) as save,
                        patch.object(
                            runner._services,
                            "stop_all",
                            wraps=runner._services.stop_all,
                        ) as stop,
                        self.assertRaisesRegex(OSError, "free space"),
                    ):
                        await runner.reload_template(path)
                    save.assert_not_awaited()
                    stop.assert_not_awaited()
                    after = runner.get_state()
                    before.pop("observed_at")
                    after.pop("observed_at")
                    for observation in (before, after):
                        for service in observation["services"]:
                            service.pop("last_status")
                    self.assertEqual(after, before)
                    state_after = state_to_document(runner._state)
                    # Heartbeat requests may be journaled while preparing the
                    # candidate. Previously reserved IDs must remain reserved.
                    self.assertLessEqual(
                        set(state_before.pop("used_request_ids")),
                        set(state_after.pop("used_request_ids")),
                    )
                    for item in (state_before, state_after):
                        item.pop("checkpoint_id")
                        for service in item["services"].values():
                            service.pop("last_status")
                    self.assertEqual(state_after, state_before)
                    self.assertIs(runner._last_attempt, attempt)
                    self.assertEqual(runner._last_response, response)
                    self.assertEqual(template_file.read_bytes(), applied)
                    self.assertIsNone(runner._state.pending_rebuild)
                    self.assertEqual(work.manifests(), [])
                    self.assertIsNot(runner._task, dag)
                    self.assertFalse(runner._task.done())
                    finished = events(runner, "operation.finished")[-1]
                    self.assertEqual(finished["data"]["status"], "failed")
                    self.assertTrue(
                        any(
                            event["operation_id"] == finished["operation_id"]
                            and "free space" in str(event["data"])
                            for event in events(runner, "error.recorded")
                        )
                    )
                    if services:
                        self.assertEqual(await work.value(42), 42)
                    if continuation == "step":
                        await asyncio.wait_for(runner.step(), 30)
                        self.assertEqual(
                            [
                                row["label"]
                                for row in trace(runner)
                                if row["event"] == "start"
                            ],
                            ["A", "B"],
                        )
                        result = await runner.reload_template(path)
                        self.assertTrue(result["changed"])
                        self.assertEqual(runner._state.template, document)
                        self.assertEqual(len(work.manifests()), 1)
                    else:
                        await runner.resume()
                        await asyncio.wait_for(runner._task, 60)
                        self.assertEqual(runner.get_state()["phase"], "completed")
                        self.assertEqual(
                            [
                                row["label"]
                                for row in trace(runner)
                                if row["event"] == "start"
                            ],
                            ["A", "B", "C", "D"],
                        )
                    await work.close()

    async def test_copy_failure_and_cancel_reap_worker_without_publication_rt08_rt17(
        self,
    ):
        work = await self.make_run()
        runner = work.runner
        old = candidate(work)
        document = candidate(work)
        document["stages"].append(copy.deepcopy(work.extra_stage))
        path = work.files.write_template(document)
        with (
            patch(
                "core.experimentassembler.shutil.copytree",
                side_effect=OSError("copy refused"),
            ),
            self.assertRaisesRegex(OSError, "copy refused"),
        ):
            await runner.reload_template(path)
        self.assertEqual(runner._state.template, old)
        self.assertEqual(work.manifests(), [])
        entered, release, finished = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
        copytree = shutil.copytree

        def copying(source, target, *args, **kwargs):
            entered.set()
            if not release.wait(20):
                raise TimeoutError("copy gate")
            try:
                return copytree(source, target, *args, **kwargs)
            finally:
                finished.set()

        with patch("core.experimentassembler.shutil.copytree", copying):
            task = asyncio.create_task(runner.reload_template(path))
            try:
                await wait_for(entered.is_set)
                task.cancel()
                await asyncio.sleep(0)
                self.assertFalse(task.done())
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertTrue(finished.is_set())
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(runner._state.template, old)
        self.assertFalse(
            list((runner._state.experiment_directory / "runner/rebuilds").iterdir())
        )

    async def test_partial_publication_rolls_back_with_original_event_ids_rt15_rt16(
        self,
    ):
        work = await self.make_run()
        runner = work.runner
        await runner.step()
        old = candidate(work)
        old_results = dict(runner._state.stage_result_ids)
        old_identity = runner._journal.client.get_journal_info()
        old_checkpoint = runner._journal.client.read_events()["checkpoint"]
        original = runner._assembler.rebuild
        captured = {}

        async def fail_after_publication(*args, **kwargs):
            await original(*args, **kwargs)
            if not kwargs.get("prepare_only"):
                captured.update(
                    {
                        e["event_id"]: e
                        for e in events(runner)
                        if e["event_type"].startswith("reload.")
                    }
                )
                raise OSError("failure after publication")

        document = candidate(work)
        document["stages"][1]["settings"]["echo"] = "candidate"
        with (
            patch.object(runner._assembler, "rebuild", fail_after_publication),
            self.assertRaisesRegex(OSError, "failure after publication"),
        ):
            await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner._state.template, old)
        self.assertEqual(runner._state.stage_result_ids, old_results)
        self.assertEqual(runner._state.mode, "paused")
        restored = {e["event_id"]: e for e in events(runner)}
        self.assertTrue(captured)
        for event_id, event in captured.items():
            self.assertEqual(restored[event_id], event)
        self.assertEqual(len(events(runner, "template.applied")), 1)
        self.assertNotEqual(
            runner._journal.client.get_journal_info()["generation"],
            old_identity["generation"],
        )
        with self.assertRaises(JournalGenerationChanged):
            runner._journal.client.read_events(old_checkpoint)

    async def test_required_audit_failure_prevents_mutation_rt16(self):
        work = await self.make_run()
        runner = work.runner
        old = candidate(work)
        append = SQLiteEventStore.append

        def fail(store, event):
            if event["event_type"] == "reload.candidate":
                raise LoggingStorageError("audit unavailable")
            return append(store, event)

        document = candidate(work)
        document["stages"][0]["settings"]["echo"] = "candidate"
        try:
            with (
                patch.object(SQLiteEventStore, "append", fail),
                self.assertRaises(LoggingStorageError),
            ):
                await runner.reload_template(work.files.write_template(document))
            self.assertEqual(runner._state.template, old)
            self.assertEqual(work.manifests(), [])
        finally:
            runner._journal.close()
            runner._journal.open(runner._state, create=False)

    async def test_explicit_event_limit_rejects_full_candidate_without_truncation_rt16(
        self,
    ):
        work = workspace()
        self.addAsyncCleanup(work.close)
        work.template["logging"]["max_event_bytes"] = 65536
        runner = await work.launch()
        old = candidate(work)
        document = candidate(work)
        document["stages"][0]["settings"]["large"] = "x" * 100000
        with self.assertRaisesRegex(ValueError, "max_event_bytes"):
            await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner._state.template, old)
        self.assertEqual(work.manifests(), [])

    async def test_optional_state_failure_is_recoverable_from_committed_journal_rt16(
        self,
    ):
        work = await self.make_run()
        runner = work.runner
        document = candidate(work)
        document["stages"][0]["settings"]["echo"] = "committed"
        with patch.object(
            RunnerStateStore, "save", side_effect=OSError("optional state refused")
        ):
            result = await runner.reload_template(work.files.write_template(document))
        await runner.close()
        (runner._state.experiment_directory / "runner/state.json").unlink(
            missing_ok=True
        )
        recovered = work.replacement()
        await recovered.recover(result["experiment_id"])
        self.assertEqual(recovered._state.template, document)
        self.assertEqual(
            recovered._state.template_revision_id, result["template_revision_id"]
        )

    async def test_uncertain_commit_uses_committed_revision_instead_of_rolling_back_rt16(
        self,
    ):
        work = await self.make_run()
        runner = work.runner
        old_revision = runner._state.template_revision_id
        append = SQLiteEventStore.append
        fired = False

        def commit_then_fail(store, event):
            nonlocal fired
            result = append(store, event)
            if (
                not fired
                and event["event_type"] == "runner.checkpoint"
                and event["data"]["template_revision_id"] != old_revision
                and event["data"]["pending_rebuild"] is None
            ):
                fired = True
                raise LoggingStorageError("commit acknowledgment lost")
            return result

        document = candidate(work)
        document["stages"][0]["settings"]["echo"] = "committed"
        with (
            patch.object(SQLiteEventStore, "append", commit_then_fail),
            self.assertRaises(LoggingStorageError),
        ):
            await runner.reload_template(work.files.write_template(document))
        self.assertTrue(fired)
        await runner.close()
        recovered = work.replacement()
        await recovered.recover(runner._state.experiment_id)
        self.assertEqual(recovered._state.template, document)
        self.assertIsNone(recovered._state.pending_rebuild)
        self.assertEqual(len(events(recovered, "template.applied")), 2)

    async def test_cancel_after_publication_retains_pending_and_recovery_restores_rt17_rt18(
        self,
    ):
        work = await self.make_run()
        runner = work.runner
        old = candidate(work)
        rebuild = runner._assembler.rebuild

        async def cancel(*args, **kwargs):
            await rebuild(*args, **kwargs)
            if not kwargs.get("prepare_only"):
                raise asyncio.CancelledError

        document = candidate(work)
        document["stages"].append(copy.deepcopy(work.extra_stage))
        with (
            patch.object(runner._assembler, "rebuild", cancel),
            self.assertRaises(asyncio.CancelledError),
        ):
            await runner.reload_template(work.files.write_template(document))
        self.assertIsNotNone(runner._state.pending_rebuild)
        snapshot_id = runner._state.pending_rebuild["snapshot_id"]
        await runner.stop()
        self.assertEqual([m["snapshot_id"] for m in work.manifests()], [snapshot_id])
        await runner.recover(runner._state.experiment_id)
        self.assertEqual(runner._state.template, old)
        self.assertIsNone(runner._state.pending_rebuild)

    async def test_snapshot_failure_never_applies_candidate_rt08(self):
        for services in (False, True):
            with self.subTest(services=services):
                work = workspace(services=services)
                self.addAsyncCleanup(work.close)
                runner = await work.launch()
                await runner.step()
                before = runner.get_state()
                original = candidate(work)
                identities = {
                    sid: instance.service_instance_id
                    for sid, instance in runner._state.services.items()
                }
                document = candidate(work)
                document["stages"][1]["settings"]["echo"] = "new"
                with (
                    patch.object(
                        runner._snapshots,
                        "_build_snapshot",
                        side_effect=OSError("snapshot storage refused"),
                    ),
                    patch.object(
                        runner._services,
                        "unfreeze",
                        wraps=runner._services.unfreeze,
                    ) as unfreeze,
                    patch.object(
                        runner._services,
                        "stop_all",
                        wraps=runner._services.stop_all,
                    ) as stop,
                    self.assertRaisesRegex(OSError, "snapshot storage refused"),
                ):
                    await runner.reload_template(work.files.write_template(document))
                unfreeze.assert_awaited_once()
                stop.assert_not_awaited()
                after = runner.get_state()
                before.pop("observed_at")
                after.pop("observed_at")
                for observation in (before, after):
                    for service in observation["services"]:
                        service.pop("last_status")
                self.assertEqual(after, before)
                self.assertEqual(runner._state.template, original)
                self.assertIsNone(runner._state.pending_rebuild)
                self.assertIsNone(runner._services._snapshot_id)
                self.assertEqual(work.manifests(), [])
                for sid, instance in runner._state.services.items():
                    self.assertEqual(instance.service_instance_id, identities[sid])
                    self.assertTrue(instance.ready)
                    self.assertIsNone(instance.prepared_freeze_id)
                    self.assertIsNone(instance.freeze_id)
                if services:
                    self.assertEqual(await work.value(27), 27)
                await asyncio.wait_for(runner.step(), 30)
                self.assertEqual(
                    [row["label"] for row in trace(runner) if row["event"] == "start"],
                    ["A", "B"],
                )
                await work.close()

    async def test_unconfirmed_unfreeze_stops_experiment_rr14(self):
        work = workspace(services=True)
        self.addAsyncCleanup(work.close)
        runner = await work.launch()
        before = candidate(work)
        controls = work.files.files.controls[work.socket["service_id"]]
        (controls / "fail-unfreeze").touch()
        document = candidate(work)
        document["stages"][0]["settings"]["echo"] = "new"
        with self.assertRaisesRegex(RuntimeError, "write resumption is unconfirmed"):
            await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner.get_state()["phase"], "failed")
        self.assertTrue(runner._task.done())
        self.assertTrue(all(item.stopped for item in runner._state.services.values()))
        self.assertEqual(runner._state.template, before)
        self.assertIsNone(runner._state.pending_rebuild)
        self.assertEqual(work.manifests(), [])

    async def test_unsafe_snapshot_rejection_requires_failure_handling_rr14(self):
        work = workspace(services=True)
        self.addAsyncCleanup(work.close)
        runner = await work.launch()
        instance = runner._state.services[work.socket["service_id"]]
        document = candidate(work)
        document["stages"][0]["settings"]["echo"] = "new"
        path = work.files.write_template(document)
        for target, attribute, value in (
            (instance, "service_instance_id", "unaccounted-instance"),
            (instance, "ready", False),
            (instance, "stopped", True),
            (instance, "stopping", True),
            (instance, "failure", {"message": "unhealthy"}),
            (instance, "prepared_freeze_id", "unconfirmed-freeze"),
            (instance, "freeze_id", "unconfirmed-unfreeze"),
            (runner._services, "_snapshot_id", "active-barrier"),
            (runner._services, "_pending_action", "stop"),
        ):
            with self.subTest(attribute=attribute):
                original = getattr(target, attribute)

                async def reject(
                    *args, target=target, attribute=attribute, value=value, **kwargs
                ):
                    # Change observations after admission, without inventing a
                    # live process with an invalid protocol identity.
                    setattr(target, attribute, value)
                    raise OSError("snapshot rejected with uncertain service state")

                async def observe_failure(
                    *args, target=target, attribute=attribute, original=original
                ):
                    # Restore the injected observation before the live scheduler
                    # can inspect it. Actual shutdown is covered by the unfreeze test.
                    setattr(target, attribute, original)

                try:
                    with (
                        patch.object(runner._snapshots, "create", reject),
                        patch.object(
                            runner,
                            "_fail",
                            new_callable=AsyncMock,
                            side_effect=observe_failure,
                        ) as fail,
                        self.assertRaisesRegex(OSError, "uncertain service state"),
                    ):
                        await runner.reload_template(path)
                    fail.assert_awaited_once()
                    self.assertIsInstance(fail.await_args.args[0], OSError)
                    self.assertIsNone(runner._state.pending_rebuild)
                    self.assertEqual(work.manifests(), [])
                finally:
                    setattr(target, attribute, original)

    async def test_snapshot_logging_failure_remains_fatal_rr14(self):
        work = workspace()
        self.addAsyncCleanup(work.close)
        runner = await work.launch()
        document = candidate(work)
        document["stages"][0]["settings"]["echo"] = "new"
        with (
            patch.object(
                runner._snapshots,
                "create",
                side_effect=LoggingStorageError("snapshot journal unavailable"),
            ),
            self.assertRaisesRegex(LoggingStorageError, "snapshot journal unavailable"),
        ):
            await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner.get_state()["phase"], "failed")
        self.assertTrue(runner._task.done())
        self.assertIsNone(runner._state.pending_rebuild)
        self.assertEqual(work.manifests(), [])

    async def test_failed_diagnostic_export_keeps_original_journal_and_pending_rt15_rt16(
        self,
    ):
        work = await self.make_run()
        runner = work.runner
        rebuild = runner._assembler.rebuild
        journal = runner._journal.client
        identity = journal.get_journal_info()

        async def fail(*args, **kwargs):
            await rebuild(*args, **kwargs)
            if not kwargs.get("prepare_only"):
                raise OSError("apply failed")

        document = candidate(work)
        document["stages"][0]["settings"]["echo"] = "failed-candidate"
        with (
            patch.object(runner._assembler, "rebuild", fail),
            patch.object(
                journal, "export_diagnostics", side_effect=OSError("export refused")
            ),
            self.assertRaisesRegex(OSError, "apply failed") as caught,
        ):
            await runner.reload_template(work.files.write_template(document))
        self.assertTrue(
            any("export refused" in note for note in caught.exception.__notes__)
        )
        self.assertEqual(journal.get_journal_info(), identity)
        self.assertIsNotNone(runner._state.pending_rebuild)
        self.assertEqual(
            events(runner, "reload.candidate")[-1]["data"]["template"], document
        )
        self.assertTrue(
            any(
                e["data"].get("caused_by_error_id")
                for e in events(runner, "error.recorded")
            )
        )
        self.assertIsNotNone(state_to_document(runner._state)["pending_rebuild"])
