"""Approved snapshots.md B/D/E/G: failure never publishes a false ready archive."""

import asyncio
import time
import unittest
from unittest.mock import AsyncMock, patch

from core.experiments.state import _process_identity_pid
from core.journal.events import LoggingStorageError
from core.journal.logger import OperationLogger
from core.primitives.json_files import read_json, write_json
from tests.helpers.dag import process_running, terminate_owned
from tests.helpers.services import wait_for
from tests.helpers.snapshots import SnapshotWorkspace


class SnapshotFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_rollback_failure_after_completed_failure_runs_new_cleanup(self):
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        snapshot = await runner.snapshot()
        await runner._fail(RuntimeError("original experiment failure"), {})
        await asyncio.wait_for(asyncio.gather(runner._task, return_exceptions=True), 5)
        previous_failure = runner._failure_task
        self.assertTrue(previous_failure.done())
        self.assertEqual(runner.get_state()["phase"], "failed")

        with (
            patch.object(
                runner._snapshots,
                "restore",
                side_effect=OSError("restore storage failed"),
            ),
            patch.object(
                runner,
                "_stop_failed_participants",
                wraps=runner._stop_failed_participants,
            ) as stop_participants,
        ):
            with self.assertRaisesRegex(OSError, "restore storage failed"):
                await runner.rollback(snapshot["snapshot_id"])
            stop_participants.assert_awaited_once_with(runner._state)
        self.assertIsNot(runner._failure_task, previous_failure)
        self.assertEqual(runner.get_state()["phase"], "failed")
        self.assertEqual(
            runner.get_state()["error"]["message"], "restore storage failed"
        )
        self.assertTrue(runner.get_state()["termination_confirmed"])
        await w.close()

    async def test_close_preserves_dag_finalizers_after_shared_failure(self):
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        runner = w.runner
        finalizer_entered = asyncio.Event()
        release_finalizer = asyncio.Event()
        finalizer_finished = asyncio.Event()
        terminal_joining_dag = asyncio.Event()
        watch = runner._watch_journal
        gather = asyncio.gather
        closing = None

        async def held_watch():
            try:
                await watch()
                # A stop request can finish the real watcher before the DAG
                # cancels its helpers. Hold only the ensuing cancellation cleanup.
                await asyncio.Event().wait()
            finally:
                finalizer_entered.set()
                await release_finalizer.wait()
                finalizer_finished.set()

        def observe_join(*tasks, **kwargs):
            if asyncio.current_task() is closing and dag in tasks:
                terminal_joining_dag.set()
            return gather(*tasks, **kwargs)

        with patch.object(runner, "_watch_journal", new=held_watch):
            await w.launch()
            dag = runner._task
            supervision = asyncio.get_running_loop().create_future()
            runner._service_task = supervision
            supervision.set_exception(RuntimeError("primary supervision failure"))
            runner._wake.set()
            try:
                await asyncio.wait_for(finalizer_entered.wait(), 5)
                self.assertEqual(runner.get_state()["phase"], "failed")
                self.assertFalse(dag.done())
                with patch("core.experiments.runner.asyncio.gather", new=observe_join):
                    closing = asyncio.create_task(runner.close())
                    closing.add_done_callback(lambda _: terminal_joining_dag.set())
                    await asyncio.wait_for(terminal_joining_dag.wait(), 5)
                    self.assertFalse(dag.done())
                    self.assertFalse(closing.done())
                    self.assertFalse(finalizer_finished.is_set())
                    release_finalizer.set()
                    await asyncio.wait_for(asyncio.shield(closing), 10)
                self.assertTrue(dag.done())
                self.assertTrue(finalizer_finished.is_set())
            finally:
                release_finalizer.set()
                await gather(
                    *(task for task in (closing, dag) if task is not None),
                    return_exceptions=True,
                )
        await w.close()

    async def test_owned_restoration_can_join_failure_without_a_wait_cycle(self):
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        runner._task.cancel()
        await asyncio.gather(runner._task, return_exceptions=True)
        runner._task = None
        failure_started = asyncio.Event()
        release_failure = asyncio.Event()
        restoration_started = asyncio.Event()
        complete_failure = runner._complete_failure

        async def held_failure(error, context):
            failure_started.set()
            await release_failure.wait()
            await complete_failure(error, context)

        async def activate_restoration():
            runner._maintenance_task = asyncio.current_task()
            restoration_started.set()
            try:
                await runner._activate_restored_state(runner._state)
            finally:
                runner._maintenance_task = None

        with patch.object(runner, "_complete_failure", new=held_failure):
            primary = asyncio.create_task(
                runner._fail(RuntimeError("primary supervision failure"), {})
            )
            runner._task = primary
            activation = None
            try:
                await asyncio.wait_for(failure_started.wait(), 5)
                activation = asyncio.create_task(activate_restoration())
                await asyncio.wait_for(restoration_started.wait(), 5)
                release_failure.set()
                done, _ = await asyncio.wait((primary, activation), timeout=5)
                self.assertEqual(done, {primary, activation})
                outcomes = await asyncio.gather(
                    primary, activation, return_exceptions=True
                )
                self.assertEqual(outcomes, [None, None])
                await asyncio.wait_for(runner._ready.wait(), 5)
                self.assertEqual(runner.get_state()["phase"], "waiting")
                self.assertIsNone(runner.get_state()["error"])
            finally:
                release_failure.set()
                if activation is not None and not activation.done():
                    # Rescue only a failed baseline after its assertion: this
                    # operation is already waiting for the shared cleanup.
                    runner._failure_waiters.add(activation)
                    runner._failure_waiters_changed.set()
                await asyncio.wait_for(
                    asyncio.gather(
                        *(task for task in (primary, activation) if task is not None),
                        return_exceptions=True,
                    ),
                    10,
                )
        await w.close()

    async def test_concurrent_failure_waits_for_cleanup_and_preserves_first_error(self):
        for control_slot, terminal_operation, first in (
            ("_maintenance_task", "stop", "failure"),
            ("_service_control_task", "close", "failure"),
            ("_maintenance_task", "stop", "terminal"),
            ("_service_control_task", "close", "terminal"),
        ):
            with self.subTest(
                control=control_slot, terminal=terminal_operation, first=first
            ):
                await self._check_failure_cleanup_handoff(
                    control_slot, terminal_operation, first
                )

    async def _check_failure_cleanup_handoff(
        self, control_slot, terminal_operation, first
    ):
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        runner._task.cancel()
        await asyncio.gather(runner._task, return_exceptions=True)
        runner._task = None
        control_started = asyncio.Event()
        cancellation_started = asyncio.Event()
        allow_secondary_failure = asyncio.Event()
        cleanup_started = asyncio.Event()
        allow_cleanup = asyncio.Event()
        terminal_started = asyncio.Event()
        cleanup_calls = []
        stop_participants = runner._stop_failed_participants

        async def controlled_operation():
            setattr(runner, control_slot, asyncio.current_task())
            control_started.set()
            try:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancellation_started.set()
                    await allow_secondary_failure.wait()
                    raise RuntimeError(
                        "secondary cancellation cleanup failure"
                    ) from None
            except RuntimeError as error:
                await runner._fail(error, {})
                raise
            finally:
                setattr(runner, control_slot, None)

        async def held_cleanup(state):
            cleanup_calls.append(state.experiment_id)
            cleanup_started.set()
            await allow_cleanup.wait()
            await stop_participants(state)

        async def finish_runner():
            terminal_started.set()
            return await getattr(runner, terminal_operation)()

        control = asyncio.create_task(controlled_operation())
        primary = terminal = None
        with patch.object(runner, "_stop_failed_participants", new=held_cleanup):
            try:
                await asyncio.wait_for(control_started.wait(), 5)
                if first == "failure":
                    primary = asyncio.create_task(
                        runner._fail(RuntimeError("primary supervision failure"), {})
                    )
                    runner._task = primary
                    expected_error = "primary supervision failure"
                else:
                    terminal = asyncio.create_task(finish_runner())
                    expected_error = "secondary cancellation cleanup failure"
                await asyncio.wait_for(cancellation_started.wait(), 5)
                allow_secondary_failure.set()
                await asyncio.wait_for(cleanup_started.wait(), 5)
                if terminal is None:
                    terminal = asyncio.create_task(finish_runner())
                await asyncio.wait_for(terminal_started.wait(), 5)
                self.assertFalse(control.done())
                if primary is not None:
                    self.assertFalse(primary.done())
                self.assertFalse(terminal.done())
                self.assertEqual(runner.get_state()["error"]["message"], expected_error)
                allow_cleanup.set()
                if primary is not None:
                    await asyncio.wait_for(asyncio.shield(primary), 10)
                with self.assertRaisesRegex(
                    RuntimeError, "secondary cancellation cleanup"
                ):
                    await asyncio.wait_for(asyncio.shield(control), 10)
                await asyncio.wait_for(asyncio.shield(terminal), 10)
                self.assertEqual(len(cleanup_calls), 1)
                self.assertEqual(runner.get_state()["phase"], "failed")
                self.assertEqual(runner.get_state()["error"]["message"], expected_error)
            finally:
                allow_secondary_failure.set()
                allow_cleanup.set()
                tasks = [
                    task for task in (control, primary, terminal) if task is not None
                ]
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        await w.close()

    async def test_copy_manifest_and_mandatory_journal_failures_never_publish_ready_snapshot(
        self,
    ):
        """B4/D4/E3: errors after export/copy do not produce a selectable archive."""
        for fault in ("copy", "manifest", "journal"):
            with self.subTest(fault=fault):
                w = SnapshotWorkspace()
                self.addAsyncCleanup(w.close)
                runner = await w.launch()
                await runner.step()
                original = runner._snapshots._build_snapshot

                def build(*args, original=original):
                    original(*args)
                    raise OSError("copy failed after creating payload")

                def publish(path, document, runner=runner):
                    if (
                        path.name == "manifest.json"
                        and path.parent.parent.name
                        == runner._state.experiment_directory.name
                    ):
                        raise PermissionError("manifest publication refused")
                    return write_json(path, document)

                if fault == "copy":
                    failing = patch.object(
                        runner._snapshots, "_build_snapshot", side_effect=build
                    )
                elif fault == "manifest":
                    failing = patch(
                        "core.experiments.snapshots.write_json", side_effect=publish
                    )
                else:
                    failing = patch.object(
                        OperationLogger,
                        "export_snapshot",
                        side_effect=LoggingStorageError("journal export refused"),
                    )
                with failing, self.assertRaises((OSError, LoggingStorageError)):
                    await runner.snapshot("cannot publish")
                self.assertEqual(w.manifests(), [])
                self.assertEqual(runner.get_state()["phase"], "failed")
                self.assertTrue(
                    all(item.stopped for item in runner._state.services.values())
                )
                commands = [
                    row["command"]
                    for row in w.files.trace(w.socket)
                    if row["event"] == "work_started"
                ]
                self.assertIn("unfreeze_writes", commands)
                await w.close()

    async def test_unfreeze_rejection_invalidates_copy_and_stops_experiment(self):
        """D4: a copied archive is unusable when write resumption is unconfirmed."""
        w = SnapshotWorkspace()
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        controls = w.files.files.controls[w.socket["service_id"]]
        (controls / "fail-unfreeze").touch()
        with self.assertRaisesRegex(RuntimeError, "resumption"):
            await runner.snapshot()
        self.assertEqual(w.manifests(), [])
        self.assertEqual(runner.get_state()["phase"], "failed")
        self.assertTrue(all(item.stopped for item in runner._state.services.values()))

    async def test_final_export_failure_reports_invalid_stop_and_prevents_completion(
        self,
    ):
        """E1/E2: explicit stop may confirm shutdown; completion requires a valid export."""
        for completion in (False, True):
            with self.subTest(completion=completion):
                w = SnapshotWorkspace(cycles=1)
                self.addAsyncCleanup(w.close)
                runner = await w.launch()
                if completion:
                    await runner.step()
                    await runner.step()
                controls = w.files.files.controls[w.socket["service_id"]]
                (controls / "fail-save").touch()
                if completion:
                    with self.assertRaisesRegex(RuntimeError, "Final snapshot"):
                        await runner.step()
                    self.assertEqual(runner.get_state()["phase"], "failed")
                    outcomes = await asyncio.wait_for(
                        asyncio.gather(runner._task, return_exceptions=True), 10
                    )
                    self.assertEqual(outcomes, [None])
                else:
                    self.assertEqual((await runner.stop())["phase"], "stopped")
                self.assertFalse(runner._last_snapshot["valid"])
                self.assertEqual(w.manifests(), [])
                self.assertTrue(
                    all(item.stopped for item in runner._state.services.values())
                )
                diagnostics = list(
                    (w.root / "controller/snapshot_failures").glob("*.json")
                )
                self.assertEqual(len(diagnostics), 1)
                self.assertFalse(read_json(diagnostics[0])["valid"])
                await w.close()

    async def test_crashed_or_hung_service_before_freeze_ack_never_produces_snapshot(
        self,
    ):
        """D2/D3: actual service crash/hang invalidates the pending freeze instance."""
        for action in ("crash", "hang"):
            with self.subTest(action=action):
                w = SnapshotWorkspace()
                self.addAsyncCleanup(w.close)
                runner = await w.launch()
                instance = runner._state.services[w.socket["service_id"]]
                controls = w.files.files.controls[instance.service_id]
                write_json(
                    controls / "fault.json",
                    {"phase": "before_freeze", "action": action},
                )
                with self.assertRaises((RuntimeError, ConnectionError)):
                    await asyncio.wait_for(runner.snapshot(), 100)
                self.assertEqual(w.manifests(), [])
                self.assertFalse(
                    process_running(_process_identity_pid(instance.process_identity))
                )
                self.assertEqual(runner.get_state()["phase"], "failed")
                await w.close()

    async def test_crashed_and_hung_owner_recover_the_prepared_freeze_without_replay(
        self,
    ):
        """D3/I6: real owner loss between freeze acknowledgement and state publication."""
        for action in ("crash", "hang"):
            with self.subTest(action=action):
                w = SnapshotWorkspace()
                self.addAsyncCleanup(w.close)
                runner = await w.launch()
                experiment_id = runner._state.experiment_id
                instance = runner._state.services[w.socket["service_id"]]
                await runner.close()
                owner = await w.start_owner(
                    "snapshot", "freeze_confirmed", action=action
                )
                await wait_for(lambda w=w: (w.root / "owner-fault.json").exists(), 30)
                fault = read_json(w.root / "owner-fault.json")
                if action == "hang":
                    with self.assertRaisesRegex(RuntimeError, "still alive"):
                        await w.replacement().recover(experiment_id)
                    terminate_owned(fault["process"])
                await asyncio.wait_for(owner.wait(), 30)
                runner = w.replacement()
                await asyncio.wait_for(runner.recover(experiment_id), 90)
                self.assertEqual(
                    runner._state.services[instance.service_id].service_instance_id,
                    instance.service_instance_id,
                )
                self.assertIsNone(
                    runner._state.services[instance.service_id].prepared_freeze_id
                )
                self.assertIsNone(runner._state.services[instance.service_id].freeze_id)
                self.assertEqual(w.manifests(), [])
                self.assertEqual(await w.value(10), 10)
                self.assertTrue((await runner.snapshot("after recovery"))["valid"])
                commands = [
                    row["command"]
                    for row in w.files.trace(w.socket)
                    if row["event"] == "work_started"
                ]
                self.assertEqual(commands.count("freeze_writes"), 2)
                await w.close()

    async def test_reset_refuses_live_participants_and_waits_for_commands_process(self):
        """D5/F2: unconfirmed service shutdown must not replace runtime files."""
        w = SnapshotWorkspace()
        self.addAsyncCleanup(w.close)
        controls = w.files.files.controls[w.commands["service_id"]]
        runner = await w.launch()
        with self.assertRaises(RuntimeError):
            await runner._services.reset(runner._state)
        snapshot = await runner.snapshot()
        sentinel = runner._state.experiment_directory / "shared_data/keep"
        sentinel.write_bytes(b"original")
        (controls / "hold-stop").touch()
        process = runner._services._processes[w.commands["service_id"]]
        started = time.monotonic()
        try:
            with (
                patch.object(
                    process, "kill", side_effect=PermissionError("owned stop denied")
                ),
                self.assertRaises((RuntimeError, asyncio.CancelledError)),
            ):
                await asyncio.wait_for(runner.rollback(snapshot["snapshot_id"]), 90)
            self.assertGreaterEqual(time.monotonic() - started, 30)
            self.assertEqual(sentinel.read_bytes(), b"original")
        finally:
            (controls / "hold-stop").unlink(missing_ok=True)

    async def test_pending_failure_checkpoint_blocks_run_recovery_and_rollback(self):
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        snapshot = await runner.snapshot()
        selected_state = runner._state
        checkpoint_started = asyncio.Event()
        release_checkpoint = asyncio.Event()
        save_state = runner._save_state_async
        operations = []

        async def held_checkpoint():
            if runner._state.phase == "failed":
                checkpoint_started.set()
                await release_checkpoint.wait()
            await save_state()

        with patch.object(runner, "_save_state_async", new=held_checkpoint):
            failure = asyncio.create_task(
                runner._fail(RuntimeError("service supervision failed"), {})
            )
            try:
                await asyncio.wait_for(checkpoint_started.wait(), 10)
                self.assertTrue(runner._termination_confirmed)
                self.assertTrue(runner._task.done())
                self.assertEqual(selected_state.phase, "failed")
                self.assertFalse(failure.done())
                with (
                    self.subTest(termination="unconfirmed"),
                    patch.object(runner, "_termination_confirmed", False),
                    self.assertRaisesRegex(RuntimeError, "unconfirmed"),
                ):
                    await runner.run(w.template_path, delayed_start=True)
                with (
                    patch.object(
                        runner,
                        "_reset_run_components",
                        new_callable=AsyncMock,
                        side_effect=AssertionError(
                            "New run reached resource replacement."
                        ),
                    ) as reset,
                    patch.object(
                        runner,
                        "_load_recovery_state",
                        new_callable=AsyncMock,
                        side_effect=AssertionError("Recovery reached state loading."),
                    ) as load,
                    patch.object(
                        runner._journal,
                        "close",
                        side_effect=AssertionError(
                            "Rollback reached journal replacement."
                        ),
                    ) as close,
                ):
                    for action, start in (
                        (
                            "run",
                            lambda: runner.run(w.template_path, delayed_start=True),
                        ),
                        (
                            "recover",
                            lambda: runner.recover(selected_state.experiment_id),
                        ),
                        (
                            "rollback",
                            lambda: runner.rollback(snapshot["snapshot_id"]),
                        ),
                    ):
                        with self.subTest(action=action):
                            operation = asyncio.create_task(start())
                            operations.append(operation)
                            with self.assertRaisesRegex(
                                RuntimeError, "failure cleanup"
                            ):
                                await asyncio.wait_for(asyncio.shield(operation), 5)
                            self.assertIs(runner._state, selected_state)
                            self.assertEqual(selected_state.phase, "failed")
                    reset.assert_not_called()
                    load.assert_not_called()
                    close.assert_not_called()
            finally:
                release_checkpoint.set()
                await asyncio.gather(failure, *operations, return_exceptions=True)

    async def test_close_cancels_new_maintenance_after_a_completed_failure(self):
        w = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        await runner._fail(RuntimeError("completed failure"), {})
        entered = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()
        closing = None

        async def archive_operation():
            entered.set()
            try:
                await release.wait()
                return {}
            finally:
                finished.set()

        archive = asyncio.create_task(runner._run_archive(archive_operation()))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            closing = asyncio.create_task(runner.close())
            await asyncio.wait_for(asyncio.shield(closing), 5)
            self.assertTrue(archive.cancelled())
            self.assertTrue(finished.is_set())
        finally:
            release.set()
            await asyncio.gather(
                *(task for task in (archive, closing) if task is not None),
                return_exceptions=True,
            )
