"""Approved archive plan B/G/I: real DAG, services, controller queues and CLI."""

import asyncio
import json
import os
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import LoggingStorageError
from core.runner_utils.experimentrunner import ExperimentRunner
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from tests.helpers.archives import ArchiveCli, ArchiveTestCase
from tests.helpers.dag import DagSession, wait_until


class ArchiveDagTests(ArchiveTestCase):
    async def test_runner_guards_active_unconfirmed_and_maintenance_states(self):
        """I/B: archive commands cannot overlap active execution or an unconfirmed shutdown."""
        await self.w.prepare(stopped=False)
        runner = self.w.runner
        with self.assertRaises(RuntimeError):
            await runner.inspect_archive(self.w.archive)
        with self.assertRaises(RuntimeError):
            await runner.create_archive(self.w.archive)
        with self.assertRaises(RuntimeError):
            await runner.install_archive(self.w.archive, self.w.destination)
        await runner.stop()
        runner._termination_confirmed = False
        try:
            with self.assertRaises(RuntimeError):
                await runner.create_archive(self.w.archive)
        finally:
            runner._termination_confirmed = True
        runner._maintenance = True
        try:
            with self.assertRaises(RuntimeError):
                await runner.inspect_archive(self.w.archive)
        finally:
            runner._maintenance = False

    async def test_runner_close_reaps_archive_worker_before_releasing_maintenance(self):
        """I: closing the runner waits for its actual archive operation, including cancellation."""
        await self.source_archive()
        runner = self.w.target_runner()
        entered, release = threading.Event(), threading.Event()
        unpack = runner._archiver._unpack

        def held_unpack(*args):
            entered.set()
            if not release.wait(10):
                raise TimeoutError("runner close test gate expired")
            return unpack(*args)

        with patch.object(runner._archiver, "_unpack", side_effect=held_unpack):
            operation = asyncio.create_task(runner.inspect_archive(self.w.archive))
            await wait_until(entered.is_set)
            closing = asyncio.create_task(runner.close())
            try:
                await asyncio.sleep(0.05)
                self.assertFalse(closing.done())
                self.assertTrue(runner._maintenance)
            finally:
                release.set()
            await asyncio.wait_for(closing, 15)
            self.assertEqual(
                (await operation)["manifest"]["source_experiment_id"], "archive-source"
            )
        self.assertTrue(runner._closed)
        self.assertFalse(runner._maintenance)
        self.assert_work_clean()

    async def test_installed_dag_runs_two_cycles_with_both_services_after_source_is_unavailable(
        self,
    ):
        """C/G: actual stage executors, TCP service and commands lifecycle use only imported inputs."""
        await self.source_archive(services=True, versions=True)
        installed = await self.w.importer.install(self.w.archive, self.w.destination)
        self.w.storage.close()
        self.w.hashes.close_connection()
        self.w.clients.remove((self.w.hashes, self.w.storage))
        self.assertTrue(self.w.source.resolve().is_relative_to(self.w.root))
        self.w.source.rename(self.w.root / "unavailable-source")
        runner = self.w.target_runner()
        await runner.run(
            Path(installed["template_path"]), "imported-dag", delayed_start=True
        )
        await asyncio.wait_for(runner._ready.wait(), 65)
        self.assertEqual(runner.get_state()["phase"], "waiting", runner.get_state())
        self.assertTrue(all(row["ready"] for row in runner.get_state()["services"]))
        socket = next(
            s for s in runner._state.services.values() if s.interface == "socket"
        )
        reply = await runner._services.request(
            runner._state, socket.service_id, "set_value", {"value": 17}
        )
        self.assertEqual(reply["data"]["value"], 17)
        for cycle in (1, 2):
            for label in ("A", "B", "C"):
                result = await asyncio.wait_for(runner.step(), 40)
                data = result["result"]["data"]
                self.assertEqual(
                    (data["cycle"], data["label"], data["text"]),
                    (cycle, label, "portable input\n"),
                )
                self.assertTrue(
                    (runner._state.experiment_directory / data["artifact"]).is_file()
                )
        await wait_until(lambda: runner._task.done())
        self.assertEqual(runner.get_state()["phase"], "completed")
        self.assertTrue(all(s["stopped"] for s in runner.get_state()["services"]))
        commands = next(
            s for s in runner._state.services.values() if s.interface == "commands"
        )
        trace = (
            runner._state.experiment_directory
            / "module_data"
            / commands.service_id
            / "controls/actions.jsonl"
        )
        self.assertEqual(
            [json.loads(line)["action"] for line in trace.read_text().splitlines()],
            ["start", "stop"],
        )
        completed = await runner.create_archive(self.w.root / "completed.tar.xz")
        self.assertEqual(completed["manifest"]["source_experiment_id"], "imported-dag")

    async def test_live_services_and_executor_identity_defeat_stale_stopped_state(self):
        """B: real native participants must exit even when saved flags incorrectly say stopped."""
        await self.w.prepare(services=True, stopped=False)
        state = self.w.state
        original_phase = state.phase
        flags = {key: value.stopped for key, value in state.services.items()}
        try:
            state.phase = "stopped"
            with self.assertRaises(RuntimeError):
                await self.w.create()
            for instance in state.services.values():
                instance.stopped = True
            with self.assertRaises(RuntimeError):
                await self.w.create()
        finally:
            state.phase = original_phase
            for key, value in flags.items():
                state.services[key].stopped = value
        self.assertFalse(self.w.archive.exists())
        await self.w.runner.stop()

    async def test_live_foreign_owner_and_stale_pid_identity_are_distinguished(self):
        """B: actual child liveness uses full identity; PID reuse is not ownership."""
        await self.w.prepare()
        child, path, identity = await self.w.start_child("hold")
        state = self.w.state
        try:
            state.owner_identity = identity
            with self.assertRaises(RuntimeError):
                await self.w.create()
            state.owner_identity = {
                **identity,
                "created_at_os": identity["created_at_os"] + 1,
            }
            await self.w.create()
            self.assertIsNone(child.returncode)
        finally:
            state.owner_identity = None
            path.with_suffix(".release").touch()
            await asyncio.wait_for(child.wait(), 15)

    async def test_runner_loads_released_experiment_without_selecting_it(self):
        """I: archive by ID respects owner/state guards and leaves runner selection unchanged."""
        await self.w.prepare()
        runner = ExperimentRunner(
            self.w.source, self.w.manager, archive_config_path=self.w.config
        )
        self.w.runners.append(runner)
        result = await runner.create_archive(
            self.w.archive, experiment_id="archive-source"
        )
        self.assertEqual(result["manifest"]["source_experiment_id"], "archive-source")
        self.assertIsNone(runner.get_state()["experiment_id"])
        with self.assertRaises(FileNotFoundError):
            await runner.create_archive(
                self.w.root / "missing.tar.xz", experiment_id="missing"
            )
        saved = self.w.state.experiment_directory / "runner/state.json"
        document = read_json(saved)
        write_json(
            saved,
            {**document, "owner_identity": process_identity(os.getpid())},
        )
        with self.assertRaises(RuntimeError):
            await runner.create_archive(
                self.w.root / "owned.tar.xz", experiment_id="archive-source"
            )
        write_json(saved, {})
        with self.assertRaises(ValueError):
            await runner.create_archive(
                self.w.root / "invalid.tar.xz", experiment_id="archive-source"
            )
        write_json(saved, document)


class ArchiveControllerTests(ArchiveTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.session = DagSession(
            SimpleNamespace(root=self.w.target, manager=self.w.target_manager, gates=[])
        )
        self.addAsyncCleanup(self.session.close)
        await self.session.start()

    async def test_commands_and_error_categories_through_real_queues(self):
        """I: real controller dispatch, arguments, installation data and storage-error notes."""
        await self.source_archive()
        reply = await self.session.send(
            "archive.inspect", {"archive_path": str(self.w.archive)}
        )
        self.assertEqual(reply["result"], "success", reply)
        before = self.session.runner.get_state()["phase"]
        for command, args, target, code in (
            ("archive.inspect", {"archive_path": "relative"}, None, "invalid_request"),
            (
                "archive.inspect",
                {"archive_path": str(self.w.root / "missing")},
                None,
                "not_found",
            ),
            (
                "archive.inspect",
                {"archive_path": str(self.w.archive), "extra": True},
                None,
                "invalid_request",
            ),
            (
                "archive.inspect",
                {"archive_path": str(self.w.archive)},
                {"kind": "stage", "position": 1},
                "invalid_request",
            ),
            (
                "archive.create",
                {"archive_path": str(self.w.root / "empty.tar.xz")},
                None,
                "invalid_state",
            ),
        ):
            with self.subTest(command=command, args=args):
                reply = await self.session.send(command, args, target)
                self.assertEqual(reply["error"]["code"], code, reply)
        self.w.filer.fail_post = 503
        args = {
            "archive_path": str(self.w.archive),
            "destination": str(self.w.destination),
        }
        reply = await self.session.send("archive.install", args)
        self.assertEqual(reply["error"]["code"], "storage_error", reply)
        self.assertTrue(reply["error"]["details"]["notes"])
        self.w.filer.fail_post = 0
        reply = await self.session.send("archive.install", args)
        self.assertEqual(reply["result"], "success", reply)
        self.assertTrue(Path(reply["data"]["template_path"]).is_file())
        reply = await self.session.send("archive.install", args)
        self.assertEqual(reply["error"]["code"], "archive_conflict")
        self.assertEqual(self.session.runner.get_state()["phase"], before)

    async def test_reads_and_priority_stop_remain_responsive_during_upload(self):
        """H/I: read requests proceed; standalone stop cancels the tail and waits for the real upload."""
        await self.source_archive()
        self.w.filer.release.clear()
        operation, tail = self.session.chain(
            [
                {
                    "command": "archive.install",
                    "args": {
                        "archive_path": str(self.w.archive),
                        "destination": str(self.w.destination),
                    },
                },
                {
                    "command": "archive.inspect",
                    "args": {"archive_path": str(self.w.archive)},
                },
            ]
        )
        try:
            await wait_until(self.w.filer.entered.is_set)
            reply = await asyncio.wait_for(self.session.send("stats.state"), 2)
            self.assertEqual(
                reply["data"]["current_command"]["command"], "archive.install"
            )
            stopped = self.session.post("stop")
            cancelled = await asyncio.wait_for(asyncio.shield(tail), 2)
            self.assertEqual(cancelled["error"]["code"], "command_cancelled")
            await asyncio.sleep(0.05)
            self.assertFalse(stopped.done())
            self.assertFalse(operation.done())
        finally:
            self.w.filer.release.set()
        self.assertEqual(
            (await asyncio.wait_for(asyncio.shield(operation), 20))["result"], "success"
        )
        self.assertEqual(
            (await asyncio.wait_for(asyncio.shield(stopped), 20))["result"], "success"
        )
        self.assertTrue((self.w.destination / "installation.json").is_file())
        self.assertFalse(self.session.runner._maintenance)

    async def test_controller_close_waits_for_owned_archive_worker(self):
        """I: closing the controller reaps the active operation before returning."""
        await self.source_archive()
        entered, release = threading.Event(), threading.Event()
        archiver = self.session.runner._archiver
        unpack = archiver._unpack

        def held_unpack(*args):
            entered.set()
            if not release.wait(10):
                raise TimeoutError("unpack test gate expired")
            return unpack(*args)

        with patch.object(archiver, "_unpack", side_effect=held_unpack):
            self.session.post("archive.inspect", {"archive_path": str(self.w.archive)})
            await wait_until(entered.is_set)
            closing = asyncio.create_task(self.session.controller.close())
            try:
                await asyncio.sleep(0.05)
                self.assertFalse(closing.done())
            finally:
                release.set()
                await asyncio.wait_for(closing, 15)
        self.assertFalse(self.session.runner._maintenance)
        self.assert_work_clean()

    async def test_archive_logging_failure_does_not_fail_selected_experiment(self):
        """I/J: an independent archive audit failure cannot change DAG outcome."""
        await self.source_archive()
        installed = await self.session.send(
            "archive.install",
            {
                "archive_path": str(self.w.archive),
                "destination": str(self.w.destination),
            },
        )
        self.assertEqual(installed["result"], "success", installed)
        started = await self.session.send(
            "run",
            {
                "template_path": installed["data"]["template_path"],
                "delayed_start": True,
            },
        )
        self.assertEqual(started["result"], "success", started)
        await wait_until(lambda: self.session.runner.get_state()["phase"] == "waiting")
        stopped = await self.session.send("stop")
        self.assertEqual(stopped["result"], "success", stopped)
        original = OperationLogger.record_event

        def fail(logger, kind, data, **kwargs):
            if kind == "archive.started":
                raise LoggingStorageError("archive audit failed")
            return original(logger, kind, data, **kwargs)

        before = self.session.runner.get_state()
        with patch.object(OperationLogger, "record_event", new=fail):
            reply = await self.session.send(
                "archive.inspect", {"archive_path": str(self.w.archive)}
            )
        self.assertEqual(reply["error"]["code"], "journal_unavailable", reply)
        self.assertEqual(self.session.runner.get_state()["phase"], before["phase"])
        self.assertIsNone(self.session.runner.get_state()["error"])


class ArchiveCliTests(ArchiveTestCase):
    async def test_cli_without_template_imports_runs_and_reads_current_experiment_logs(
        self,
    ):
        """I/G: real uv/CLI/queues/SQLite/HTTP round trip from an initially empty selection."""
        await self.source_archive()
        cli = ArchiveCli(self.w)
        self.addAsyncCleanup(cli.close)
        await cli.start()
        reply = await cli.send("state")
        self.assertEqual(reply["data"]["phase"], "idle", reply)
        checked = await cli.send(
            "archive.inspect", {"archive_path": str(self.w.archive)}
        )
        self.assertEqual(checked["result"], "success", checked)
        self.w.destination = self.w.target / "imports/эксперимент"
        installed = await cli.send(
            "archive.install",
            {
                "archive_path": str(self.w.archive),
                "destination": str(self.w.destination),
            },
        )
        self.assertEqual(installed["result"], "success", installed)
        run = await cli.send(
            "run",
            {
                "template_path": installed["data"]["template_path"],
                "delayed_start": True,
            },
        )
        self.assertEqual(run["result"], "success", run)
        async with asyncio.timeout(40):
            while True:
                reply = await cli.send("state")
                self.assertNotEqual(reply["data"]["phase"], "failed", reply)
                if reply["data"]["phase"] == "waiting":
                    break
                await asyncio.sleep(0.05)
        step = await cli.send("step")
        self.assertEqual(step["result"], "success", step)
        logs = await cli.send("logs")
        self.assertEqual(logs["result"], "success", logs)
        self.assertEqual(logs["experiment_id"], run["experiment_id"])
        created = await cli.send(
            "archive.create",
            {"archive_path": str(self.w.root / "повторный архив.tar.xz")},
        )
        self.assertEqual(created["result"], "success", created)
        self.assertTrue(Path(created["data"]["archive_path"]).is_file())
        await cli.close()
        self.assertEqual(cli.process.returncode, 0)

    async def test_cli_uses_explicit_archive_settings(self):
        """I/A: CLI configuration reaches the archiver rather than using silent defaults."""
        await self.source_archive()
        settings = read_json(self.w.config)
        tiny = self.w.root / "tiny-settings.json"
        write_json(tiny, {**settings, "max_archive_bytes": 1})
        cli = ArchiveCli(self.w)
        self.addAsyncCleanup(cli.close)
        await cli.start(config=tiny)
        reply = await cli.send("archive.inspect", {"archive_path": str(self.w.archive)})
        self.assertEqual(reply["error"]["code"], "storage_capacity", reply)
        self.assertEqual((await cli.send("state"))["data"]["phase"], "idle")
