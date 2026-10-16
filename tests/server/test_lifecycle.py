"""Approved B/C/G: real owned processes, two queues and bounded fault injection."""

import asyncio
import multiprocessing
import os
import threading
from pathlib import Path
from unittest.mock import patch

from core.experiments.runner import ExperimentRunner
from core.journal.logger import OperationLogger
from core.journal.supervisor import JournalSupervisor
from core.primitives.json_files import read_json
from core.server.runtime import ServerRuntime
from core.server.settings import load_server_settings
from tests.helpers.dag import process_running, terminate_owned, wait_until
from tests.helpers.http_runtime import HTTPServer, ServerTestCase


class ServerLifecycleTests(ServerTestCase):
    async def test_ready_runner_can_resume_before_next_journal_watch(self):
        writer_release = threading.Event()
        watcher_release = asyncio.Event()
        startup_observed = asyncio.Event()
        watcher_task = None
        watch = ExperimentRunner._watch_journal
        spawn = JournalSupervisor._spawn
        sleep = asyncio.sleep

        async def observe_journal(runner):
            nonlocal watcher_task
            watcher_task = asyncio.current_task()
            await watch(runner)

        async def hold_next_watch(delay, result=None):
            # Only the runner watcher is delayed; journal I/O and preparation
            # continue on their real tasks and threads.
            if (
                asyncio.current_task() is watcher_task
                and self.w.runner._journal_pause_reason == "journal_starting"
            ):
                startup_observed.set()
                writer_release.set()
                await watcher_release.wait()
            return await sleep(delay, result)

        def start_after_observation(supervisor):
            if (
                supervisor.directory.is_relative_to(self.w.source)
                and not writer_release.wait(15)
            ):
                raise TimeoutError("Runner did not observe journal startup.")
            return spawn(supervisor)

        async with asyncio.timeout(45):
            with (
                patch.object(ExperimentRunner, "_watch_journal", new=observe_journal),
                patch.object(JournalSupervisor, "_spawn", new=start_after_observation),
                patch("core.experiments.runner.asyncio.sleep", new=hold_next_watch),
            ):
                try:
                    await self.w.prepare(stopped=False)
                    runner = self.w.runner
                    self.assertTrue(startup_observed.is_set())
                    self.assertEqual(
                        runner._journal._supervisor.get_status()["state"], "ready"
                    )
                    self.assertEqual(runner.get_state()["phase"], "waiting")
                    await runner.resume()
                    self.assertEqual(runner.get_state()["mode"], "running")
                finally:
                    writer_release.set()
                    watcher_release.set()

    async def test_listener_and_token_errors_precede_controller_start(self):
        for settings, env, expected in (
            ({"host": "0.0.0.0"}, {}, "non-loopback"),
            ({"token_env": "EMP_FIXTURE_EMPTY"}, {"EMP_FIXTURE_EMPTY": ""}, "empty"),
            (
                {"token_env": "EMP_FIXTURE_BAD"},
                {"EMP_FIXTURE_BAD": "secret with spaces"},
                "printable ASCII",
            ),
        ):
            with self.subTest(settings=settings):
                server = HTTPServer(
                    self.w, settings=settings, env=env, fault_mode="controlled"
                )
                self.addAsyncCleanup(server.close)
                await server.start(expect_ready=False)
                self.assertNotEqual(
                    await asyncio.wait_for(server.process.wait(), 15), 0
                )
                text = (server.control / "server.log").read_text()
                self.assertIn(expected, text)
                self.assertNotIn("secret with spaces", text)
                self.assertFalse((server.control / "controller-owner.json").exists())

    async def test_spawn_uses_exactly_two_queues_and_releases_project_lock(self):
        owner = HTTPServer(self.w)
        runtime = ServerRuntime(load_server_settings(owner.config))
        self.addAsyncCleanup(runtime.close)
        context = multiprocessing.get_context("spawn")
        with patch.object(context, "Queue", wraps=context.Queue) as queues:
            await runtime.start()
        self.assertEqual(queues.call_count, 2)
        identity = runtime.health()["controller"]
        self.assertNotEqual(identity["pid"], multiprocessing.current_process().pid)
        self.assertTrue(process_running(identity["pid"]))
        self.assertEqual((await runtime.read("stats.state"))["data"]["phase"], "idle")
        competing = HTTPServer(self.w)
        self.addAsyncCleanup(competing.close)
        await competing.start(expect_ready=False)
        self.assertNotEqual(await asyncio.wait_for(competing.process.wait(), 15), 0)
        expected_error = "PermissionError" if os.name == "nt" else "BlockingIOError"
        self.assertIn(expected_error, (competing.control / "server.log").read_text())
        await runtime.close()
        self.assertFalse(process_running(identity["pid"]))
        fresh = await self.start_server()
        self.assertTrue((await fresh.get("/health"))["controller_alive"])

    async def test_startup_error_and_deadline_reap_the_owned_controller(self):
        for mode, options, expected in (
            (
                "controlled",
                {"hash_config_path": str(self.w.root / "missing.json")},
                "FileNotFoundError",
            ),
            (
                "startup_hang",
                {"startup_timeout_seconds": 2, "shutdown_timeout_seconds": 0.2},
                "TimeoutError",
            ),
        ):
            with self.subTest(mode=mode):
                server = HTTPServer(self.w, settings=options, fault_mode=mode)
                self.addAsyncCleanup(server.close)
                await server.start(expect_ready=False)
                self.assertNotEqual(
                    await asyncio.wait_for(server.process.wait(), 15), 0
                )
                identity = read_json(server.control / "controller-owner.json")[
                    "process"
                ]
                self.assertFalse(process_running(identity["pid"]))
                self.assertIn(expected, (server.control / "server.log").read_text())

    async def test_controller_death_marks_pending_unknown_without_restart(self):
        server = await self.start_server(fault_mode="controlled")
        receipt = await server.submit("fixture.wait")
        await wait_until((server.control / "command.entered").exists)
        identity = (await server.get("/health"))["controller"]
        terminate_owned(identity)
        result = await server.result(receipt["command_id"])
        self.assertEqual(result["state"], "unknown", result)
        self.assertIsNone(result["result"])
        response = await server.client.get("/health")
        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()["controller_alive"])
        self.assertEqual(response.json()["controller"], identity)
        self.assertEqual(
            (
                await server.client.post("/commands", json={"command": "stop"})
            ).status_code,
            503,
        )

    async def test_parent_death_stops_controller_and_releases_lock(self):
        server = await self.start_server()
        await server.launch(self.w.template(services=True))
        ready = read_json(server.control / "ready.json")
        server.observe_children()
        terminate_owned(ready["process"])
        await asyncio.wait_for(server.process.wait(), 15)
        await wait_until(
            lambda: not process_running(ready["controller"]["pid"]), timeout=20
        )
        # The resource tracker may finish just after the controller exits.
        await wait_until(
            lambda: all(
                not process_running(identity["pid"])
                for identity in server.owned.values()
            ),
            timeout=15,
        )
        for identity in server.owned.values():
            self.assertFalse(process_running(identity["pid"]), identity)
        fresh = await self.start_server()
        self.assertEqual((await fresh.get("/state"))["recovery_required"], [])

    async def test_corrupted_frames_disable_runtime_in_each_direction(self):
        for direction in ("request", "response"):
            with self.subTest(direction=direction):
                server = await self.start_server(fault_mode="controlled")
                receipt = await server.submit("fixture.wait")
                await wait_until((server.control / "command.entered").exists)
                (server.control / f"truncate-{direction}").touch()
                result = await server.result(receipt["command_id"], timeout=15)
                self.assertEqual(result["state"], "unknown", result)
                self.assertEqual((await server.client.get("/health")).status_code, 503)
                (server.control / "stop").touch()
                await asyncio.wait_for(server.process.wait(), 15)
                identity = read_json(server.control / "ready.json")["controller"]
                self.assertFalse(process_running(identity["pid"]))

    async def test_blocked_response_reader_does_not_hold_watchdog_or_http_exit(self):
        server = await self.start_server(fault_mode="controlled")
        reader_config = next(
            (server.project / "controller/server").glob("*/reader.json")
        )
        endpoint = Path(read_json(reader_config)["logging"]["writer_endpoint_path"])
        writer_identity = read_json(endpoint)["process"]
        receipt = await server.submit("fixture.wait")
        await wait_until((server.control / "command.entered").exists)
        (server.control / "block-response").touch()
        await wait_until((server.control / "response-read.entered").exists)
        self.assertEqual(
            (await server.result(receipt["command_id"]))["state"], "unknown"
        )
        (server.control / "stop").touch()
        await asyncio.wait_for(server.process.wait(), 12)
        self.assertIn(
            "corrupted IPC reader", (server.control / "server.log").read_text()
        )
        self.assertTrue(process_running(writer_identity["pid"]))
        with OperationLogger(reader_config, read_only=True) as reader:
            events = reader.read_events(limit=100)["events"]
        self.assertIn(
            "server.controller_started",
            [item["event"]["event_type"] for item in events],
        )
        self.assertEqual(read_json(endpoint)["process"], writer_identity)

    async def test_blocked_request_reader_does_not_hold_orphan_shutdown(self):
        server = await self.start_server(fault_mode="controlled")
        identity = (await server.get("/health"))["controller"]
        (server.control / "block-request").touch()
        await wait_until((server.control / "request-read.entered").exists)
        await asyncio.wait_for(server.process.wait(), 12)
        await wait_until(lambda: not process_running(identity["pid"]), timeout=12)

    async def test_shutdown_deadline_reports_unconfirmed_cleanup(self):
        server = await self.start_server(
            settings={"shutdown_timeout_seconds": 0.3}, fault_mode="controlled"
        )
        state = await server.launch(self.w.template())
        server.observe_children()
        identity = (await server.get("/health"))["controller"]
        (server.control / "hold-shutdown").touch()
        (server.control / "stop").touch()
        await wait_until((server.control / "shutdown.entered").exists)
        await asyncio.wait_for(server.process.wait(), 12)
        self.assertFalse(process_running(identity["pid"]))
        self.assertIn(
            "inspect and recover unfinished experiments",
            (server.control / "server.log").read_text(),
        )
        fresh = await self.start_server()
        self.assertEqual(
            (await fresh.get("/state"))["recovery_required"], [state["experiment_id"]]
        )
        recovered = await fresh.command(
            "recover", {"experiment_id": state["experiment_id"]}
        )
        self.assertEqual(recovered["result"], "success", recovered)
        self.assertEqual((await fresh.get("/state"))["recovery_required"], [])

    async def test_completed_is_published_only_after_final_snapshot_is_ready(self):
        server = await self.start_server(fault_mode="controlled")
        await server.launch(self.w.template())
        (server.control / "hold-snapshot").touch()
        receipt = await server.submit("step")
        await wait_until((server.control / "snapshot.entered").exists, timeout=15)
        state = await server.get("/state")
        self.assertEqual(state["phase"], "snapshotting", state)
        self.assertEqual(
            (await server.get("/commands/" + receipt["command_id"]))["state"], "pending"
        )
        (server.control / "snapshot.release").touch()
        result = await server.result(receipt["command_id"])
        self.assertEqual(result["state"], "succeeded", result)
        state = await server.wait_state("completed")
        self.assertTrue(state["snapshot"]["valid"], state)
