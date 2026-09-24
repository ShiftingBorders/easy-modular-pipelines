"""Approved RR-01 through RR-18: runtime lifecycle and retained HTTP ownership."""

import asyncio
import io
import json
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from queue import Queue
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import cli
from core.runner_utils.runtimeio import read_json
from core.serverruntime import ServerError, ServerRuntime
from tests import test_server_results
from tests.helpers.dag import process_running, wait_until
from tests.helpers.http_runtime import ServerTestCase


class RuntimeRestartCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_mode_read_output_errors_and_no_mutations_rr01(self):
        parser = cli.build_parser()
        for mode in ("run", "maintenance"):
            for as_json in (False, True):
                client = SimpleNamespace(
                    request=AsyncMock(return_value={"server_mode": mode})
                )
                output = io.StringIO()
                with self.subTest(mode=mode, as_json=as_json), redirect_stdout(output):
                    self.assertEqual(
                        await cli.execute(
                            parser.parse_args(["server", "mode"]),
                            client,
                            as_json=as_json,
                        ),
                        0,
                    )
                self.assertEqual(json.loads(output.getvalue()), {"server_mode": mode})
                client.request.assert_awaited_once_with("GET", "/health")
        for document in ({}, {"server_mode": None}, {"server_mode": "invalid"}):
            client = SimpleNamespace(request=AsyncMock(return_value=document))
            with (
                self.subTest(document=document),
                self.assertRaises(cli.ClientError) as failure,
            ):
                await cli.execute(
                    parser.parse_args(["server", "mode"]), client, as_json=True
                )
            self.assertEqual(failure.exception.code, "invalid_response")
        for code in ("unauthorized", "http_timeout", "connection_error", "http_error"):
            error = cli.ClientError("unavailable", code=code)
            client = SimpleNamespace(request=AsyncMock(side_effect=error))
            with self.subTest(code=code), self.assertRaises(cli.ClientError) as failure:
                await cli.execute(
                    parser.parse_args(["server", "mode"]), client, as_json=True
                )
            self.assertIs(failure.exception, error)
        for options in (
            ["--wait"],
            ["--no-wait"],
            ["--wait-timeout", "1"],
            ["--command-id", str(uuid4())],
        ):
            client = SimpleNamespace(request=AsyncMock())
            with self.subTest(options=options), self.assertRaises(ValueError):
                await cli.execute(
                    parser.parse_args(["server", "mode", *options]),
                    client,
                    as_json=True,
                )
            client.request.assert_not_awaited()

    async def test_mutation_envelopes_wait_and_invalid_syntax_rr02(self):
        parser = cli.build_parser()
        identifier = str(uuid4())
        for arguments, command, args in (
            (["server", "restart"], "server.restart", {}),
            (["server", "mode", "run"], "server.mode", {"mode": "run"}),
            (["server", "mode", "maintenance"], "server.mode", {"mode": "maintenance"}),
        ):
            for wait in ("--wait", "--no-wait"):
                receipt = {
                    "command_id": identifier,
                    "server_instance_id": str(uuid4()),
                    "state": "pending",
                    "result": None,
                }
                completed = {
                    **receipt,
                    "state": "succeeded",
                    "result": "success",
                    "data": {"changed": True},
                }
                client = SimpleNamespace(
                    request=AsyncMock(return_value=receipt),
                    wait=AsyncMock(return_value=completed),
                    wait_timeout=30,
                )
                options = parser.parse_args(
                    [
                        *arguments,
                        wait,
                        "--wait-timeout",
                        "2",
                        "--command-id",
                        identifier,
                    ]
                )
                with (
                    self.subTest(arguments=arguments, wait=wait),
                    redirect_stdout(io.StringIO()),
                    redirect_stderr(io.StringIO()),
                ):
                    self.assertEqual(
                        await cli.execute(options, client, as_json=True), 0
                    )
                client.request.assert_awaited_once_with(
                    "POST",
                    "/commands",
                    document={
                        "api_version": 1,
                        "command_id": identifier,
                        "command": command,
                        "args": args,
                    },
                )
                if wait == "--wait":
                    client.wait.assert_awaited_once_with(receipt, 2)
                else:
                    client.wait.assert_not_awaited()
        for arguments in (
            ["server"],
            ["server", "mode", "other"],
            ["server", "restart", "run"],
            ["server", "restart", "--wait-timeout", "0"],
        ):
            with (
                self.subTest(arguments=arguments),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                parser.parse_args(arguments)


class RuntimeRestartStateTests(unittest.IsolatedAsyncioTestCase):
    setUp = test_server_results.ResultCacheTests.setUp
    submit = test_server_results.ResultCacheTests.submit
    reply = test_server_results.ResultCacheTests.reply

    async def test_same_mode_keeps_runtime_and_bounded_receipts_rr07_rr17(self):
        runtime = self.runtime
        before = runtime.health()
        with patch.object(runtime, "close", new_callable=AsyncMock) as close:
            for _ in range(6):
                receipt = runtime.submit(
                    {"command": "server.mode", "args": {"mode": "run"}}
                )
                await runtime._restart_task
                result = runtime.result(receipt["command_id"])
                self.assertEqual(result["state"], "succeeded")
                self.assertFalse(result["data"]["changed"])
                self.assertEqual(result["data"]["runtime_id"], before["runtime_id"])
                self.assertLessEqual(
                    len(runtime._records), runtime.settings.max_records
                )
                self.assertLessEqual(
                    runtime._cache_bytes, runtime.settings.max_cache_bytes
                )
            close.assert_not_awaited()
        self.assertEqual(
            runtime.health()["server_instance_id"], before["server_instance_id"]
        )
        self.assertTrue(runtime._requests.empty())

    async def test_admission_validation_in_both_modes_rr03(self):
        for mode in ("run", "maintenance"):
            self.runtime.settings.server_mode = mode
            for document in (
                {"command": "server.restart", "args": {"mode": mode}},
                {"command": "server.mode"},
                {"command": "server.mode", "args": {"mode": "other"}},
                {"command": "server.mode", "args": {"mode": mode, "extra": True}},
                {
                    "command": "server.restart",
                    "target": {"kind": "service", "position": 1},
                },
                {"command": "server.shutdown"},
            ):
                with (
                    self.subTest(mode=mode, document=document),
                    self.assertRaises(ValueError),
                ):
                    self.runtime.submit(document)
            for command in ("server.restart", "server.mode"):
                with (
                    self.subTest(mode=mode, chain=command),
                    self.assertRaises(ValueError),
                ):
                    self.runtime.submit(
                        {"commands": [{"command": command, "args": {"mode": mode}}]},
                        chain=True,
                    )
        self.assertEqual(self.runtime._records, {})
        self.assertTrue(self.runtime._requests.empty())
        self.assertIsNone(self.runtime._restart_task)

    async def test_pending_receipts_readiness_replay_and_busy_admission_rr08_rr10(self):
        runtime = self.runtime
        runtime.settings.max_records = 10
        known = self.submit()
        self.reply(known)
        unknown = self.submit()
        entered, release = asyncio.Event(), asyncio.Event()
        original = ServerRuntime.start

        async def close(*, restarting=False):
            self.assertTrue(restarting)
            runtime._unavailable("old controller closed")
            runtime._state = "closed"
            runtime._process = None

        async def start(owner, *, restart_command=None):
            if restart_command is not None:
                return await original(owner, restart_command=restart_command)
            owner._state = "starting"
            entered.set()
            await release.wait()
            owner._process = SimpleNamespace(is_alive=lambda: True)
            owner._state = "ready"

        with (
            patch.object(runtime, "close", side_effect=close),
            patch.object(ServerRuntime, "start", new=start),
        ):
            document = {"command": "server.restart", "command_id": str(uuid4())}
            receipt = runtime.submit(document)
            task = runtime._restart_task
            try:
                await asyncio.wait_for(entered.wait(), 2)
                self.assertEqual(
                    runtime.result(receipt["command_id"])["state"], "pending"
                )
                self.assertEqual(
                    runtime.submit(document)["command_id"], receipt["command_id"]
                )
                self.assertIs(runtime._restart_task, task)
                self.assertEqual(runtime.health()["state"], "restarting")
                self.assertEqual(runtime.result(known)["state"], "succeeded")
                self.assertEqual(runtime.result(unknown)["state"], "unknown")
                for request, code in (
                    ({"command": "server.restart"}, "restart_pending"),
                    ({"command": "pause"}, "controller_unavailable"),
                    (
                        {**document, "command": "server.mode", "args": {"mode": "run"}},
                        "command_id_conflict",
                    ),
                ):
                    with (
                        self.subTest(request=request),
                        self.assertRaises(ServerError) as failure,
                    ):
                        runtime.submit(request)
                    self.assertEqual(failure.exception.code, code)
                with self.assertRaises(ServerError):
                    await runtime.read("stats.state")
                runtime.list_commands()
            finally:
                release.set()
                await asyncio.wait_for(task, 2)
        self.assertEqual(runtime.result(receipt["command_id"])["state"], "succeeded")
        self.assertEqual(
            runtime.result(receipt["command_id"])["server_instance_id"],
            receipt["server_instance_id"],
        )

    async def test_lifecycle_priority_slot_queue_bypass_and_record_limits_rr11(self):
        runtime = self.runtime
        self.submit()
        self.submit()
        runtime._requests = Queue(maxsize=1)
        runtime._requests.put_nowait("saturated")
        receipt = runtime.submit({"command": "server.mode", "args": {"mode": "run"}})
        self.assertEqual(len(runtime._records), 3)
        await runtime._restart_task
        self.assertEqual(runtime.result(receipt["command_id"])["state"], "succeeded")
        self.assertEqual(runtime._requests.get_nowait(), "saturated")
        runtime.settings.max_request_bytes = 1
        with self.assertRaises(ServerError) as failure:
            runtime.submit({"command": "server.restart"})
        self.assertEqual(failure.exception.code, "request_too_large")
        runtime.settings.max_request_bytes = 10000
        runtime.settings.max_records = 2
        with self.assertRaises(ServerError) as failure:
            runtime.submit({"command": "server.restart"})
        self.assertEqual(failure.exception.code, "queue_full")
        self.assertIsNone(runtime._restart_command)

    async def test_shutdown_failures_and_stuck_reader_block_replacement_rr12_rr13(self):
        for fault in ("nonzero", "timeout", "reader", "join"):
            self.setUp()
            runtime = self.runtime
            runtime._requests = Mock()
            process = Mock(pid=42, exitcode=26 if fault == "nonzero" else 0)
            process.is_alive.return_value = fault == "timeout"
            process.terminate.side_effect = lambda process=process: setattr(
                process.is_alive, "return_value", False
            )
            if fault == "join":
                process.join.side_effect = OSError("join failed")
            runtime._process = process
            if fault == "reader":
                runtime._reader_thread = Mock()
                runtime._reader_thread.is_alive.return_value = True
            with (
                self.subTest(fault=fault),
                patch("core.serverruntime.multiprocessing.get_context") as spawn,
            ):
                receipt = runtime.submit({"command": "server.restart"})
                await asyncio.wait_for(runtime._restart_task, 3)
                result = runtime.result(receipt["command_id"])
                self.assertEqual(result["state"], "failed")
                self.assertEqual(result["error"]["code"], "runtime_restart_failed")
                self.assertTrue(runtime.health()["restart_blocked"])
                self.assertIn(
                    {
                        "nonzero": "code 26",
                        "timeout": "timed out",
                        "reader": "IPC reader",
                        "join": "join failed",
                    }[fault],
                    result["error"]["message"],
                )
                with self.assertRaises(ServerError) as failure:
                    runtime.submit({"command": "server.restart"})
                self.assertEqual(failure.exception.code, "restart_blocked")
                spawn.assert_not_called()

    async def test_late_reader_and_watchdog_callbacks_cannot_touch_new_generation_rr14(
        self,
    ):
        runtime = self.runtime
        identifier = self.submit()
        old_id = runtime._runtime_id
        old_process = runtime._process
        watching = asyncio.create_task(runtime._watch_process())
        await asyncio.sleep(0)
        runtime._runtime_id = str(uuid4())
        runtime._process = SimpleNamespace(is_alive=lambda: True)
        runtime._ready = asyncio.get_running_loop().create_future()
        for message in (
            {"_runtime": "ready", "process": {}},
            {"_runtime": "error", "message": "stale"},
            {"_runtime": "stopped"},
            {"command_id": identifier, "state": "succeeded", "result": "success"},
        ):
            runtime._accept_response(message, old_id)
        runtime._unavailable("late reader failure", old_id)
        old_process.is_alive = lambda: False
        old_process.exitcode = 26
        await asyncio.wait_for(watching, 2)
        self.assertEqual(runtime.health()["state"], "ready")
        self.assertIsNone(runtime.health()["error"])
        self.assertFalse(runtime._ready.done())
        self.assertEqual(runtime.result(identifier)["state"], "pending")
        runtime._ready.cancel()

    async def test_response_reader_keeps_its_original_stop_event_rr14(self):
        runtime = self.runtime
        entered, release = threading.Event(), threading.Event()
        old_stop = runtime._reader_stop

        def get(*args, **kwargs):
            entered.set()
            if not release.wait(3):
                raise TimeoutError("Test did not release response reader")
            return {"_runtime": "error", "message": "old generation"}

        responses = Mock(get=Mock(side_effect=get))
        runtime._responses = responses
        reader = asyncio.create_task(
            asyncio.to_thread(runtime._read_responses, asyncio.get_running_loop())
        )
        try:
            await wait_until(entered.is_set)
            runtime._runtime_id = str(uuid4())
            runtime._reader_stop = threading.Event()
            old_stop.set()
        finally:
            release.set()
            await asyncio.wait_for(reader, 3)
        responses.get.assert_called_once()
        self.assertEqual(runtime.health()["state"], "ready")
        self.assertFalse(runtime._reader_stop.is_set())

    async def test_startup_failure_cleans_partial_resources_rr15(self):
        runtime = self.runtime
        runtime._requests = Mock()
        runtime._process = Mock(pid=42, exitcode=0)
        runtime._process.is_alive.return_value = False
        queues = [Mock(), Mock()]
        process = Mock(pid=None)
        process.start.side_effect = OSError("spawn failed")
        context = Mock()
        context.Queue.side_effect = queues
        context.Process.return_value = process
        with patch(
            "core.serverruntime.multiprocessing.get_context", return_value=context
        ):
            receipt = runtime.submit({"command": "server.restart"})
            await asyncio.wait_for(runtime._restart_task, 3)
        self.assertEqual(runtime.result(receipt["command_id"])["state"], "failed")
        self.assertIn(
            "spawn failed", runtime.result(receipt["command_id"])["error"]["message"]
        )
        self.assertEqual(runtime.health()["state"], "unavailable")
        self.assertIsNone(runtime._process)
        self.assertIsNone(runtime._restart_blocked)
        for queue in queues:
            queue.close.assert_called_once()
        self.assertTrue(runtime._ready.cancelled())
        original = ServerRuntime.start

        async def start(owner, *, restart_command=None):
            if restart_command is not None:
                return await original(owner, restart_command=restart_command)
            owner._state = "ready"
            owner._process = SimpleNamespace(is_alive=lambda: True)

        with patch.object(ServerRuntime, "start", new=start):
            retry = runtime.submit({"command": "server.restart"})
            await asyncio.wait_for(runtime._restart_task, 3)
        self.assertEqual(runtime.result(retry["command_id"])["state"], "succeeded")
        self.assertIsNone(runtime.health()["error"])

    async def test_http_shutdown_during_restart_waits_without_starting_replacement_rr16(
        self,
    ):
        runtime = self.runtime
        runtime._requests = Mock()
        runtime._process = Mock(pid=42, exitcode=0)
        runtime._process.is_alive.return_value = False
        entered, release = asyncio.Event(), asyncio.Event()
        original = runtime.close

        async def close(*, restarting=False):
            if restarting:
                entered.set()
                await release.wait()
            await original(restarting=restarting)

        with (
            patch.object(runtime, "close", side_effect=close),
            patch("core.serverruntime.multiprocessing.get_context") as spawn,
        ):
            receipt = runtime.submit({"command": "server.restart"})
            await asyncio.wait_for(entered.wait(), 2)
            closing = asyncio.create_task(runtime.close())
            try:
                await wait_until(lambda: runtime._http_closing)
                self.assertFalse(closing.done())
            finally:
                release.set()
                await asyncio.wait_for(closing, 3)
            self.assertEqual(
                runtime.result(receipt["command_id"])["state"], "cancelled"
            )
            self.assertIsNone(runtime._process)
            spawn.assert_not_called()

    async def test_http_shutdown_closes_replacement_already_starting_rr16(self):
        runtime = self.runtime
        runtime._requests = Mock()
        runtime._process = Mock(pid=42, exitcode=0)
        runtime._process.is_alive.return_value = False
        process = Mock(pid=43, exitcode=0)
        process.is_alive.return_value = True
        process.join.side_effect = lambda *args: setattr(
            process.is_alive, "return_value", False
        )
        context = Mock()
        context.Queue.side_effect = [Mock(), Mock()]
        context.Process.return_value = process
        reader = Mock()
        reader.is_alive.return_value = False
        real_thread = threading.Thread

        def thread(*args, **kwargs):
            if kwargs.get("name") == "controller-response-reader":
                return reader
            return real_thread(*args, **kwargs)

        with (
            patch(
                "core.serverruntime.multiprocessing.get_context", return_value=context
            ),
            patch("core.serverruntime.threading.Thread", side_effect=thread),
        ):
            receipt = runtime.submit({"command": "server.restart"})
            await wait_until(lambda: process.start.called)
            self.assertEqual(runtime.result(receipt["command_id"])["state"], "pending")
            closing = asyncio.create_task(runtime.close())
            try:
                await wait_until(lambda: runtime._http_closing)
                self.assertFalse(closing.done())
            finally:
                runtime._accept_response({"_runtime": "ready", "process": {"pid": 43}})
                await asyncio.wait_for(closing, 3)
            self.assertIsNone(runtime._process)
            self.assertFalse(process.is_alive())
            process.close.assert_called_once()
            self.assertTrue(runtime._restart_task.done())

    async def test_completed_lifecycle_receipts_obey_response_size_and_ttl_rr11(self):
        runtime = self.runtime
        runtime.settings.max_response_bytes = 10
        receipt = runtime.submit({"command": "server.mode", "args": {"mode": "run"}})
        await runtime._restart_task
        result = runtime.result(receipt["command_id"])
        self.assertEqual(result["state"], "unavailable")
        self.assertEqual(result["error"]["code"], "response_too_large")
        self.assertEqual(result["error"]["details"]["command_state"], "succeeded")
        runtime.settings.result_ttl = 0
        with self.assertRaises(ServerError) as failure:
            runtime.result(receipt["command_id"])
        self.assertEqual(failure.exception.code, "unknown_command")
        self.assertEqual(runtime._cache_bytes, 0)


class RuntimeRestartProcessTests(ServerTestCase):
    async def test_real_shutdown_timeout_blocks_replacement_rr12(self):
        server = await self.start_server(
            settings={"shutdown_timeout_seconds": 0.5}, fault_mode="controlled"
        )
        before = await server.get("/health")
        (server.control / "hold-shutdown").touch()
        result = await server.command("server.restart", timeout=15)
        self.assertEqual(result["state"], "failed", result)
        self.assertEqual(result["error"]["code"], "runtime_restart_failed")
        self.assertIn("timed out", result["error"]["message"])
        health = await server.client.get("/health")
        self.assertEqual(health.status_code, 503)
        self.assertEqual(health.json()["runtime_id"], before["runtime_id"])
        self.assertEqual(
            health.json()["server_instance_id"], before["server_instance_id"]
        )
        self.assertFalse(health.json()["controller_alive"])
        self.assertTrue(health.json()["restart_blocked"])
        self.assertFalse(process_running(before["controller"]["pid"]))
        refused = await server.client.post(
            "/commands", json={"command": "server.restart"}
        )
        self.assertEqual(refused.status_code, 409)
        self.assertEqual(refused.json()["error"]["code"], "restart_blocked")
        self.assertEqual(len(list((server.project / "controller/server").iterdir())), 1)

    async def test_repeated_restart_and_mode_switch_keep_http_receipts_and_config_rr04_rr06_rr17(
        self,
    ):
        server = await self.start_server(
            settings={"token_env": "EMP_RESTART_TEST_TOKEN"},
            env={"EMP_RESTART_TEST_TOKEN": "test-token"},
        )
        config = server.config.read_bytes()
        owner = read_json(server.control / "owner.json")["process"]
        first = await server.get("/health")
        retained = await server.command("pause")
        current = first
        for command, args, mode in (
            ("server.restart", {}, "run"),
            ("server.mode", {"mode": "maintenance"}, "maintenance"),
            ("server.restart", {}, "maintenance"),
            ("server.mode", {"mode": "run"}, "run"),
        ):
            with self.subTest(command=command, mode=mode):
                result = await server.command(command, args)
                self.assertEqual(result["state"], "succeeded", result)
                health = await server.get("/health")
                server.observe_children()
                self.assertEqual(
                    health["server_instance_id"], first["server_instance_id"]
                )
                self.assertEqual(health["server_mode"], mode)
                self.assertNotEqual(health["runtime_id"], current["runtime_id"])
                self.assertFalse(process_running(current["controller"]["pid"]))
                self.assertEqual(
                    result["data"]["previous_runtime_id"], current["runtime_id"]
                )
                self.assertEqual(result["data"]["runtime_id"], health["runtime_id"])
                self.assertTrue(
                    (
                        server.project
                        / "controller/server"
                        / health["runtime_id"]
                        / "events.sqlite"
                    ).is_file()
                )
                unauthorized = await server.client.post(
                    "/commands",
                    headers={"Authorization": "Bearer wrong"},
                    json={"command": "server.restart"},
                )
                self.assertEqual(unauthorized.status_code, 401)
                for body in (
                    {"command": "server.restart", "args": {"extra": True}},
                    {"command": "server.mode", "args": {"mode": "invalid"}},
                    {
                        "command": "server.restart",
                        "target": {"kind": "stage", "position": 1},
                    },
                ):
                    rejected = await server.client.post("/commands", json=body)
                    self.assertEqual(rejected.status_code, 400, rejected.text)
                rejected = await server.client.post(
                    "/chains", json={"commands": [{"command": "server.restart"}]}
                )
                self.assertEqual(rejected.status_code, 400)
                self.assertEqual(
                    (await server.get("/health"))["runtime_id"], health["runtime_id"]
                )
                if mode == "maintenance":
                    self.assertEqual(
                        (await server.command("run", {}))["error"]["code"],
                        "invalid_mode",
                    )
                    self.assertEqual(
                        (
                            await server.command(
                                "module.validate", {"name": "absent", "version": "1"}
                            )
                        )["error"]["code"],
                        "not_found",
                    )
                else:
                    refused = await server.client.post(
                        "/commands",
                        json={
                            "command": "module.remove",
                            "args": {"name": "absent", "version": "1"},
                        },
                    )
                    self.assertEqual(refused.status_code, 409)
                current = health
        same = await server.command("server.mode", {"mode": "run"})
        self.assertFalse(same["data"]["changed"])
        self.assertEqual(
            (await server.get("/health"))["runtime_id"], current["runtime_id"]
        )
        self.assertEqual(
            await server.get("/commands/" + retained["command_id"]), retained
        )
        self.assertEqual(read_json(server.control / "owner.json")["process"], owner)
        self.assertTrue(process_running(owner["pid"]))
        self.assertEqual(server.config.read_bytes(), config)

    async def test_restart_stops_active_stage_and_services_without_resuming_rr05(self):
        server = await self.start_server()
        gate = self.w.root / "stage.release"
        self.w.gates.append(gate)
        state = await server.launch(self.w.template(gate=gate, services=True))
        service_pids = [item["process"]["pid"] for item in state["services"]]
        await server.submit("step")
        await server.wait_state("stage_running")
        ready_paths = await wait_until(
            lambda: list(
                server.project.glob(
                    "experiments/*/shared_artifacts/epoch_*/**/ready.json"
                )
            )
        )
        process_path = ready_paths[0].with_name("process.json")
        await wait_until(process_path.is_file)
        owners = read_json(process_path)
        stage_pids = [owners[key]["pid"] for key in ("stage", "executor")]
        server.observe_children()
        result = await server.command("server.restart", timeout=60)
        self.assertEqual(result["state"], "succeeded", result)
        state = await server.get("/state")
        self.assertIsNone(state["experiment_id"])
        self.assertEqual(state["phase"], "idle")
        for pid in service_pids + stage_pids:
            self.assertFalse(process_running(pid))
        self.assertFalse(ready_paths[0].with_name("output.json").exists())

    async def test_slow_restart_wait_timeout_and_disconnected_client_leave_operation_running_rr08_rr09(
        self,
    ):
        server = await self.start_server(fault_mode="controlled")
        (server.control / "hold-shutdown").touch()
        identifier = str(uuid4())
        client = cli.APIClient(cli.load_settings(None, {"server_url": server.url}))
        try:
            async with client:
                receipt = await client.request(
                    "POST",
                    "/commands",
                    document={"command": "server.restart", "command_id": identifier},
                )
                await wait_until((server.control / "shutdown.entered").exists)
                health = await server.client.get("/health")
                self.assertEqual(health.status_code, 503)
                self.assertEqual(health.json()["state"], "restarting")
                with self.assertRaises(cli.ClientError) as failure:
                    await client.wait(receipt, 0.05)
                self.assertEqual(failure.exception.code, "wait_timeout")
            self.assertEqual(
                (await server.get("/commands/" + identifier))["state"], "pending"
            )
            self.assertEqual((await server.client.get("/state")).status_code, 503)
            other = await server.client.post(
                "/commands", json={"command": "server.restart"}
            )
            self.assertEqual(other.status_code, 409)
            repeated = await server.client.post(
                "/commands",
                json={"command": "server.restart", "command_id": identifier},
            )
            self.assertEqual(repeated.status_code, 202)
            self.assertEqual(repeated.json()["command_id"], identifier)
        finally:
            (server.control / "shutdown.release").touch()
        self.assertEqual((await server.result(identifier))["state"], "succeeded")
        server.observe_children()
