"""Approved server_shutdown plan: graceful owner exit and delivered outcomes."""

import asyncio
import io
import json
import os
import socket
import subprocess
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import httpx

import cli
import webserver
from core.runner_utils.runtimeio import process_identity, read_json
from core.serverruntime import ProjectLock, ServerError
from tests import test_cli_discovery, test_server_results
from tests.helpers.dag import REPOSITORY, process_running, terminate_owned, wait_until
from tests.helpers.http_runtime import HTTPServer, ServerTestCase


class ShutdownClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_and_no_wait_submit_correct_command_and_deadline(self):
        parser = cli.build_parser()
        identifier = str(uuid4())
        for option, waits in (([], True), (["--wait"], True), (["--no-wait"], False)):
            receipt = {
                "command_id": identifier,
                "server_instance_id": str(uuid4()),
                "state": "succeeded" if waits else "pending",
                "result": "success" if waits else None,
            }
            client = Mock()
            client.wait_timeout = 30
            client.request = AsyncMock(return_value=receipt)
            client.wait = AsyncMock(return_value=receipt)
            options = parser.parse_args(
                [
                    "server",
                    "shutdown",
                    *option,
                    "--wait-timeout",
                    "2",
                    "--command-id",
                    identifier,
                ]
            )
            output = io.StringIO()
            with (
                self.subTest(option=option),
                redirect_stdout(output),
                redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(await cli.execute(options, client, as_json=True), 0)
            arguments = {
                "document": {
                    "api_version": 1,
                    "command_id": identifier,
                    "command": "server.shutdown",
                    "args": {},
                }
            }
            if waits:
                arguments.update(params={"wait": "true"}, timeout=2)
            client.request.assert_awaited_once_with("POST", "/commands", **arguments)
            self.assertEqual(json.loads(output.getvalue())["command_id"], identifier)
            if not waits:
                client.wait.assert_not_awaited()

    async def test_wait_timeout_has_exit_code_four_without_cancelling_server_work(self):
        client = Mock(wait_timeout=1)
        client.request = AsyncMock(
            side_effect=cli.ClientError("deadline", code="http_timeout")
        )
        options = cli.build_parser().parse_args(["server", "shutdown", "--wait"])
        with (
            redirect_stderr(io.StringIO()),
            self.assertRaises(cli.ClientError) as failure,
        ):
            await cli.execute(options, client, as_json=True)
        self.assertEqual(failure.exception.code, "wait_timeout")
        self.assertEqual(failure.exception.exit_code, 4)
        self.assertEqual(client.request.await_count, 1)

    async def test_shutdown_request_can_outlast_the_normal_request_timeout(self):
        client = cli.APIClient(cli.load_settings(None, {}))
        client.timeout = 0.01
        observed = []

        async def respond(request):
            observed.append(request.extensions["timeout"]["read"])
            await asyncio.sleep(0.05)
            return httpx.Response(200, json={"done": True})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            client.http = http
            with self.assertRaises(cli.ClientError) as failure:
                await client.request("GET", "/health")
            self.assertEqual(failure.exception.code, "http_timeout")
            self.assertEqual(
                await client.request("POST", "/commands", timeout=1), {"done": True}
            )
        self.assertEqual(observed, [0.01, 1])


class ShutdownRuntimeTests(unittest.IsolatedAsyncioTestCase):
    setUp = test_server_results.ResultCacheTests.setUp
    reply = test_server_results.ResultCacheTests.reply

    async def test_missing_owner_hook_rejects_without_effects(self):
        with self.assertRaises(ServerError) as failure:
            self.runtime.submit({"command": "server.shutdown"})
        self.assertEqual(failure.exception.status, 501)
        self.assertEqual(failure.exception.code, "unsupported_feature")
        self.assertEqual(self.runtime._records, {})
        self.assertTrue(self.runtime._requests.empty())
        self.assertFalse(self.runtime._closing)

    async def test_pending_shutdown_replays_and_excludes_other_work(self):
        runtime = self.runtime
        runtime.settings.max_records = 10
        runtime._stop_http = Mock()
        old = runtime.submit({"command": "pause"})
        self.reply(old["command_id"])
        entered, release = asyncio.Event(), asyncio.Event()

        async def close(*, restarting=False):
            self.assertTrue(restarting)
            entered.set()
            await release.wait()
            runtime._closing = True
            runtime._unavailable("controller stopped")
            runtime._process = None
            runtime._state = "closed"

        with patch.object(runtime, "close", side_effect=close) as cleanup:
            document = {"command": "server.shutdown", "command_id": str(uuid4())}
            receipt = runtime.submit(document)
            try:
                await asyncio.wait_for(entered.wait(), 2)
                self.assertEqual(runtime.health()["state"], "shutting_down")
                self.assertEqual(runtime.submit(document), receipt)
                self.assertEqual(
                    runtime.result(old["command_id"])["state"], "succeeded"
                )
                for command, code in (
                    ("pause", "controller_unavailable"),
                    ("server.restart", "shutdown_pending"),
                    ("server.shutdown", "shutdown_pending"),
                ):
                    with (
                        self.subTest(command=command),
                        self.assertRaises(ServerError) as failure,
                    ):
                        runtime.submit({"command": command})
                    self.assertEqual(failure.exception.code, code)
                with self.assertRaises(ServerError) as conflict:
                    runtime.submit(
                        {
                            "command": "server.restart",
                            "command_id": document["command_id"],
                        }
                    )
                self.assertEqual(conflict.exception.code, "command_id_conflict")
                with self.assertRaises(ServerError):
                    await runtime.read("stats.state")
            finally:
                release.set()
                await asyncio.wait_for(runtime._restart_task, 3)
            cleanup.assert_awaited_once()
        runtime._stop_http.assert_called_once()
        result = runtime.result(document["command_id"])
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(
            result["data"],
            {
                "runtime_id": runtime._runtime_id,
                "runtime_stopped": True,
                "http_shutdown_requested": True,
            },
        )

    async def test_failed_cleanup_never_requests_http_exit(self):
        for fault in ("nonzero", "timeout"):
            self.setUp()
            runtime = self.runtime
            runtime._stop_http = Mock()
            runtime._requests = Mock()
            process = Mock(pid=42, exitcode=17 if fault == "nonzero" else 0)
            process.is_alive.return_value = fault == "timeout"
            process.terminate.side_effect = lambda process=process: setattr(
                process.is_alive, "return_value", False
            )
            runtime._process = process
            with self.subTest(fault=fault):
                receipt = runtime.submit({"command": "server.shutdown"})
                await asyncio.wait_for(runtime._restart_task, 3)
                result = runtime.result(receipt["command_id"])
                self.assertEqual(result["state"], "failed")
                self.assertEqual(result["error"]["code"], "server_shutdown_failed")
                runtime._stop_http.assert_not_called()
                self.assertEqual(runtime.health()["state"], "unavailable")
                self.assertTrue(runtime.health()["restart_blocked"])

    async def test_owner_hook_exception_is_not_reported_as_success(self):
        runtime = self.runtime
        runtime._requests = Mock()
        runtime._process = Mock(pid=42, exitcode=0)
        runtime._process.is_alive.return_value = False
        runtime._stop_http = Mock(side_effect=RuntimeError("owner refused exit"))
        receipt = runtime.submit({"command": "server.shutdown"})
        await asyncio.wait_for(runtime._restart_task, 3)
        result = runtime.result(receipt["command_id"])
        self.assertEqual(result["state"], "failed")
        self.assertIn("owner refused exit", result["error"]["message"])
        self.assertIsNone(runtime._process)
        self.assertEqual(runtime.health()["state"], "unavailable")


class ShutdownAPITests(unittest.IsolatedAsyncioTestCase):
    setUp = test_cli_discovery.DiscoveryAPITests.setUp
    request = test_cli_discovery.DiscoveryAPITests.request

    async def test_invalid_or_unauthorized_requests_do_not_admit_shutdown(self):
        self.runtime._state = "ready"
        self.runtime._stop_http = Mock()
        for path, body in (
            ("/api/commands?wait=maybe", {"command": "server.shutdown"}),
            ("/api/commands?wait=true&wait=false", {"command": "server.shutdown"}),
            ("/api/commands?extra=true", {"command": "server.shutdown"}),
            ("/api/commands?wait=true", {"command": "server.restart"}),
            ("/api/commands", {"command": "server.shutdown", "args": {"force": True}}),
            (
                "/api/commands",
                {
                    "command": "server.shutdown",
                    "target": {"kind": "service", "position": 1},
                },
            ),
            ("/api/chains", {"commands": [{"command": "server.shutdown"}]}),
        ):
            with self.subTest(path=path, body=body):
                response = await self.request("POST", path, json=body)
                self.assertEqual(response.status_code, 400, response.text)
        unauthorized = await self.request(
            "POST",
            "/api/commands?wait=true",
            headers={},
            json={"command": "server.shutdown"},
        )
        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(self.runtime._records, {})
        self.assertIsNone(self.runtime._restart_task)
        self.runtime._stop_http.assert_not_called()

    async def test_default_entry_point_binds_graceful_owner_hook(self):
        server = Mock(started=True, should_exit=False)
        with (
            patch.object(
                sys,
                "argv",
                [
                    "webserver.py",
                    "--project-root",
                    str(self.files.root),
                    "--filer-url",
                    "http://127.0.0.1:1",
                ],
            ),
            patch.object(webserver.uvicorn, "Server", return_value=server),
            patch.object(webserver.app.state, "settings", None, create=True),
            patch.object(webserver.app.state, "stop_http", None, create=True),
        ):
            webserver.main()
            server.run.assert_called_once()
            self.assertFalse(server.should_exit)
            webserver.app.state.stop_http()
            self.assertTrue(server.should_exit)


class ShutdownProcessTests(ServerTestCase):
    async def test_real_webserver_entry_point_supports_remote_shutdown(self):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        server = HTTPServer(self.w, settings={"port": port})
        self.addAsyncCleanup(server.close)
        server.url = f"http://127.0.0.1:{port}/api"
        server.output = (server.control / "server.log").open("wb")
        server.process = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--project",
            str(REPOSITORY),
            "--no-sync",
            "python",
            "-B",
            str(REPOSITORY / "webserver.py"),
            "--config",
            str(server.config),
            cwd=REPOSITORY,
            env={**os.environ, "PYTHONPATH": str(REPOSITORY)},
            stdout=server.output,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        server.owned[server.process.pid] = process_identity(server.process.pid)
        server.client = httpx.AsyncClient(
            base_url=server.url, timeout=30, trust_env=False
        )
        try:
            async with asyncio.timeout(30):
                while True:
                    if server.process.returncode is not None:
                        self.fail(
                            (server.control / "server.log").read_text(encoding="utf-8")
                        )
                    try:
                        response = await server.client.get("/health")
                    except httpx.HTTPError:
                        await asyncio.sleep(0.025)
                        continue
                    if response.status_code == 200:
                        break
                    await asyncio.sleep(0.025)
            server.observe_children()
            response = await server.client.post(
                "/commands?wait=true", json={"command": "server.shutdown"}
            )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["state"], "succeeded", response.text)
            self.assertEqual(await asyncio.wait_for(server.process.wait(), 15), 0)
        finally:
            if server.process.returncode is None:
                server.observe_children()
                for identity in reversed(list(server.owned.values())):
                    terminate_owned(identity)
                await asyncio.wait_for(server.process.wait(), 15)

    async def test_cli_wait_receives_outcome_before_actual_exit_in_both_modes(self):
        for mode in ("run", "maintenance"):
            with self.subTest(mode=mode):
                server = await self.start_server(settings={"server_mode": mode})
                health = await server.get("/health")
                http_owner = read_json(server.control / "owner.json")["process"]
                client = await self.start_cli(server, ["server", "shutdown", "--wait"])
                code, output, errors = await client.finish(timeout=30)
                self.assertEqual(code, 0, errors)
                result = json.loads(output)
                self.assertEqual(result["state"], "succeeded", result)
                self.assertTrue(result["data"]["runtime_stopped"])
                self.assertTrue(result["data"]["http_shutdown_requested"])
                self.assertEqual(await asyncio.wait_for(server.process.wait(), 15), 0)
                self.assertFalse(process_running(http_owner["pid"]))
                self.assertFalse(process_running(health["controller"]["pid"]))
                with ProjectLock(server.project):
                    pass

    async def test_shutdown_stops_active_stage_executor_and_services(self):
        server = await self.start_server()
        gate = self.w.root / "stage.release"
        self.w.gates.append(gate)
        state = await server.launch(self.w.template(gate=gate, services=True))
        processes = [item["process"]["pid"] for item in state["services"]]
        await server.submit("step")
        await server.wait_state("stage_running")
        paths = await wait_until(
            lambda: list(
                server.project.glob(
                    "experiments/*/shared_artifacts/epoch_*/**/ready.json"
                )
            )
        )
        ownership = paths[0].with_name("process.json")
        await wait_until(ownership.is_file)
        identities = read_json(ownership)
        processes.extend(identities[key]["pid"] for key in ("stage", "executor"))
        server.observe_children()
        response = await server.client.post(
            "/commands?wait=true", json={"command": "server.shutdown"}, timeout=60
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["state"], "succeeded", response.text)
        self.assertEqual(await asyncio.wait_for(server.process.wait(), 15), 0)
        for pid in processes:
            self.assertFalse(process_running(pid))
        self.assertFalse(paths[0].with_name("output.json").exists())

    async def test_no_wait_admission_does_not_depend_on_later_polling(self):
        server = await self.start_server(fault_mode="controlled")
        (server.control / "hold-shutdown").touch()
        try:
            client = await self.start_cli(server, ["server", "shutdown", "--no-wait"])
            code, output, errors = await client.finish()
            self.assertEqual(code, 0, errors)
            receipt = json.loads(output)
            self.assertEqual(receipt["state"], "pending")
            await wait_until((server.control / "shutdown.entered").exists)
            self.assertIsNone(server.process.returncode)
            health = await server.client.get("/health")
            self.assertEqual(health.status_code, 503)
            self.assertEqual(health.json()["state"], "shutting_down")
            self.assertEqual(
                (await server.get("/commands/" + receipt["command_id"]))["state"],
                "pending",
            )
            repeated = await server.client.post(
                "/commands",
                json={
                    "command": "server.shutdown",
                    "command_id": receipt["command_id"],
                },
            )
            self.assertEqual(repeated.status_code, 202)
            self.assertEqual(repeated.json()["command_id"], receipt["command_id"])
        finally:
            (server.control / "shutdown.release").touch()
        self.assertEqual(await asyncio.wait_for(server.process.wait(), 20), 0)

    async def test_wait_timeout_and_client_exit_do_not_cancel_shutdown(self):
        server = await self.start_server(fault_mode="controlled")
        (server.control / "hold-shutdown").touch()
        try:
            client = await self.start_cli(
                server, ["server", "shutdown", "--wait", "--wait-timeout", "0.2"]
            )
            await wait_until((server.control / "shutdown.entered").exists)
            code, _, errors = await client.finish()
            self.assertEqual(code, 4, errors)
            self.assertIsNone(server.process.returncode)
        finally:
            (server.control / "shutdown.release").touch()
        self.assertEqual(await asyncio.wait_for(server.process.wait(), 20), 0)

    async def test_failed_shutdown_leaves_real_http_available_for_diagnostics(self):
        server = await self.start_server(
            settings={"shutdown_timeout_seconds": 0.5}, fault_mode="controlled"
        )
        (server.control / "hold-shutdown").touch()
        response = await server.client.post(
            "/commands?wait=true", json={"command": "server.shutdown"}
        )
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        self.assertEqual(result["state"], "failed")
        self.assertEqual(result["error"]["code"], "server_shutdown_failed")
        self.assertIn("timed out", result["error"]["message"])
        self.assertIsNone(server.process.returncode)
        self.assertEqual((await server.client.get("/health")).status_code, 503)
        self.assertEqual(await server.get("/commands/" + result["command_id"]), result)
