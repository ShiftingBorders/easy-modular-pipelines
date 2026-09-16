"""Approved F: argument contracts and real TCP failures without unsafe network faults."""

import asyncio
import json
import unittest
from types import SimpleNamespace
from uuid import uuid4

import cli
from core.runner_utils.runtimeio import write_json
from tests.helpers.dag import wait_until
from tests.helpers.http_runtime import ServerTestCase


class CliArgumentsTests(unittest.TestCase):
    def test_all_mutation_subcommands_preserve_arguments_and_server_paths(self):
        parser = cli.build_parser()
        identifier = str(uuid4())
        path = r"C:\server\данные с пробелом\file.yaml"
        cases = [
            (
                [
                    "run",
                    "--template",
                    path,
                    "--experiment-id",
                    "sample",
                    "--delayed-start",
                ],
                "run",
                {
                    "template_path": path,
                    "experiment_id": "sample",
                    "delayed_start": True,
                },
                None,
            ),
            (
                ["run", "--continue-from", "sample"],
                "run",
                {"experiment_id": "sample", "continue": True, "delayed_start": False},
                None,
            ),
            (
                ["rerun", "stage", "--position", "2"],
                "rerun",
                {"scope": "stage", "position": 2},
                None,
            ),
            (
                ["rerun", "experiment", "--experiment-id", "copy"],
                "rerun",
                {"scope": "experiment", "experiment_id": "copy"},
                None,
            ),
            (["move", "2"], "move", {"position": 2}, None),
            (["retry", "1"], "retry", {}, {"kind": "service", "position": 1}),
            (
                ["reset-retries", "stage", "2"],
                "reset_retries",
                {},
                {"kind": "stage", "position": 2},
            ),
            (["recover", "sample"], "recover", {"experiment_id": "sample"}, None),
            (
                ["snapshot", "--label", "контрольная точка"],
                "snapshot",
                {"label": "контрольная точка"},
                None,
            ),
            (["rollback", identifier], "rollback", {"snapshot_id": identifier}, None),
            (
                ["archive", "create", path, "--experiment-id", "sample"],
                "archive.create",
                {"archive_path": path, "experiment_id": "sample"},
                None,
            ),
            (
                ["archive", "inspect", path],
                "archive.inspect",
                {"archive_path": path},
                None,
            ),
            (
                ["archive", "install", path, "destination"],
                "archive.install",
                {"archive_path": path, "destination": "destination"},
                None,
            ),
            (
                [
                    "command",
                    "retry",
                    "--args",
                    '{"position":2}',
                    "--target",
                    '{"kind":"service","position":2}',
                ],
                "retry",
                {"position": 2},
                {"kind": "service", "position": 2},
            ),
        ]
        cases.extend(
            ([name], name, {}, None) for name in ("pause", "resume", "stop", "step")
        )
        for arguments, name, args, target in cases:
            with self.subTest(arguments=arguments):
                options = parser.parse_args([*arguments, "--command-id", identifier])
                document = cli.command_document(options)
                self.assertEqual(document["command"], name)
                self.assertEqual(document["args"], args)
                self.assertEqual(document.get("target"), target)
                self.assertEqual(document["command_id"], identifier)
                self.assertEqual(document["api_version"], 1)
        for arguments in (
            ["run", "--continue-from", "old", "--experiment-id", "new"],
            ["rerun", "stage"],
            ["rerun", "experiment", "--position", "1"],
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                cli.command_document(parser.parse_args(arguments))


class CliProtocolTests(ServerTestCase):
    async def test_watch_follow_and_ctrl_c_do_not_stop_running_stage(self):
        gate = self.w.root / "stage.release"
        self.w.gates.append(gate)
        server = await self.start_server()
        await server.launch(self.w.template(gate=gate))
        step = await server.submit("step")
        await server.wait_state("stage_running")
        for arguments in (
            ["status", "--watch", "0.1"],
            ["resources", "--watch", "0.1"],
            ["logs", "--follow"],
        ):
            with self.subTest(arguments=arguments):
                client = await self.start_cli(server, arguments)
                for _ in range(2):
                    self.assertIsInstance(
                        await asyncio.wait_for(client.responses.get(), 10), dict
                    )
                client.ownership.with_suffix(".interrupt").touch()
                code, output, errors = await client.finish(timeout=10)
                self.assertEqual(code, 130, errors)
                for line in output.splitlines():
                    self.assertIsInstance(json.loads(line), dict)
                self.assertEqual((await server.get("/state"))["phase"], "stage_running")
        gate.touch()
        self.assertEqual(
            (await server.result(step["command_id"]))["state"], "succeeded"
        )

    async def test_authenticated_cli_chain_files_no_wait_and_later_result(self):
        server = await self.start_server(
            fault_mode="controlled",
            settings={"token_env": "EMP_FIXTURE_TOKEN"},
            env={"EMP_FIXTURE_TOKEN": "fixture-token"},
        )
        env = {"EMP_FIXTURE_TOKEN": "fixture-token"}
        config = self.w.root / "client.json"
        write_json(
            config, {"token_env": "EMP_FIXTURE_TOKEN", "poll_interval_seconds": 0.05}
        )
        args = self.w.root / "аргументы с пробелом.json"
        write_json(args, {"path": r"C:\data\данные.txt"})
        client = await self.start_cli(
            server, ["--config", str(config), "shell"], env=env
        )
        reply = await client.send(f'command fixture.echo --args "@{args}" --wait')
        self.assertEqual(reply["data"]["args"]["path"], r"C:\data\данные.txt")
        chain = self.w.root / "chain.json"
        chain_id = str(uuid4())
        chain.write_text(
            json.dumps([{"command": "fixture.wait"}, {"command": "fixture.echo"}]),
            encoding="utf-8",
        )
        receipt = await client.send(f'chain "{chain}" --chain-id {chain_id} --no-wait')
        self.assertEqual(receipt["chain_id"], chain_id)
        await wait_until((server.control / "command.entered").exists)
        self.assertEqual(receipt["commands"][0]["state"], "pending")
        (server.control / "command.release").touch()
        for ticket in receipt["commands"]:
            reply = await client.send(f"result {ticket['command_id']} --wait")
            self.assertEqual(reply["state"], "succeeded")
        help_client = await self.start_cli(server, ["--help"])
        code, output, _ = await help_client.finish()
        self.assertEqual(code, 0)
        self.assertIn("Start webserver.py separately", output)

    async def test_chain_wait_deadline_is_shared_and_does_not_cancel_tail(self):
        server = await self.start_server(fault_mode="controlled")
        chain = self.w.root / "waiting-chain.json"
        write_json(
            chain,
            {
                "commands": [
                    {"command": "fixture.echo"},
                    {"command": "fixture.wait"},
                    {"command": "fixture.echo"},
                ]
            },
        )
        client = await self.start_cli(
            server, ["chain", str(chain), "--wait-timeout", "0.3"]
        )
        code, _, error = await client.finish()
        self.assertEqual(code, 4, error)
        ids = error.split("command_id=", 1)[1].splitlines()[0].split(",")
        self.assertEqual((await server.result(ids[0]))["state"], "succeeded")
        self.assertEqual((await server.get("/commands/" + ids[2]))["state"], "pending")
        (server.control / "command.release").touch()
        self.assertEqual((await server.result(ids[2]))["state"], "succeeded")

    async def test_tcp_errors_invalid_responses_and_lost_post_are_never_retried(self):
        requests = []
        current = {"mode": "invalid_json"}
        instance = str(uuid4())

        async def handle(reader, writer):
            try:
                header = await reader.readuntil(b"\r\n\r\n")
                lines = header.decode("ascii").split("\r\n")
                method, path, _ = lines[0].split()
                length = next(
                    (
                        int(line.split(":", 1)[1])
                        for line in lines[1:]
                        if line.lower().startswith("content-length:")
                    ),
                    0,
                )
                body = await reader.readexactly(length)
                document = json.loads(body) if body else None
                requests.append((method, path, document))
                mode = current["mode"]
                if mode == "disconnect":
                    return
                status, payload = 200, b"{}"
                if mode == "invalid_json":
                    payload = b"not json"
                elif mode == "oversized":
                    payload = b'{"data":"' + b"x" * 4096 + b'"}'
                elif mode == "redirect":
                    status = 307
                elif mode == "rejected":
                    status, payload = (
                        409,
                        b'{"error":{"code":"conflict","message":"fixture"}}',
                    )
                elif mode.startswith("chain_"):
                    tickets = [
                        {
                            "command_id": item["command_id"],
                            "server_instance_id": instance,
                            "state": "succeeded",
                            "result": "success",
                        }
                        for item in document["commands"]
                    ]
                    if mode == "chain_empty":
                        tickets = []
                    if mode == "chain_instance":
                        tickets[0]["server_instance_id"] = str(uuid4())
                    payload = json.dumps(
                        {
                            "chain_id": str(uuid4())
                            if mode == "chain_id"
                            else document["chain_id"],
                            "server_instance_id": instance,
                            "commands": tickets,
                        }
                    ).encode()
                elif mode in ("restarted", "wrong_id", "wrong_poll_id"):
                    identifier = (
                        document["command_id"] if document else path.rsplit("/", 1)[1]
                    )
                    payload = json.dumps(
                        {
                            "command_id": str(uuid4())
                            if mode == "wrong_id"
                            or (mode == "wrong_poll_id" and method == "GET")
                            else identifier,
                            "server_instance_id": instance
                            if method == "POST" or mode == "wrong_poll_id"
                            else str(uuid4()),
                            "state": "pending"
                            if mode == "restarted"
                            or (mode == "wrong_poll_id" and method == "POST")
                            else "succeeded",
                            "result": None
                            if mode == "restarted"
                            or (mode == "wrong_poll_id" and method == "POST")
                            else "success",
                        }
                    ).encode()
                writer.write(
                    f"HTTP/1.1 {status} Fixture\r\nContent-Length: {len(payload)}\r\nLocation: /redirected\r\nConnection: close\r\n\r\n".encode()
                    + payload
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        listener = await asyncio.start_server(handle, "127.0.0.1", 0)
        self.addAsyncCleanup(listener.wait_closed)
        self.addCleanup(listener.close)
        endpoint = SimpleNamespace(
            url=f"http://127.0.0.1:{listener.sockets[0].getsockname()[1]}/api"
        )
        config = self.w.root / "small-client.json"
        write_json(config, {"max_response_bytes": 1024, "poll_interval_seconds": 0.01})
        for mode, expected in (
            ("invalid_json", 3),
            ("missing_receipt", 3),
            ("oversized", 3),
            ("redirect", 1),
            ("rejected", 1),
            ("disconnect", 3),
            ("restarted", 3),
            ("wrong_id", 3),
            ("wrong_poll_id", 3),
            ("chain_empty", 3),
            ("chain_instance", 3),
            ("chain_id", 3),
        ):
            with self.subTest(mode=mode):
                current["mode"] = mode
                requests.clear()
                identifier = str(uuid4())
                arguments = ["pause", "--command-id", identifier]
                if mode.startswith("chain_"):
                    chain = self.w.root / "invalid-response-chain.json"
                    write_json(
                        chain,
                        {"commands": [{"command": "pause", "command_id": identifier}]},
                    )
                    arguments = ["chain", str(chain)]
                client = await self.start_cli(
                    endpoint,
                    ["--config", str(config), *arguments],
                )
                code, _, errors = await client.finish()
                self.assertEqual(code, expected, errors)
                self.assertIn(identifier, errors)
                self.assertEqual(
                    sum(method == "POST" for method, _, _ in requests), 1, requests
                )
                self.assertFalse(any(path == "/redirected" for _, path, _ in requests))
