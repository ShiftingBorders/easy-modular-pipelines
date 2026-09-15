"""Approved basic_dag.md E1-E5: real CLI, queues, and subprocess stages."""

import asyncio
import codecs
import json
import subprocess
import unittest

from tests.helpers.dag import (
    REPOSITORY,
    DagWorkspace,
    process_running,
    terminate_owned,
    wait_until,
)


class CliTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.process = None
        self.reader = None
        self.stderr = None
        self.responses = asyncio.Queue()
        self.ownership = self.workspace.root / "cli-owner.json"
        self.addAsyncCleanup(self.close_cli)

    async def start_cli(self, args):
        self.process = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--project",
            str(REPOSITORY),
            "--no-sync",
            "python",
            "-B",
            "-m",
            "tests.helpers.dag_cli",
            "--ownership",
            str(self.ownership),
            *args,
            cwd=REPOSITORY,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.reader = asyncio.create_task(self.read_responses())
        self.stderr = asyncio.create_task(self.process.stderr.read())

    async def read_responses(self):
        buffer = ""
        decoder = codecs.getincrementaldecoder("utf-8")()
        parser = json.JSONDecoder()
        while chunk := await self.process.stdout.read(4096):
            buffer += decoder.decode(chunk)
            while "{" in buffer:
                start = buffer.index("{")
                try:
                    response, end = parser.raw_decode(buffer, start)
                except json.JSONDecodeError:
                    break
                buffer = buffer[end:]
                if "command_id" in response:
                    self.responses.put_nowait(response)

    async def write(self, command):
        self.process.stdin.write((command + "\n").encode("utf-8"))
        await self.process.stdin.drain()

    async def reply(self):
        return await asyncio.wait_for(self.responses.get(), 10)

    async def prepared(self, template):
        path = self.workspace.write_template(template)
        await self.start_cli(
            [
                "--project-root",
                str(self.workspace.root),
                "--template",
                str(path),
                "--hash-config",
                str(self.workspace.hash_config),
            ]
        )
        while True:
            await self.write("state")
            response = await self.reply()
            self.assertEqual(response["result"], "success", response)
            if response["data"]["phase"] == "waiting":
                return response["data"]
            self.assertNotEqual(response["data"]["phase"], "failed", response)

    async def close_cli(self):
        for gate in self.workspace.gates:
            gate.touch(exist_ok=True)
        if self.process is not None and self.process.returncode is None:
            try:
                await self.write("quit")
                await asyncio.wait_for(self.process.wait(), 8)
            except (OSError, ConnectionError, TimeoutError):
                if self.ownership.is_file():
                    terminate_owned(json.loads(self.ownership.read_text())["cli"])
                await asyncio.wait_for(self.process.wait(), 5)
        if self.reader is not None:
            await self.reader
        if self.stderr is not None:
            await self.stderr
        for path in self.workspace.root.glob("experiments/**/process.json"):
            record = json.loads(path.read_text())
            for name in ("stage", "executor"):
                if record.get(name):
                    terminate_owned(record[name])
                    await wait_until(
                        lambda pid=record[name]["pid"]: not process_running(pid),
                        timeout=5,
                    )

    async def test_arguments_report_errors_with_nonzero_exit(self):
        """E1: incomplete CLI arguments produce diagnostics and a failing exit code."""
        async with asyncio.timeout(30):
            await self.start_cli(["--project-root"])
            await self.process.wait()
            self.assertNotEqual(self.process.returncode, 0)
            self.assertIn(b"expected one argument", await self.stderr)

    async def test_state_logs_and_stop_work_while_step_is_pending(self):
        """E2/E4: interactive input remains usable while a real stage is gated."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            template = self.workspace.template(
                [self.workspace.stage(settings={"gate": str(gate)})]
            )
            await self.prepared(template)
            await self.write("step")
            ready_path = await wait_until(
                lambda: next(
                    self.workspace.root.glob("experiments/**/ready.json"), None
                )
            )
            pid = json.loads(ready_path.read_text())["pid"]
            await self.write("state")
            state = await self.reply()
            self.assertEqual(state["data"]["phase"], "stage_running")
            await self.write("logs")
            self.assertEqual((await self.reply())["result"], "success")
            self.assertTrue(process_running(pid))
            await self.write("stop")
            replies = [await self.reply(), await self.reply()]
            self.assertEqual(
                {reply["state"] for reply in replies}, {"cancelled", "succeeded"}
            )
            self.assertFalse(process_running(pid))
            await self.write("quit")
            await self.process.wait()
            self.assertEqual(self.process.returncode, 0, (await self.stderr).decode())

    async def test_json_target_command_is_forwarded_and_unsupported_command_is_explicit(
        self,
    ):
        """E1/E5: JSON targets survive CLI parsing; unsupported commands are not successes."""
        async with asyncio.timeout(30):
            await self.prepared(self.workspace.template())
            await self.write(
                json.dumps(
                    {
                        "command": "reset_retries",
                        "target": {"kind": "stage", "position": 1},
                    }
                )
            )
            reply = await self.reply()
            self.assertEqual(reply["result"], "success", reply)
            for command, args in (
                ("snapshot", {}),
                ("rollback", {"snapshot_id": "absent"}),
                ("reload_template", {}),
                ("retry", {"position": 1}),
            ):
                await self.write(json.dumps({"command": command, "args": args}))
                response = await self.reply()
                self.assertEqual(response["result"], "fail")
                self.assertEqual(response["error"]["code"], "unsupported_feature")
            await self.write("quit")
            await self.process.wait()
            self.assertEqual(self.process.returncode, 0)

    async def test_eof_closes_prepared_experiment_and_own_cli_process(self):
        """E1/E4: EOF is a normal shutdown with no stage or CLI left running."""
        async with asyncio.timeout(30):
            await self.prepared(self.workspace.template())
            identity = json.loads(self.ownership.read_text())["cli"]
            self.process.stdin.close()
            await self.process.stdin.wait_closed()
            await self.process.wait()
            self.assertEqual(self.process.returncode, 0, (await self.stderr).decode())
            self.assertFalse(process_running(identity["pid"]))
            self.assertEqual(
                list(self.workspace.root.glob("experiments/**/ready.json")), []
            )

    async def test_invalid_command_json_fails_with_diagnostics(self):
        """E1: malformed JSON terminates with an error and still performs cleanup."""
        async with asyncio.timeout(30):
            await self.prepared(self.workspace.template())
            await self.write("{invalid")
            await self.process.wait()
            self.assertNotEqual(self.process.returncode, 0)
            self.assertIn(b"JSONDecodeError", await self.stderr)
