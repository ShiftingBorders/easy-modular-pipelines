"""Approved resource_collector.md A/E: actual CLI settings and monitoring reads."""

import asyncio
import codecs
import json
import os
import subprocess
import unittest

from tests.helpers.dag import (
    REPOSITORY,
    DagWorkspace,
    process_running,
    terminate_owned,
    wait_until,
)
from tests.helpers.resources import write_settings


class ResourceCliTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.process = None
        self.reader = None
        self.stderr = None
        self.responses = asyncio.Queue()
        self.ownership = self.workspace.root / "cli-owner.json"
        self.collector_pid = None
        self.addAsyncCleanup(self.close_cli)

    async def start_cli(self, config):
        template = self.workspace.write_template(self.workspace.template())
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
            "--project-root",
            str(self.workspace.root),
            "--hash-config",
            str(self.workspace.hash_config),
            "--template",
            str(template),
            "--resource-config",
            config.name,
            cwd=self.workspace.root,
            env={
                **os.environ,
                "PYTHONPATH": str(REPOSITORY),
                "PYTHONIOENCODING": "utf-8",
            },
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.reader = asyncio.create_task(self.read_responses())
        self.stderr = asyncio.create_task(self.process.stderr.read())

    async def read_responses(self):
        decoder = codecs.getincrementaldecoder("utf-8")()
        parser = json.JSONDecoder()
        buffer = ""
        while chunk := await self.process.stdout.read(4096):
            buffer += decoder.decode(chunk)
            while "{" in buffer:
                start = buffer.index("{")
                try:
                    message, end = parser.raw_decode(buffer, start)
                except json.JSONDecodeError:
                    break
                buffer = buffer[end:]
                if "command_id" in message:
                    self.responses.put_nowait(message)

    async def command(self, name, args=None):
        payload = json.dumps({"command": name, "args": args or {}})
        self.process.stdin.write((payload + "\n").encode())
        await self.process.stdin.drain()
        return await asyncio.wait_for(self.responses.get(), 10)

    async def close_cli(self):
        if self.process is not None and self.process.returncode is None:
            try:
                self.process.stdin.write(b"quit\n")
                await self.process.stdin.drain()
                await asyncio.wait_for(self.process.wait(), 8)
            except (OSError, ConnectionError, TimeoutError):
                if self.ownership.exists():
                    terminate_owned(json.loads(self.ownership.read_text())["cli"])
                await asyncio.wait_for(self.process.wait(), 5)
        if self.reader is not None:
            await self.reader
        if self.stderr is not None:
            await self.stderr
        for path in self.workspace.root.glob("experiments/**/process.json"):
            record = json.loads(path.read_text())
            for key in ("stage", "executor"):
                if record.get(key):
                    terminate_owned(record[key])
                    await wait_until(
                        lambda pid=record[key]["pid"]: not process_running(pid)
                    )

    async def test_relative_custom_config_and_real_resource_commands(self):
        """A/E: CLI resolves its config against its invocation cwd and serves real samples."""
        async with asyncio.timeout(30):
            config = write_settings(self.workspace.root, history_seconds=123)
            await self.start_cli(config)
            while True:
                status = await self.command("stats.resources")
                self.assertEqual(status["result"], "success", status)
                if status["data"]["latest"]:
                    break
            self.assertEqual(status["data"]["config_path"], str(config))
            self.collector_pid = status["data"]["pid"]
            self.assertTrue(process_running(self.collector_pid))
            history = await self.command("stats.resources.history", {"limit": 2})
            self.assertEqual(history["result"], "success")
            self.assertTrue(history["data"]["samples"])
            self.assertLessEqual(len(history["data"]["samples"]), 2)
            self.assertEqual((await self.command("step"))["result"], "success")
            await self.close_cli()
            self.assertEqual(self.process.returncode, 0, (await self.stderr).decode())
            self.assertFalse(process_running(self.collector_pid))

    async def test_invalid_collector_config_does_not_prevent_cli_experiment_completion(
        self,
    ):
        """A/E: a bad monitoring file is reported while the real CLI still executes its DAG."""
        async with asyncio.timeout(30):
            config = self.workspace.root / "invalid-collector.json"
            config.write_text("{", encoding="utf-8")
            await self.start_cli(config)
            while True:
                status = await self.command("stats.resources")
                self.assertEqual(status["result"], "success")
                if status["data"]["state"] == "configuration_error":
                    break
            self.assertTrue(status["data"]["error"])
            self.assertEqual((await self.command("step"))["result"], "success")
            self.assertEqual(
                (await self.command("stats.state"))["data"]["phase"], "completed"
            )
            await self.close_cli()
            self.assertEqual(self.process.returncode, 0)
