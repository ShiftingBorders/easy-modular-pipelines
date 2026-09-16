"""Approved service_integration.md D5/E: real CLI shutdown with live services."""

import asyncio
import os
import subprocess
import unittest

from core.runner_utils.runtimeio import read_json
from tests.helpers.dag import REPOSITORY, process_running, terminate_owned
from tests.helpers.service_integration import ServiceDagWorkspace
from tests.helpers.services import wait_for


class ServiceCliTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ServiceDagWorkspace()
        self.addAsyncCleanup(self.w.close)
        self.process = None
        self.ownership = self.w.root / "cli-owner.json"
        self.output = None
        self.addAsyncCleanup(self.close_cli)

    async def close_cli(self):
        if self.process is not None:
            if self.process.returncode is None:
                if self.ownership.exists():
                    terminate_owned(read_json(self.ownership)["cli"])
                else:
                    self.process.terminate()
            await asyncio.wait_for(self.process.wait(), 15)
            if self.output is not None:
                await self.output

    async def check_shutdown(self, ending):
        w = self.w
        service = w.service()
        template = w.write_template(w.template())
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
            str(w.root),
            "--template",
            str(template),
            "--hash-config",
            str(w.root / "hashes.json"),
            "--filer-url",
            "http://127.0.0.1:1",
            cwd=REPOSITORY,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        await wait_for(
            lambda: any(row.get("command") == "heartbeat" for row in w.trace(service))
        )
        identity = next(
            row["process"] for row in w.trace(service) if row["event"] == "started"
        )
        self.assertTrue(process_running(identity["pid"]))
        self.output = asyncio.create_task(self.process.communicate(ending))
        stdout, stderr = await asyncio.wait_for(asyncio.shield(self.output), 45)
        self.assertEqual(self.process.returncode, 0, stderr.decode())
        self.assertIn(b"Experiment:", stdout)
        self.assertFalse(process_running(identity["pid"]))
        self.assertTrue(
            any(row.get("command") == "shutdown" for row in w.trace(service))
        )

    async def test_quit_stops_the_cli_service(self):
        """D5/E4: ordinary quit invokes the integrated shutdown path."""
        async with asyncio.timeout(120):
            await self.check_shutdown(b"quit\n")

    async def test_eof_stops_the_cli_service(self):
        """D5/E4: EOF leaves no service behind even without an explicit stop command."""
        async with asyncio.timeout(120):
            await self.check_shutdown(b"")
