"""Approved H: Windows Edge UI over real HTTP, with real local journal data."""

import asyncio
import json
import os
import shutil
import socket
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import uvicorn

from core.runner_utils.runtimeio import process_identity
from dashboard.application import create_app
from tests.dashboard_tests.helpers import (
    FIXTURES,
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import JournalWorkspace, resource_status
from tests.helpers.dag import terminate_owned

EDGE = (
    Path(os.environ.get("ProgramFiles(x86)", "C:/Program Files (x86)"))
    / "Microsoft/Edge/Application/msedge.exe"
)


@unittest.skipUnless(
    os.name == "nt" and EDGE.is_file() and shutil.which("node"),
    "Windows Edge and Node required; no Linux browser execution",
)
class BrowserTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_screens_filters_bookmarks_forecast_alerts_and_keyboard(self):
        tmp = temporary_directory()
        self.addCleanup(cleanup_directory, tmp)
        root = Path(tmp.name)
        workspace = JournalWorkspace(root / "project")
        self.addCleanup(workspace.close)
        app = create_app(
            write_settings(
                root,
                project_root=str(workspace.root),
                system_api_url="http://fixture/api/",
                refresh_seconds=1,
            )
        )

        async def respond(request):
            if request.url.path == "/api/state":
                return httpx.Response(
                    200,
                    json={
                        "experiment_id": "exp-test",
                        "fresh": True,
                        "phase": "waiting",
                        "mode": "paused",
                        "cycle_number": 3,
                        "services": [],
                    },
                )
            if request.url.path == "/api/resources":
                return httpx.Response(200, json=resource_status())
            return httpx.Response(
                200,
                json={
                    "history_id": "history",
                    "cursor": 0,
                    "samples": [],
                    "gap": False,
                },
            )

        upstream = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.setblocking(False)
        self.addCleanup(listener.close)
        server = uvicorn.Server(
            uvicorn.Config(app, log_level="error", access_log=False)
        )
        with patch("dashboard.api_client.httpx.AsyncClient", return_value=upstream):
            task = asyncio.create_task(server.serve(sockets=[listener]))
            browser = None
            identity = None
            try:
                async with asyncio.timeout(15):
                    while not server.started:
                        await asyncio.sleep(0.02)
                profile = root / "browser"
                browser = await asyncio.create_subprocess_exec(
                    str(EDGE),
                    "--headless=new",
                    "--disable-gpu",
                    "--no-first-run",
                    "--remote-debugging-port=0",
                    f"--user-data-dir={profile}",
                    "about:blank",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
                identity = process_identity(browser.pid)
                async with asyncio.timeout(15):
                    while not (profile / "DevToolsActivePort").exists():
                        await asyncio.sleep(0.025)
                port = (profile / "DevToolsActivePort").read_text().splitlines()[0]
                node = await asyncio.create_subprocess_exec(
                    shutil.which("node"),
                    str(FIXTURES / "browser_check.mjs"),
                    port,
                    f"http://127.0.0.1:{listener.getsockname()[1]}/",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    stdout, stderr = await asyncio.wait_for(node.communicate(), 60)
                finally:
                    if node.returncode is None:
                        node.kill()
                        await node.wait()
                self.assertEqual(node.returncode, 0, stderr.decode(errors="replace"))
                result = json.loads(stdout)
                self.assertEqual(result["runtimeErrors"], [])
                self.assertTrue(
                    all(
                        not screen["unavailable"]
                        for screen in result["screens"].values()
                    ),
                    result,
                )
                self.assertGreater(result["traceRows"], 4)
                self.assertTrue(result["bookmarked"])
                self.assertEqual(result["filteredCount"], "0")
                self.assertGreater(result["resetCount"], 0)
                self.assertTrue(result["detailsOpen"])
                self.assertIn("50%", result["forecastText"])
                self.assertIn("3 h", result["forecastText"])
                self.assertEqual(result["refreshChoices"][0], "1")
                self.assertEqual(result["refreshChoices"][-1], "600")
                self.assertIn("custom", result["rangeChoices"])
                self.assertFalse(result["gpuVisible"])
                self.assertEqual(result["bell"], "1")
                self.assertTrue(result["ruleDeleted"])
                self.assertIn("Compute resources", result["searchMatches"])
                self.assertTrue(result["searchClosed"])
                self.assertTrue(result["fontsLoaded"])
                self.assertEqual(result["connection"], "System connected")
                self.assertTrue(result["narrowLayout"])
            finally:
                if browser is not None:
                    try:
                        await asyncio.wait_for(browser.wait(), 5)
                    except TimeoutError:
                        if identity:
                            terminate_owned(identity)
                        await browser.wait()
                server.should_exit = True
                await asyncio.wait_for(task, 15)
