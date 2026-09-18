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
from uuid import uuid4

import httpx
import uvicorn

from core.runner_utils.runtimeio import process_identity
from dashboard.application import create_app
from tests.dashboard_tests.helpers import (
    FIXTURES,
    PROJECT_ROOT,
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
            browser_output = None
            phase = "server startup"
            port_text = ""
            last_port_error = None
            port_attempts = 0
            stdout = stderr = b""
            try:
                async with asyncio.timeout(15):
                    while not server.started:
                        await asyncio.sleep(0.02)
                profile = root / "browser"
                phase = "browser startup"
                browser_output = (root / "browser-stderr.log").open("wb")
                browser = await asyncio.create_subprocess_exec(
                    str(EDGE),
                    "--headless=new",
                    "--disable-gpu",
                    "--no-first-run",
                    "--remote-debugging-port=0",
                    f"--user-data-dir={profile}",
                    "about:blank",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=browser_output,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
                identity = process_identity(browser.pid)
                phase = "port publication"
                async with asyncio.timeout(15):
                    while True:
                        if browser.returncode is not None:
                            raise RuntimeError(
                                f"Edge exited before publishing its port: {browser.returncode}"
                            )
                        port_attempts += 1
                        try:
                            port_text = await asyncio.to_thread(
                                (profile / "DevToolsActivePort").read_text,
                                encoding="utf-8",
                            )
                            lines = port_text.splitlines()
                            if (
                                len(lines) != 2
                                or not lines[1].startswith("/devtools/browser/")
                                or not lines[0].isdigit()
                                or not 0 < int(lines[0]) < 65536
                            ):
                                raise ValueError("Incomplete or invalid Edge port marker.")
                        except (FileNotFoundError, PermissionError, ValueError) as error:
                            last_port_error = repr(error)
                        else:
                            port = lines[0]
                            break
                        await asyncio.sleep(0.025)
                phase = "browser checks"
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
            except Exception as failure:
                destination = (
                    PROJECT_ROOT / ".artifacts/ci-failures" / f"browser-{uuid4()}.json"
                )
                report = {
                    "test": self.id(),
                    "phase": phase,
                    "exception": repr(failure),
                    "browser_identity": identity,
                    "browser_exit_code": None if browser is None else browser.returncode,
                    "port_attempts": port_attempts,
                    "last_port_error": last_port_error,
                    "port_text": port_text[:1024],
                    "node_stdout": stdout[-65536:].decode("utf-8", errors="replace"),
                    "node_stderr": stderr[-65536:].decode("utf-8", errors="replace"),
                }
                try:
                    with (root / "browser-stderr.log").open("rb") as stream:
                        stream.seek(max(0, stream.seek(0, os.SEEK_END) - 65536))
                        report["browser_stderr"] = stream.read(65536).decode(
                            "utf-8", errors="replace"
                        )
                except OSError as error:
                    report["browser_stderr_error"] = repr(error)
                try:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_text(json.dumps(report, indent=2), encoding="utf-8")
                    failure.add_note(f"Browser diagnostics: {destination}")
                except OSError as error:
                    failure.add_note(f"Could not save browser diagnostics: {error!r}")
                raise
            finally:
                if browser is not None:
                    try:
                        await asyncio.wait_for(browser.wait(), 5)
                    except TimeoutError:
                        if identity:
                            terminate_owned(identity)
                        await browser.wait()
                if browser_output is not None:
                    browser_output.close()
                server.should_exit = True
                await asyncio.wait_for(task, 15)
