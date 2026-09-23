"""Approved H: Windows Edge UI over real HTTP, with real local journal data."""

import asyncio
import json
import os
import shutil
import socket
import subprocess
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import httpx
import psutil
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
from tests.dashboard_tests.integration_helpers import (
    JournalWorkspace,
    history,
    resource_status,
)
from tests.helpers.dag import terminate_owned

EDGE = (
    Path(os.environ.get("ProgramFiles(x86)", "C:/Program Files (x86)"))
    / "Microsoft/Edge/Application/msedge.exe"
)
BROWSER = (
    EDGE
    if os.name == "nt"
    else Path(
        shutil.which("google-chrome")
        or shutil.which("chromium")
        or shutil.which("chromium-browser")
        or "/nonexistent-browser"
    )
)


@unittest.skipUnless(
    BROWSER.is_file() and shutil.which("node"),
    "Chromium-compatible browser and Node with built-in WebSocket required",
)
class BrowserTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_screens_filters_bookmarks_forecast_alerts_and_keyboard(self):
        """T079/T080/T081/T082/T083/T084/T085/T086/T087/T091: real DOM and 500-event latency."""
        tmp = temporary_directory()
        self.addCleanup(cleanup_directory, tmp)
        root = Path(tmp.name)
        workspace = JournalWorkspace(root / "project")
        self.addCleanup(workspace.close)
        for number in range(487):
            workspace.logger.record_event("browser.sample", {"number": number})
        cold_data = history()
        cold_data["state"]["phase"] = "stopped"
        cold_workspace = JournalWorkspace(
            root / "project", cold_data, identifier="cold-test", folder="cold"
        )
        self.addCleanup(cold_workspace.close)
        for number in range(487):
            cold_workspace.logger.record_event("browser.sample", {"number": number})
        registry_path = workspace.root / "experiments.json"
        registry_path.write_text(json.dumps({"exp-test": "recorded"}), encoding="utf-8")
        app = create_app(
            write_settings(
                root,
                project_root=str(workspace.root),
                system_api_url="http://fixture/api/",
                refresh_seconds=1,
            )
        )

        async def register_cold_history():
            registry_path.write_text(
                json.dumps({"exp-test": "recorded", "cold-test": "cold"}),
                encoding="utf-8",
            )
            app.state.views._registry = app.state.views.journals.registry()
            await app.state.views._refresh_cache_selection()
            return {"registered": "cold-test"}

        app.add_api_route(
            "/test/register-cold", register_cold_history, methods=["POST"]
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
            # Use the same platform loop as the real CLI server, while the test
            # separately owns the browser and Node subprocess transports.
            server_thread = ThreadPoolExecutor(max_workers=1)
            task = asyncio.wrap_future(
                server_thread.submit(
                    asyncio.run,
                    server.serve(sockets=[listener]),
                    loop_factory=server.config.get_loop_factory(),
                )
            )
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
                    str(BROWSER),
                    "--headless=new",
                    "--disable-gpu",
                    "--no-first-run",
                    *(
                        ["--no-sandbox", "--disable-dev-shm-usage"]
                        if os.name != "nt" and os.geteuid() == 0
                        else []
                    ),
                    "--remote-debugging-port=0",
                    f"--user-data-dir={profile}",
                    "about:blank",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=browser_output,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
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
                                raise ValueError(
                                    "Incomplete or invalid Edge port marker."
                                )
                        except (
                            FileNotFoundError,
                            PermissionError,
                            ValueError,
                        ) as error:
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
                raw_report = (
                    PROJECT_ROOT
                    / ".artifacts/logs"
                    / f"dashboard-browser-results-{os.name}.json"
                )
                raw_report.parent.mkdir(parents=True, exist_ok=True)
                raw_report.write_text(json.dumps(result, indent=2), encoding="utf-8")
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
                self.assertTrue(all(result["domState"].values()), result["domState"])
                self.assertTrue(result["tabsPreserved"])
                self.assertTrue(result["lateResponseIgnored"])
                self.assertEqual(result["historyRetryCount"], 2)
                self.assertTrue(result["previousDataRetained"])
                self.assertTrue(result["detailRaceSafe"])
                self.assertTrue(result["paginationRetained"])
                self.assertTrue(result["settingsModeRetained"])
                self.assertTrue(result["initialCacheNotice"])
                self.assertEqual(result["historyEvents"], 500)
                self.assertEqual(result["summaryRequests"], 0)
                self.assertEqual(result["coldHistoryEvents"], 500)
                self.assertLessEqual(result["coldOpenMs"], 200)
                for page, timings in result["pageTimings"].items():
                    with self.subTest(page=page):
                        self.assertLessEqual(max(timings), 200, f"{page}: {timings}")
                memory = psutil.Process().memory_info().rss
                memory += sum(
                    psutil.Process(process.pid).memory_info().rss
                    for process in app.state.views._cache_pool._processes.values()
                )
                self.assertLessEqual(memory, 2_000_000_000)
                report_path = (
                    PROJECT_ROOT
                    / ".artifacts/logs"
                    / f"dashboard-browser-performance-{os.name}.json"
                )
                report_path.parent.mkdir(parents=True, exist_ok=True)
                report_path.write_text(
                    json.dumps(
                        {
                            "platform": os.name,
                            "events": 500,
                            "event_loop": server.config.get_loop_factory().__name__,
                            "page_timings_ms": result["pageTimings"],
                            "cold_open_ms": result["coldOpenMs"],
                            "http_timings_ms": result["httpTimings"],
                            "dashboard_tree_rss_bytes": memory,
                        },
                        indent=2,
                    ),
                    encoding="utf-8",
                )
            except Exception as failure:
                destination = (
                    PROJECT_ROOT / ".artifacts/ci-failures" / f"browser-{uuid4()}.json"
                )
                report = {
                    "test": self.id(),
                    "phase": phase,
                    "exception": repr(failure),
                    "browser_identity": identity,
                    "browser_exit_code": None
                    if browser is None
                    else browser.returncode,
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
                    destination.write_text(
                        json.dumps(report, indent=2), encoding="utf-8"
                    )
                    failure.add_note(f"Browser diagnostics: {destination}")
                except OSError as error:
                    failure.add_note(f"Could not save browser diagnostics: {error!r}")
                raise
            finally:
                server.should_exit = True
                try:
                    await asyncio.wait_for(task, 15)
                finally:
                    if browser is not None and browser.returncode is None:
                        if os.name == "nt" and identity:
                            terminate_owned(identity)
                        else:
                            browser.terminate()
                        await browser.wait()
                    if browser_output is not None:
                        browser_output.close()
                    server_thread.shutdown(wait=True)
