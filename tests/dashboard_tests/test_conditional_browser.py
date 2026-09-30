"""Approved D08: real browser refresh of conditional DAG badges and move edges."""

import copy
import json
import os
import shutil
import subprocess
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from core.primitives.processes import process_identity
from dashboard.projections import experiment_views
from tests.dashboard_tests.conditional_helpers import ConditionalHistory
from tests.dashboard_tests.helpers import (
    DASHBOARD,
    FIXTURES,
    PROJECT_ROOT,
    cleanup_directory,
    temporary_directory,
)
from tests.dashboard_tests.test_browser import BROWSER
from tests.helpers.dag import terminate_owned


class ConditionalBrowserTests(unittest.TestCase):
    def test_badges_refresh_without_stale_success_and_self_loop_is_visible(self):
        """D08: update real DOM in place, with escaped labels and visible loop geometry."""
        if not BROWSER.is_file() or not shutil.which("node"):
            self.skipTest("Chromium and Node with built-in WebSocket are required")
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        root = Path(temporary.name)
        history = ConditionalHistory()
        frames = [
            copy.deepcopy(
                experiment_views(history.dataset(), history.live())["template"]
            )
        ]
        history.move()
        frames.append(
            copy.deepcopy(
                experiment_views(history.dataset(), history.live())["template"]
            )
        )
        history.start_target()
        frames.append(
            copy.deepcopy(
                experiment_views(history.dataset(), history.live())["template"]
            )
        )
        history.finish_target()
        frames.append(
            copy.deepcopy(
                experiment_views(history.dataset(), history.live())["template"]
            )
        )
        history.move("C", "C", number=3)
        frames.append(
            copy.deepcopy(
                experiment_views(history.dataset(), history.live())["template"]
            )
        )
        documents = {
            "/": (
                "text/html",
                b'<!doctype html><html><head><link rel="stylesheet" href="/app.css"></head><body><main id="dag-root" style="margin:0;padding:16px"></main></body></html>',
            ),
            "/frames.json": ("application/json", json.dumps(frames).encode("utf-8")),
        }
        for name, mime in (
            ("views.js", "text/javascript"),
            ("ui.js", "text/javascript"),
            ("app.css", "text/css"),
        ):
            documents["/" + name] = (mime, (DASHBOARD / "static" / name).read_bytes())

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path not in documents:
                    self.send_error(404)
                    return
                mime, body = documents[self.path]
                self.send_response(200)
                self.send_header("Content-Type", mime + "; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        browser = identity = None
        try:
            profile = root / "browser"
            with (root / "browser.log").open("wb") as output:
                browser = subprocess.Popen(
                    [
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
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=output,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                identity = process_identity(browser.pid)
                deadline = time.monotonic() + 20
                port = None
                while time.monotonic() < deadline:
                    try:
                        lines = (
                            (profile / "DevToolsActivePort")
                            .read_text(encoding="utf-8")
                            .splitlines()
                        )
                        if len(lines) == 2 and lines[0].isdigit():
                            port = lines[0]
                            break
                    except (FileNotFoundError, PermissionError):
                        pass
                    if browser.poll() is not None:
                        break
                    time.sleep(0.05)
                self.assertIsNotNone(
                    port,
                    (root / "browser.log").read_text(
                        encoding="utf-8", errors="replace"
                    ),
                )
                screenshot = (
                    PROJECT_ROOT / ".artifacts/logs/dashboard-conditional-move.png"
                )
                screenshot.parent.mkdir(parents=True, exist_ok=True)
                node = subprocess.run(
                    [
                        shutil.which("node"),
                        str(FIXTURES / "conditional_browser_check.mjs"),
                        port,
                        f"http://127.0.0.1:{server.server_port}/",
                        str(screenshot),
                    ],
                    capture_output=True,
                    timeout=40,
                    check=False,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                self.assertEqual(
                    node.returncode, 0, node.stderr.decode(errors="replace")
                )
                result = json.loads(node.stdout)
                self.assertEqual(
                    result["before"], ["Succeeded", "Completed", "Success"]
                )
                for name in (
                    "escaped",
                    "sameNode",
                    "sameBadge",
                    "pending",
                    "cursor",
                    "conditional",
                    "running",
                    "accepted",
                ):
                    self.assertTrue(result[name], (name, result))
                self.assertEqual(result["prefix"], "Succeeded")
                self.assertEqual(result["activeMoves"], 1)
                self.assertGreater(result["loopWidth"], 80)
                self.assertGreater(result["loopHeight"], 30)
        finally:
            if identity is not None:
                terminate_owned(identity)
            if browser is not None:
                browser.wait(timeout=15)
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
