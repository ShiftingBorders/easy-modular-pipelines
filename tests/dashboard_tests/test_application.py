"""HTTP boundary, application lifecycle and independent resource/ICMP failures."""

import asyncio
import json
import socket
import unittest
from contextlib import AsyncExitStack
from pathlib import Path
from unittest.mock import patch

import httpx

from dashboard.application import create_app
from tests.dashboard_tests.helpers import (
    DASHBOARD,
    ProbeProcess,
    cleanup_directory,
    icmp_settings,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import JournalWorkspace, resource_status

WRITE_HEADERS = {"X-Dashboard-Request": "1", "Origin": "http://dashboard.test"}


class ApplicationTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_page_success_does_not_change_global_runtime_connection(self):
        self.app.state.views._live = {
            "available": False,
            "fresh": False,
            "connection_error": "offline",
        }
        for page in ("overview", "experiments", "modules", "compute", "alerts"):
            self.assertEqual(
                (await self.http.get("/api/system/" + page)).status_code, 200
            )
            self.assertFalse(
                (await self.http.get("/api/application")).json()["system_connection"][
                    "connected"
                ]
            )
        self.app.state.views._live_at = 0
        await self.app.state.views.state(refresh=True)
        self.assertTrue(
            (await self.http.get("/api/application")).json()["system_connection"][
                "connected"
            ]
        )

    async def asyncSetUp(self) -> None:
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        self.directory = Path(temporary.name)
        self.workspace = JournalWorkspace(self.directory / "project")
        self.addCleanup(self.workspace.close)
        self.config = write_settings(
            self.directory,
            system_api_url="http://upstream.test/api/",
            project_root=str(self.workspace.root),
        )
        self.upstream_requests = []
        self.action = None
        self.probes = []
        self.spawn_patch = patch(
            "dashboard.icmp.asyncio.create_subprocess_exec", side_effect=self.spawn
        )
        self.spawn_patch.start()
        self.addCleanup(self.spawn_patch.stop)
        self.app = create_app(self.config)
        self.upstream = httpx.AsyncClient(transport=httpx.MockTransport(self.respond))
        self.http = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url="http://dashboard.test",
        )
        self.addAsyncCleanup(self.http.aclose)
        self.stack = AsyncExitStack()
        self.addAsyncCleanup(self.stack.aclose)
        with patch(
            "dashboard.api_client.httpx.AsyncClient", return_value=self.upstream
        ):
            await self.stack.enter_async_context(
                self.app.router.lifespan_context(self.app)
            )

        for task in self.app.state.views._source_tasks:
            task.cancel()
        await asyncio.gather(
            *self.app.state.views._source_tasks, return_exceptions=True
        )
        self.app.state.views._source_tasks.clear()
        await self.app.state.views.state(refresh=True)
        await self.app.state.views._refresh_resources()
        self.upstream_requests.clear()

    async def spawn(self, *args, **kwargs):
        process = ProbeProcess()
        self.probes.append((args, process))
        return process

    async def respond(self, request: httpx.Request) -> httpx.Response:
        self.upstream_requests.append(request)
        if self.action:
            return await self.action(request)
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
            json={"samples": [], "cursor": 0, "history_id": "history", "gap": False},
        )

    async def test_application_information_identifies_dashboard_host(self) -> None:
        response = await self.http.get("/api/application")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["dashboard_host"], socket.gethostname())
        self.assertEqual(response.json()["icmp_source"], "dashboard_host")
        self.assertEqual(self.upstream_requests, [])

    async def test_html_scripts_fonts_and_api_schema_are_served(self) -> None:
        for path, content_type in [
            ("/", "text/html"),
            ("/static/app.js", "javascript"),
            ("/static/app.css", "text/css"),
            ("/static/fonts/Manrope.ttf", None),
            ("/openapi.json", "application/json"),
        ]:
            with self.subTest(path=path):
                response = await self.http.get(path)
                self.assertEqual(response.status_code, 200)
                if content_type is not None:
                    self.assertIn(content_type, response.headers["content-type"])
                else:
                    self.assertEqual(
                        response.content,
                        (DASHBOARD / "static/fonts/Manrope.ttf").read_bytes(),
                    )
                self.assertGreater(len(response.content), 0)
        self.assertEqual(
            (await self.http.get("/static/../settings.json")).status_code, 404
        )

    async def test_root_routes_combine_local_history_and_cached_live_sources(self):
        for resource in [
            "overview",
            "experiments",
            "modules",
            "services",
            "compute",
            "alerts",
        ]:
            with self.subTest(resource=resource):
                response = await self.http.get(f"/api/system/{resource}")
                self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.upstream_requests, [])
        self.assertEqual(
            (await self.http.get("/api/system/experiments")).json()["items"][0][
                "experiment_id"
            ],
            "exp-test",
        )
        self.assertEqual(
            (await self.http.get("/api/system/compute")).json()["metrics"]["cpu"][
                "value"
            ],
            25,
        )

    async def test_experiment_routes_read_real_local_journal_and_page_it(self):
        for view in [
            "summary",
            "runs",
            "operations",
            "events",
            "errors",
            "measurements",
            "template",
            "parameters",
            "commands",
            "snapshots",
            "artifacts",
            "forecast",
        ]:
            with self.subTest(view=view):
                response = await self.http.get(
                    f"/api/system/experiments/exp-test/{view}",
                    params={"run_id": "run-test", "limit": 1},
                )
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()["experiment_id"], "exp-test")
        first = (
            await self.http.get(
                "/api/system/experiments/exp-test/events", params={"limit": 1}
            )
        ).json()
        second = (
            await self.http.get(
                "/api/system/experiments/exp-test/events",
                params={"limit": 1, "cursor": json.dumps(first["next_cursor"])},
            )
        ).json()
        self.assertNotEqual(
            first["items"][0]["event_id"], second["items"][0]["event_id"]
        )
        self.assertEqual(self.upstream_requests, [])

    async def test_unknown_resources_and_bad_identifiers_never_reach_upstream(
        self,
    ) -> None:
        for path in [
            "/api/system/private",
            "/api/system/experiments/exp-test/delete",
            "/api/system/experiments/exp%5Cbad/events",
            "/api/system/experiments/exp%00bad/events",
            "/api/system/experiments/" + "x" * 513 + "/events",
        ]:
            with self.subTest(path=path):
                self.assertIn((await self.http.get(path)).status_code, {400, 404})
        self.assertEqual(self.upstream_requests, [])

    async def test_query_validation_rejects_unknown_large_and_out_of_range_values(
        self,
    ) -> None:
        for params in [
            {"db_path": "secret"},
            {"q": "x" * 4097},
            {"limit": "0"},
            {"limit": "1001"},
            {"limit": "1.5"},
            {"limit": "abc"},
        ]:
            with self.subTest(params=str(params)[:40]):
                self.assertEqual(
                    (
                        await self.http.get("/api/system/compute", params=params)
                    ).status_code,
                    400,
                )
        self.assertEqual(self.upstream_requests, [])
        for limit in [1, 1000]:
            self.assertEqual(
                (
                    await self.http.get("/api/system/compute", params={"limit": limit})
                ).status_code,
                200,
            )

    async def test_nonfinite_and_corrupt_resources_are_visible_without_fake_zero(self):
        for response in [
            httpx.Response(200, content=b'{"cpu":NaN}'),
            httpx.Response(200, content=b'{"metrics":'),
            httpx.Response(
                503,
                json={
                    "error": {
                        "code": "resource_journal_corrupted",
                        "message": "Invalid resource journal",
                    }
                },
            ),
        ]:

            async def respond(request, response=response):
                return response

            self.action = respond
            self.app.state.views._resource_at = 0
            await self.app.state.views._refresh_resources()
            result = (await self.http.get("/api/system/compute")).json()
            self.assertTrue(result["error"])
            self.assertFalse(result["metrics"]["cpu"]["fresh"])
            self.assertEqual(result["metrics"]["cpu"]["value"], 25)
            self.assertEqual((await self.http.get("/api/icmp")).status_code, 200)

    async def test_resource_disconnect_and_recovery_preserve_dashboard(self):
        async def fail(request):
            raise httpx.ReadError("source disconnected")

        self.action = fail
        self.app.state.views._resource_at = 0
        await self.app.state.views._refresh_resources()
        result = (await self.http.get("/api/system/compute")).json()
        self.assertFalse(result["metrics"]["cpu"]["fresh"])
        self.assertIn("connect", result["error"])
        self.action = None
        self.app.state.views._resource_at = 0
        await self.app.state.views._refresh_resources()
        self.assertTrue(
            (await self.http.get("/api/system/compute")).json()["metrics"]["cpu"][
                "fresh"
            ]
        )

    async def test_pending_source_does_not_block_pages_or_local_icmp(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def respond(request):
            started.set()
            await release.wait()
            raise httpx.ReadTimeout("source timeout")

        self.action = respond
        self.app.state.views._resource_at = 0
        pending = asyncio.create_task(self.app.state.views._refresh_resources())
        try:
            await asyncio.wait_for(started.wait(), 1)
            page = await asyncio.wait_for(self.http.get("/api/system/compute"), 1)
            self.assertEqual(page.status_code, 200)
            await self.http.put(
                "/api/icmp/settings", headers=WRITE_HEADERS, json=icmp_settings()
            )
            result = await asyncio.wait_for(
                self.http.post("/api/icmp/probe", headers=WRITE_HEADERS), 1
            )
            self.assertEqual(result.json()["status"], "reply")
            self.assertFalse(pending.done())
        finally:
            release.set()
            await pending
        self.assertIn(
            "deadline", (await self.http.get("/api/system/compute")).json()["error"]
        )

    async def test_icmp_configuration_and_probe_persist_on_dashboard_host(self) -> None:
        response = await self.http.put(
            "/api/icmp/settings", headers=WRITE_HEADERS, json=icmp_settings()
        )
        self.assertEqual(response.status_code, 200)
        observation = (
            await self.http.post("/api/icmp/probe", headers=WRITE_HEADERS)
        ).json()
        self.assertEqual(observation["latest"]["host"], "www.google.com")
        self.assertEqual(observation["latest"]["probe_host"], socket.gethostname())
        self.assertEqual(observation["source"], "dashboard_host")
        self.assertEqual(self.upstream_requests, [])
        stored = json.loads(
            (self.app.state.settings["state_directory"] / "icmp.json").read_text()
        )
        self.assertEqual(stored["settings"], icmp_settings())
        self.assertEqual(stored["history"][-1]["status"], "reply")

    async def test_rejects_cross_origin_or_unmarked_writes_without_changes(
        self,
    ) -> None:
        original = (await self.http.get("/api/icmp")).json()["settings"]
        for headers in [
            {},
            {"X-Dashboard-Request": "1", "Origin": "http://other.test"},
            {"X-Dashboard-Request": "1", "Origin": "null"},
        ]:
            with self.subTest(headers=headers):
                self.assertEqual(
                    (
                        await self.http.put(
                            "/api/icmp/settings", headers=headers, json=icmp_settings()
                        )
                    ).status_code,
                    403,
                )
                self.assertEqual(
                    (
                        await self.http.post("/api/icmp/probe", headers=headers)
                    ).status_code,
                    403,
                )
        self.assertEqual(
            (await self.http.get("/api/icmp")).json()["settings"], original
        )
        self.assertEqual(self.probes, [])

    async def test_rejects_oversized_nonjson_and_invalid_configuration_bodies(
        self,
    ) -> None:
        self.assertEqual(
            (
                await self.http.put(
                    "/api/icmp/settings", headers=WRITE_HEADERS, content="text"
                )
            ).status_code,
            415,
        )
        for content, expected in [
            (b"x" * 4097, 413),
            (b"{", 422),
            (b"null", 422),
            (b"\xff", 422),
        ]:
            with self.subTest(expected=expected, body=content[:8]):
                response = await self.http.put(
                    "/api/icmp/settings",
                    headers={**WRITE_HEADERS, "Content-Type": "application/json"},
                    content=content,
                )
                self.assertEqual(response.status_code, expected)
        self.assertFalse(
            (self.app.state.settings["state_directory"] / "icmp.json").exists()
        )

    async def test_config_write_failure_preserves_previous_settings(self) -> None:
        old = (await self.http.get("/api/icmp")).json()["settings"]
        with patch.object(
            self.app.state.icmp, "_write", side_effect=PermissionError("read only")
        ):
            response = await self.http.put(
                "/api/icmp/settings", headers=WRITE_HEADERS, json=icmp_settings()
            )
        self.assertEqual(response.status_code, 503)
        self.assertEqual((await self.http.get("/api/icmp")).json()["settings"], old)
        self.assertEqual(self.probes, [])

    async def test_disabled_probe_has_no_side_effects(self) -> None:
        self.assertEqual(
            (
                await self.http.post("/api/icmp/probe", headers=WRITE_HEADERS)
            ).status_code,
            409,
        )
        self.assertEqual(self.probes, [])

    async def test_lifespan_closes_http_client_monitor_and_state_lock(self) -> None:
        state_path = self.app.state.settings["state_directory"]
        await self.stack.aclose()
        self.assertTrue(self.upstream.is_closed)
        self.assertTrue(self.app.state.icmp._task.done())
        from dashboard.icmp import ICMPMonitor

        replacement = ICMPMonitor(state_path)
        await replacement.open()
        await replacement.close()

    async def test_failed_startup_closes_upstream_client(self) -> None:
        directory = self.directory / "broken"
        directory.mkdir()
        config = write_settings(directory)
        (directory / ".state").mkdir()
        (directory / ".state" / "icmp.json").write_text("{", encoding="utf-8")
        app = create_app(config)
        underlying = httpx.AsyncClient(transport=httpx.MockTransport(self.respond))
        with (
            patch("dashboard.api_client.httpx.AsyncClient", return_value=underlying),
            self.assertRaises(ValueError),
        ):
            async with app.router.lifespan_context(app):
                self.fail("Corrupt state must prevent startup")
        self.assertTrue(underlying.is_closed)
