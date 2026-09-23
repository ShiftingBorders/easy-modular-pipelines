"""Approved C-F: real HTTP runtime, collector, stage processes, journal and rollback."""

import asyncio
import json
from datetime import datetime

import httpx
import yaml

from dashboard.application import create_app
from tests.dashboard_tests.helpers import write_settings
from tests.helpers.http_runtime import ServerTestCase


class RuntimeIntegrationTests(ServerTestCase):
    async def test_real_run_step_snapshot_resume_rollback_and_offline_history(self):
        """T074/T076: real command effects, rollback generation and offline history."""
        template = self.w.template()
        template["cycles"] = 2
        path = self.w.source / "dashboard.yaml"
        path.write_text(yaml.safe_dump(template), encoding="utf-8")
        server = await self.start_server()
        directory = self.w.root / "dashboard"
        directory.mkdir()
        config = write_settings(
            directory,
            project_root=str(self.w.source),
            system_api_url=server.url + "/",
            refresh_seconds=1,
        )
        app = create_app(config)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://dashboard.test",
                headers={"X-Dashboard-Request": "1"},
            ) as client,
        ):

            async def command(name, args=None):
                submitted = await client.post(
                    "/api/commands", json={"command": name, "args": args or {}}
                )
                self.assertEqual(submitted.status_code, 202, submitted.text)
                async with asyncio.timeout(60):
                    while True:
                        response = await client.get(
                            "/api/commands/" + submitted.json()["command_id"]
                        )
                        result = response.json()
                        if result["state"] != "pending":
                            break
                        await asyncio.sleep(0.025)
                self.assertEqual(result["result"], "success", result)
                return result

            self.assertEqual(
                (await client.get("/api/system/experiments")).json()["items"], []
            )
            async with asyncio.timeout(15):
                while True:
                    app.state.views._resource_at = 0
                    await app.state.views._refresh_resources()
                    if (await client.get("/api/system/compute")).json()["collector"][
                        "state"
                    ] == "running":
                        break
                    await asyncio.sleep(0.05)
            self.assertEqual(
                (await client.get("/api/system/compute")).json()["collector"]["state"],
                "running",
            )
            await command(
                "run",
                {
                    "experiment_id": "dashboard-real",
                    "template_path": str(path),
                    "delayed_start": True,
                },
            )
            await server.wait_state("waiting")
            await command("step")
            app.state.views.journals.load("dashboard-real", force=True)
            base = "/api/system/experiments/dashboard-real/"
            parameters = (await client.get(base + "parameters")).json()["items"]
            attempt = parameters[0]
            measured = (
                datetime.fromisoformat(attempt["finished_at"])
                - datetime.fromisoformat(attempt["started_at"])
            ).total_seconds()
            self.assertGreater(measured, 0)
            self.assertAlmostEqual(attempt["duration_seconds"], measured, places=5)
            forecast = (await client.get(base + "forecast")).json()
            self.assertEqual(forecast["sample_cycles"], 1)
            self.assertAlmostEqual(forecast["sample_mean_seconds"], measured, places=5)
            self.assertAlmostEqual(forecast["eta_seconds"], measured, places=5)
            snapshot = (await command("snapshot"))["data"]["snapshot_id"]
            page = (await client.get(base + "events", params={"limit": 1})).json()
            self.assertIn(
                snapshot,
                [
                    row["snapshot_id"]
                    for row in (await client.get(base + "snapshots")).json()["items"]
                ],
            )
            artifacts = (await client.get(base + "artifacts")).json()["items"]
            download = await client.get(
                "/api/experiments/dashboard-real/artifacts/"
                + artifacts[0]["artifact_id"]
                + "/download"
            )
            self.assertEqual(download.status_code, 200)
            await command("resume")
            await server.wait_state("completed")
            await command("rollback", {"snapshot_id": snapshot})
            stale = await client.get(
                base + "events", params={"cursor": json.dumps(page["next_cursor"])}
            )
            self.assertEqual(stale.status_code, 409, stale.text)
            summary = (await client.get(base + "summary")).json()
            self.assertNotEqual(
                summary["journal"]["generation"], page["journal"]["generation"]
            )
            self.assertEqual(summary["completed_cycles"], 1)
            await command("stop")
            await server.close()
            app.state.views._live_at = 0
            await app.state.views.state(refresh=True)
            self.assertFalse(
                (await client.get("/api/application")).json()["system_connection"][
                    "connected"
                ]
            )
            self.assertEqual((await client.get(base + "events")).status_code, 200)
            self.assertFalse((await client.get(base + "summary")).json()["fresh"])
