"""Independent FastAPI application and its explicit HTTP boundaries."""

import json
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

from dashboard.alerts import AlertMonitor
from dashboard.api_client import SystemAPIClient, SystemAPIError
from dashboard.config import load_settings
from dashboard.icmp import ICMPMonitor
from dashboard.notifications import deliver
from dashboard.views import DashboardViews


def query_parameters(request: Request) -> dict:
    allowed = {
        "run_id",
        "view",
        "cursor",
        "limit",
        "q",
        "module",
        "metric",
        "since",
        "until",
        "revision",
        "ref",
        "compact",
    }
    if request.query_params.keys() - allowed:
        raise HTTPException(400, "Unknown query parameter.")
    result = dict(request.query_params)
    for value in result.values():
        if len(value) > 4096:
            raise HTTPException(400, "Query parameter is too long.")
    if "limit" in result:
        try:
            limit = int(result["limit"])
        except ValueError as error:
            raise HTTPException(400, "limit must be an integer.") from error
        if not 1 <= limit <= 1000:
            raise HTTPException(400, "limit must be between 1 and 1000.")
    return result


def check_write_origin(request: Request) -> None:
    origin = request.headers.get("origin")
    if origin is not None:
        parsed = urlsplit(origin)
        if parsed.scheme != request.url.scheme or parsed.netloc != request.url.netloc:
            raise HTTPException(403, "Cross-origin writes are not allowed.")
    if request.headers.get("x-dashboard-request") != "1":
        raise HTTPException(403, "Dashboard request header is required.")


def create_app(
    config_path: str | Path | None = None, *, overrides: dict | None = None
) -> FastAPI:
    settings = load_settings(
        Path(config_path) if config_path else Path(__file__).with_name("settings.json"),
        overrides,
    )
    system = SystemAPIClient(settings)
    monitor = ICMPMonitor(settings["state_directory"])
    views = DashboardViews(settings, system)
    alerts = AlertMonitor(settings["state_directory"], views, monitor)

    async def lifespan(application: FastAPI):
        await system.open()
        try:
            await monitor.open()
            await views.open()
            await alerts.open()
            yield
        finally:
            await alerts.close()
            await views.close()
            await monitor.close()
            await system.close()

    app = FastAPI(title="EMP Dashboard", lifespan=asynccontextmanager(lifespan))
    app.state.settings = settings
    app.state.icmp = monitor
    app.state.views = views
    app.state.alerts = alerts
    assets = Path(__file__).parent / "static"

    async def index() -> FileResponse:
        return FileResponse(
            assets / "index.html", headers={"Cache-Control": "no-cache"}
        )

    async def information() -> dict:
        live = await views.state()
        return {
            "application": "EMP Dashboard",
            "system_api_configured": system.base_url is not None,
            "refresh_seconds": settings["refresh_seconds"],
            "icmp_source": "dashboard_host",
            "dashboard_host": monitor.host_name,
            "journals_configured": settings["project_root"] is not None,
            "data_mode": "local_journals_and_system_api",
            "cache_activity": views.cache_activity(),
            "system_connection": {
                "connected": bool(live.get("available") and live.get("fresh")),
                "observed_at": live.get("observed_at"),
                "error": live.get("connection_error")
                if not live.get("available")
                else None
                if live.get("fresh")
                else "Runtime observation is stale.",
            },
        }

    async def read_resource(resource: str, request: Request) -> dict:
        paths = {
            "overview": "overview",
            "experiments": "experiments",
            "modules": "modules",
            "services": "services",
            "compute": "compute",
            "alerts": "alerts",
        }
        if resource not in paths:
            raise HTTPException(404, "Unknown resource.")
        params = query_parameters(request)
        if resource == "alerts":
            return alerts.status(system_only=True)
        result = await views.read(resource, params)
        if resource == "overview":
            result["active_alerts"] = alerts.status(system_only=True)["active_count"]
        metrics = (
            result.get("compute")
            if resource == "overview"
            else result.get("metrics")
            if resource == "compute"
            else None
        )
        if metrics is not None:
            for rule in alerts.rules:
                if not rule["enabled"] or rule["kind"] != "resource":
                    continue
                key = rule["metric"]
                metric = metrics.get(
                    "disk"
                    if key == "disk_free_gib"
                    else "internet"
                    if key.startswith("internet_")
                    else key,
                    {},
                )
                value = metric.get("value")
                if key == "disk_free_gib":
                    value = metric.get("free_bytes")
                    value = value / 1073741824 if value is not None else None
                elif key.startswith("internet_"):
                    value = metric.get(key.removeprefix("internet_") + "_mbps")
                if value is not None and metric.get("fresh"):
                    metric["exceeded"] = metric.get("exceeded", False) or (
                        value > rule["threshold"]
                        if rule["operator"] == "above"
                        else value < rule["threshold"]
                    )
        return result

    async def read_experiment(
        experiment_id: str, view: str, request: Request
    ) -> Response:
        allowed_views = {
            "summary",
            "runs",
            "operations",
            "timeline",
            "events",
            "errors",
            "measurements",
            "template",
            "parameters",
            "commands",
            "snapshots",
            "artifacts",
            "forecast",
            "detail",
        }
        if view not in allowed_views:
            raise HTTPException(404, "Unknown experiment view.")
        if (
            len(experiment_id) > 512
            or experiment_id in {".", ".."}
            or any(char in experiment_id for char in "/\\\x00")
        ):
            raise HTTPException(400, "Invalid experiment identifier.")
        result = await views.experiment(
            experiment_id, view, query_parameters(request), defer_cache=True
        )
        encoded = json.dumps(result, ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
        if len(encoded) > settings["max_response_bytes"]:
            raise SystemAPIError(
                "response_too_large",
                "This response exceeds max_response_bytes; request a smaller page.",
                413,
            )
        background = None
        if views._cache_pool is not None and experiment_id in views._cache_requests:
            background = BackgroundTask(views._submit_cache, experiment_id)
        return Response(
            content=encoded, media_type="application/json", background=background
        )

    async def download_artifact(experiment_id: str, artifact_id: str) -> FileResponse:
        import asyncio

        path = await asyncio.to_thread(
            views.journals.artifact, experiment_id, artifact_id
        )
        return FileResponse(
            path, filename=path.name, media_type="application/octet-stream"
        )

    async def command(request: Request) -> dict:
        check_write_origin(request)
        payload = await read_document(request)
        try:
            return await views.command(payload)
        except (ValueError, TypeError) as error:
            raise HTTPException(422, str(error)) from error
        except OSError as error:
            raise HTTPException(
                503,
                "Could not persist command status; check its outcome before submitting again.",
            ) from error

    async def command_result(command_id: str) -> dict:
        from uuid import UUID

        try:
            UUID(command_id)
        except ValueError as error:
            raise HTTPException(400, "Invalid command ID.") from error
        return await views.command_result(command_id)

    async def read_document(request: Request) -> dict:
        if request.headers.get("content-type", "").split(";")[0] != "application/json":
            raise HTTPException(415, "Use application/json.")
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > 1048576:
                raise HTTPException(413, "Request is too large.")
            body.extend(chunk)
        try:
            document = json.loads(body)
            if not isinstance(document, dict):
                raise TypeError("Expected a JSON object.")
            return document
        except (TypeError, ValueError, UnicodeError) as error:
            raise HTTPException(422, str(error)) from error

    async def alert_status() -> dict:
        return alerts.status()

    async def alert_rule(request: Request) -> dict:
        check_write_origin(request)
        try:
            return await alerts.configure(await read_document(request))
        except (ValueError, TypeError) as error:
            raise HTTPException(422, str(error)) from error
        except OSError as error:
            raise HTTPException(
                503, "Could not persist Alert settings; no changes were applied."
            ) from error

    async def delete_rule(rule_id: str, request: Request) -> dict:
        check_write_origin(request)
        try:
            return await alerts.configure(delete=rule_id)
        except OSError as error:
            raise HTTPException(
                503, "Could not persist Alert settings; no changes were applied."
            ) from error

    async def notification_settings(request: Request) -> dict:
        check_write_origin(request)
        try:
            return await alerts.configure(channels=await read_document(request))
        except (TypeError, ValueError) as error:
            raise HTTPException(422, str(error)) from error
        except OSError as error:
            raise HTTPException(
                503, "Could not persist notification settings; no changes were applied."
            ) from error

    async def test_notification(request: Request) -> dict:
        check_write_origin(request)
        return await deliver(
            "EMP Dashboard", f"Notification from {monitor.host_name}", alerts.channels
        )

    async def read_icmp() -> dict:
        return monitor.snapshot()

    async def configure_icmp(request: Request) -> dict:
        check_write_origin(request)
        if request.headers.get("content-type", "").split(";")[0] != "application/json":
            raise HTTPException(415, "ICMP settings require application/json.")
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 4096:
                raise HTTPException(413, "ICMP settings are too large.")
        try:
            return await monitor.configure(json.loads(body))
        except (TypeError, ValueError, UnicodeError) as error:
            raise HTTPException(422, str(error)) from error
        except OSError as error:
            raise HTTPException(
                503, "Could not persist ICMP settings; no changes were applied."
            ) from error

    async def probe_icmp(request: Request) -> dict:
        check_write_origin(request)
        try:
            return await monitor.probe()
        except ValueError as error:
            raise HTTPException(409, str(error)) from error

    async def api_failure(request: Request, error: SystemAPIError) -> JSONResponse:
        return JSONResponse(
            {
                "error": {
                    "code": error.code,
                    "message": str(error),
                    "upstream_code": error.upstream_code,
                },
                "available": False,
            },
            status_code=error.status_code,
            headers={"Cache-Control": "no-store"},
        )

    app.add_exception_handler(SystemAPIError, api_failure)
    app.add_api_route("/", index, methods=["GET"], include_in_schema=False)
    app.add_api_route("/api/application", information, methods=["GET"])
    app.add_api_route("/api/icmp", read_icmp, methods=["GET"])
    app.add_api_route("/api/icmp/settings", configure_icmp, methods=["PUT"])
    app.add_api_route("/api/icmp/probe", probe_icmp, methods=["POST"])
    app.add_api_route("/api/commands", command, methods=["POST"], status_code=202)
    app.add_api_route("/api/commands/{command_id}", command_result, methods=["GET"])
    app.add_api_route("/api/alerts", alert_status, methods=["GET"])
    app.add_api_route("/api/alerts/rules", alert_rule, methods=["POST"])
    app.add_api_route("/api/alerts/rules/{rule_id}", delete_rule, methods=["DELETE"])
    app.add_api_route(
        "/api/alerts/notifications", notification_settings, methods=["PUT"]
    )
    app.add_api_route(
        "/api/alerts/notifications/test", test_notification, methods=["POST"]
    )
    app.add_api_route(
        "/api/experiments/{experiment_id}/artifacts/{artifact_id}/download",
        download_artifact,
        methods=["GET"],
    )
    app.add_api_route(
        "/api/system/experiments/{experiment_id}/{view}",
        read_experiment,
        methods=["GET"],
    )
    app.add_api_route("/api/system/{resource}", read_resource, methods=["GET"])
    app.mount("/static", StaticFiles(directory=assets), name="static")
    return app
