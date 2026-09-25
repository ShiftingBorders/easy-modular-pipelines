"""Independent HTTP server: uv run python webserver.py --config /path/server.json."""

from __future__ import annotations

import argparse
import asyncio
import io
import ipaddress
import json
import math
import os
import secrets
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse

from core.logger_utils.events import copy_json_object
from core.runner_utils.state import JsonObject
from core.serverruntime import (
    ServerError,
    ServerRuntime,
    ServerSettings,
    load_server_settings,
)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    """Own the controller for the application lifespan, independently of HTTP clients."""
    settings = getattr(application.state, "settings", None)
    if settings is None:
        configured = os.environ.get("EMP_SERVER_CONFIG")
        settings = load_server_settings(
            None if configured is None else Path(configured)
        )
    if not isinstance(settings, ServerSettings):
        raise TypeError("Application settings must be ServerSettings.")
    token = None if settings.token_env is None else os.environ.get(settings.token_env)
    if settings.token_env is not None and not token:
        raise ValueError(
            f"Server token environment variable is empty: {settings.token_env}"
        )
    if token is not None and any(
        not 33 <= ord(character) <= 126 for character in token
    ):
        raise ValueError(
            "The bearer token must contain printable ASCII without whitespace."
        )
    try:
        loopback = ipaddress.ip_address(settings.host).is_loopback
    except ValueError:
        loopback = settings.host.lower() == "localhost"
    if not loopback and token is None:
        raise ValueError(
            "A non-loopback listener requires token_env with a configured token."
        )
    runtime = ServerRuntime(
        settings, stop_http=getattr(application.state, "stop_http", None)
    )
    application.state.runtime = runtime
    application.state.api_token = token
    try:
        await runtime.start()
        yield
    finally:
        await runtime.close()


def runtime_for(request: Request) -> ServerRuntime:
    runtime = getattr(request.app.state, "runtime", None)
    if not isinstance(runtime, ServerRuntime):
        raise ServerError("server_unavailable", "Server runtime is not initialized.")
    token = request.app.state.api_token
    if token is not None and not secrets.compare_digest(
        request.headers.get("authorization", "").encode("utf-8"),
        f"Bearer {token}".encode(),
    ):
        raise ServerError("unauthorized", "A valid bearer token is required.", 401)
    return runtime


async def request_document(request: Request, runtime: ServerRuntime) -> JsonObject:
    if (
        request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        != "application/json"
    ):
        raise ServerError(
            "unsupported_media_type", "Use Content-Type: application/json.", 415
        )
    payload = bytearray()
    try:
        async with asyncio.timeout(runtime.settings.read_timeout):
            async for chunk in request.stream():
                if len(payload) + len(chunk) > runtime.settings.max_request_bytes:
                    raise ServerError(
                        "request_too_large",
                        "Request body exceeds its configured limit.",
                        413,
                    )
                payload.extend(chunk)
    except TimeoutError as error:
        raise ServerError(
            "request_timeout", "Request body was not received before the deadline.", 408
        ) from error
    try:
        return copy_json_object(json.loads(payload), "request body")
    except (ValueError, TypeError, UnicodeError) as error:
        raise ServerError("invalid_request", str(error), 400) from error


async def server_error(request: Request, error: Exception) -> JSONResponse:
    if not isinstance(error, ServerError):
        raise error
    runtime = getattr(request.app.state, "runtime", None)
    headers = {"WWW-Authenticate": "Bearer"} if error.status == 401 else None
    return JSONResponse(
        status_code=error.status,
        content={
            "server_instance_id": None if runtime is None else runtime.instance_id,
            "error": {"code": error.code, "message": str(error), "details": {}},
        },
        headers=headers,
    )


async def health(request: Request) -> JSONResponse:
    """Observe process liveness; this does not claim the controller is responsive."""
    data = runtime_for(request).health()
    return JSONResponse(
        data,
        status_code=200
        if data["state"] == "ready" and data["controller_alive"]
        else 503,
    )


async def submit_command(request: Request, wait: str = "false") -> JSONResponse:
    """Submit a command; HTTP 202 is admission, not completion.

    Maintenance accepts module.add {folder}, module.validate {folder} or
    {name, version}, and module.remove {name, version}. Successful add and
    stored validation return data.module with name/version/hash for templates.
    Module folders are absolute paths on the server. Run mode rejects module.*.
    For server.shutdown only, wait=true keeps this response open until runtime
    cleanup finishes; the HTTP owner drains this response before exiting.
    """
    runtime = runtime_for(request)
    document = await request_document(request, runtime)
    query_fields(
        request, {"wait"} if document.get("command") == "server.shutdown" else set()
    )
    if wait not in ("true", "false"):
        raise ServerError("invalid_request", "wait must be true or false.", 400)
    try:
        result = runtime.submit(document)
    except (TypeError, ValueError, KeyError) as error:
        raise ServerError("invalid_request", str(error), 400) from error
    if wait == "true" and result["state"] == "pending":
        await asyncio.shield(runtime._restart_task)
        result = runtime.result(result["command_id"])
    return JSONResponse(
        result,
        status_code=200 if wait == "true" else 202,
        headers={
            "Location": str(
                request.url_for("command_result", command_id=result["command_id"])
            )
        },
    )


async def submit_chain(request: Request) -> JSONResponse:
    """Submit a commands array with optional chain_id; each command keeps its own result."""
    runtime = runtime_for(request)
    document = await request_document(request, runtime)
    try:
        result = runtime.submit(document, chain=True)
    except (TypeError, ValueError, KeyError) as error:
        raise ServerError("invalid_request", str(error), 400) from error
    return JSONResponse(result, status_code=202)


async def command_result(command_id: str, request: Request) -> JsonObject:
    """Return a pending or terminal outcome; expired/unknown IDs are never replayed here."""
    runtime = runtime_for(request)
    try:
        return runtime.result(command_id)
    except ValueError as error:
        raise ServerError("invalid_request", str(error), 400) from error


async def read_controller(
    request: Request, command: str, args: JsonObject | None = None
) -> JSONResponse:
    runtime = runtime_for(request)
    result = await runtime.read(command, args)
    if result.get("result") != "success":
        error = copy_json_object(result.get("error", {}), "controller error")
        status = {
            "invalid_request": 400,
            "not_found": 404,
            "invalid_state": 409,
            "invalid_mode": 409,
            "unsupported_feature": 501,
            "journal_unavailable": 503,
            "too_many_reads": 429,
            "response_too_large": 502,
        }.get(str(error.get("code")), 500)
        return JSONResponse(
            {**result, "server_instance_id": runtime.instance_id}, status_code=status
        )
    data = copy_json_object(result.get("data"), "read response data")
    return JSONResponse({**data, "server_instance_id": runtime.instance_id})


def query_fields(request: Request, allowed: set[str]) -> None:
    if set(request.query_params) - allowed:
        raise ServerError("invalid_request", "Unknown query parameters.", 400)
    if len(request.query_params.multi_items()) != len(request.query_params):
        raise ServerError("invalid_request", "Duplicate query parameters.", 400)


async def state(request: Request, experiment_id: str | None = None) -> JSONResponse:
    query_fields(request, {"experiment_id"})
    return await read_controller(
        request,
        "stats.state",
        {} if experiment_id is None else {"experiment_id": experiment_id},
    )


async def experiments(request: Request) -> JSONResponse:
    query_fields(request, set())
    return await read_controller(request, "stats.experiments")


async def experiment(request: Request, experiment_id: str) -> JSONResponse:
    query_fields(request, {"experiment_id"})
    return await read_controller(
        request, "stats.experiment", {"experiment_id": experiment_id}
    )


async def snapshots(request: Request, experiment_id: str | None = None) -> JSONResponse:
    query_fields(request, {"experiment_id"})
    return await read_controller(
        request,
        "stats.snapshots",
        {} if experiment_id is None else {"experiment_id": experiment_id},
    )


async def snapshot(
    request: Request, snapshot_id: str, experiment_id: str | None = None
) -> JSONResponse:
    query_fields(request, {"snapshot_id", "experiment_id"})
    args = {"snapshot_id": snapshot_id}
    if experiment_id is not None:
        args["experiment_id"] = experiment_id
    return await read_controller(request, "stats.snapshot", args)


async def modules(request: Request) -> JSONResponse:
    query_fields(request, set())
    return await read_controller(request, "stats.modules")


async def module(request: Request, name: str, version: str) -> JSONResponse:
    query_fields(request, {"name", "version"})
    return await read_controller(
        request, "stats.module", {"name": name, "version": version}
    )


async def validate_template(request: Request) -> JSONResponse:
    query_fields(request, set())
    document = await request_document(request, runtime_for(request))
    args = copy_json_object(document, "template validation")
    return await read_controller(request, "stats.template", args)


async def commands(
    request: Request,
    after: str | None = None,
    limit: int = 100,
    state: str | None = None,
    command: str | None = None,
) -> JSONResponse:
    query_fields(request, {"after", "limit", "state", "command"})
    try:
        result = runtime_for(request).list_commands(
            after=after, limit=limit, state=state, command=command
        )
    except ValueError as error:
        raise ServerError("invalid_request", str(error), 400) from error
    response = JSONResponse(result)
    if len(response.body) > runtime_for(request).settings.max_response_bytes:
        raise ServerError(
            "response_too_large",
            "Command list exceeds the response limit; reduce limit.",
            502,
        )
    return response


async def artifacts(request: Request, experiment_id: str) -> JSONResponse:
    query_fields(request, {"experiment_id"})
    return await read_controller(
        request, "stats.artifacts", {"experiment_id": experiment_id}
    )


async def download_artifact(request: Request, experiment_id: str, artifact_id: str):
    query_fields(request, {"experiment_id", "artifact_id"})
    response = await read_controller(
        request,
        "stats.artifact",
        {
            "experiment_id": experiment_id,
            "artifact_id": artifact_id,
        },
    )
    if response.status_code != 200:
        return response
    document = json.loads(response.body)
    path = Path(document["path"]).resolve()
    root = runtime_for(request).settings.project_root.resolve()
    if not path.is_relative_to(root / "experiments"):
        raise ServerError("invalid_artifact", "Artifact escapes the project.", 400)
    if not path.is_file():
        raise ServerError("not_found", "Artifact file is no longer available.", 404)
    return FileResponse(path, media_type="application/octet-stream", filename=path.name)


async def resources(request: Request) -> JSONResponse:
    query_fields(request, set())
    return await read_controller(request, "stats.resources")


async def resource_history(
    request: Request, after: int = 0, limit: int = 100
) -> JSONResponse:
    query_fields(request, {"after", "limit"})
    if after < 0 or not 1 <= limit <= 1000:
        raise ServerError(
            "invalid_request",
            "after must be nonnegative and limit must be 1..1000.",
            400,
        )
    return await read_controller(
        request, "stats.resources.history", {"after": after, "limit": limit}
    )


async def events(
    experiment_id: str, request: Request, cursor: str | None = None, limit: int = 100
) -> JSONResponse:
    """Read the selected experiment's journal using its original opaque JSON cursor."""
    query_fields(request, {"cursor", "limit"})
    if not 1 <= limit <= 1000:
        raise ServerError("invalid_request", "limit must be 1..1000.", 400)
    checkpoint = None
    if cursor is not None:
        if len(cursor) > 4096:
            raise ServerError("invalid_request", "Journal cursor is too large.", 400)
        try:
            checkpoint = copy_json_object(json.loads(cursor), "cursor")
        except (ValueError, TypeError) as error:
            raise ServerError("invalid_request", str(error), 400) from error
    return await read_controller(
        request,
        "logs.read",
        {"experiment_id": experiment_id, "cursor": checkpoint, "limit": limit},
    )


app = FastAPI(title="Easy Modular Pipelines API", version="1", lifespan=lifespan)
COMMAND_SCHEMA = {
    "type": "object",
    "required": ["command"],
    "additionalProperties": False,
    "properties": {
        "api_version": {"type": "integer", "const": 1, "default": 1},
        "command_id": {"type": "string", "format": "uuid"},
        "command": {"type": "string", "minLength": 1},
        "args": {"type": "object", "additionalProperties": True},
        "target": {
            "type": "object",
            "required": ["kind", "position"],
            "additionalProperties": False,
            "properties": {
                "kind": {"enum": ["stage", "service"]},
                "position": {"type": "integer", "minimum": 1},
            },
        },
    },
}
CHAIN_SCHEMA = {
    "type": "object",
    "required": ["commands"],
    "additionalProperties": False,
    "properties": {
        "api_version": {"type": "integer", "const": 1, "default": 1},
        "chain_id": {"type": "string", "format": "uuid"},
        "commands": {"type": "array", "minItems": 1, "items": COMMAND_SCHEMA},
    },
}
app.add_exception_handler(ServerError, server_error)
app.add_api_route("/api/health", health, methods=["GET"])
app.add_api_route(
    "/api/commands",
    submit_command,
    methods=["POST"],
    status_code=202,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": COMMAND_SCHEMA}},
        }
    },
)
app.add_api_route(
    "/api/chains",
    submit_chain,
    methods=["POST"],
    status_code=202,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": CHAIN_SCHEMA}},
        }
    },
)
app.add_api_route("/api/commands/{command_id}", command_result, methods=["GET"])
app.add_api_route("/api/commands", commands, methods=["GET"])
app.add_api_route("/api/modules", modules, methods=["GET"])
app.add_api_route("/api/modules/inspect", module, methods=["GET"])
app.add_api_route("/api/templates/validate", validate_template, methods=["POST"])
app.add_api_route("/api/artifacts", artifacts, methods=["GET"])
app.add_api_route("/api/artifacts/download", download_artifact, methods=["GET"])
app.add_api_route("/api/state", state, methods=["GET"])
app.add_api_route("/api/experiments", experiments, methods=["GET"])
app.add_api_route("/api/experiments/inspect", experiment, methods=["GET"])
app.add_api_route("/api/snapshots", snapshots, methods=["GET"])
app.add_api_route("/api/snapshots/inspect", snapshot, methods=["GET"])
app.add_api_route("/api/resources", resources, methods=["GET"])
app.add_api_route("/api/resources/history", resource_history, methods=["GET"])
app.add_api_route(
    "/api/experiments/{experiment_id:path}/events", events, methods=["GET"]
)


def main() -> None:
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        if isinstance(stream, io.TextIOWrapper):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        help="Server JSON settings; relative entries resolve from this file.",
    )
    parser.add_argument(
        "--project-root", type=Path, help="Project directory owned by this server."
    )
    parser.add_argument(
        "--mode", choices=("run", "maintenance"), help="Server operating mode."
    )
    parser.add_argument(
        "--hash-config",
        type=Path,
        help="Existing HashDB JSON configuration; otherwise initialize project hash_db.",
    )
    parser.add_argument(
        "--filer-url",
        help="Use an external Filer instead of the owned local SeaweedFS.",
    )
    parser.add_argument(
        "--resource-config", type=Path, help="Resource collector settings JSON."
    )
    parser.add_argument(
        "--archive-config", type=Path, help="Experiment archiver settings JSON."
    )
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument(
        "--token-env", help="Environment variable containing the bearer token."
    )
    parser.add_argument(
        "--log-level",
        choices=("critical", "error", "warning", "info", "debug"),
        default="info",
    )
    options = parser.parse_args()
    overrides: JsonObject = {}
    if options.mode is not None:
        overrides["server_mode"] = options.mode
    for name, value in (
        ("project_root", options.project_root),
        ("hash_config_path", options.hash_config),
        ("resource_config_path", options.resource_config),
        ("archive_config_path", options.archive_config),
    ):
        if value is not None:
            overrides[name] = str(value.resolve())
    for name in ("host", "port", "token_env", "filer_url"):
        value = getattr(options, name)
        if value is not None:
            overrides[name] = value
    try:
        configured = os.environ.get("EMP_SERVER_CONFIG")
        config = (
            (None if configured is None else Path(configured))
            if options.config is None
            else options.config.resolve()
        )
        settings = load_server_settings(config, overrides)
    except (OSError, TypeError, ValueError, KeyError) as error:
        parser.error(str(error))
    app.state.settings = settings
    # The project lock also rejects competing owners started by an external manager.
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=settings.host,
            port=settings.port,
            workers=1,
            log_level=options.log_level,
            timeout_graceful_shutdown=math.ceil(settings.shutdown_timeout + 5),
        )
    )
    app.state.stop_http = partial(setattr, server, "should_exit", True)
    server.run()
    if not server.started:
        raise SystemExit(3)


if __name__ == "__main__":
    main()
