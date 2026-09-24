"""HTTP client for an independently running experiment server."""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import shlex
import sys
import tempfile
import time
from pathlib import Path
from typing import Self
from urllib.parse import quote, urlsplit
from uuid import UUID, uuid4

import httpx

from core.logger_utils.events import (
    JsonObject,
    copy_json_object,
    require_number,
    require_text,
)

DEFAULT_CONFIG = Path(__file__).resolve().parent / "default_settings/cli.json"


class ClientError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        exit_code: int = 3,
        details: JsonObject | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code
        self.details = details or {}


def positive_number(value: str) -> float:
    try:
        number = require_number(float(value), "number")
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError(
            "Expected a finite positive number."
        ) from error
    if number <= 0:
        raise argparse.ArgumentTypeError("Expected a finite positive number.")
    return float(number)


def positive_integer(value: str) -> int:
    try:
        result = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("Expected a positive integer.") from error
    if result < 1:
        raise argparse.ArgumentTypeError("Expected a positive integer.")
    return result


def uuid_text(value: str) -> str:
    try:
        return str(UUID(value))
    except ValueError as error:
        raise argparse.ArgumentTypeError("Expected a UUID.") from error


def json_argument(value: str) -> JsonObject:
    """Read a JSON object literal or @file; command paths stay in the server namespace."""
    text = (
        Path(value[1:]).read_text(encoding="utf-8") if value.startswith("@") else value
    )
    return copy_json_object(json.loads(text), "JSON argument")


def command_receipt(value: object) -> JsonObject:
    """Reject malformed success responses instead of treating absent fields as success."""
    try:
        result = copy_json_object(value, "command receipt")
        UUID(require_text(result.get("command_id"), "command_id"))
        UUID(require_text(result.get("server_instance_id"), "server_instance_id"))
        expected = {
            "pending": None,
            "unknown": None,
            "unavailable": None,
            "succeeded": "success",
            "failed": "fail",
            "cancelled": "fail",
        }
        state = require_text(result.get("state"), "state")
        if (
            state not in expected
            or "result" not in result
            or result["result"] != expected[state]
        ):
            raise ValueError("Invalid command state/result combination.")
        return result
    except (ValueError, TypeError) as error:
        raise ClientError(
            "Server returned an invalid command receipt.", code="invalid_response"
        ) from error


def load_settings(config_path: Path | None, overrides: JsonObject) -> JsonObject:
    if config_path is not None and not config_path.is_absolute():
        raise ValueError("config_path must be absolute.")
    settings = copy_json_object(
        json.loads(DEFAULT_CONFIG.read_text(encoding="utf-8")), "CLI settings"
    )
    fields = set(settings)
    if config_path is not None and config_path.resolve() != DEFAULT_CONFIG:
        custom = copy_json_object(
            json.loads(config_path.read_text(encoding="utf-8")), "CLI settings"
        )
        if custom.keys() - fields:
            raise ValueError(f"Unknown CLI settings: {sorted(custom.keys() - fields)}")
        settings.update(custom)
    settings.update(overrides)
    if type(settings["schema_version"]) is not int or settings["schema_version"] != 1:
        raise ValueError("Only CLI settings schema_version 1 is supported.")
    url = urlsplit(require_text(settings["server_url"], "server_url"))
    if (
        url.scheme not in ("http", "https")
        or not url.hostname
        or url.username is not None
        or url.password is not None
        or url.query
        or url.fragment
    ):
        raise ValueError(
            "server_url must be an HTTP(S) base URL without credentials, query or fragment."
        )
    if url.port is not None and not 1 <= url.port <= 65535:
        raise ValueError("Invalid server_url port.")
    if settings["token_env"] is not None:
        require_text(settings["token_env"], "token_env")
    for name in (
        "request_timeout_seconds",
        "wait_timeout_seconds",
        "poll_interval_seconds",
    ):
        if require_number(settings[name], name) <= 0:
            raise ValueError(f"{name} must be positive.")
    value = settings["max_response_bytes"]
    if type(value) is not int or value < 1:
        raise ValueError("max_response_bytes must be a positive integer.")
    return settings


class APIClient:
    """Own only an HTTP connection; closing it never sends a runtime stop."""

    def __init__(self, settings: JsonObject) -> None:
        self.url = require_text(settings["server_url"], "server_url").rstrip("/")
        self.timeout = require_number(
            settings["request_timeout_seconds"], "request_timeout_seconds"
        )
        self.wait_timeout = require_number(
            settings["wait_timeout_seconds"], "wait_timeout_seconds"
        )
        self.interval = require_number(
            settings["poll_interval_seconds"], "poll_interval_seconds"
        )
        value = settings["max_response_bytes"]
        if type(value) is not int or value < 1:
            raise ValueError("max_response_bytes must be positive.")
        self.max_bytes = value
        token_env = settings["token_env"]
        self.token = (
            None
            if token_env is None
            else os.environ.get(require_text(token_env, "token_env"))
        )
        if token_env is not None and not self.token:
            raise ValueError(f"CLI token environment variable is empty: {token_env}")
        if self.token is not None and any(
            not 33 <= ord(character) <= 126 for character in self.token
        ):
            raise ValueError(
                "The bearer token must contain printable ASCII without whitespace."
            )
        self.http: httpx.AsyncClient | None = None

    async def __aenter__(self) -> Self:
        self.http = httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=False,
            trust_env=False,
            headers={}
            if self.token is None
            else {"Authorization": f"Bearer {self.token}"},
        )
        return self

    async def __aexit__(self, *_error: object) -> None:
        if self.http is not None:
            await self.http.aclose()
            self.http = None

    async def request(
        self,
        method: str,
        path: str,
        *,
        document: JsonObject | None = None,
        params: dict[str, str | int] | None = None,
        timeout: float | None = None,
    ) -> JsonObject:
        if self.http is None:
            raise RuntimeError("HTTP client is not open.")
        deadline = (
            self.timeout
            if timeout is None
            else require_number(timeout, "request timeout")
        )
        if deadline <= 0:
            raise ValueError("Request timeout must be positive.")
        try:
            async with asyncio.timeout(deadline):
                async with self.http.stream(
                    method,
                    self.url + path,
                    json=document,
                    params=params,
                    timeout=deadline,
                ) as response:
                    payload = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(payload) + len(chunk) > self.max_bytes:
                            raise ClientError(
                                "Server response exceeds its configured size limit.",
                                code="response_too_large",
                            )
                        payload.extend(chunk)
                    try:
                        result = copy_json_object(
                            json.loads(payload), "server response"
                        )
                    except (ValueError, TypeError, UnicodeError) as error:
                        raise ClientError(
                            "Server returned invalid JSON.", code="invalid_response"
                        ) from error
                    if response.status_code not in (200, 202):
                        failure = result.get("error")
                        if isinstance(failure, dict):
                            message = str(
                                failure.get("message", "Server rejected the request.")
                            )
                            code = str(failure.get("code", "http_error"))
                        else:
                            message = f"HTTP {response.status_code}: {result.get('detail', result)}"
                            code = "http_error"
                        raise ClientError(
                            message, code=code, exit_code=1, details=result
                        )
                    return result
        except (TimeoutError, httpx.TimeoutException) as error:
            raise ClientError(
                "HTTP deadline expired; an accepted command may still be running.",
                code="http_timeout",
            ) from error
        except httpx.HTTPError as error:
            raise ClientError(
                "Cannot communicate with the server; no command was automatically retried.",
                code="connection_error",
            ) from error

    async def download(
        self, experiment_id: str, artifact_id: str, destination: Path
    ) -> JsonObject:
        if self.http is None:
            raise RuntimeError("HTTP client is not open.")
        destination = destination.absolute()
        if destination.exists():
            raise FileExistsError(f"Destination already exists: {destination}")
        try:
            with tempfile.TemporaryDirectory(
                prefix=".emp-download-", dir=destination.parent
            ) as work:
                temporary = Path(work) / "artifact"
                async with asyncio.timeout(self.timeout):
                    async with self.http.stream(
                        "GET",
                        self.url + "/artifacts/download",
                        params={
                            "experiment_id": experiment_id,
                            "artifact_id": artifact_id,
                        },
                    ) as response:
                        if response.status_code != 200:
                            payload = bytearray()
                            async for chunk in response.aiter_bytes():
                                if len(payload) + len(chunk) > self.max_bytes:
                                    raise ClientError(
                                        "Download error response is too large.",
                                        code="response_too_large",
                                    )
                                payload.extend(chunk)
                            try:
                                details = copy_json_object(
                                    json.loads(payload), "download error"
                                )
                            except (ValueError, TypeError) as error:
                                raise ClientError(
                                    "Invalid download error response.",
                                    code="invalid_response",
                                ) from error
                            failure = details.get("error", {})
                            if not isinstance(failure, dict):
                                raise ClientError(
                                    "Invalid download error envelope.",
                                    code="invalid_response",
                                )
                            raise ClientError(
                                str(failure.get("message", details)),
                                code=str(failure.get("code", "http_error")),
                                exit_code=1,
                                details=details,
                            )
                        if (
                            response.headers.get("content-type", "").split(";")[0]
                            != "application/octet-stream"
                        ):
                            raise ClientError(
                                "Unexpected artifact response type.",
                                code="invalid_response",
                            )
                        size = 0
                        with temporary.open("xb") as output:
                            async for chunk in response.aiter_bytes():
                                output.write(chunk)
                                size += len(chunk)
                        expected = response.headers.get("content-length")
                        if expected is not None and (
                            not expected.isdecimal() or size != int(expected)
                        ):
                            raise ClientError(
                                "Artifact download is incomplete.",
                                code="invalid_response",
                            )
                # Same-volume publication refuses to replace an existing destination.
                os.link(temporary, destination)
            return {
                "path": str(destination),
                "size_bytes": size,
                "artifact_id": artifact_id,
            }
        except (TimeoutError, httpx.TimeoutException) as error:
            raise ClientError(
                "Artifact download timed out.", code="http_timeout"
            ) from error
        except httpx.HTTPError as error:
            raise ClientError(
                "Artifact download failed.", code="connection_error"
            ) from error

    async def wait(self, receipt: JsonObject, timeout: float) -> JsonObject:
        receipt = command_receipt(receipt)
        identifier = require_text(receipt.get("command_id"), "command_id")
        instance = receipt.get("server_instance_id")
        deadline = time.monotonic() + timeout
        result = receipt
        while result.get("state") == "pending":
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ClientError(
                    "Client wait expired; the command continues on the server. Use result with this ID.",
                    code="wait_timeout",
                    exit_code=4,
                    details={"command_id": identifier, "server_instance_id": instance},
                )
            await asyncio.sleep(min(self.interval, remaining))
            try:
                async with asyncio.timeout(max(0.001, deadline - time.monotonic())):
                    result = command_receipt(
                        await self.request(
                            "GET", "/commands/" + quote(identifier, safe="")
                        )
                    )
            except TimeoutError as error:
                raise ClientError(
                    "Client wait expired; command execution was not cancelled.",
                    code="wait_timeout",
                    exit_code=4,
                    details={"command_id": identifier, "server_instance_id": instance},
                ) from error
            except ClientError as error:
                error.details.update(
                    command_id=identifier, expected_server_instance_id=instance
                )
                raise
            if result.get("server_instance_id") != instance:
                raise ClientError(
                    "Server instance changed; the previous command outcome is unknown.",
                    code="server_restarted",
                    details={"command_id": identifier, "server_instance_id": instance},
                )
            if result["command_id"] != identifier:
                raise ClientError(
                    "Server returned a result for another command.",
                    code="invalid_response",
                    details={"command_id": identifier},
                )
        return result


def execution_options(
    parser: argparse.ArgumentParser, *, command_id: bool = True
) -> None:
    waiting = parser.add_mutually_exclusive_group()
    waiting.add_argument(
        "--wait",
        dest="wait",
        action="store_true",
        help="Wait for the command response, not completion of the entire experiment.",
    )
    waiting.add_argument(
        "--no-wait",
        dest="wait",
        action="store_false",
        help="Return the command ID immediately after admission.",
    )
    parser.set_defaults(wait=None)
    parser.add_argument(
        "--wait-timeout",
        type=positive_number,
        help="Client wait deadline in seconds; does not cancel server work.",
    )
    if command_id:
        parser.add_argument(
            "--command-id",
            type=uuid_text,
            help="Explicit request UUID; reuse only to retrieve the same retained request.",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="Start webserver.py separately. Experiment paths refer to the server's filesystem.",
    )
    parser.add_argument("--config", type=Path, help="CLI JSON configuration.")
    parser.add_argument("--url", help="Server API base URL, including /api.")
    parser.add_argument(
        "--token-env", help="Environment variable containing a bearer token."
    )
    parser.add_argument("--request-timeout", type=positive_number)
    parser.add_argument(
        "--json",
        action="store_true",
        help="JSON output; watch/follow produces JSON Lines.",
    )
    commands = parser.add_subparsers(dest="action", required=True)
    server = commands.add_parser(
        "server", help="Inspect or restart the server runtime."
    )
    server_actions = server.add_subparsers(dest="server_action", required=True)
    server_mode = server_actions.add_parser(
        "mode", help="Read or change the runtime mode."
    )
    server_mode.add_argument("mode", nargs="?", choices=("run", "maintenance"))
    execution_options(server_mode)
    execution_options(
        server_actions.add_parser(
            "restart", help="Restart the runtime while keeping HTTP available."
        )
    )
    execution_options(
        server_actions.add_parser(
            "shutdown", help="Stop the runtime and HTTP server gracefully."
        )
    )
    template = commands.add_parser("template", help="Create a local experiment draft.")
    template_actions = template.add_subparsers(dest="template_action", required=True)
    create = template_actions.add_parser("create")
    create.add_argument("destination", type=Path)
    create.add_argument("--name", required=True)
    validate_template = template_actions.add_parser(
        "validate", help="Validate a server-side template and module references."
    )
    validate_template.add_argument("path")
    module = commands.add_parser(
        "module", help="Manage modules on a maintenance server."
    )
    module_actions = module.add_subparsers(dest="module_action", required=True)
    module_actions.add_parser("list", help="List registered module hashes.")
    inspect_module = module_actions.add_parser(
        "inspect", help="Read module registration and package availability."
    )
    inspect_module.add_argument("--name", required=True)
    inspect_module.add_argument("--version", required=True)
    add = module_actions.add_parser("add", help="Register and install a source folder.")
    add.add_argument(
        "--folder", required=True, help="Absolute source folder on the server."
    )
    execution_options(add)
    validate = module_actions.add_parser(
        "validate", help="Check a source or a stored package."
    )
    source = validate.add_mutually_exclusive_group(required=True)
    source.add_argument("--folder", help="Absolute source folder on the server.")
    source.add_argument("--name")
    validate.add_argument("--version")
    execution_options(validate)
    remove = module_actions.add_parser(
        "remove", help="Remove the archive and registered hash."
    )
    remove.add_argument("--name", required=True)
    remove.add_argument("--version", required=True)
    execution_options(remove)
    commands.add_parser("health", help="Read server and controller-process health.")
    receipts = commands.add_parser("commands", help="Browse retained command receipts.")
    receipt_actions = receipts.add_subparsers(dest="commands_action", required=True)
    receipts_list = receipt_actions.add_parser("list")
    receipts_list.add_argument("--after", type=uuid_text)
    receipts_list.add_argument("--limit", type=positive_integer, default=100)
    receipts_list.add_argument(
        "--state",
        choices=(
            "pending",
            "succeeded",
            "failed",
            "cancelled",
            "unknown",
            "unavailable",
        ),
    )
    receipts_list.add_argument("--command")
    artifact = commands.add_parser(
        "artifact", help="Browse and download recorded artifacts."
    )
    artifact_actions = artifact.add_subparsers(dest="artifact_action", required=True)
    artifacts_list = artifact_actions.add_parser("list")
    artifacts_list.add_argument("--experiment-id", required=True)
    artifact_get = artifact_actions.add_parser("get")
    artifact_get.add_argument("artifact_id")
    artifact_get.add_argument("--experiment-id", required=True)
    artifact_get.add_argument("--output", type=Path, required=True)
    experiment = commands.add_parser("experiment", help="Browse saved experiments.")
    experiment_actions = experiment.add_subparsers(
        dest="experiment_action", required=True
    )
    experiment_actions.add_parser("list", help="List registered experiments.")
    inspect = experiment_actions.add_parser(
        "inspect", help="Read saved experiment state."
    )
    inspect.add_argument("experiment_id")
    status = commands.add_parser(
        "status", aliases=["state"], help="Read current experiment state."
    )
    status.add_argument("--experiment-id")
    status.add_argument("--watch", type=positive_number, nargs="?", const=1.0)
    resources = commands.add_parser(
        "resources", help="Read resource collector observations."
    )
    resources.add_argument("--watch", type=positive_number, nargs="?", const=1.0)
    history = commands.add_parser(
        "resource-history", help="Read a page of RAM resource history."
    )
    history.add_argument("--after", type=int, default=0)
    history.add_argument("--limit", type=positive_integer, default=100)
    logs = commands.add_parser("logs", help="Read the selected experiment's journal.")
    logs.add_argument("--experiment-id")
    logs.add_argument("--cursor", help="Opaque JSON checkpoint, or @file.")
    logs.add_argument("--limit", type=positive_integer, default=100)
    logs.add_argument("--follow", action="store_true")
    run = commands.add_parser(
        "run", help="Create a run or continue a saved experiment."
    )
    source = run.add_mutually_exclusive_group(required=True)
    source.add_argument("--template", help="Absolute template path on the server.")
    source.add_argument("--continue-from", metavar="EXPERIMENT_ID")
    run.add_argument("--experiment-id", help="ID of a new experiment.")
    run.add_argument("--delayed-start", action="store_true")
    execution_options(run)
    for name in ("pause", "resume", "stop", "step"):
        execution_options(
            commands.add_parser(
                name, help=f"{name.capitalize()} the selected experiment."
            )
        )
    rerun = commands.add_parser(
        "rerun", help="Rerun a stage or create another experiment run."
    )
    rerun.add_argument("scope", choices=("stage", "experiment"))
    rerun.add_argument("--position", type=positive_integer)
    rerun.add_argument("--experiment-id")
    execution_options(rerun)
    for name in ("retry", "move"):
        item = commands.add_parser(
            name,
            help="Retry a service."
            if name == "retry"
            else "Move the paused stage pointer.",
        )
        item.add_argument("position", type=positive_integer)
        execution_options(item)
    service = commands.add_parser(
        "service", help="Control services in a paused experiment."
    )
    service_actions = service.add_subparsers(dest="service_action", required=True)
    for name in ("start", "stop"):
        item = service_actions.add_parser(
            name, help=f"{name.capitalize()} one service."
        )
        item.add_argument("position", type=positive_integer)
        execution_options(item)
    reset = commands.add_parser(
        "reset-retries", help="Reset a stage/service retry counter."
    )
    reset.add_argument("kind", choices=("stage", "service"))
    reset.add_argument("position", type=positive_integer)
    execution_options(reset)
    recover = commands.add_parser(
        "recover", help="Explicitly recover a previously owned experiment."
    )
    recover.add_argument("experiment_id")
    execution_options(recover)
    snapshot = commands.add_parser(
        "snapshot", help="Create an experiment snapshot at a paused boundary."
    )
    snapshot.add_argument("--label")
    execution_options(snapshot)
    snapshot_actions = snapshot.add_subparsers(dest="snapshot_action")
    snapshot_list = snapshot_actions.add_parser("list", help="List snapshot metadata.")
    snapshot_list.add_argument("--experiment-id")
    snapshot_inspect = snapshot_actions.add_parser(
        "inspect", help="Read a snapshot manifest."
    )
    snapshot_inspect.add_argument("snapshot_id", type=uuid_text)
    snapshot_inspect.add_argument("--experiment-id")
    rollback = commands.add_parser(
        "rollback", help="Restore the selected experiment from a snapshot."
    )
    rollback.add_argument("snapshot_id", type=uuid_text)
    execution_options(rollback)
    archive = commands.add_parser(
        "archive", help="Create, validate or install exchange archives."
    )
    actions = archive.add_subparsers(dest="archive_action", required=True)
    for name in ("create", "inspect", "install"):
        item = actions.add_parser(name)
        item.add_argument("archive_path", help="Absolute archive path on the server.")
        if name == "create":
            item.add_argument("--experiment-id")
        if name == "install":
            item.add_argument("destination", help="New directory on the server.")
        execution_options(item)
    command = commands.add_parser(
        "command", help="Send an arbitrary supported controller command."
    )
    command.add_argument("name")
    command.add_argument("--args", default="{}", help="JSON object or @file.")
    command.add_argument("--target", help="JSON target object or @file.")
    execution_options(command)
    chain = commands.add_parser(
        "chain", help="Submit a JSON file containing a command array or chain envelope."
    )
    chain.add_argument("file", type=Path)
    chain.add_argument("--chain-id", type=uuid_text)
    execution_options(chain, command_id=False)
    result = commands.add_parser(
        "result", help="Retrieve a previously accepted command result."
    )
    result.add_argument("identifier", type=uuid_text)
    execution_options(result, command_id=False)
    commands.add_parser(
        "shell", help="Interactive HTTP client; quit/EOF never stop the server."
    )
    return parser


def command_document(options: argparse.Namespace) -> JsonObject:
    name = options.action
    args: JsonObject = {}
    target: JsonObject | None = None
    if name == "module":
        name = "module." + options.module_action
        folder = getattr(options, "folder", None)
        version = getattr(options, "version", None)
        if folder is not None:
            if version is not None:
                raise ValueError("--folder cannot be combined with --version.")
            args["folder"] = folder
        else:
            if version is None:
                raise ValueError("--name requires --version.")
            args.update({"name": options.name, "version": version})
    elif name == "run":
        args["delayed_start"] = options.delayed_start
        if options.continue_from is not None:
            if options.experiment_id is not None:
                raise ValueError(
                    "--experiment-id cannot be combined with --continue-from."
                )
            args.update({"experiment_id": options.continue_from, "continue": True})
        else:
            args["template_path"] = options.template
            if options.experiment_id is not None:
                args["experiment_id"] = options.experiment_id
    elif name == "rerun":
        args["scope"] = options.scope
        if options.scope == "stage":
            if options.position is None or options.experiment_id is not None:
                raise ValueError(
                    "Stage rerun requires --position and no --experiment-id."
                )
            args["position"] = options.position
        elif options.position is not None:
            raise ValueError("Experiment rerun does not accept --position.")
        elif options.experiment_id is not None:
            args["experiment_id"] = options.experiment_id
    elif name == "move":
        args["position"] = options.position
    elif name in ("retry", "reset-retries"):
        target = {
            "kind": "service" if name == "retry" else options.kind,
            "position": options.position,
        }
        name = name.replace("-", "_")
    elif name == "service":
        name = "service." + options.service_action
        target = {"kind": "service", "position": options.position}
    elif name == "server":
        name = "server." + options.server_action
        if options.server_action == "mode":
            args["mode"] = options.mode
    elif name == "recover":
        args["experiment_id"] = options.experiment_id
    elif name == "snapshot" and options.label is not None:
        args["label"] = options.label
    elif name == "rollback":
        args["snapshot_id"] = options.snapshot_id
    elif name == "archive":
        name = "archive." + options.archive_action
        args["archive_path"] = options.archive_path
        if options.archive_action == "create" and options.experiment_id is not None:
            args["experiment_id"] = options.experiment_id
        if options.archive_action == "install":
            args["destination"] = options.destination
    elif name == "command":
        name = options.name
        args = json_argument(options.args)
        target = None if options.target is None else json_argument(options.target)
    document: JsonObject = {
        "api_version": 1,
        "command_id": options.command_id or str(uuid4()),
        "command": name,
        "args": args,
    }
    if target is not None:
        document["target"] = target
    return document


def display(document: JsonObject, *, as_json: bool, streaming: bool = False) -> None:
    if not as_json and "phase" in document:
        print(
            f"Experiment: {document.get('experiment_id') or '(none)'} | {document['phase']} / {document.get('mode')} | cycle {document.get('cycle_number')} | stage {document.get('stage_position')}",
            flush=True,
        )
        if document.get("recovery_required"):
            print(
                "Recovery required:",
                json.dumps(document["recovery_required"], ensure_ascii=False),
                flush=True,
            )
        if document.get("error"):
            print(
                json.dumps(document["error"], ensure_ascii=False, indent=2), flush=True
            )
        return
    if not as_json and "command_id" in document:
        print(f"Command {document['command_id']}: {document.get('state')}", flush=True)
    print(
        json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            indent=None if streaming else 2,
        ),
        flush=True,
    )


def outcome_code(document: JsonObject) -> int:
    if document.get("state") in ("unknown", "unavailable"):
        return 3
    return 1 if document.get("result") == "fail" else 0


async def execute(
    options: argparse.Namespace,
    client: APIClient,
    *,
    as_json: bool,
    interactive: bool = False,
) -> int:
    action = options.action
    if action == "server" and options.server_action == "mode" and options.mode is None:
        if (
            options.wait is not None
            or options.wait_timeout is not None
            or options.command_id is not None
        ):
            raise ValueError(
                "Reading server mode does not accept command execution options."
            )
        document = await client.request("GET", "/health")
        mode = document.get("server_mode")
        if mode not in ("run", "maintenance"):
            raise ClientError(
                "Server returned an invalid mode.", code="invalid_response"
            )
        display({"server_mode": mode}, as_json=as_json)
        return 0
    if action == "template" and options.template_action == "validate":
        document = await client.request(
            "POST", "/templates/validate", document={"template_path": options.path}
        )
        display(document, as_json=as_json)
        return 0
    if action == "module" and options.module_action in ("list", "inspect"):
        params = (
            {}
            if options.module_action == "list"
            else {"name": options.name, "version": options.version}
        )
        path = "/modules" if options.module_action == "list" else "/modules/inspect"
        display(await client.request("GET", path, params=params), as_json=as_json)
        return 0
    if action == "commands":
        if options.limit > 1000:
            raise ValueError("limit must be between 1 and 1000.")
        params = {
            key: value
            for key in ("after", "limit", "state", "command")
            if (value := getattr(options, key)) is not None
        }
        display(
            await client.request("GET", "/commands", params=params), as_json=as_json
        )
        return 0
    if action == "artifact":
        if options.artifact_action == "get":
            document = await client.download(
                options.experiment_id, options.artifact_id, options.output
            )
        else:
            document = await client.request(
                "GET", "/artifacts", params={"experiment_id": options.experiment_id}
            )
        display(document, as_json=as_json)
        return 0
    if action == "experiment" or (
        action == "snapshot" and options.snapshot_action is not None
    ):
        params = {}
        if action == "experiment":
            path = "/experiments"
            if options.experiment_action == "inspect":
                path += "/inspect"
                params["experiment_id"] = options.experiment_id
        else:
            if (
                options.label is not None
                or options.wait is not None
                or options.wait_timeout is not None
                or options.command_id is not None
            ):
                raise ValueError(
                    "Snapshot reads do not accept creation or wait options."
                )
            path = "/snapshots"
            if options.experiment_id is not None:
                params["experiment_id"] = options.experiment_id
            if options.snapshot_action == "inspect":
                path += "/inspect"
                params["snapshot_id"] = options.snapshot_id
        document = await client.request("GET", path, params=params)
        display(document, as_json=as_json)
        return 0
    if action in ("health", "status", "state", "resources", "resource-history"):
        path = {
            "health": "/health",
            "status": "/state",
            "state": "/state",
            "resources": "/resources",
            "resource-history": "/resources/history",
        }[action]
        params = {}
        if action in ("status", "state") and options.experiment_id is not None:
            params["experiment_id"] = options.experiment_id
        if action == "resource-history":
            if options.after < 0 or options.limit > 1000:
                raise ValueError("after must be nonnegative and limit must be 1..1000.")
            params = {"after": options.after, "limit": options.limit}
        watch = getattr(options, "watch", None)
        while True:
            document = await client.request("GET", path, params=params)
            display(document, as_json=as_json, streaming=watch is not None)
            if watch is None:
                return 0
            await asyncio.sleep(watch)
    if action == "logs":
        identifier = options.experiment_id
        if identifier is None:
            current = await client.request("GET", "/state")
            identifier = require_text(
                current.get("experiment_id"),
                "selected experiment_id; use --experiment-id",
            )
        cursor = None if options.cursor is None else json_argument(options.cursor)
        if options.limit > 1000:
            raise ValueError("limit must be 1..1000.")
        while True:
            params = {"limit": options.limit}
            if cursor is not None:
                params["cursor"] = json.dumps(cursor, separators=(",", ":"))
            document = await client.request(
                "GET",
                "/experiments/" + quote(identifier, safe="") + "/events",
                params=params,
            )
            display(document, as_json=as_json, streaming=options.follow)
            if not options.follow:
                return 0
            cursor = copy_json_object(document.get("checkpoint"), "journal checkpoint")
            if not document.get("has_more"):
                await asyncio.sleep(client.interval)
    timeout = (
        client.wait_timeout if options.wait_timeout is None else options.wait_timeout
    )
    if action == "result":
        response = command_receipt(
            await client.request(
                "GET", "/commands/" + quote(options.identifier, safe="")
            )
        )
        if response["command_id"] != options.identifier:
            raise ClientError(
                "Server returned a result for another command.",
                code="invalid_response",
                details={"command_id": options.identifier},
            )
        if options.wait:
            response = await client.wait(response, timeout)
        display(response, as_json=as_json)
        return outcome_code(response)
    if action == "chain":
        loaded = json.loads(options.file.read_text(encoding="utf-8"))
        document = copy_json_object(
            {"commands": loaded} if isinstance(loaded, list) else loaded, "chain"
        )
        entries = document.get("commands")
        if not isinstance(entries, list) or not entries:
            raise ValueError("A chain requires a nonempty commands array.")
        prepared = []
        for item in entries:
            item = copy_json_object(item, "chain command")
            item.setdefault("command_id", str(uuid4()))
            prepared.append(item)
        document["commands"] = copy_json_object({"items": prepared}, "commands")[
            "items"
        ]
        document["chain_id"] = options.chain_id or document.get(
            "chain_id", str(uuid4())
        )
        document["chain_id"] = str(UUID(require_text(document["chain_id"], "chain_id")))
        identifiers = [
            str(UUID(require_text(item["command_id"], "command_id")))
            for item in prepared
        ]
        path = "/chains"
    else:
        document = command_document(options)
        identifiers = [require_text(document["command_id"], "command_id")]
        path = "/commands"
    print("command_id=" + ",".join(identifiers), file=sys.stderr, flush=True)
    wait = not interactive if options.wait is None else options.wait
    wait_for_shutdown = document.get("command") == "server.shutdown" and wait
    try:
        if wait_for_shutdown:
            receipt = await client.request(
                "POST",
                path,
                document=document,
                params={"wait": "true"},
                timeout=timeout,
            )
        else:
            receipt = await client.request("POST", path, document=document)
    except ClientError as error:
        error.details.update(
            copy_json_object({"command_ids": identifiers}, "command IDs")
        )
        if wait_for_shutdown and error.code == "http_timeout":
            raise ClientError(
                "Shutdown wait expired; the server operation was not cancelled.",
                code="wait_timeout",
                exit_code=4,
                details=error.details,
            ) from error
        raise
    if action != "chain":
        receipt = command_receipt(receipt)
        if receipt["command_id"] != identifiers[0]:
            raise ClientError(
                "Server returned a receipt for another command.",
                code="invalid_response",
                details={"command_id": identifiers[0]},
            )
    else:
        tickets = receipt.get("commands")
        if not isinstance(tickets, list):
            raise ClientError(
                "Server returned no chain receipts.", code="invalid_response"
            )
        validated = [command_receipt(item) for item in tickets]
        if (
            receipt.get("chain_id") != document["chain_id"]
            or [item["command_id"] for item in validated] != identifiers
            or any(
                item["server_instance_id"] != receipt.get("server_instance_id")
                for item in validated
            )
        ):
            raise ClientError(
                "Server returned receipts for another chain.",
                code="invalid_response",
                details=copy_json_object({"command_ids": identifiers}, "command IDs"),
            )
    wait = not interactive if options.wait is None else options.wait
    if not wait:
        display(receipt, as_json=as_json)
        if action == "chain":
            tickets = receipt.get("commands")
            if not isinstance(tickets, list):
                raise ClientError(
                    "Server returned no chain receipts.", code="invalid_response"
                )
            return max(
                (outcome_code(command_receipt(item)) for item in tickets),
                default=0,
            )
        return outcome_code(receipt)
    if action != "chain":
        result = await client.wait(receipt, timeout)
        display(result, as_json=as_json)
        return outcome_code(result)
    tickets = receipt.get("commands")
    if not isinstance(tickets, list):
        raise ClientError("Server returned no chain receipts.", code="invalid_response")
    results = []
    deadline = time.monotonic() + timeout
    for item in tickets:
        ticket = command_receipt(item)
        results.append(await client.wait(ticket, max(0, deadline - time.monotonic())))
    receipt["commands"] = copy_json_object({"items": results}, "chain results")["items"]
    display(receipt, as_json=as_json)
    return max((outcome_code(result) for result in results), default=0)


async def run_command(
    options: argparse.Namespace,
    settings: JsonObject,
    *,
    as_json: bool,
    interactive: bool = False,
) -> int:
    if options.action == "template" and options.template_action == "create":
        from core.experimenttemplate import create_template

        path = create_template(options.destination.absolute(), options.name)
        display(
            {
                "path": str(path),
                "status": "draft",
                "message": "Fill stages before running.",
            },
            as_json=as_json,
        )
        return 0
    async with APIClient(settings) as client:
        return await execute(options, client, as_json=as_json, interactive=interactive)


def report_error(error: Exception, *, as_json: bool) -> int:
    if isinstance(error, ClientError):
        document = {
            "error": {
                "code": error.code,
                "message": str(error),
                "details": error.details,
            }
        }
        code = error.exit_code
    else:
        document = {
            "error": {"code": "invalid_input", "message": str(error), "details": {}}
        }
        code = 2
    print(
        json.dumps(document, ensure_ascii=False) if as_json else f"Error: {error}",
        file=sys.stderr,
        flush=True,
    )
    return code


def shell(
    parser: argparse.ArgumentParser, settings: JsonObject, *, as_json: bool
) -> int:
    print(
        "Connected client shell. Commands run asynchronously by default; use result ID or --wait. quit/EOF disconnect only.",
        file=sys.stderr,
    )
    while True:
        try:
            # Keep terminal input outside an executor so Ctrl+C can exit immediately.
            print("emp> ", file=sys.stderr, end="", flush=True)
            line = input().strip()
        except (EOFError, KeyboardInterrupt):
            return 0
        if not line:
            continue
        if line in ("quit", "exit"):
            return 0
        if line == "help":
            parser.print_help()
            continue
        try:
            tokens = shlex.split(line, posix=False)
            tokens = [
                value[1:-1]
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'"
                else value
                for value in tokens
            ]
            options = parser.parse_args(tokens)
            if options.action == "shell":
                raise ValueError("Nested shells are not supported.")
            if (
                options.config
                or options.url
                or options.token_env
                or options.request_timeout
            ):
                raise ValueError(
                    "Connection settings are selected when entering the shell."
                )
            asyncio.run(
                run_command(
                    options, settings, as_json=as_json or options.json, interactive=True
                )
            )
        except SystemExit:
            continue
        except KeyboardInterrupt:
            print(
                "Client wait interrupted. Submitted commands continue on the server.",
                file=sys.stderr,
            )
        except (ClientError, OSError, TypeError, ValueError, KeyError) as error:
            report_error(error, as_json=as_json)


def main() -> None:
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        if isinstance(stream, io.TextIOWrapper):
            stream.reconfigure(encoding="utf-8")
    parser = build_parser()
    options = parser.parse_args()
    if options.action == "template" and options.template_action == "create":
        try:
            code = asyncio.run(run_command(options, {}, as_json=options.json))
        except (OSError, TypeError, ValueError) as error:
            code = report_error(error, as_json=options.json)
        raise SystemExit(code)
    overrides: JsonObject = {}
    for name, value in (
        ("server_url", options.url),
        ("token_env", options.token_env),
        ("request_timeout_seconds", options.request_timeout),
    ):
        if value is not None:
            overrides[name] = value
    try:
        settings = load_settings(
            None if options.config is None else options.config.resolve(), overrides
        )
        code = (
            shell(parser, settings, as_json=options.json)
            if options.action == "shell"
            else asyncio.run(run_command(options, settings, as_json=options.json))
        )
    except KeyboardInterrupt:
        print(
            "Client interrupted. Submitted commands continue on the server; use their command IDs to inspect results.",
            file=sys.stderr,
        )
        code = 130
    except (ClientError, OSError, TypeError, ValueError, KeyError) as error:
        code = report_error(error, as_json=options.json)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
