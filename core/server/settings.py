"""One controller process and two queues behind the independent HTTP server."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlsplit

from core.primitives.json_files import read_json
from core.primitives.json_values import JsonObject, require_number, require_text
from core.primitives.paths import repository_root

DEFAULT_CONFIG = repository_root() / "default_settings/webserver.json"


SETTING_FIELDS = frozenset(
    {
        "schema_version",
        "project_root",
        "hash_config_path",
        "server_mode",
        "filer_url",
        "seaweed_config_path",
        "seaweed_min_free_gb",
        "resource_config_path",
        "archive_config_path",
        "host",
        "port",
        "token_env",
        "startup_timeout_seconds",
        "shutdown_timeout_seconds",
        "read_timeout_seconds",
        "result_ttl_seconds",
        "max_pending_commands",
        "max_read_requests",
        "max_command_records",
        "max_request_bytes",
        "max_response_bytes",
        "max_cached_result_bytes",
    }
)


def integer_setting(document: JsonObject, name: str, minimum: int = 1) -> int:
    value = document[name]
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}.")
    return value


class ServerSettings:
    """Validated startup values; construction never starts runtime components."""

    def __init__(self, document: JsonObject) -> None:
        if document.keys() != SETTING_FIELDS:
            raise ValueError("Server settings have missing or unknown fields.")
        self.project_root = Path(
            require_text(document["project_root"], "project_root (required)")
        )
        self.default_hash_config = document["hash_config_path"] is None
        self.hash_config_path = (
            self.project_root / "hash_db/config.json"
            if self.default_hash_config
            else Path(require_text(document["hash_config_path"], "hash_config_path"))
        )
        self.server_mode = require_text(document["server_mode"], "server_mode")
        if self.server_mode not in ("run", "maintenance"):
            raise ValueError("server_mode must be run or maintenance.")
        self.seaweed_config_path = Path(
            require_text(document["seaweed_config_path"], "seaweed_config_path")
        )
        self.seaweed_min_free_gb = require_number(
            document["seaweed_min_free_gb"], "seaweed_min_free_gb"
        )
        if self.seaweed_min_free_gb <= 0:
            raise ValueError("seaweed_min_free_gb must be positive.")
        self.resource_config_path = Path(
            require_text(document["resource_config_path"], "resource_config_path")
        )
        self.archive_config_path = Path(
            require_text(document["archive_config_path"], "archive_config_path")
        )
        for path in (
            self.project_root,
            self.hash_config_path,
            self.resource_config_path,
            self.archive_config_path,
            self.seaweed_config_path,
        ):
            if not path.is_absolute():
                raise ValueError(
                    "Server settings paths must be absolute after resolution."
                )
        self.filer_url = (
            None
            if document["filer_url"] is None
            else require_text(document["filer_url"], "filer_url")
        )
        url = urlsplit(self.filer_url or "http://127.0.0.1")
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "filer_url must be an HTTP(S) URL without credentials, query or fragment."
            )
        self.host = require_text(document["host"], "host")
        if url.port is not None and not 1 <= url.port <= 65535:
            raise ValueError("Invalid filer_url port.")
        self.port = integer_setting(document, "port")
        if self.port > 65535:
            raise ValueError("port must not exceed 65535.")
        self.token_env = (
            None
            if document["token_env"] is None
            else require_text(document["token_env"], "token_env")
        )
        self.startup_timeout = require_number(
            document["startup_timeout_seconds"], "startup_timeout_seconds"
        )
        self.shutdown_timeout = require_number(
            document["shutdown_timeout_seconds"], "shutdown_timeout_seconds"
        )
        self.read_timeout = require_number(
            document["read_timeout_seconds"], "read_timeout_seconds"
        )
        self.result_ttl = require_number(
            document["result_ttl_seconds"], "result_ttl_seconds"
        )
        if (
            min(
                self.startup_timeout,
                self.shutdown_timeout,
                self.read_timeout,
                self.result_ttl,
            )
            <= 0
        ):
            raise ValueError("Server timeouts and result TTL must be positive.")
        self.max_pending = integer_setting(document, "max_pending_commands")
        self.max_reads = integer_setting(document, "max_read_requests")
        self.max_records = integer_setting(document, "max_command_records")
        self.max_request_bytes = integer_setting(document, "max_request_bytes")
        self.max_response_bytes = integer_setting(document, "max_response_bytes")
        self.max_cache_bytes = integer_setting(document, "max_cached_result_bytes")
        if self.max_records <= self.max_pending:
            raise ValueError("max_command_records must leave room for a priority stop.")
        if self.max_cache_bytes < self.max_response_bytes:
            raise ValueError("max_cached_result_bytes must be >= max_response_bytes.")
        if integer_setting(document, "schema_version") != 1:
            raise ValueError("Only server settings schema_version 1 is supported.")


def load_server_settings(
    config_path: Path | None = None, overrides: JsonObject | None = None
) -> ServerSettings:
    """Resolve each configured path against the file which actually supplied it."""
    files = [DEFAULT_CONFIG]
    if config_path is not None:
        config_path = Path(config_path)
        if not config_path.is_absolute():
            raise ValueError("config_path must be absolute.")
        if config_path.resolve() != DEFAULT_CONFIG:
            files.append(config_path)
    settings: JsonObject = {}
    fields = SETTING_FIELDS
    path_fields = {
        "project_root",
        "hash_config_path",
        "resource_config_path",
        "archive_config_path",
        "seaweed_config_path",
    }
    for path in files:
        values = read_json(path)
        if values.keys() - fields:
            raise ValueError(
                f"Unknown server settings: {sorted(values.keys() - fields)}"
            )
        for name in path_fields & values.keys():
            if values[name] is not None:
                configured = Path(require_text(values[name], name))
                if not configured.is_absolute() and (
                    configured.drive or configured.root
                ):
                    raise ValueError(f"Ambiguous configured path: {name}")
                values[name] = str((path.parent / configured).resolve())
        settings.update(values)
    if overrides:
        if overrides.keys() - fields:
            raise ValueError("Unknown server setting override.")
        for name in path_fields & overrides.keys():
            if not Path(require_text(overrides[name], name)).is_absolute():
                raise ValueError("Explicit path overrides must be absolute.")
        settings.update(overrides)
    return ServerSettings(settings)
