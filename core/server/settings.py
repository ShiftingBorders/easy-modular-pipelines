"""One controller process and two queues behind the independent HTTP server."""

from __future__ import annotations

from pathlib import Path

from core.models.server_settings import ServerConfiguration
from core.primitives.json_files import read_json
from core.primitives.json_values import JsonObject, require_text
from core.primitives.paths import repository_root

DEFAULT_CONFIG = repository_root() / "default_settings/webserver.json"


SETTING_FIELDS = frozenset(ServerConfiguration.model_fields)


def integer_setting(document: JsonObject, name: str, minimum: int = 1) -> int:
    value = document[name]
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}.")
    return value


class ServerSettings:
    """Validated startup values; construction never starts runtime components."""

    def __init__(self, document: JsonObject) -> None:
        self._configure(ServerConfiguration.model_validate(document))

    def _configure(self, settings: ServerConfiguration) -> None:
        self.project_root = settings.project_root
        self.default_hash_config = settings.hash_config_path is None
        self.hash_config_path = (
            self.project_root / "hash_db/config.json"
            if self.default_hash_config else settings.hash_config_path
        )
        self.server_mode = settings.server_mode
        self.seaweed_config_path = settings.seaweed_config_path
        self.seaweed_min_free_gb = settings.seaweed_min_free_gb
        self.resource_config_path = settings.resource_config_path
        self.archive_config_path = settings.archive_config_path
        self.filer_url = settings.filer_url
        self.host = settings.host
        self.port = settings.port
        self.token_env = settings.token_env
        self.startup_timeout = settings.startup_timeout_seconds
        self.shutdown_timeout = settings.shutdown_timeout_seconds
        self.read_timeout = settings.read_timeout_seconds
        self.result_ttl = settings.result_ttl_seconds
        self.max_pending = settings.max_pending_commands
        self.max_reads = settings.max_read_requests
        self.max_records = settings.max_command_records
        self.max_request_bytes = settings.max_request_bytes
        self.max_response_bytes = settings.max_response_bytes
        self.max_cache_bytes = settings.max_cached_result_bytes


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
