"""Journal configuration loading and validation."""

import json
from pathlib import Path

from core.journal.events import (
    LoggingConfigurationError,
    validate_context,
    validate_journal_identity,
)
from core.primitives.json_values import (
    JsonObject,
    copy_json_object,
    require_number,
    require_text,
)


def load_logging_settings(config_path: Path) -> tuple[JsonObject, JsonObject]:
    """Read one file; resolve its configured paths against that file's directory."""
    try:
        if not config_path.is_absolute():
            raise ValueError("config_path must be absolute.")
        with config_path.open(encoding="utf-8") as config_file:
            document = json.load(config_file)
        document = copy_json_object(document, "configuration")
        settings = document.get("logging")
        if not isinstance(settings, dict):
            raise TypeError("Configuration must contain a logging object.")
        required = {
            "db_path",
            "busy_timeout_seconds",
            "max_event_bytes",
            "open_mode",
            "min_free_bytes",
            "expected_journal",
            "filtered_refresh_interval_seconds",
        }
        missing = required - settings.keys()
        if missing:
            raise ValueError(f"Missing logging fields: {', '.join(sorted(missing))}.")
        unknown = settings.keys() - required
        if unknown:
            raise ValueError(f"Unknown logging fields: {', '.join(sorted(unknown))}.")
        db_path, timeout, max_bytes, open_mode, min_free_bytes, expected, refresh = (
            _normalize_logging_values(settings, config_path)
        )
        context = validate_context(document.get("operation_context", {}))
        settings = {
            "db_path": str(db_path),
            "busy_timeout_seconds": float(timeout),
            "max_event_bytes": max_bytes,
            "open_mode": open_mode,
            "min_free_bytes": min_free_bytes,
            "expected_journal": expected,
            "filtered_refresh_interval_seconds": refresh,
        }
    except (OSError, UnicodeError, ValueError, TypeError, RecursionError) as error:
        raise LoggingConfigurationError(
            f"Cannot load logging configuration {config_path}: {error}"
        ) from error
    return settings, context


def _normalize_logging_values(
    settings: JsonObject, config_path: Path
) -> tuple[Path, int | float, int | None, str, int, JsonObject | None, int | float]:
    configured_path = require_text(settings.get("db_path"), "logging.db_path")
    db_path = Path(configured_path)
    if not db_path.is_absolute():
        # Drive-relative/root-relative Windows paths cannot be anchored reliably.
        if db_path.drive or db_path.root:
            raise ValueError("db_path must be absolute or relative to the config.")
        db_path = config_path.parent / db_path
    timeout = require_number(settings["busy_timeout_seconds"], "timeout")
    if not 0 < timeout <= 60:
        raise ValueError("busy_timeout_seconds must be greater than 0 and at most 60.")
    max_bytes = settings["max_event_bytes"]
    if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 1):
        raise ValueError("max_event_bytes must be a positive integer or null.")
    open_mode = settings["open_mode"]
    if open_mode not in ("create", "existing"):
        raise ValueError("logging.open_mode must be create or existing.")
    min_free_bytes = settings["min_free_bytes"]
    if type(min_free_bytes) is not int or min_free_bytes < 0:
        raise ValueError("logging.min_free_bytes must be a nonnegative integer.")
    expected = settings["expected_journal"]
    if open_mode == "existing":
        expected = validate_journal_identity(expected)
    elif expected is not None:
        raise ValueError("create requires expected_journal=null.")
    refresh = require_number(
        settings["filtered_refresh_interval_seconds"], "refresh interval"
    )
    if refresh <= 0:
        raise ValueError("filtered_refresh_interval_seconds must be positive.")
    return db_path, timeout, max_bytes, open_mode, min_free_bytes, expected, refresh
