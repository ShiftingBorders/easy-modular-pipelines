"""Journal configuration I/O and per-file path anchoring."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from core.journal.events import LoggingConfigurationError, validate_context
from core.primitives.json_values import JsonObject, copy_json_object, require_text

if TYPE_CHECKING:
    from core.models.journal_settings import LoggingConfiguration


def load_logging_settings(config_path: Path) -> tuple[JsonObject, JsonObject]:
    """Preserve the JSON settings contract for public callers."""
    settings, context = _load_logging_settings(config_path)
    document = settings.model_dump()
    document["db_path"] = str(settings.db_path)
    document["busy_timeout_seconds"] = float(settings.busy_timeout_seconds)
    return document, context


def _load_logging_settings(
    config_path: Path,
) -> tuple[LoggingConfiguration, JsonObject]:
    """Read one file and validate once before the logger consumes typed settings."""
    try:
        if not config_path.is_absolute():
            raise ValueError("config_path must be absolute.")
        with config_path.open(encoding="utf-8") as config_file:
            document = copy_json_object(json.load(config_file), "configuration")
        values = document.get("logging")
        if not isinstance(values, dict):
            raise TypeError("Configuration must contain a logging object.")
        path = Path(require_text(values.get("db_path"), "logging.db_path"))
        if not path.is_absolute():
            # Drive-relative/root-relative Windows paths have no reliable anchor.
            if path.drive or path.root:
                raise ValueError("db_path must be absolute or relative to the config.")
            path = config_path.parent / path
        # Load models only after explicit file I/O; logger imports stay side-effect free.
        from core.models.journal_settings import LoggingConfiguration

        settings = LoggingConfiguration.model_validate({**values, "db_path": path})
        context = validate_context(document.get("operation_context", {}))
    except (OSError, UnicodeError, ValueError, TypeError, RecursionError) as error:
        raise LoggingConfigurationError(
            f"Cannot load logging configuration {config_path}: {error}"
        ) from error
    return settings, context
