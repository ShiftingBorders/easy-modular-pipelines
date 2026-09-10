"""JSON event contract and configuration validation for core.logger."""

import json
import math
from datetime import datetime, timedelta
from pathlib import Path

type JsonValue = (
    None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
)
type JsonObject = dict[str, JsonValue]

SCHEMA_VERSION = 1
RESERVED_EVENT_TYPES = frozenset(
    {
        "operation.started",
        "operation.finished",
        "error.recorded",
        "resources.recorded",
        "progress.recorded",
        "artifact.recorded",
    }
)
CONTEXT_TEXT_FIELDS = frozenset(
    {
        "run_id",
        "node_id",
        "node_execution_id",
        "runner_session_id",
        "service_instance_id",
        "module_name",
        "module_version",
        "module_hash",
        "configuration_hash",
        "worker_id",
        "host_name",
        "source",
        "parent_operation_id",
    }
)
EVENT_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "producer_instance_id",
        "sequence_number",
        "occurred_at",
        "event_type",
        "context",
        "operation_id",
        "data",
    }
)


class LoggingError(Exception):
    """An expected journal failure; a failed write may have committed."""


class LoggingConfigurationError(LoggingError):
    """Settings or the existing journal schema are incompatible."""


class LoggingStorageError(LoggingError):
    """The journal could not complete a storage operation."""


class LoggingStateError(LoggingError):
    """The client or operation is not in a state that permits this call."""


def require_text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string.")
    if not value.strip() or "\x00" in value:
        raise ValueError(f"{name} must be nonempty and contain no null characters.")
    return value


def require_number(value: object, name: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, not a boolean.")
    if value < 0 or (isinstance(value, float) and not math.isfinite(value)):
        raise ValueError(f"{name} must be finite and nonnegative.")
    return value


def _validate_json(value: object, depth: int = 0) -> None:
    # A depth limit also rejects cycles without invoking user serialization hooks.
    if depth > 32:
        raise ValueError("JSON data must not be cyclic or nested more than 32 levels.")
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("JSON data must not contain NaN or infinity.")
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("JSON object keys must be strings.")
            _validate_json(item, depth + 1)
        return
    if type(value) is list:
        for item in value:
            _validate_json(item, depth + 1)
        return
    raise TypeError("Data must contain only JSON objects, arrays, and scalar values.")


def copy_json_object(value: object, name: str) -> JsonObject:
    """Validate and detach caller-owned JSON data before any persistent write."""
    if type(value) is not dict:
        raise TypeError(f"{name} must be a JSON object.")
    _validate_json(value)
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
    try:
        encoded.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError(f"{name} contains invalid Unicode.") from error
    return json.loads(encoded)


def validate_context(value: object) -> JsonObject:
    context = copy_json_object(value, "operation_context")
    unknown = context.keys() - CONTEXT_TEXT_FIELDS - {"attempt_number", "process_id"}
    if unknown:
        raise ValueError(f"Unknown context fields: {', '.join(sorted(unknown))}.")
    for name, item in context.items():
        if item is None:
            continue
        if name in CONTEXT_TEXT_FIELDS:
            require_text(item, name)
        elif type(item) is not int or item < 1:
            raise ValueError(f"{name} must be a positive integer.")
    return context


def load_logging_config(config_path: Path) -> tuple[Path, float, int, JsonObject]:
    """Read one file; resolve its configured paths against that file's directory."""
    try:
        with config_path.open(encoding="utf-8") as config_file:
            document = json.load(config_file)
        document = copy_json_object(document, "configuration")
        settings = document.get("logging")
        if not isinstance(settings, dict):
            raise TypeError("Configuration must contain a logging object.")
        unknown = settings.keys() - {
            "db_path",
            "busy_timeout_seconds",
            "max_event_bytes",
        }
        if unknown:
            raise ValueError(f"Unknown logging fields: {', '.join(sorted(unknown))}.")
        configured_path = require_text(settings.get("db_path"), "logging.db_path")
        db_path = Path(configured_path)
        if not db_path.is_absolute():
            # Drive-relative/root-relative Windows paths cannot be anchored reliably.
            if db_path.drive or db_path.root:
                raise ValueError("db_path must be absolute or relative to the config.")
            db_path = config_path.parent / db_path
        timeout = require_number(settings.get("busy_timeout_seconds", 5), "timeout")
        if not 0 < timeout <= 60:
            raise ValueError(
                "busy_timeout_seconds must be greater than 0 and at most 60."
            )
        max_bytes = settings.get("max_event_bytes", 1048576)
        if type(max_bytes) is not int or not 1024 <= max_bytes <= 16777216:
            raise ValueError(
                "max_event_bytes must be an integer from 1024 to 16777216."
            )
        context = validate_context(document.get("operation_context", {}))
    except (OSError, UnicodeError, ValueError, TypeError, RecursionError) as error:
        raise LoggingConfigurationError(
            f"Cannot load logging configuration {config_path}: {error}"
        ) from error
    return db_path, float(timeout), max_bytes, context


def encode_event(event: object, max_bytes: int) -> str:
    """Validate the common envelope without depending on application event kinds."""
    event = copy_json_object(event, "event")
    if event.keys() != EVENT_FIELDS:
        raise ValueError("Event fields do not match the journal envelope.")
    if (
        type(event["schema_version"]) is not int
        or event["schema_version"] != SCHEMA_VERSION
    ):
        raise ValueError("Unsupported event schema_version.")
    for name in ("event_id", "producer_instance_id", "event_type", "occurred_at"):
        require_text(event[name], name)
    if event["operation_id"] is not None:
        require_text(event["operation_id"], "operation_id")
    sequence = event["sequence_number"]
    if type(sequence) is not int or not 1 <= sequence <= 9223372036854775807:
        raise ValueError("sequence_number must be a positive SQLite integer.")
    timestamp = datetime.fromisoformat(event["occurred_at"])
    if timestamp.utcoffset() != timedelta(0):
        raise ValueError("occurred_at must include the UTC timezone.")
    validate_context(event["context"])
    copy_json_object(event["data"], "event.data")
    encoded = json.dumps(
        event, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )
    if len(encoded.encode("utf-8")) > max_bytes:
        raise ValueError(f"Encoded event exceeds max_event_bytes ({max_bytes}).")
    return encoded
