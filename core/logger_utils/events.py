"""JSON event contract and configuration validation for core.logger."""

import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

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
        "template.applied",
        "attempt.parameters",
        "command.result",
    }
)
CONTEXT_TEXT_FIELDS = frozenset(
    {
        "experiment_id",
        "previous_experiment_id",
        "previous_run_id",
        "dag_revision_id",
        "template_revision_id",
        "stage_id",
        "stage_execution_id",
        "cycle_id",
        "attempt_id",
        "service_id",
        "request_id",
        "command_id",
        "command_chain_id",
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


class JournalGenerationChanged(LoggingStateError):
    """A checkpoint or connection belongs to a different journal state."""

    code = "journal_generation_changed"

    def __init__(self, expected: JsonObject, actual: JsonObject) -> None:
        super().__init__("Journal identity or generation changed; restart reading.")
        self.expected = dict(expected)
        self.actual = dict(actual)


def validate_journal_identity(value: object) -> JsonObject:
    identity = copy_json_object(value, "journal identity")
    if identity.keys() != {"journal_id", "generation"}:
        raise ValueError("Journal identity requires journal_id and generation.")
    for name in identity:
        identity[name] = UUID(require_text(identity[name], name)).hex
    return identity


def validate_checkpoint(value: object, key: str) -> JsonObject | None:
    if value is None:
        return None
    checkpoint = copy_json_object(value, "checkpoint")
    if checkpoint.keys() != {"journal_id", "generation", key}:
        raise ValueError(f"Checkpoint requires journal_id, generation and {key}.")
    if (
        type(checkpoint[key]) is not int
        or not 0 <= checkpoint[key] <= 9223372036854775807
    ):
        raise ValueError(f"{key} must be a nonnegative SQLite integer.")
    identity = validate_journal_identity(
        {name: checkpoint[name] for name in ("journal_id", "generation")}
    )
    return {**identity, key: checkpoint[key]}


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
    text_fields = CONTEXT_TEXT_FIELDS
    integer_fields = {"attempt_number", "process_id", "cycle_number", "stage_position"}
    unknown = context.keys() - text_fields - integer_fields
    if unknown:
        raise ValueError(f"Unknown context fields: {', '.join(sorted(unknown))}.")
    for name, item in context.items():
        if item is None:
            continue
        if name in text_fields:
            require_text(item, name)
        elif type(item) is not int or item < 1:
            raise ValueError(f"{name} must be a positive integer.")
    return context


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
        configured_path = require_text(settings.get("db_path"), "logging.db_path")
        db_path = Path(configured_path)
        if not db_path.is_absolute():
            # Drive-relative/root-relative Windows paths cannot be anchored reliably.
            if db_path.drive or db_path.root:
                raise ValueError("db_path must be absolute or relative to the config.")
            db_path = config_path.parent / db_path
        timeout = require_number(settings["busy_timeout_seconds"], "timeout")
        if not 0 < timeout <= 60:
            raise ValueError(
                "busy_timeout_seconds must be greater than 0 and at most 60."
            )
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


def encode_event(event: object, max_bytes: int | None) -> str:
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
    if max_bytes is not None and len(encoded.encode("utf-8")) > max_bytes:
        raise ValueError(f"Encoded event exceeds max_event_bytes ({max_bytes}).")
    return encoded


def validate_command_result(data: object) -> JsonObject:
    """Validate a command observation without deciding runner/service precedence."""
    result = copy_json_object(data, "command result")
    if result.keys() != {
        "request_id",
        "author",
        "outcome",
        "response",
        "ignored",
        "supersedes",
    }:
        raise ValueError("Command result fields do not match the contract.")
    require_text(result["request_id"], "request_id")
    if result["author"] not in ("runner", "service"):
        raise ValueError("Command result author must be runner or service.")
    if result["outcome"] not in (
        "succeeded",
        "failed",
        "cancelled",
        "timed_out",
        "invalidated",
    ):
        raise ValueError("Unsupported command outcome.")
    copy_json_object(result["response"], "response")
    if result["ignored"] is not None:
        require_text(result["ignored"], "ignored")
    if type(result["supersedes"]) is not list:
        raise TypeError("supersedes must be a list.")
    for item in result["supersedes"]:
        if type(item) is not dict or item.keys() != {"event_id", "ignored"}:
            raise ValueError("Invalid superseded observation.")
        require_text(item["event_id"], "supersedes.event_id")
        require_text(item["ignored"], "supersedes.ignored")
    return result
