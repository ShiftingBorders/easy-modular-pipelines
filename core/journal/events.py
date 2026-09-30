"""JSON event contract and configuration validation for core.journal.logger."""

import json
from datetime import datetime, timedelta
from uuid import UUID

from core.primitives.json_values import (
    JsonObject,
    copy_json_object,
    require_text,
)

SCHEMA_VERSION = 2
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
        "participant_id",
        "participant_instance_id",
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
    """Validate a command observation without deciding runner/participant precedence."""
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
    if result["author"] not in ("runner", "participant"):
        raise ValueError("Command result author must be runner or participant.")
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
