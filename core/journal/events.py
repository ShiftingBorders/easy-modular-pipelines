"""JSON event contract and configuration validation for core.journal.logger."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from uuid import UUID

from core.primitives.json_values import (
    JsonObject,
    copy_json_object,
    require_text,
)

if TYPE_CHECKING:
    from core.models.journal_records import CommandObservation, JournalCheckpoint

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
    checkpoint = _validated_checkpoint(value, key)
    if checkpoint is None:
        return None
    return {
        "journal_id": checkpoint.journal_id,
        "generation": checkpoint.generation,
        key: checkpoint.position,
    }


def _validated_checkpoint(value: object, key: str) -> JournalCheckpoint | None:
    if value is None:
        return None
    from core.models.journal_records import JournalCheckpoint

    return JournalCheckpoint.model_validate(value, context={"key": key})


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
    from core.models.journal_records import JournalEvent

    event = copy_json_object(event, "event")
    JournalEvent.model_validate(event)
    # Preserve the existing JSON key order and complete UTF-8 byte contract.
    encoded = json.dumps(
        event, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )
    if max_bytes is not None and len(encoded.encode("utf-8")) > max_bytes:
        raise ValueError(f"Encoded event exceeds max_event_bytes ({max_bytes}).")
    return encoded


def validate_command_result(data: object) -> JsonObject:
    """Validate a command observation without deciding runner/participant precedence."""
    return _validated_command_result(data).model_dump()


def _validated_command_result(data: object) -> CommandObservation:
    """Retain the validated observation until its journal output boundary."""
    from core.models.journal_records import CommandObservation

    return CommandObservation.model_validate(data)
