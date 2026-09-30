"""Compatibility imports; use the responsibility packages for new code."""

from core.journal.events import (
    SCHEMA_VERSION,
    JournalGenerationChanged,
    LoggingConfigurationError,
    LoggingStateError,
    LoggingStorageError,
    encode_event,
    validate_checkpoint,
    validate_command_result,
    validate_context,
    validate_journal_identity,
)
from core.journal.storage import SQLiteEventStore
from core.primitives.json_values import (
    JsonObject,
    copy_json_object,
    require_number,
    require_text,
)

__all__ = [
    "SCHEMA_VERSION",
    "JournalGenerationChanged",
    "JsonObject",
    "LoggingConfigurationError",
    "LoggingStateError",
    "LoggingStorageError",
    "SQLiteEventStore",
    "copy_json_object",
    "encode_event",
    "require_number",
    "require_text",
    "validate_checkpoint",
    "validate_command_result",
    "validate_context",
    "validate_journal_identity",
]
