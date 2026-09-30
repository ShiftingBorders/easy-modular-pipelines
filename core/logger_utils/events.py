"""Compatibility imports; use the responsibility packages for new code."""

from core.journal.events import (
    CONTEXT_TEXT_FIELDS,
    EVENT_FIELDS,
    RESERVED_EVENT_TYPES,
    SCHEMA_VERSION,
    JournalGenerationChanged,
    LoggingConfigurationError,
    LoggingError,
    LoggingStateError,
    LoggingStorageError,
    encode_event,
    validate_checkpoint,
    validate_command_result,
    validate_context,
    validate_journal_identity,
)
from core.journal.settings import load_logging_settings
from core.primitives.json_values import (
    JsonObject,
    JsonValue,
    copy_json_object,
    require_number,
    require_text,
)

__all__ = [
    "CONTEXT_TEXT_FIELDS",
    "EVENT_FIELDS",
    "RESERVED_EVENT_TYPES",
    "SCHEMA_VERSION",
    "JournalGenerationChanged",
    "JsonObject",
    "JsonValue",
    "LoggingConfigurationError",
    "LoggingError",
    "LoggingStateError",
    "LoggingStorageError",
    "copy_json_object",
    "encode_event",
    "load_logging_settings",
    "require_number",
    "require_text",
    "validate_checkpoint",
    "validate_command_result",
    "validate_context",
    "validate_journal_identity",
]
