"""Compatibility imports; use the responsibility packages for new code."""

from core.participants.protocol import (
    IDENTITY_FIELDS,
    PROTOCOL_VERSION,
    encode_frame,
    error_details,
    participant_identity,
    read_frame,
    validate_request,
    validate_response,
)
from core.primitives.json_values import JsonObject, copy_json_object, require_text

__all__ = [
    "IDENTITY_FIELDS",
    "PROTOCOL_VERSION",
    "JsonObject",
    "copy_json_object",
    "encode_frame",
    "error_details",
    "participant_identity",
    "read_frame",
    "require_text",
    "validate_request",
    "validate_response",
]
