"""Compatibility imports; use the responsibility packages for new code."""

from core.participants.connection import ParticipantConnection
from core.participants.protocol import (
    PROTOCOL_VERSION,
    encode_frame,
    participant_identity,
    read_frame,
    validate_response,
)
from core.primitives.json_files import read_json
from core.primitives.json_values import JsonObject, copy_json_object, require_text
from core.primitives.processes import process_identity

__all__ = [
    "PROTOCOL_VERSION",
    "JsonObject",
    "ParticipantConnection",
    "copy_json_object",
    "encode_frame",
    "participant_identity",
    "process_identity",
    "read_frame",
    "read_json",
    "require_text",
    "validate_response",
]
