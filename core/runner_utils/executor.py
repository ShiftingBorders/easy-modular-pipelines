"""Compatibility imports; use the responsibility packages for new code."""

from core.journal.logger import OperationLogger
from core.journal.streams import capture_stream
from core.participants.executor import StageExecutor, main
from core.participants.protocol import participant_identity
from core.participants.server import ParticipantServer
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import JsonObject, copy_json_object, require_number
from core.primitives.processes import process_identity

__all__ = [
    "JsonObject",
    "OperationLogger",
    "ParticipantServer",
    "StageExecutor",
    "capture_stream",
    "copy_json_object",
    "main",
    "participant_identity",
    "process_identity",
    "read_json",
    "require_number",
    "write_json",
]


if __name__ == "__main__":
    main()
