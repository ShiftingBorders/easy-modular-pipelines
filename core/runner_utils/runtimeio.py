"""Compatibility imports; use the responsibility packages for new code."""

from core.journal.streams import capture_stream
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import copy_json_object
from core.primitives.processes import process_identity, process_running

__all__ = [
    "capture_stream",
    "copy_json_object",
    "process_identity",
    "process_running",
    "read_json",
    "write_json",
]
