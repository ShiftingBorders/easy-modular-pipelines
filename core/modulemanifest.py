"""Compatibility imports; use the responsibility packages for new code."""

from core.modules.manifest import read_module_manifest
from core.primitives.json_values import JsonObject, copy_json_object, require_text
from core.storage.module_identity import check_input_metadata

__all__ = [
    "JsonObject",
    "check_input_metadata",
    "copy_json_object",
    "read_module_manifest",
    "require_text",
]
