"""Compatibility imports; use the responsibility packages for new code."""

from core.modules.errors import HashMismatch
from core.modules.manager import ModuleManager
from core.modules.manifest import read_module_manifest
from core.primitives.json_values import JsonObject, require_text
from core.storage.contracts import HashDatabase, ModuleAddResult, ModuleDatabase
from core.storage.errors import StorageConflict, StorageError, StoredObjectNotFound
from core.storage.module_identity import (
    HEX_DIGITS,
    INVALID_MODULE_NAME_CHARACTERS,
    INVALID_MODULE_VERSION_CHARACTERS,
)

__all__ = [
    "HEX_DIGITS",
    "INVALID_MODULE_NAME_CHARACTERS",
    "INVALID_MODULE_VERSION_CHARACTERS",
    "HashDatabase",
    "HashMismatch",
    "JsonObject",
    "ModuleAddResult",
    "ModuleDatabase",
    "ModuleManager",
    "StorageConflict",
    "StorageError",
    "StoredObjectNotFound",
    "read_module_manifest",
    "require_text",
]
