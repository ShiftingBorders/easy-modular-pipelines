"""Compatibility imports; use the responsibility packages for new code."""

from core.primitives.json_files import load_json
from core.storage.contracts import ModuleAddResult
from core.storage.errors import (
    StorageAccessError,
    StorageCapacityError,
    StorageClosedError,
    StorageConfigurationError,
    StorageConflict,
    StorageError,
    StorageIOError,
    StorageUnavailable,
)
from core.storage.hash_config import HashDBConfig
from core.storage.hash_db import HashDB
from core.storage.hash_states import ColumnValidationResult, SchemaValidationStatus
from core.storage.hash_validation import validate_column_desc
from core.storage.module_identity import clear_module_data_input

__all__ = [
    "ColumnValidationResult",
    "HashDB",
    "HashDBConfig",
    "ModuleAddResult",
    "SchemaValidationStatus",
    "StorageAccessError",
    "StorageCapacityError",
    "StorageClosedError",
    "StorageConfigurationError",
    "StorageConflict",
    "StorageError",
    "StorageIOError",
    "StorageUnavailable",
    "clear_module_data_input",
    "load_json",
    "validate_column_desc",
]
