"""Compatibility imports; use the responsibility packages for new code."""

from core.storage.errors import (
    StorageAccessError,
    StorageCapacityError,
    StorageClosedError,
    StorageConfigurationError,
    StorageConflict,
    StorageError,
    StorageInputError,
    StorageIOError,
    StorageUnavailable,
    StoredObjectNotFound,
)
from core.storage.module_identity import check_input_metadata, clear_str
from core.storage.seaweed_client import SeaweedDB

__all__ = [
    "SeaweedDB",
    "StorageAccessError",
    "StorageCapacityError",
    "StorageClosedError",
    "StorageConfigurationError",
    "StorageConflict",
    "StorageError",
    "StorageIOError",
    "StorageInputError",
    "StorageUnavailable",
    "StoredObjectNotFound",
    "check_input_metadata",
    "clear_str",
]
