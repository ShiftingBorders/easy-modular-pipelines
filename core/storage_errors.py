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

__all__ = [
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
]
