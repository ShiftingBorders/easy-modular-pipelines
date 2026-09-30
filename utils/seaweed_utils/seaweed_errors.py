"""Legacy import aliases; use core.storage.errors for semantic handling.

Aliases preserve imports, not the former distinctions between operation names.
"""

from core.storage import errors as storage_errors

IncorrectVolumePath = storage_errors.StorageConfigurationError
SeaweedInputFailure = storage_errors.StorageInputError
SeaweedReadError = storage_errors.StorageError
SeaweedWriteError = storage_errors.StorageError
SeaweedStartFailure = storage_errors.StorageUnavailable
SeaweedStopFailure = storage_errors.StorageError
