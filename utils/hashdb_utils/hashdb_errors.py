"""Legacy import aliases; use core.storage_errors for semantic handling.

Aliases preserve imports, not the former distinctions between operation names.
"""

from core import storage_errors

FailedOpenHashDB = storage_errors.StorageUnavailable
UnexpectedSchemaValErr = storage_errors.StorageError
TableCreationErr = storage_errors.StorageError
SchemaValidationFail = storage_errors.StorageConfigurationError
ModuleRegisterError = storage_errors.StorageInputError
ModuleRemoveError = storage_errors.StorageError
HashDBConnectionClosedError = storage_errors.StorageClosedError
HashDBConfigError = storage_errors.StorageConfigurationError
