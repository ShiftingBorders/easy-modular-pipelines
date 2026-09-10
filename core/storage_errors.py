"""Semantic failures shared by hash and archive storage implementations.

Catch StorageError for an expected storage failure, or a narrower category
when recovery differs. The operation identifies which storage failed; no
backend-specific exception imports are needed. Original causes are chained.
Unavailable does not promise that retrying is safe: a write may have completed.
Programming errors are not part of this contract and must not be disguised.
"""


class StorageError(Exception):
    """An expected storage operation failed; its outcome may be uncertain."""


class StorageConfigurationError(StorageError):
    """Configuration or the required storage schema is invalid."""


class StorageInputError(StorageError):
    """The supplied identity, path or other operation argument is invalid."""


class StorageUnavailable(StorageError):
    """The service or connection cannot currently perform the operation."""


class StorageClosedError(StorageUnavailable):
    """The caller used a resource after closing it."""


class StorageConflict(StorageError):
    """The operation conflicts with existing stored state."""


class StoredObjectNotFound(StorageError):
    """An operation requires a stored object that does not exist."""


class StorageCapacityError(StorageError):
    """A storage capacity, reserve or object-size limit prevents the operation."""


class StorageAccessError(StorageError):
    """Storage denied access or does not permit the requested modification."""


class StorageIOError(StorageError):
    """Local input/output failed while transferring storage data."""
