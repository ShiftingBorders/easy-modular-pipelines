class FailedOpenHashDB(Exception):
    """Raised when SQLite cannot open the hash database."""


class UnexpectedSchemaValErr(Exception):
    """Raised when SQLite fails while inspecting the database schema."""


class TableCreationErr(Exception):
    """Raised when SQLite cannot create the hash database table."""


class SchemaValidationFail(Exception):
    """Raised when a configured or existing database schema is invalid."""


class ModuleRegisterError(Exception):
    """Raised when module registration data is invalid."""


class ModuleRemoveError(Exception):
    """Raised when SQLite cannot remove a module hash."""


class HashDBConnectionClosedError(Exception):
    """Raised when an operation requires an open HashDB connection."""


class HashDBConfigError(Exception):
    """Raised when the HashDB configuration is invalid."""
