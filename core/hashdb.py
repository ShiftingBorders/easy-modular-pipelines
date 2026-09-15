import sqlite3
from pathlib import Path
from typing import NoReturn

from pydantic import ValidationError

from core.storage_contracts import ModuleAddResult
from core.storage_errors import (
    StorageAccessError,
    StorageCapacityError,
    StorageClosedError,
    StorageConfigurationError,
    StorageConflict,
    StorageError,
    StorageUnavailable,
)
from utils.dataloading import load_json
from utils.hashdb_utils.dataclasses import HashDBConfig
from utils.hashdb_utils.hashdb_states import (
    ColumnValidationResult,
    SchemaValidationStatus,
)
from utils.hashdb_utils.hashdb_validation import (
    clear_module_data_input,
    validate_column_desc,
)


class HashDB:
    """Store and retrieve versioned module hashes in a SQLite database."""

    def __init__(self, config_path) -> None:
        """Initialize the hash database from a JSON configuration file.

        Args:
            config_path: Path to a configuration containing `schema_path` and
                `db_path`.
        """
        loaded_config = self._load_db_config(config_path)
        config = self._validate_config(loaded_config)
        schema = self._validate_schema_file(config.schema_path)
        self._load_db(config.db_path, schema)

    def _load_db_config(self, config_path: Path) -> dict:
        """Load configuration and resolve its paths relative to its file.

        Args:
            config_path: Path to the database configuration file.

        Returns:
            The loaded configuration with absolute schema and database paths
            when their original values are valid path-like values.
        """
        try:
            config_path = Path(config_path).resolve()
            config = load_json(config_path)
        except (OSError, ValueError, TypeError) as error:
            raise StorageConfigurationError(
                f"Cannot load hash storage configuration: {config_path}"
            ) from error
        if not isinstance(config, dict):
            return config

        for path_key in ("schema_path", "db_path"):
            configured_path = config.get(path_key)
            if (
                not isinstance(configured_path, (str, Path))
                or not str(configured_path).strip()
            ):
                continue

            configured_path = Path(configured_path)
            if not configured_path.is_absolute():
                configured_path = config_path.parent / configured_path
            config[path_key] = configured_path.resolve()
        return config

    def _validate_config(self, config: object) -> HashDBConfig:
        """Validate the required database configuration fields.

        Args:
            config: Loaded database configuration.

        Returns:
            The validated database configuration.

        Raises:
            StorageConfigurationError: If the configuration structure or either path
                is invalid.
        """
        try:
            return HashDBConfig.model_validate(config)
        except ValidationError as error:
            raise StorageConfigurationError(
                f"Invalid database configuration: {error}"
            ) from error

    def _validate_schema_file(self, schema_path: Path) -> dict:
        """Load and validate a database schema file.

        Args:
            schema_path: Path to the JSON schema file.

        Returns:
            A mapping of column names to SQLite type declarations.

        Raises:
            StorageConfigurationError: If the schema differs from the locked schema
                or contains an invalid column description.
        """
        try:
            schema = load_json(schema_path)
        except (OSError, ValueError) as error:
            raise StorageConfigurationError(
                f"Cannot load hash storage schema: {schema_path}"
            ) from error
        locked_schema = {
            "Mname": "VARCHAR(255) NOT NULL",
            "MVersion": "VARCHAR(255) NOT NULL",
            "MHash": "VARCHAR(255) NOT NULL",
        }
        if not isinstance(schema, dict):
            raise StorageConfigurationError(
                "Invalid database schema: expected a JSON object mapping column "
                "names to SQLite column types."
            )
        if len(schema) < 3:
            raise StorageConfigurationError(
                "Invalid database schema: at least three columns are required."
            )
        if list(schema.items()) != list(locked_schema.items()):
            raise StorageConfigurationError(
                "Invalid database schema: the schema must exactly match the "
                "first-version HashDB schema."
            )

        for column_name, column_type in schema.items():
            val_res = validate_column_desc(column_name, column_type)
            if val_res != ColumnValidationResult.column_valid:
                raise StorageConfigurationError(
                    f"Column {column_name}, {column_type} validation failed "
                    f"due to error: {val_res}."
                )
        return schema

    def _validate_db_schema(
        self,
        db: sqlite3.Connection,
        db_schema: dict[str, str],
    ) -> SchemaValidationStatus:
        """Compare the schema of table `MAIN` with the configured schema.

        Args:
            db: Open SQLite database connection.
            db_schema: Expected column names and SQLite type declarations.

        Returns:
            The status indicating that `MAIN` is absent, matches, or differs.
        """
        main_table = db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = ? COLLATE NOCASE",
            ("MAIN",),
        ).fetchone()
        if main_table is None:
            return SchemaValidationStatus.empty
        if main_table[0] != "MAIN":
            return SchemaValidationStatus.mismatch

        actual_schema = db.execute('PRAGMA table_info("MAIN")').fetchall()
        actual_unique_constraints = []
        for index in db.execute('PRAGMA index_list("MAIN")').fetchall():
            is_unique = bool(index[2])
            is_partial = bool(index[4])
            if not is_unique or is_partial:
                continue
            escaped_index_name = index[1].replace('"', '""')
            index_columns = db.execute(
                f'PRAGMA index_info("{escaped_index_name}")'
            ).fetchall()
            actual_unique_constraints.append(
                tuple(column[2] for column in index_columns)
            )

        expected_db = sqlite3.connect(":memory:")
        try:
            self._create_table(expected_db, db_schema)
            expected_schema = expected_db.execute(
                'PRAGMA table_info("MAIN")'
            ).fetchall()
            expected_unique_constraints = []
            for index in expected_db.execute('PRAGMA index_list("MAIN")').fetchall():
                is_unique = bool(index[2])
                is_partial = bool(index[4])
                if not is_unique or is_partial:
                    continue
                escaped_index_name = index[1].replace('"', '""')
                index_columns = expected_db.execute(
                    f'PRAGMA index_info("{escaped_index_name}")'
                ).fetchall()
                expected_unique_constraints.append(
                    tuple(column[2] for column in index_columns)
                )
        finally:
            expected_db.close()

        schemas_match = actual_schema == expected_schema
        unique_constraints_match = sorted(actual_unique_constraints) == sorted(
            expected_unique_constraints
        )
        if schemas_match and unique_constraints_match:
            return SchemaValidationStatus.correct
        return SchemaValidationStatus.mismatch

    def _create_table(self, db: sqlite3.Connection, db_schema: dict[str, str]):
        """Create table `MAIN` using the configured database schema.

        Args:
            db: Open SQLite database connection.
            db_schema: Column names and SQLite type declarations to create.
        """
        column_definitions = []
        for column_name, column_type in db_schema.items():
            escaped_column_name = column_name.replace('"', '""')
            column_definitions.append(f'"{escaped_column_name}" {column_type.strip()}')
        column_definitions.append('UNIQUE ("Mname", "MVersion")')

        create_statement = f'CREATE TABLE "MAIN" ({", ".join(column_definitions)})'
        with db:
            db.execute(create_statement)

    def _load_db(self, db_file_path: Path, db_schema):
        """Open storage and validate its schema before any persistent changes."""
        self._connection_closed = True
        try:
            self.hash_db = sqlite3.connect(db_file_path)
        except (sqlite3.ProgrammingError, sqlite3.InterfaceError):
            raise
        except sqlite3.Error as error:
            raise StorageUnavailable(
                f"Cannot open hash storage at {db_file_path}."
            ) from error
        self._connection_closed = False

        operation = "validate the hash storage schema"
        try:
            try:
                schema_status = self._validate_db_schema(self.hash_db, db_schema)
                if schema_status == SchemaValidationStatus.empty:
                    operation = "create the hash storage table"
                    self._create_table(self.hash_db, db_schema)
                elif schema_status == SchemaValidationStatus.mismatch:
                    raise StorageConfigurationError(
                        "Database schema mismatch: table 'MAIN' does not match the "
                        "configured column names, order, types, or constraints."
                    )
            except sqlite3.Error as error:
                self._raise_storage_error(error, operation)
        except BaseException as error:
            try:
                self.close_connection()
            except (StorageError, sqlite3.Error) as cleanup_error:
                error.add_note(f"Closing hash storage also failed: {cleanup_error}")
            raise

    def add_module_hash(
        self,
        module_name: str,
        module_version: str,
        module_hash: str,
    ) -> ModuleAddResult:
        """Store a hash if the module name and version are not already present.

        Args:
            module_name: Name of the module.
            module_version: Version of the module.
            module_hash: Hash associated with the module.

        Returns:
            `module_added` when the hash was stored, otherwise
            `module_exists_err`.

        Raises:
            StorageClosedError: If the database connection is closed.
            StorageInputError: If any module value is invalid.
            StorageError: If the driver cannot complete the insert or commit.
        """

        if self._connection_closed:
            raise StorageClosedError(
                "Cannot register a module because the HashDB connection is closed."
            )

        module_name, module_version, module_hash = clear_module_data_input(
            module_name,
            module_version,
            module_hash,
        )

        try:
            with self.hash_db:
                insert_result = self.hash_db.execute(
                    'INSERT INTO "MAIN" ("Mname", "MVersion", "MHash") '
                    "VALUES (?, ?, ?) "
                    'ON CONFLICT ("Mname", "MVersion") DO NOTHING',
                    (module_name, module_version, module_hash),
                )
        except sqlite3.Error as error:
            self._raise_storage_error(error, "add a module hash")
        if insert_result.rowcount == 0:
            return ModuleAddResult.module_exists_err
        return ModuleAddResult.module_added

    def get_module_hash(self, module_name: str, module_version: str) -> str:
        """Return the hash stored for a module.

        Args:
            module_name: Name of the module to find.
            module_version: Version of the module to find.

        Returns:
            The stored module hash, or an empty string when no module is found.

        Raises:
            StorageClosedError: If the database connection is closed.
            StorageInputError: If the module name or version is invalid.
            StorageError: If the driver cannot complete the lookup.
        """

        if self._connection_closed:
            raise StorageClosedError(
                "Cannot get a module hash because the HashDB connection is closed."
            )

        module_name, module_version, _ = clear_module_data_input(
            module_name,
            module_version,
            "_",
        )

        try:
            module = self.hash_db.execute(
                'SELECT "MHash" FROM "MAIN" WHERE "Mname" = ? AND "MVersion" = ? LIMIT 1',
                (module_name, module_version),
            ).fetchone()
        except sqlite3.Error as error:
            self._raise_storage_error(error, "get a module hash")
        if module is None:
            return ""
        return module[0]

    def remove_module_hash(self, module_name: str, module_version: str) -> bool:
        """Remove the hash stored for a module name and version.

        Args:
            module_name: Name of the module to remove.
            module_version: Version of the module to remove.

        Returns:
            True when a stored hash was removed, otherwise False.

        Raises:
            StorageClosedError: If the database connection is closed.
            StorageInputError: If the module name or version is invalid.
            StorageError: If SQLite cannot remove the module hash.
        """
        if self._connection_closed:
            raise StorageClosedError(
                "Cannot remove a module hash because the HashDB connection is closed."
            )

        module_name, module_version, _ = clear_module_data_input(
            module_name,
            module_version,
            "_",
        )

        try:
            with self.hash_db:
                delete_result = self.hash_db.execute(
                    'DELETE FROM "MAIN" WHERE "Mname" = ? AND "MVersion" = ?',
                    (module_name, module_version),
                )
        except sqlite3.Error as error:
            self._raise_storage_error(error, "remove a module hash")
        return delete_result.rowcount > 0

    def close_connection(self) -> None:
        """Close the SQLite connection if it is currently open."""
        if self._connection_closed:
            return
        try:
            self.hash_db.close()
        except sqlite3.Error as error:
            self._raise_storage_error(error, "close hash storage")
        self._connection_closed = True

    def _raise_storage_error(self, error: sqlite3.Error, operation: str) -> NoReturn:
        """Translate known driver failures without hiding programming mistakes."""
        if isinstance(error, (sqlite3.ProgrammingError, sqlite3.InterfaceError)):
            raise error
        code = getattr(error, "sqlite_errorcode", 0) & 0xFF
        if code in {
            sqlite3.SQLITE_BUSY,
            sqlite3.SQLITE_LOCKED,
            sqlite3.SQLITE_CANTOPEN,
        }:
            failure = StorageUnavailable
        elif code == sqlite3.SQLITE_FULL:
            failure = StorageCapacityError
        elif code in {
            sqlite3.SQLITE_AUTH,
            sqlite3.SQLITE_PERM,
            sqlite3.SQLITE_READONLY,
        }:
            failure = StorageAccessError
        elif isinstance(error, sqlite3.IntegrityError):
            failure = StorageConflict
        else:
            failure = StorageError
        raise failure(f"Failed to {operation}.") from error
