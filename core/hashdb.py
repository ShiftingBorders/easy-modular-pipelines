# TODO: Согласовать фиксированную схему таблицы с её конфигурацией.

import sqlite3
from pathlib import Path

from pydantic import ValidationError

from core.hashdb_utils.dataclasses import HashDBConfig
from core.hashdb_utils.hashdb_errors import (
    FailedOpenHashDB,
    HashDBConfigError,
    HashDBConnectionClosedError,
    ModuleRemoveError,
    SchemaValidationFail,
    TableCreationErr,
    UnexpectedSchemaValErr,
)
from core.hashdb_utils.hashdb_states import (
    ColumnValidationResult,
    ModuleAddResult,
    SchemaValidationStatus,
)
from core.hashdb_utils.hashdb_validation import (
    clear_module_data_input,
    validate_column_desc,
)
from utils.dataloading import load_json


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
        config_path = Path(config_path).resolve()
        config = load_json(config_path)
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
            HashDBConfigError: If the configuration structure or either path
                is invalid.
        """
        try:
            return HashDBConfig.model_validate(config)
        except ValidationError as error:
            raise HashDBConfigError(
                f"Invalid database configuration: {error}"
            ) from error

    def _validate_schema_file(self, schema_path: Path) -> dict:
        """Load and validate a database schema file.

        Args:
            schema_path: Path to the JSON schema file.

        Returns:
            A mapping of column names to SQLite type declarations.

        Raises:
            SchemaValidationFail: If the schema differs from the locked schema
                or contains an invalid column description.
        """
        schema = load_json(schema_path)
        locked_schema = {
            "Mname": "VARCHAR(255) NOT NULL",
            "MVersion": "VARCHAR(255) NOT NULL",
            "MHash": "VARCHAR(255) NOT NULL",
        }
        if not isinstance(schema, dict):
            raise SchemaValidationFail(
                "Invalid database schema: expected a JSON object mapping column "
                "names to SQLite column types."
            )
        if len(schema) < 3:
            raise SchemaValidationFail(
                "Invalid database schema: at least three columns are required."
            )
        if list(schema.items()) != list(locked_schema.items()):
            raise SchemaValidationFail(
                "Invalid database schema: the schema must exactly match the "
                "first-version HashDB schema."
            )

        for column_name, column_type in schema.items():
            val_res = validate_column_desc(column_name, column_type)
            if val_res != ColumnValidationResult.column_valid:
                raise SchemaValidationFail(
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
        """Open the database and ensure that table `MAIN` has the expected schema.

        Args:
            db_file_path: Path to the SQLite database file.
            db_schema: Expected column names and SQLite type declarations.

        Raises:
            FailedOpenHashDB: If SQLite cannot open the database file.
            UnexpectedSchemaValErr: If SQLite fails while validating the
                database schema.
            TableCreationErr: If SQLite cannot create table `MAIN`.
            SchemaValidationFail: If an existing `MAIN` table has a different
                schema.
        """
        try:
            self.hash_db = sqlite3.connect(db_file_path)
            self._connection_closed = False
        except sqlite3.Error as error:
            raise FailedOpenHashDB(
                f"Failed to open DB file with error: {error}"
            ) from error
        try:
            schema_val = self._validate_db_schema(self.hash_db, db_schema)
        except sqlite3.Error as error:
            self.close_connection()
            raise UnexpectedSchemaValErr(
                f"Failed to validate DB schema with unexpected error: {error}"
            ) from error
        except Exception:
            self.close_connection()
            raise
        if schema_val == SchemaValidationStatus.empty:
            try:
                self._create_table(self.hash_db, db_schema)
            except sqlite3.Error as error:
                self.close_connection()
                raise TableCreationErr(
                    f"Failed to create HashDB table with error: {error}"
                ) from error
            except Exception:
                self.close_connection()
                raise
        elif schema_val == SchemaValidationStatus.mismatch:
            self.close_connection()
            raise SchemaValidationFail(
                "Database schema mismatch: table 'MAIN' does not match the "
                "configured column names, order, types, or constraints."
            )

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
            HashDBConnectionClosedError: If the database connection is closed.
            ModuleRegisterError: If any module value is invalid.
        """

        if self._connection_closed:
            raise HashDBConnectionClosedError(
                "Cannot register a module because the HashDB connection is closed."
            )

        module_name, module_version, module_hash = clear_module_data_input(
            module_name,
            module_version,
            module_hash,
        )

        with self.hash_db:
            insert_result = self.hash_db.execute(
                'INSERT INTO "MAIN" ("Mname", "MVersion", "MHash") '
                "VALUES (?, ?, ?) "
                'ON CONFLICT ("Mname", "MVersion") DO NOTHING',
                (module_name, module_version, module_hash),
            )
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
            HashDBConnectionClosedError: If the database connection is closed.
            ModuleRegisterError: If the module name or version is invalid.
        """

        if self._connection_closed:
            raise HashDBConnectionClosedError(
                "Cannot get a module hash because the HashDB connection is closed."
            )

        module_name, module_version, _ = clear_module_data_input(
            module_name,
            module_version,
            "_",
        )

        module = self.hash_db.execute(
            'SELECT "MHash" FROM "MAIN" WHERE "Mname" = ? AND "MVersion" = ? LIMIT 1',
            (module_name, module_version),
        ).fetchone()
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
            HashDBConnectionClosedError: If the database connection is closed.
            ModuleRegisterError: If the module name or version is invalid.
            ModuleRemoveError: If SQLite cannot remove the module hash.
        """
        if self._connection_closed:
            raise HashDBConnectionClosedError(
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
            raise ModuleRemoveError(
                "Failed to remove the module hash from HashDB."
            ) from error
        return delete_result.rowcount > 0

    def close_connection(self) -> None:
        """Close the SQLite connection if it is currently open."""
        if self._connection_closed:
            return
        self.hash_db.close()
        self._connection_closed = True
