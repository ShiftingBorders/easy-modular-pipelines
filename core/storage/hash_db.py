"""Hash db operations."""

import sqlite3
from pathlib import Path
from typing import NoReturn

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
from core.storage.hash_schema import (
    _create_table,
    _load_db_config,
    _unique_indexes,
    _validate_config,
    _validate_schema_file,
)
from core.storage.hash_states import SchemaValidationStatus
from core.storage.module_identity import clear_module_data_input


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
        return _load_db_config(config_path)

    def _validate_config(self, config: object) -> HashDBConfig:
        return _validate_config(config)

    def _validate_schema_file(self, schema_path: Path) -> dict:
        return _validate_schema_file(schema_path)

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
        actual_unique_constraints = _unique_indexes(db)

        expected_db = sqlite3.connect(":memory:")
        try:
            self._create_table(expected_db, db_schema)
            expected_schema = expected_db.execute(
                'PRAGMA table_info("MAIN")'
            ).fetchall()
            expected_unique_constraints = _unique_indexes(expected_db)
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
        return _create_table(db, db_schema)

    def _load_db(self, db_file_path: Path | str, db_schema):
        """Open storage and validate its schema before any persistent changes."""
        self._connection_closed = True
        try:
            if str(db_file_path) != ":memory:":
                db_file_path = Path(db_file_path)
                db_file_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise StorageIOError(
                f"Cannot create hash storage directory for {db_file_path}."
            ) from error
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

    def list_module_hashes(self) -> list[dict[str, str]]:
        if self._connection_closed:
            raise StorageClosedError("Cannot list modules: HashDB is closed.")
        try:
            rows = self.hash_db.execute(
                'SELECT "Mname", "MVersion", "MHash" FROM "MAIN" '
                'ORDER BY "Mname", "MVersion"'
            ).fetchall()
        except sqlite3.Error as error:
            self._raise_storage_error(error, "list module hashes")
        return [
            {"name": name, "version": version, "hash": digest}
            for name, version, digest in rows
        ]

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
        failures_by_code = {
            sqlite3.SQLITE_BUSY: StorageUnavailable,
            sqlite3.SQLITE_LOCKED: StorageUnavailable,
            sqlite3.SQLITE_CANTOPEN: StorageUnavailable,
            sqlite3.SQLITE_FULL: StorageCapacityError,
            sqlite3.SQLITE_AUTH: StorageAccessError,
            sqlite3.SQLITE_PERM: StorageAccessError,
            sqlite3.SQLITE_READONLY: StorageAccessError,
        }
        failure = failures_by_code.get(code)
        if failure is None:
            if isinstance(error, sqlite3.IntegrityError):
                failure = StorageConflict
            else:
                failure = StorageError
        raise failure(f"Failed to {operation}.") from error
