import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from core.hashdb import HashDB
from core.hashdb_utils.hashdb_errors import (
    FailedOpenHashDB,
    HashDBConfigError,
    HashDBConnectionClosedError,
    ModuleRegisterError,
    SchemaValidationFail,
    TableCreationErr,
    UnexpectedSchemaValErr,
)
from core.hashdb_utils.hashdb_states import (
    ColumnValidationResult,
    ModuleAddResult,
    SchemaValidationStatus,
)

LOCKED_SCHEMA = {
    "Mname": "VARCHAR(255) NOT NULL",
    "MVersion": "VARCHAR(255) NOT NULL",
    "MHash": "VARCHAR(255) NOT NULL",
}


class HashDBTestCase(unittest.TestCase):
    """Provide isolated files and helpers for HashDB tests."""

    def setUp(self) -> None:
        """Create a temporary directory containing the locked schema."""
        self.temp_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_directory.cleanup)
        self.temp_path = Path(self.temp_directory.name)
        self.schema_path = self.temp_path / "schema.json"
        self._write_json(self.schema_path, LOCKED_SCHEMA)

    def _write_json(self, path: Path, value) -> None:
        """Write a test value as UTF-8 JSON."""
        path.write_text(json.dumps(value), encoding="utf-8")

    def _write_config(
        self,
        schema_path="schema.json",
        db_path="hash.db",
        filename="config.json",
    ) -> Path:
        """Create a HashDB configuration and return its path."""
        config_path = self.temp_path / filename
        self._write_json(
            config_path,
            {"schema_path": schema_path, "db_path": db_path},
        )
        return config_path

    def _create_hash_db(self, db_path="hash.db") -> HashDB:
        """Create a configured HashDB in the temporary directory."""
        return HashDB(self._write_config(db_path=db_path))

    def _new_uninitialized_hash_db(self) -> HashDB:
        """Create an instance for testing methods without initialization."""
        return HashDB.__new__(HashDB)


class HashDBConfigTests(HashDBTestCase):
    """Verify configuration loading, validation, and path resolution."""

    def test_rejects_missing_config_file(self) -> None:
        """A nonexistent configuration file must fail during JSON loading."""
        with self.assertRaises(FileNotFoundError):
            HashDB(self.temp_path / "missing.json")

    def test_rejects_config_directory(self) -> None:
        """A directory cannot be used as the configuration file."""
        with self.assertRaises(FileNotFoundError):
            HashDB(self.temp_path)

    def test_rejects_invalid_json_config(self) -> None:
        """Malformed configuration JSON must fail during loading."""
        config_path = self.temp_path / "config.json"
        config_path.write_text("", encoding="utf-8")

        with self.assertRaises(ValueError) as raised:
            HashDB(config_path)

        self.assertIsInstance(raised.exception.__cause__, json.JSONDecodeError)

    def test_rejects_non_object_config(self) -> None:
        """The top-level configuration value must be an object."""
        config_path = self.temp_path / "config.json"
        self._write_json(config_path, [])

        with self.assertRaises(HashDBConfigError):
            HashDB(config_path)

    def test_rejects_missing_config_fields(self) -> None:
        """Both schema_path and db_path are required."""
        invalid_configs = ({}, {"schema_path": "schema.json"}, {"db_path": "hash.db"})

        for number, config in enumerate(invalid_configs):
            with self.subTest(config=config):
                config_path = self.temp_path / f"config_{number}.json"
                self._write_json(config_path, config)
                with self.assertRaises(HashDBConfigError):
                    HashDB(config_path)

    def test_rejects_invalid_config_path_values(self) -> None:
        """Configured paths must be non-empty path-like values."""
        invalid_values = ("", "   ", None, 12, [], {})
        base_config = {"schema_path": "schema.json", "db_path": "hash.db"}

        for field_name in ("schema_path", "db_path"):
            for number, invalid_value in enumerate(invalid_values):
                with self.subTest(field=field_name, value=invalid_value):
                    config = dict(base_config)
                    config[field_name] = invalid_value
                    config_path = self.temp_path / f"{field_name}_{number}.json"
                    self._write_json(config_path, config)
                    with self.assertRaises(HashDBConfigError):
                        HashDB(config_path)

    def test_rejects_missing_schema_file(self) -> None:
        """schema_path must reference an existing file."""
        config_path = self._write_config(schema_path="missing.json")

        with self.assertRaises(HashDBConfigError):
            HashDB(config_path)

    def test_rejects_schema_without_json_extension(self) -> None:
        """The schema filename must have a JSON extension."""
        schema_path = self.temp_path / "schema.txt"
        self._write_json(schema_path, LOCKED_SCHEMA)
        config_path = self._write_config(schema_path="schema.txt")

        with self.assertRaises(HashDBConfigError):
            HashDB(config_path)

    def test_rejects_schema_directory(self) -> None:
        """A directory with a JSON-like name is not a schema file."""
        schema_directory = self.temp_path / "schema.json"
        self.schema_path.unlink()
        schema_directory.mkdir()
        config_path = self._write_config(schema_path="schema.json")

        with self.assertRaises(HashDBConfigError):
            HashDB(config_path)

    def test_rejects_directory_as_database_path(self) -> None:
        """db_path must not reference a directory."""
        database_directory = self.temp_path / "database"
        database_directory.mkdir()
        config_path = self._write_config(db_path="database")

        with self.assertRaises(HashDBConfigError):
            HashDB(config_path)

    def test_rejects_database_path_with_missing_parent(self) -> None:
        """The parent directory of a new database must already exist."""
        config_path = self._write_config(db_path="missing/hash.db")

        with self.assertRaises(HashDBConfigError):
            HashDB(config_path)

    def test_creates_missing_database_file_in_existing_directory(self) -> None:
        """Initialization creates a missing database in a valid directory."""
        database_path = self.temp_path / "hash.db"
        hash_db = self._create_hash_db()
        self.addCleanup(hash_db.close_connection)

        self.assertTrue(database_path.is_file())

    def test_resolves_relative_paths_from_config_directory(self) -> None:
        """Relative paths use the configuration directory, not process cwd."""
        config_directory = self.temp_path / "config"
        config_directory.mkdir()
        schema_path = config_directory / "schema.json"
        self._write_json(schema_path, LOCKED_SCHEMA)
        config_path = config_directory / "hashdb.json"
        self._write_json(
            config_path,
            {"schema_path": "schema.json", "db_path": "hash.db"},
        )
        original_working_directory = Path.cwd()

        try:
            os.chdir(self.temp_path)
            config = self._new_uninitialized_hash_db()._load_db_config(config_path)
        finally:
            os.chdir(original_working_directory)

        self.assertEqual(config["schema_path"], schema_path.resolve())
        self.assertEqual(
            config["db_path"],
            (config_directory / "hash.db").resolve(),
        )

    def test_preserves_absolute_configured_paths(self) -> None:
        """Absolute configured paths remain absolute and unchanged."""
        database_path = self.temp_path / "absolute.db"
        config_path = self._write_config(
            schema_path=str(self.schema_path.resolve()),
            db_path=str(database_path.resolve()),
        )

        config = self._new_uninitialized_hash_db()._load_db_config(config_path)

        self.assertEqual(config["schema_path"], self.schema_path.resolve())
        self.assertEqual(config["db_path"], database_path.resolve())

    def test_wraps_database_open_error_and_preserves_cause(self) -> None:
        """SQLite open failures retain their cause in FailedOpenHashDB."""
        hash_db = self._new_uninitialized_hash_db()
        open_error = sqlite3.OperationalError("cannot open database")

        with (
            patch("core.hashdb.sqlite3.connect", side_effect=open_error),
            self.assertRaises(FailedOpenHashDB) as raised,
        ):
            hash_db._load_db(self.temp_path / "hash.db", LOCKED_SCHEMA)

        self.assertIs(raised.exception.__cause__, open_error)


class HashDBSchemaFileTests(HashDBTestCase):
    """Verify the locked schema file and individual column descriptions."""

    def test_rejects_non_object_schema(self) -> None:
        """The top-level schema value must be an object."""
        for number, schema in enumerate(([], "schema", None)):
            with self.subTest(schema=schema):
                path = self.temp_path / f"schema_{number}.json"
                self._write_json(path, schema)
                with self.assertRaises(SchemaValidationFail):
                    self._new_uninitialized_hash_db()._validate_schema_file(path)

    def test_rejects_schema_with_fewer_than_three_columns(self) -> None:
        """The schema must contain all three locked columns."""
        self._write_json(self.schema_path, {"Mname": "VARCHAR(255) NOT NULL"})

        with self.assertRaises(SchemaValidationFail):
            self._new_uninitialized_hash_db()._validate_schema_file(self.schema_path)

    def test_rejects_any_change_to_locked_schema(self) -> None:
        """Names, order, types, constraints, and count are fixed."""
        changed_schemas = (
            {**LOCKED_SCHEMA, "Extra": "TEXT"},
            {
                "MVersion": LOCKED_SCHEMA["MVersion"],
                "Mname": LOCKED_SCHEMA["Mname"],
                "MHash": LOCKED_SCHEMA["MHash"],
            },
            {**LOCKED_SCHEMA, "Other": LOCKED_SCHEMA["MHash"]},
            {**LOCKED_SCHEMA, "MHash": "TEXT NOT NULL"},
            {**LOCKED_SCHEMA, "MHash": "VARCHAR(255)"},
        )

        for number, schema in enumerate(changed_schemas):
            with self.subTest(schema=schema):
                path = self.temp_path / f"changed_schema_{number}.json"
                self._write_json(path, schema)
                with self.assertRaises(SchemaValidationFail):
                    self._new_uninitialized_hash_db()._validate_schema_file(path)

    def test_accepts_locked_schema_and_preserves_order(self) -> None:
        """The exact locked schema is accepted in its declared order."""
        schema = self._new_uninitialized_hash_db()._validate_schema_file(
            self.schema_path
        )

        self.assertEqual(list(schema), ["Mname", "MVersion", "MHash"])
        self.assertEqual(schema, LOCKED_SCHEMA)

    def test_column_description_validation_results(self) -> None:
        """Each invalid column condition returns its specific result."""
        hash_db = self._new_uninitialized_hash_db()
        cases = (
            ("Mname", "TEXT", ColumnValidationResult.column_valid),
            ("bad-name", "TEXT", ColumnValidationResult.column_name_invalid_char),
            ("", "TEXT", ColumnValidationResult.column_name_err),
            (None, "TEXT", ColumnValidationResult.column_name_err),
            ("Mname", "TEXT;", ColumnValidationResult.column_type_invalid_char),
            ("Mname", "", ColumnValidationResult.column_type_err),
            ("Mname", None, ColumnValidationResult.column_type_err),
        )

        for column_name, column_type, expected_result in cases:
            with self.subTest(name=column_name, column_type=column_type):
                result = hash_db._validate_column_desc(column_name, column_type)
                self.assertIs(result, expected_result)


class HashDBDatabaseSchemaTests(HashDBTestCase):
    """Verify comparison between existing and expected database schemas."""

    def setUp(self) -> None:
        """Create an uninitialized HashDB and an in-memory SQLite database."""
        super().setUp()
        self.hash_db = self._new_uninitialized_hash_db()
        self.db = sqlite3.connect(":memory:")

    def tearDown(self) -> None:
        """Close the in-memory database after each schema test."""
        self.db.close()

    def test_returns_empty_when_main_is_absent(self) -> None:
        """A database without MAIN reports an empty schema state."""
        self.assertIs(
            self.hash_db._validate_db_schema(self.db, LOCKED_SCHEMA),
            SchemaValidationStatus.empty,
        )

    def test_rejects_case_variants_of_main(self) -> None:
        """Only the exact stored table name MAIN is accepted."""
        for table_name in ("main", "Main"):
            with self.subTest(table_name=table_name):
                db = sqlite3.connect(":memory:")
                self.addCleanup(db.close)
                db.execute(f'CREATE TABLE "{table_name}" (value TEXT)')
                self.assertIs(
                    self.hash_db._validate_db_schema(db, LOCKED_SCHEMA),
                    SchemaValidationStatus.mismatch,
                )

    def test_ignores_unrelated_table_when_main_is_absent(self) -> None:
        """Unrelated tables do not prevent MAIN from being considered absent."""
        self.db.execute('CREATE TABLE "OTHER" (value TEXT)')

        self.assertIs(
            self.hash_db._validate_db_schema(self.db, LOCKED_SCHEMA),
            SchemaValidationStatus.empty,
        )

    def test_rejects_mismatched_main_columns(self) -> None:
        """Column count, names, order, types, and constraints must match."""
        definitions = (
            '"Mname" TEXT',
            '"Mname" TEXT, "MVersion" TEXT, "MHash" TEXT, "Extra" TEXT',
            (
                '"Other" VARCHAR(255) NOT NULL, '
                '"MVersion" VARCHAR(255) NOT NULL, '
                '"MHash" VARCHAR(255) NOT NULL'
            ),
            (
                '"MVersion" VARCHAR(255) NOT NULL, '
                '"Mname" VARCHAR(255) NOT NULL, '
                '"MHash" VARCHAR(255) NOT NULL'
            ),
            (
                '"Mname" TEXT NOT NULL, '
                '"MVersion" VARCHAR(255) NOT NULL, '
                '"MHash" VARCHAR(255) NOT NULL'
            ),
            (
                '"Mname" VARCHAR(255), '
                '"MVersion" VARCHAR(255) NOT NULL, '
                '"MHash" VARCHAR(255) NOT NULL'
            ),
        )

        for definition in definitions:
            with self.subTest(definition=definition):
                db = sqlite3.connect(":memory:")
                self.addCleanup(db.close)
                db.execute(f'CREATE TABLE "MAIN" ({definition})')
                self.assertIs(
                    self.hash_db._validate_db_schema(db, LOCKED_SCHEMA),
                    SchemaValidationStatus.mismatch,
                )

    def test_rejects_missing_or_different_unique_constraint(self) -> None:
        """MAIN requires uniqueness of the module name and version pair."""
        definitions = (
            (
                '"Mname" VARCHAR(255) NOT NULL, '
                '"MVersion" VARCHAR(255) NOT NULL, '
                '"MHash" VARCHAR(255) NOT NULL'
            ),
            (
                '"Mname" VARCHAR(255) NOT NULL UNIQUE, '
                '"MVersion" VARCHAR(255) NOT NULL, '
                '"MHash" VARCHAR(255) NOT NULL'
            ),
        )

        for definition in definitions:
            with self.subTest(definition=definition):
                db = sqlite3.connect(":memory:")
                self.addCleanup(db.close)
                db.execute(f'CREATE TABLE "MAIN" ({definition})')
                self.assertIs(
                    self.hash_db._validate_db_schema(db, LOCKED_SCHEMA),
                    SchemaValidationStatus.mismatch,
                )

    def test_rejects_partial_unique_constraint(self) -> None:
        """A partial index does not guarantee uniqueness for every row."""
        self.db.execute(
            'CREATE TABLE "MAIN" ('
            '"Mname" VARCHAR(255) NOT NULL, '
            '"MVersion" VARCHAR(255) NOT NULL, '
            '"MHash" VARCHAR(255) NOT NULL)'
        )
        self.db.execute(
            'CREATE UNIQUE INDEX "partial_pair" '
            'ON "MAIN" ("Mname", "MVersion") '
            'WHERE length("MHash") > 0'
        )

        self.assertIs(
            self.hash_db._validate_db_schema(self.db, LOCKED_SCHEMA),
            SchemaValidationStatus.mismatch,
        )

    def test_accepts_matching_main_schema(self) -> None:
        """A table created from the locked schema is accepted."""
        self.hash_db._create_table(self.db, LOCKED_SCHEMA)

        self.assertIs(
            self.hash_db._validate_db_schema(self.db, LOCKED_SCHEMA),
            SchemaValidationStatus.correct,
        )

    def test_mismatch_closes_connection_without_changing_table(self) -> None:
        """A mismatch closes HashDB without modifying the existing table."""
        database_path = self.temp_path / "mismatch.db"
        connection = sqlite3.connect(database_path)
        connection.execute('CREATE TABLE "MAIN" (value TEXT)')
        connection.commit()
        connection.close()
        hash_db = self._new_uninitialized_hash_db()

        with self.assertRaises(SchemaValidationFail):
            hash_db._load_db(database_path, LOCKED_SCHEMA)

        self.assertTrue(hash_db._connection_closed)
        with closing(sqlite3.connect(database_path)) as verification_db:
            columns = verification_db.execute('PRAGMA table_info("MAIN")').fetchall()
        self.assertEqual([column[1] for column in columns], ["value"])

    def test_wraps_schema_sqlite_error_and_closes_connection(self) -> None:
        """SQLite inspection failures are wrapped and close the connection."""
        hash_db = self._new_uninitialized_hash_db()
        validation_error = sqlite3.OperationalError("validation failed")

        with (
            patch.object(
                hash_db,
                "_validate_db_schema",
                side_effect=validation_error,
            ),
            self.assertRaises(UnexpectedSchemaValErr) as raised,
        ):
            hash_db._load_db(":memory:", LOCKED_SCHEMA)

        self.assertIs(raised.exception.__cause__, validation_error)
        self.assertTrue(hash_db._connection_closed)

    def test_propagates_unexpected_validation_error_and_closes(self) -> None:
        """Non-SQLite inspection errors retain their type and close HashDB."""
        hash_db = self._new_uninitialized_hash_db()
        validation_error = RuntimeError("validation failed")

        with (
            patch.object(
                hash_db,
                "_validate_db_schema",
                side_effect=validation_error,
            ),
            self.assertRaises(RuntimeError) as raised,
        ):
            hash_db._load_db(":memory:", LOCKED_SCHEMA)

        self.assertIs(raised.exception, validation_error)
        self.assertTrue(hash_db._connection_closed)


class HashDBTableCreationTests(HashDBTestCase):
    """Verify creation and persistence of the locked MAIN table."""

    def test_creates_expected_table_and_constraints(self) -> None:
        """A new MAIN has the expected columns and composite uniqueness."""
        hash_db = self._create_hash_db()
        self.addCleanup(hash_db.close_connection)

        columns = hash_db.hash_db.execute('PRAGMA table_info("MAIN")').fetchall()
        column_descriptions = [(column[1], column[2], column[3]) for column in columns]
        unique_indexes = hash_db.hash_db.execute('PRAGMA index_list("MAIN")').fetchall()
        unique_columns = []
        for index in unique_indexes:
            if index[2]:
                index_columns = hash_db.hash_db.execute(
                    f'PRAGMA index_info("{index[1]}")'
                ).fetchall()
                unique_columns.append(tuple(column[2] for column in index_columns))

        self.assertEqual(
            column_descriptions,
            [
                ("Mname", "VARCHAR(255)", 1),
                ("MVersion", "VARCHAR(255)", 1),
                ("MHash", "VARCHAR(255)", 1),
            ],
        )
        self.assertEqual(unique_columns, [("Mname", "MVersion")])

    def test_database_constraint_allows_versions_and_rejects_duplicate_pair(
        self,
    ) -> None:
        """SQLite permits versions but rejects a duplicate name/version pair."""
        hash_db = self._create_hash_db()
        self.addCleanup(hash_db.close_connection)
        hash_db.hash_db.execute(
            'INSERT INTO "MAIN" VALUES (?, ?, ?)',
            ("module", "1", "hash-1"),
        )
        hash_db.hash_db.execute(
            'INSERT INTO "MAIN" VALUES (?, ?, ?)',
            ("module", "2", "hash-2"),
        )

        with self.assertRaises(sqlite3.IntegrityError):
            hash_db.hash_db.execute(
                'INSERT INTO "MAIN" VALUES (?, ?, ?)',
                ("module", "1", "other-hash"),
            )

    def test_reopens_created_database_with_correct_schema(self) -> None:
        """A newly created database remains valid when reopened."""
        config_path = self._write_config()
        hash_db = HashDB(config_path)
        hash_db.close_connection()

        reopened_hash_db = HashDB(config_path)
        self.addCleanup(reopened_hash_db.close_connection)

        self.assertFalse(reopened_hash_db._connection_closed)

    def test_wraps_table_creation_sqlite_error_and_closes(self) -> None:
        """SQLite creation failures are wrapped and close the connection."""
        hash_db = self._new_uninitialized_hash_db()
        creation_error = sqlite3.OperationalError("creation failed")

        with (
            patch.object(
                hash_db,
                "_validate_db_schema",
                return_value=SchemaValidationStatus.empty,
            ),
            patch.object(hash_db, "_create_table", side_effect=creation_error),
            self.assertRaises(TableCreationErr) as raised,
        ):
            hash_db._load_db(":memory:", LOCKED_SCHEMA)

        self.assertIs(raised.exception.__cause__, creation_error)
        self.assertTrue(hash_db._connection_closed)

    def test_propagates_unexpected_creation_error_and_closes(self) -> None:
        """Non-SQLite creation errors retain their type and close HashDB."""
        hash_db = self._new_uninitialized_hash_db()
        creation_error = RuntimeError("creation failed")

        with (
            patch.object(
                hash_db,
                "_validate_db_schema",
                return_value=SchemaValidationStatus.empty,
            ),
            patch.object(hash_db, "_create_table", side_effect=creation_error),
            self.assertRaises(RuntimeError) as raised,
        ):
            hash_db._load_db(":memory:", LOCKED_SCHEMA)

        self.assertIs(raised.exception, creation_error)
        self.assertTrue(hash_db._connection_closed)


class HashDBModuleOperationsTests(HashDBTestCase):
    """Verify registration and lookup of versioned module hashes."""

    def setUp(self) -> None:
        """Create a configured HashDB for each module operation test."""
        super().setUp()
        self.config_path = self._write_config()
        self.hash_db = HashDB(self.config_path)

    def tearDown(self) -> None:
        """Close the test HashDB connection."""
        self.hash_db.close_connection()

    def test_adds_and_reads_module_hash(self) -> None:
        """A valid module hash can be stored and retrieved."""
        result = self.hash_db.add_module_hash("module", "1.0", "hash-1")

        self.assertIs(result, ModuleAddResult.module_added)
        self.assertEqual(self.hash_db.get_module_hash("module", "1.0"), "hash-1")

    def test_normalizes_values_for_storage_and_lookup(self) -> None:
        """Registration and lookup remove surrounding whitespace."""
        self.hash_db.add_module_hash(" module ", " 1.0 ", " hash-1 ")

        stored_row = self.hash_db.hash_db.execute(
            'SELECT "Mname", "MVersion", "MHash" FROM "MAIN"'
        ).fetchone()
        found_hash = self.hash_db.get_module_hash(" module ", " 1.0 ")

        self.assertEqual(stored_row, ("module", "1.0", "hash-1"))
        self.assertEqual(found_hash, "hash-1")

    def test_rejects_invalid_registration_values(self) -> None:
        """Registration rejects every invalid name, version, or hash value."""
        invalid_values = ("", "   ", None, 1, [], {})

        for position in range(3):
            for invalid_value in invalid_values:
                with self.subTest(position=position, value=invalid_value):
                    values = ["module", "1.0", "hash-1"]
                    values[position] = invalid_value
                    with self.assertRaises(ModuleRegisterError):
                        self.hash_db.add_module_hash(*values)

        row_count = self.hash_db.hash_db.execute(
            'SELECT COUNT(*) FROM "MAIN"'
        ).fetchone()[0]
        self.assertEqual(row_count, 0)

    def test_rejects_invalid_lookup_values(self) -> None:
        """Lookup rejects invalid module names and versions."""
        invalid_values = ("", "   ", None, 1, [], {})

        for position in range(2):
            for invalid_value in invalid_values:
                with self.subTest(position=position, value=invalid_value):
                    values = ["module", "1.0"]
                    values[position] = invalid_value
                    with self.assertRaises(ModuleRegisterError):
                        self.hash_db.get_module_hash(*values)

    def test_duplicate_pair_preserves_original_hash(self) -> None:
        """Duplicate registration reports an error and retains the first hash."""
        first_result = self.hash_db.add_module_hash("module", "1", "hash-1")
        duplicate_result = self.hash_db.add_module_hash(
            "module",
            "1",
            "hash-2",
        )

        self.assertIs(first_result, ModuleAddResult.module_added)
        self.assertIs(duplicate_result, ModuleAddResult.module_exists_err)
        self.assertEqual(self.hash_db.get_module_hash("module", "1"), "hash-1")
        row_count = self.hash_db.hash_db.execute(
            'SELECT COUNT(*) FROM "MAIN"'
        ).fetchone()[0]
        self.assertEqual(row_count, 1)

    def test_stores_different_versions_independently(self) -> None:
        """One module name can have independent hashes for multiple versions."""
        self.hash_db.add_module_hash("module", "1", "hash-1")
        self.hash_db.add_module_hash("module", "2", "hash-2")

        self.assertEqual(self.hash_db.get_module_hash("module", "1"), "hash-1")
        self.assertEqual(self.hash_db.get_module_hash("module", "2"), "hash-2")

    def test_returns_empty_string_for_unknown_pair(self) -> None:
        """Lookup returns an empty string for an unknown name/version pair."""
        self.assertEqual(self.hash_db.get_module_hash("unknown", "1"), "")

    def test_persists_module_after_reopening_database(self) -> None:
        """Committed module data survives closing and reopening HashDB."""
        self.hash_db.add_module_hash("module", "1", "hash-1")
        self.hash_db.close_connection()

        self.hash_db = HashDB(self.config_path)

        self.assertEqual(self.hash_db.get_module_hash("module", "1"), "hash-1")

    def test_treats_sql_metacharacters_as_data(self) -> None:
        """Parameterized operations treat SQL-like module values as data."""
        module_name = 'module"; DROP TABLE "MAIN"; --'
        module_version = "1' OR '1'='1"
        module_hash = "hash'); DELETE FROM MAIN; --"

        result = self.hash_db.add_module_hash(
            module_name,
            module_version,
            module_hash,
        )

        self.assertIs(result, ModuleAddResult.module_added)
        self.assertEqual(
            self.hash_db.get_module_hash(module_name, module_version),
            module_hash,
        )
        table = self.hash_db.hash_db.execute(
            "SELECT name FROM sqlite_master WHERE name = 'MAIN'"
        ).fetchone()
        self.assertEqual(table, ("MAIN",))

    def test_closed_database_rejects_public_operations(self) -> None:
        """Public operations use a custom error after HashDB is closed."""
        self.hash_db.close_connection()

        operations = (
            lambda: self.hash_db.add_module_hash("module", "1", "hash"),
            lambda: self.hash_db.get_module_hash("module", "1"),
        )
        for operation in operations:
            with (
                self.subTest(operation=operation),
                self.assertRaises(HashDBConnectionClosedError),
            ):
                operation()


class HashDBConnectionLifecycleTests(HashDBTestCase):
    """Verify explicit and repeated connection closure."""

    def test_closes_connection_and_allows_repeated_close(self) -> None:
        """Closing releases SQLite and a second close is harmless."""
        hash_db = self._create_hash_db()
        connection = hash_db.hash_db

        hash_db.close_connection()
        hash_db.close_connection()

        self.assertTrue(hash_db._connection_closed)
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")


if __name__ == "__main__":
    unittest.main()
