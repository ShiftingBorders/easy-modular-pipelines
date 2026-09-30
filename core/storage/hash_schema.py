"""Hash storage configuration and SQLite schema validation."""

import sqlite3
from pathlib import Path

from pydantic import ValidationError

from core.primitives.json_files import load_json
from core.storage.errors import StorageConfigurationError
from core.storage.hash_config import HashDBConfig
from core.storage.hash_states import ColumnValidationResult
from core.storage.hash_validation import validate_column_desc


def _load_db_config(config_path: Path) -> dict:
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


def _validate_config(config: object) -> HashDBConfig:
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


def _validate_schema_file(schema_path: Path) -> dict:
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


def _unique_indexes(db: sqlite3.Connection) -> list[tuple[str | None, ...]]:
    """Read column tuples of non-partial unique indexes, retaining duplicates."""
    unique_constraints = []
    for index in db.execute('PRAGMA index_list("MAIN")').fetchall():
        is_unique = bool(index[2])
        is_partial = bool(index[4])
        if not is_unique or is_partial:
            continue
        escaped_index_name = index[1].replace('"', '""')
        index_columns = db.execute(
            f'PRAGMA index_info("{escaped_index_name}")'
        ).fetchall()
        unique_constraints.append(tuple(column[2] for column in index_columns))
    return unique_constraints


def _create_table(db: sqlite3.Connection, db_schema: dict[str, str]):
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
