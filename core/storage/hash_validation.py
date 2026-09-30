"""Character sets used by module and storage input validation."""

from string import ascii_letters, digits
from typing import Any

from core.storage.hash_states import ColumnValidationResult


def validate_column_desc(
    column_name: Any,
    column_type: Any,
) -> ColumnValidationResult:
    """Validate a column name and its SQLite type declaration."""
    if (
        not isinstance(column_name, str)
        or not column_name.strip()
        or "\x00" in column_name
    ):
        return ColumnValidationResult.column_name_err
    if any(character not in SQL_COLUMN_NAME_CHARACTERS for character in column_name):
        return ColumnValidationResult.column_name_invalid_char

    if not isinstance(column_type, str) or not column_type.strip():
        return ColumnValidationResult.column_type_err
    if any(character not in SQL_COLUMN_TYPE_CHARACTERS for character in column_type):
        return ColumnValidationResult.column_type_invalid_char

    return ColumnValidationResult.column_valid


SQL_COLUMN_NAME_CHARACTERS = frozenset(ascii_letters + digits + "_")


SQL_COLUMN_TYPE_CHARACTERS = frozenset(ascii_letters + digits + "_ (),")
