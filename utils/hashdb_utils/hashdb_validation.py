from typing import Any

from core.storage_errors import StorageInputError
from utils.hashdb_utils.hashdb_states import ColumnValidationResult


def validate_column_desc(
    column_name: Any,
    column_type: Any,
) -> ColumnValidationResult:
    """Validate a column name and its SQLite type declaration."""
    allowed_name_characters = set(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
    )
    allowed_type_characters = set(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_ (),"
    )

    if (
        not isinstance(column_name, str)
        or not column_name.strip()
        or "\x00" in column_name
    ):
        return ColumnValidationResult.column_name_err
    if any(character not in allowed_name_characters for character in column_name):
        return ColumnValidationResult.column_name_invalid_char

    if not isinstance(column_type, str) or not column_type.strip():
        return ColumnValidationResult.column_type_err
    if any(character not in allowed_type_characters for character in column_type):
        return ColumnValidationResult.column_type_invalid_char

    return ColumnValidationResult.column_valid


def clear_module_data_input(
    module_name: str,
    module_version: str,
    module_hash: str,
) -> tuple[str, str, str]:
    """Validate and normalize module identity and hash values."""
    module_data = (module_name, module_version, module_hash)
    if any(not isinstance(value, str) or not value.strip() for value in module_data):
        raise StorageInputError(
            "Can't register module with following values: "
            f"{module_name}, {module_version}, {module_hash}"
        )
    return (
        module_name.strip(),
        module_version.strip(),
        module_hash.strip(),
    )
