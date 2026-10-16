"""Hash states operations."""

from enum import Enum

from core.storage.contracts import ModuleAddResult

__all__ = ["ColumnValidationResult", "ModuleAddResult", "SchemaValidationStatus"]


class SchemaValidationStatus(Enum):
    """Whether the hash table is absent, matches, or differs from its schema."""
    correct = "CORRECT"
    mismatch = "MISMATCH"
    empty = "EMPTY"


class ColumnValidationResult(Enum):
    """Column validation outcome for names, types, and allowed characters."""
    column_valid = "column_valid"
    column_name_err = "column_name_err"
    column_name_invalid_char = "column_name_invalid_char"
    column_type_err = "column_type_empty"
    column_type_invalid_char = "column_type_invalid_char"
