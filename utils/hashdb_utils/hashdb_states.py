from enum import Enum

from core.storage_contracts import ModuleAddResult

__all__ = ["ColumnValidationResult", "ModuleAddResult", "SchemaValidationStatus"]


class SchemaValidationStatus(Enum):
    correct = "CORRECT"
    mismatch = "MISMATCH"
    empty = "EMPTY"


class ColumnValidationResult(Enum):
    column_valid = "column_valid"
    column_name_err = "column_name_err"
    column_name_invalid_char = "column_name_invalid_char"
    column_type_err = "column_type_empty"
    column_type_invalid_char = "column_type_invalid_char"
