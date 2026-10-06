"""Hash config operations."""

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class HashDBConfig(BaseModel):
    """Validated paths required to initialize a HashDB instance.

    Args:
        schema_path: JSON schema path; configuration loading resolves relative
            values from the configuration file.
        db_path: SQLite hash-database path; relative configuration values
            resolve beside the containing file.
    """

    model_config = ConfigDict(extra="allow")

    schema_path: Path
    db_path: Path

    @field_validator("schema_path", "db_path", mode="before")
    @classmethod
    def validate_path_value(cls, value: Any) -> str | Path:
        """Reject values that cannot represent a non-empty filesystem path.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            The original nonempty string or Path, ready for Pydantic path
            conversion.
        """
        if not isinstance(value, (str, Path)) or not str(value).strip():
            raise ValueError("must be a non-empty filesystem path")
        return value

    @field_validator("schema_path")
    @classmethod
    def validate_schema_path(cls, value: Path) -> Path:
        """Validate syntax; HashDB explicitly checks actual filesystem conditions.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            The original Path when its suffix is .json, case-insensitively; file
            existence is checked separately.
        """
        if value.suffix.lower() != ".json":
            raise ValueError("must reference an existing JSON file")
        return value
