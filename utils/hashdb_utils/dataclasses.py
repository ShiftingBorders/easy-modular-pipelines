from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class HashDBConfig(BaseModel):
    """Validated paths required to initialize a HashDB instance."""

    model_config = ConfigDict(extra="allow")

    schema_path: Path
    db_path: Path

    @field_validator("schema_path", "db_path", mode="before")
    @classmethod
    def validate_path_value(cls, value: Any) -> str | Path:
        """Reject values that cannot represent a non-empty filesystem path."""
        if not isinstance(value, (str, Path)) or not str(value).strip():
            raise ValueError("must be a non-empty filesystem path")
        return value

    @field_validator("schema_path")
    @classmethod
    def validate_schema_path(cls, value: Path) -> Path:
        """Require an existing JSON file for the database schema."""
        if value.suffix.lower() != ".json" or not value.is_file():
            raise ValueError("must reference an existing JSON file")
        return value

    @field_validator("db_path")
    @classmethod
    def validate_db_path(cls, value: Path) -> Path:
        """Require a file path whose parent directory already exists."""
        if value.exists() and not value.is_file():
            raise ValueError("must not reference a directory")
        if not value.parent.is_dir():
            raise ValueError("parent directory must exist")
        return value
