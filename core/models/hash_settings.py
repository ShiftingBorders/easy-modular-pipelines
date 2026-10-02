"""Hash config operations."""

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
        """Validate syntax; HashDB explicitly checks actual filesystem conditions."""
        if value.suffix.lower() != ".json":
            raise ValueError("must reference an existing JSON file")
        return value
