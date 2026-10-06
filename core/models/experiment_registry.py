"""The selected experiment registry entry, independent of filesystem existence."""

from pathlib import Path

from pydantic import BaseModel, ConfigDict, field_validator


class RegistryEntry(BaseModel):
    """Experiment registry entry naming one folder beneath the experiment root."""
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    folder: str

    @field_validator("folder")
    @classmethod
    def validate_folder(cls, value: str) -> str:
        """Return a single nonempty native folder name or raise ValueError.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            A single nonempty native folder name or raise ValueError.
        """
        if not value or Path(value).name != value or value in (".", ".."):
            raise ValueError("Invalid experiment registry folder.")
        return value
