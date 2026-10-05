"""Artifact metadata and lexical locations; filesystem ownership stays with readers."""

from pathlib import PurePosixPath, PureWindowsPath
from typing import Annotated, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from core.models.values import NonnegativeInteger, PositiveInteger, Text


def _relative_path(value: str) -> str:
    windows = PureWindowsPath(value)
    relative = PurePosixPath(value.replace("\\", "/"))
    if (
        windows.drive
        or windows.root
        or relative.is_absolute()
        or ".." in relative.parts
        or ":" in value
    ):
        raise ValueError("Artifact path must stay relative to its attempt directory.")
    return relative.as_posix()


RelativeArtifactPath = Annotated[Text, AfterValidator(_relative_path)]


class ArtifactRegistration(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    artifact_id: Text | None
    path: RelativeArtifactPath
    purpose: Text
    size_bytes: NonnegativeInteger | None
    content_hash: Text | None

    @model_validator(mode="after")
    def require_file_path(self) -> Self:
        # Registration excludes dot paths; historical lookup still checks the file.
        if not PurePosixPath(self.path).parts:
            raise ValueError("Artifact path must stay relative to its attempt directory.")
        return self


class ArtifactAttemptMetadata(BaseModel):
    model_config = ConfigDict(
        extra="ignore", strict=True, frozen=True, hide_input_in_errors=True
    )

    cycle_number: PositiveInteger
    attempt_number: PositiveInteger
    attempt_id: Text
    module_name: Text
    stage_id: Text


class ArtifactContext(ArtifactAttemptMetadata):
    @field_validator("attempt_id", "module_name", "stage_id")
    @classmethod
    def portable_component(cls, value: str, info: ValidationInfo) -> str:
        if value in (".", "..") or any(char in value for char in '/\\:*?"<>|'):
            raise ValueError(f"Invalid artifact context: {info.field_name}.")
        return value


class ArtifactLocation(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    context: ArtifactContext
    path: RelativeArtifactPath


class RecordedArtifactLocation(BaseModel):
    """Dashboard historical lookup; resolved filesystem confinement stays explicit.

    Recorded names may contain nested components and paths may contain harmless
    parent segments. The dashboard checks their resolved paths within the same
    existing roots, rather than applying the stricter registration/CLI contract.
    """

    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    context: ArtifactAttemptMetadata
    path: Annotated[str, Field(strict=True, min_length=1)]
