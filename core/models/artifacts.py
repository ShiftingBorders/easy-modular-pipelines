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
    """Artifact metadata with a path relative to its attempt directory.

    Args:
        artifact_id: Artifact identifier used for journal lookup; registration
            may leave it unassigned.
        path: Portable path relative to the attempt artifact directory; absolute
            paths and parent traversal are rejected.
        purpose: Nonempty human-readable reason the artifact was produced.
        size_bytes: Recorded nonnegative artifact size in bytes, or None when
            not supplied.
        content_hash: Optional recorded artifact content hash; registration does
            not itself hash the file.
    """
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
        """Return the registration after rejecting a path naming the attempt root."""
        if not PurePosixPath(self.path).parts:
            raise ValueError("Artifact path must stay relative to its attempt directory.")
        return self


class ArtifactAttemptMetadata(BaseModel):
    """Recorded attempt coordinates needed to locate an artifact.

    Args:
        cycle_number: One-based DAG cycle number.
        attempt_number: One-based attempt number for this stage in the current
            cycle.
        attempt_id: UUID identifying one stage attempt.
        module_name: Registered name of the module that produced this attempt.
        stage_id: Stable UUID of a node in the applied DAG.
    """
    model_config = ConfigDict(
        extra="ignore", strict=True, frozen=True, hide_input_in_errors=True
    )

    cycle_number: PositiveInteger
    attempt_number: PositiveInteger
    attempt_id: Text
    module_name: Text
    stage_id: Text


class ArtifactContext(ArtifactAttemptMetadata):
    """Attempt coordinates restricted to portable filesystem components.

    Args:
        cycle_number: One-based DAG cycle number.
        attempt_number: One-based attempt number for this stage in the current
            cycle.
        attempt_id: UUID identifying one stage attempt.
        module_name: Registered name of the module that produced this attempt.
        stage_id: Stable UUID of a node in the applied DAG.
    """
    @field_validator("attempt_id", "module_name", "stage_id")
    @classmethod
    def portable_component(cls, value: str, info: ValidationInfo) -> str:
        """Return a safe component or raise ValueError naming the invalid field.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.
            info: Pydantic validation context identifying the field and any explicit
                caller context.

        Returns:
            A safe component or raise ValueError naming the invalid field.
        """
        if value in (".", "..") or any(char in value for char in '/\\:*?"<>|'):
            raise ValueError(f"Invalid artifact context: {info.field_name}.")
        return value


class ArtifactLocation(BaseModel):
    """Validated attempt context and an attempt-relative artifact path.

    Args:
        context: Validated attempt coordinates used to construct its artifact
            directory.
        path: Portable path relative to the attempt artifact directory; absolute
            paths and parent traversal are rejected.
    """
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

    Args:
        context: Historical attempt coordinates; actual filesystem confinement
            is checked during lookup.
        path: Recorded attempt-relative path; historical harmless parent
            segments are resolved and checked by the reader.
    """

    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    context: ArtifactAttemptMetadata
    path: Annotated[str, Field(strict=True, min_length=1)]
