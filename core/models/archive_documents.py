"""Portable archive manifest and member contracts, separate from extraction I/O."""

from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from core.experiments.archive_inputs import _member
from core.models.snapshot_documents import UTCText
from core.models.values import NonnegativeInteger, Text, UUIDText
from core.primitives.json_values import JsonObject, copy_json_object

ArchiveMember = Annotated[str, BeforeValidator(_member)]


class ArchiveFile(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    size: NonnegativeInteger
    sha256: Annotated[
        str, Field(pattern=r"^[0-9a-f]{64}$", min_length=64, max_length=64)
    ]


class ArchiveModule(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    name: ArchiveMember
    version: ArchiveMember
    hash: str
    role: Literal["stage", "service"]

    @field_validator("name", "version")
    @classmethod
    def validate_component(cls, value: str) -> str:
        if "/" in value:
            raise ValueError("Module identities must be single path components.")
        return value


class ArchiveManifest(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    schema_version: Annotated[int, Field(ge=2, le=2)]
    archive_id: UUIDText
    created_at: UTCText
    source_experiment_id: Text
    template: Literal["experiment.yaml"]
    modules: list[ArchiveModule]
    directories: list[ArchiveMember]
    files: dict[ArchiveMember, ArchiveFile]

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "archive manifest")

    @model_validator(mode="after")
    def validate_members(self) -> Self:
        names = (*self.directories, *self.files)
        if "manifest.json" in names or len({name.casefold() for name in names}) != len(
            names
        ):
            raise ValueError("Archive manifest contains colliding or reserved paths.")
        return self
