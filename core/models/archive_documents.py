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
from core.primitives.json_values import copy_json_object

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


def _archive_module_inputs(value: object) -> tuple[object, dict[int, ArchiveModule]]:
    if not isinstance(value, list):
        return value, {}
    retained = {
        index: item for index, item in enumerate(value) if type(item) is ArchiveModule
    }
    projected = [
        {
            "name": item.name,
            "version": item.version,
            "hash": item.hash,
            "role": item.role,
        }
        if index in retained
        else item
        for index, item in enumerate(value)
    ]
    return projected, retained


def _archive_file_inputs(value: object) -> tuple[object, dict[str, ArchiveFile]]:
    if not isinstance(value, dict):
        return value, {}
    retained = {name: item for name, item in value.items() if type(item) is ArchiveFile}
    projected = {
        name: {"size": item.size, "sha256": item.sha256} if name in retained else item
        for name, item in value.items()
    }
    return projected, retained


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
    def detach(cls, document: object) -> object:
        if not isinstance(document, dict):
            return copy_json_object(document, "archive manifest")
        values = dict(document)
        # Check the original JSON limits using known scalar projections, then
        # retain the validated nested inputs rather than reconstructing them.
        modules, retained_modules = _archive_module_inputs(values.get("modules"))
        files, retained_files = _archive_file_inputs(values.get("files"))
        if "modules" in values:
            values["modules"] = modules
        if "files" in values:
            values["files"] = files
        detached: dict[str, object] = dict(copy_json_object(values, "archive manifest"))
        detached_modules = detached.get("modules")
        if retained_modules and isinstance(detached_modules, list):
            detached["modules"] = [
                retained_modules.get(index, item)
                for index, item in enumerate(detached_modules)
            ]
        detached_files = detached.get("files")
        if retained_files and isinstance(detached_files, dict):
            detached["files"] = {**detached_files, **retained_files}
        return detached

    @model_validator(mode="after")
    def validate_members(self) -> Self:
        names = (*self.directories, *self.files)
        if "manifest.json" in names or len({name.casefold() for name in names}) != len(
            names
        ):
            raise ValueError("Archive manifest contains colliding or reserved paths.")
        return self
