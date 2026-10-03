"""The immutable module manifest contract; actual code inspection stays outside."""

from pathlib import PureWindowsPath
from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from core.models.values import Text
from core.primitives.json_values import JsonObject, copy_json_object
from core.storage.module_identity import check_input_metadata


class ModuleCommands(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    start: list[Text] = Field(min_length=1)


class ModuleManifest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        hide_input_in_errors=True,
    )

    schema_version: Annotated[int, Field(ge=2, le=2)]
    name: Text
    version: Text
    role: Literal["stage", "service"]
    implementation: Literal["full", "action"]
    commands: ModuleCommands
    defaults: JsonObject
    stage_kind: Literal["conditional"] | None = None

    @model_validator(mode="before")
    @classmethod
    def detach_document(cls, value: object) -> JsonObject:
        document = copy_json_object(value, "module")
        if "stage_kind" in document and document["stage_kind"] != "conditional":
            raise ValueError(
                "stage_kind is only supported as conditional for stage modules."
            )
        return document

    @field_validator("role", "implementation", mode="before")
    @classmethod
    def validate_supported_variant(cls, value: object, info: ValidationInfo) -> object:
        choices = (
            ("stage", "service") if info.field_name == "role" else ("full", "action")
        )
        if value not in choices:
            raise NotImplementedError(
                "Modules require a stage/service role and full/action implementation."
            )
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        check_input_metadata(self.name, self.version)
        for label, value in (("name", self.name), ("version", self.version)):
            if (
                value != value.strip()
                or value in (".", "..")
                or value.endswith(".")
                or PureWindowsPath(value).is_reserved()
                or len(value) > 255
            ):
                raise ValueError(f"module.{label} must be a portable folder name.")
        if self.stage_kind is not None and self.role != "stage":
            raise ValueError(
                "stage_kind is only supported as conditional for stage modules."
            )
        return self
