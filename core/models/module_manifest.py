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
    """Nonempty argument vector used to start an immutable module."""
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    start: list[Text] = Field(min_length=1)


class ModuleManifest(BaseModel):
    """Versioned module identity, role, implementation, commands, and defaults.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        name: Registered module name forming a portable directory component.
        version: Registered module version forming a portable directory
            component.
        role: Participant role: stage executes one attempt, service remains
            available across calls.
        implementation: Module implementation kind, full or action; does not
            change the participant role contract.
        commands: Nonempty start argument vector; no shell or separate stop
            command is implied.
        defaults: JSON settings merged recursively with template overrides
            before invocation.
        stage_kind: Conditional for conditional stage modules; ordinary
            stages/services omit this field. Defaults to None.
    """
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
        """Copy manifest JSON and reject an explicitly unsupported stage kind.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        document = copy_json_object(value, "module")
        if "stage_kind" in document and document["stage_kind"] != "conditional":
            raise ValueError(
                "stage_kind is only supported as conditional for stage modules."
            )
        return document

    @field_validator("role", "implementation", mode="before")
    @classmethod
    def validate_supported_variant(cls, value: object, info: ValidationInfo) -> object:
        """Return a supported role or implementation, else raise NotImplementedError.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.
            info: Pydantic validation context identifying the field and any explicit
                caller context.

        Returns:
            A supported role or implementation, else raise NotImplementedError.
        """
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
        """Return the manifest after checking portable names and conditional role.

        Raises:
            ValueError: Name/version metadata is unsafe or a service declares a
                conditional stage kind.
        """
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
