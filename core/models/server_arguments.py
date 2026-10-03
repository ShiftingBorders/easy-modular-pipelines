"""Arguments of existing controller operations, before their runtime effects."""

from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.models.server_commands import CommandState, CommandTarget
from core.models.values import (
    AbsolutePath,
    Boolean,
    NonnegativeInteger,
    NormalizedUUIDText,
    PositiveInteger,
    Text,
    UUIDText,
)
from core.primitives.json_values import JsonObject, copy_json_object


class _Arguments(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "command arguments")


class NoArguments(_Arguments):
    pass


class RunArguments(_Arguments):
    template_path: AbsolutePath | None = None
    experiment_id: Text | None = None
    continue_run: Boolean = False
    delayed_start: Boolean = False


class PositionArguments(_Arguments):
    position: PositiveInteger


class RerunArguments(_Arguments):
    scope: Literal["stage", "experiment"]
    position: PositiveInteger | None = None
    experiment_id: Text | None = None


class ResetRetriesArguments(PositionArguments):
    kind: Literal["stage", "service"]


class TemplateArguments(_Arguments):
    template_path: AbsolutePath | None = None


class SnapshotArguments(_Arguments):
    label: Text | None = None


class SnapshotReference(_Arguments):
    snapshot_id: UUIDText


class ExperimentReference(_Arguments):
    experiment_id: Text


class ArchiveArguments(_Arguments):
    archive_path: AbsolutePath


class ArchiveSelection(ArchiveArguments):
    experiment_id: Text | None = None


class ArchiveInstall(ArchiveArguments):
    destination: AbsolutePath


class ModuleCoordinates(_Arguments):
    name: Text
    version: Text


class ModuleSource(_Arguments):
    folder: AbsolutePath


class TemplatePathArguments(_Arguments):
    template_path: AbsolutePath


class StateQueryArguments(_Arguments):
    experiment_id: Text | None = None


class ResourceHistoryArguments(_Arguments):
    after: NonnegativeInteger = 0
    limit: Annotated[PositiveInteger, Field(le=1000)] = 100


class CommandListArguments(_Arguments):
    limit: Annotated[PositiveInteger, Field(le=1000)] = 100
    state: CommandState | None = None
    command: Text | None = None
    after: NormalizedUUIDText | None = None


class EventReadArguments(ExperimentReference):
    cursor: JsonObject | None = None
    limit: Annotated[PositiveInteger, Field(le=1000)] = 100


class SnapshotMetadataReference(ExperimentReference):
    snapshot_id: UUIDText


class ArtifactReference(ExperimentReference):
    artifact_id: Text


class ControlInvocation(_Arguments):
    command: Text
    args: JsonObject
    target: CommandTarget | None = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        document = copy_json_object(document, "control invocation")
        if document.get("target") == {}:
            document["target"] = None
        return document

    @model_validator(mode="after")
    def validate_target(self) -> Self:
        if self.command in ("service.start", "service.stop") and (
            self.target is None or self.args
        ):
            raise ValueError("Service control requires a service target and empty args.")
        if self.target is not None:
            if self.command in ("retry", "service.start", "service.stop"):
                if self.target.kind != "service":
                    raise ValueError(f"{self.command} targets a service.")
            elif self.command not in ("replace", "reset_retries"):
                raise ValueError("This command does not accept target.")
        return self

    def arguments(self) -> JsonObject:
        """Apply the existing target binding and continue alias to detached args."""
        result = dict(self.args)
        if self.target is not None:
            if self.command in ("retry", "service.start", "service.stop"):
                result["position"] = self.target.position
            else:
                result.update(self.target.model_dump())
        if self.command == "run" and "continue" in result:
            result["continue_run"] = result.pop("continue")
        return result


class MaintenanceInvocation(ControlInvocation):
    @model_validator(mode="after")
    def validate_target(self) -> Self:
        if self.target is not None:
            raise ValueError("Maintenance commands do not accept target.")
        return self
