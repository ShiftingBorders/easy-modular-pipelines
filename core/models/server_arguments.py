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
    """Strict detached arguments for one controller operation."""
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of command arguments.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "command arguments")


class NoArguments(_Arguments):
    """Empty argument object for commands that accept no parameters."""


class RunArguments(_Arguments):
    """Optional template/experiment selection and run startup flags.

    Args:
        template_path: Applied template path used by the operation; runtime
            models require an absolute path. Defaults to None.
        experiment_id: Experiment identifier associating this document with its
            execution history. Defaults to None.
        continue_run: Whether to create a continuation from the source
            experiment's snapshot. Defaults to False.
        delayed_start: Whether startup should pause before executing the first
            DAG node. Defaults to False.
    """
    template_path: AbsolutePath | None = None
    experiment_id: Text | None = None
    continue_run: Boolean = False
    delayed_start: Boolean = False


class PositionArguments(_Arguments):
    """One-based stage or service position supplied to a controller operation."""
    position: PositiveInteger


class RerunArguments(_Arguments):
    """Rerun scope with optional stage position and experiment selection.

    Args:
        scope: Stage reruns the paused current node; experiment creates a new
            run from a source template.
        position: One-based current stage position for stage rerun; omitted for
            experiment rerun. Defaults to None.
        experiment_id: Experiment identifier associating this document with its
            execution history. Defaults to None.
    """
    scope: Literal["stage", "experiment"]
    position: PositiveInteger | None = None
    experiment_id: Text | None = None


class ResetRetriesArguments(PositionArguments):
    """Stage or service kind and one-based position whose retries are reset.

    Args:
        position: One-based position in the relevant stage/service definition
            list.
        kind: Whether the one-based position selects a stage retry count or a
            service restart count.
    """
    kind: Literal["stage", "service"]


class TemplateArguments(_Arguments):
    """Optional absolute template path for a template update operation."""
    template_path: AbsolutePath | None = None


class SnapshotArguments(_Arguments):
    """Optional nonempty label for a new snapshot."""
    label: Text | None = None


class SnapshotReference(_Arguments):
    """UUID identifying a snapshot selected for restoration."""
    snapshot_id: UUIDText


class ExperimentReference(_Arguments):
    """Experiment identifier used by read or control operations."""
    experiment_id: Text


class ArchiveArguments(_Arguments):
    """Absolute server-side path to an experiment archive."""
    archive_path: AbsolutePath


class ArchiveSelection(ArchiveArguments):
    """Archive path with optional selection of a stopped experiment.

    Args:
        archive_path: Absolute archive file path on the server's filesystem.
        experiment_id: Experiment identifier associating this document with its
            execution history. Defaults to None.
    """
    experiment_id: Text | None = None


class ArchiveInstall(ArchiveArguments):
    """Absolute archive and destination paths for installation on the server.

    Args:
        archive_path: Absolute archive file path on the server's filesystem.
        destination: Absolute installation destination on the server's
            filesystem.
    """
    destination: AbsolutePath


class ModuleCoordinates(_Arguments):
    """Name and version identifying a registered module.

    Args:
        name: Registered module name forming a portable directory component.
        version: Registered module version forming a portable directory
            component.
    """
    name: Text
    version: Text


class ModuleSource(_Arguments):
    """Absolute server-side directory containing module source files."""
    folder: AbsolutePath


class TemplatePathArguments(_Arguments):
    """Required absolute template path for template inspection."""
    template_path: AbsolutePath


class StateQueryArguments(_Arguments):
    """Optional experiment selection for a state query."""
    experiment_id: Text | None = None


class ResourceHistoryArguments(_Arguments):
    """Resource-history cursor and page limit of at most 1000 samples.

    Args:
        after: Nonnegative exclusive resource-history sample cursor. Defaults to
            0.
        limit: Maximum page item count, from 1 through 1000. Defaults to 100.
    """
    after: NonnegativeInteger = 0
    limit: Annotated[PositiveInteger, Field(le=1000)] = 100


class CommandListArguments(_Arguments):
    """Command-history filters and continuation command UUID.

    Args:
        limit: Maximum page item count, from 1 through 1000. Defaults to 100.
        state: Optional lifecycle-state filter for retained command records.
            Defaults to None.
        command: Optional exact command-name filter. Defaults to None.
        after: Exclusive retained command UUID cursor; unknown/expired cursors
            require restarting pagination. Defaults to None.
    """
    limit: Annotated[PositiveInteger, Field(le=1000)] = 100
    state: CommandState | None = None
    command: Text | None = None
    after: NormalizedUUIDText | None = None


class EventReadArguments(ExperimentReference):
    """Experiment selection, journal cursor, and bounded event page size.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        cursor: Identity-bound journal checkpoint object, or None to start
            reading. Defaults to None.
        limit: Maximum page item count, from 1 through 1000. Defaults to 100.
    """
    cursor: JsonObject | None = None
    limit: Annotated[PositiveInteger, Field(le=1000)] = 100


class SnapshotMetadataReference(ExperimentReference):
    """Experiment and snapshot identifiers for metadata inspection.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        snapshot_id: UUID of the snapshot selected or referenced by this
            document.
    """
    snapshot_id: UUIDText


class ArtifactReference(ExperimentReference):
    """Experiment and artifact identifiers for protected artifact lookup.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        artifact_id: Artifact identifier used for journal lookup; registration
            may leave it unassigned.
    """
    artifact_id: Text


class ControlInvocation(_Arguments):
    """Command, detached arguments, and optional stage/service target.

    Args:
        command: Command name selecting the operation to execute.
        args: JSON argument object supplied to the command handler.
        target: Optional one-based stage/service target; only compatible control
            commands accept it. Defaults to None.
    """
    command: Text
    args: JsonObject
    target: CommandTarget | None = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Copy invocation JSON and normalize an empty target object to None.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        document = copy_json_object(document, "control invocation")
        if document.get("target") == {}:
            document["target"] = None
        return document

    @model_validator(mode="after")
    def validate_target(self) -> Self:
        """Return the invocation after checking target compatibility with its command.

        Raises:
            ValueError: Service control lacks a service target or has arguments,
                or another command receives an unsupported target.
        """
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
                result.update(kind=self.target.kind, position=self.target.position)
        if self.command == "run" and "continue" in result:
            result["continue_run"] = result.pop("continue")
        return result


class MaintenanceInvocation(ControlInvocation):
    """Maintenance operation that never accepts a stage or service target.

    Args:
        command: Command name selecting the operation to execute.
        args: JSON argument object supplied to the command handler.
        target: Optional one-based stage/service target; only compatible control
            commands accept it. Defaults to None.
    """
    @model_validator(mode="after")
    def validate_target(self) -> Self:
        """Return the invocation after rejecting any maintenance command target."""
        if self.target is not None:
            raise ValueError("Maintenance commands do not accept target.")
        return self
