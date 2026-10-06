"""Snapshot metadata, inventory and restoration marker data contracts."""

import json
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

from core.models.journal_diagnostics import JournalSnapshotManifest
from core.models.process_identity import ProcessIdentity
from core.models.runner_state import SavedRunnerState
from core.models.values import (
    Boolean,
    NonnegativeInteger,
    PositiveInteger,
    Text,
    UUIDText,
)
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object

STAGE_CONTROL_FILES = frozenset(
    {
        "launch.json",
        "context.json",
        "process.json",
        "ready.json",
        "executor.token",
        "executor.lock.json",
        "stop.emergency.json",
    }
)


def _folder(value: str) -> str:
    if (
        value in (".", "..")
        or Path(value).name != value
        or any(c in value for c in '/\\:*?"<>|')
    ):
        raise ValueError("Unsafe transaction or experiment folder.")
    return value


def _utc(value: str) -> str:
    if datetime.fromisoformat(value).utcoffset() != UTC.utcoffset(None):
        raise ValueError("Snapshot/archive time must be UTC.")
    return value


def _snapshot_member(name: str) -> str:
    relative = PurePosixPath(name)
    if (
        not name
        or "\\" in name
        or ":" in name
        or PureWindowsPath(name).anchor
        or relative.is_absolute()
        or any(
            part in (".", "..") or part.rstrip(" .") != part for part in name.split("/")
        )
        or relative.parts[0] not in ("files", "journal")
    ):
        raise ValueError("Unsafe snapshot member path.")
    parts = relative.parts
    if (
        parts[0] == "files"
        and len(parts) > 1
        and parts[1]
        not in {
            "modules",
            "module_data",
            "shared_settings",
            "shared_data",
            "shared_artifacts",
            "experiment.yaml",
        }
    ):
        raise ValueError("Snapshot contains a runtime control file.")
    if parts[:3] == ("files", "shared_artifacts", "services"):
        raise ValueError("Snapshot contains live service control artifacts.")
    if (
        len(parts) == 7
        and parts[:2] == ("files", "shared_artifacts")
        and parts[2].startswith("epoch_")
        and parts[5].startswith("attempt_")
        and (
            parts[6] in STAGE_CONTROL_FILES
            or parts[6].startswith("executor.lock.")
            and parts[6].endswith(".token")
        )
    ):
        raise ValueError("Snapshot contains live stage control artifacts.")
    return name


FolderName = Annotated[Text, BeforeValidator(_folder)]
UTCText = Annotated[Text, BeforeValidator(_utc)]
SnapshotMember = Annotated[str, BeforeValidator(_snapshot_member)]


class _SnapshotHeader(BaseModel):
    """Snapshot identity, UTC creation time, ordering sequence, and kind.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        snapshot_id: UUID of the snapshot selected or referenced by this
            document.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        experiment_folder: Portable experiment folder name used to locate this
            snapshot's owner.
        created_at: ISO 8601 creation timestamp in UTC.
        sequence: Positive monotonically ordered snapshot sequence used for
            newest-first selection.
        kind: Regular snapshot or final snapshot taken during terminal shutdown.
        label: Optional nonempty human-readable snapshot label.
    """
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    schema_version: Annotated[int, Field(ge=2, le=2)]
    snapshot_id: UUIDText
    experiment_id: Text
    experiment_folder: FolderName
    created_at: UTCText
    sequence: PositiveInteger = Field(le=9223372036854775807)
    kind: Literal["regular", "final"]
    label: Text | None


class SnapshotManifest(_SnapshotHeader):
    """Validate the outer document without claiming file or payload integrity.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        snapshot_id: UUID of the snapshot selected or referenced by this
            document.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        experiment_folder: Portable experiment folder name used to locate this
            snapshot's owner.
        created_at: ISO 8601 creation timestamp in UTC.
        sequence: Positive monotonically ordered snapshot sequence used for
            newest-first selection.
        kind: Regular snapshot or final snapshot taken during terminal shutdown.
        label: Optional nonempty human-readable snapshot label.
        state: Outer saved-state JSON; SnapshotPayload validates the restoration
            contract separately.
        services: Outer export mapping retained as JSON until payload
            validation.
        journal: Outer journal metadata retained as JSON until payload
            validation.
        directories: Outer directory inventory retained as JSON until payload
            validation.
        files: Outer file inventory retained as JSON until payload validation.
    """

    state: JsonValue
    services: JsonValue
    journal: JsonValue
    directories: JsonValue
    files: JsonValue

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of the outer snapshot manifest.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "snapshot manifest")


class SnapshotRetentionHeader(_SnapshotHeader):
    """Sort stored candidates without asserting their payload or file integrity.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        snapshot_id: UUID of the snapshot selected or referenced by this
            document.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        experiment_folder: Portable experiment folder name used to locate this
            snapshot's owner.
        created_at: ISO 8601 creation timestamp in UTC.
        sequence: Positive monotonically ordered snapshot sequence used for
            newest-first selection.
        kind: Regular snapshot or final snapshot taken during terminal shutdown.
        label: Optional nonempty human-readable snapshot label.
    """

    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)


class SnapshotMetadata(BaseModel):
    """The legacy inspection subset, without full restoration/inventory requirements.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        snapshot_id: UUID of the snapshot selected or referenced by this
            document.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        state: Historical saved-state object used for inspection, without full
            restoration validation.
    """

    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    schema_version: Annotated[int, Field(ge=2, le=2)]
    snapshot_id: UUIDText
    experiment_id: Text
    state: JsonObject

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of snapshot inspection metadata.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "snapshot metadata")


class SnapshotFile(BaseModel):
    """Snapshot file size in bytes and recorded SHA-256 digest.

    Args:
        size_bytes: Nonnegative file size in bytes.
        sha256: Expected SHA-256 content digest used when verifying the file.
    """
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    size_bytes: NonnegativeInteger
    sha256: str


class SnapshotInventory(BaseModel):
    """Portable snapshot member paths and their expected file metadata.

    Args:
        directories: Portable snapshot-relative directories beneath files or
            journal.
        files: Snapshot member paths mapped to expected size and SHA-256
            metadata.
    """
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    directories: list[SnapshotMember]
    files: dict[SnapshotMember, SnapshotFile]

    @model_validator(mode="before")
    @classmethod
    def validate_collisions(cls, document: object) -> object:
        """Return inventory input after rejecting case-insensitive path collisions.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Inventory input after rejecting case-insensitive path collisions.
        """
        if isinstance(document, dict):
            directories, files = document.get("directories"), document.get("files")
            if isinstance(directories, list) and isinstance(files, dict):
                names = (*directories, *files)
                if all(isinstance(name, str) for name in names) and len(
                    {name.casefold() for name in names}
                ) != len(names):
                    raise ValueError("Snapshot paths collide.")
        return document


class SnapshotPayload(_SnapshotHeader):
    """Validated restoration components; filesystem integrity is checked separately.

    Original input JSON is retained for the public document facade, preserving
    legacy omissions, field order and schema-three migration representation.
    Runtime operations consume the typed components rather than that JSON.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        snapshot_id: UUID of the snapshot selected or referenced by this
            document.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        experiment_folder: Portable experiment folder name used to locate this
            snapshot's owner.
        created_at: ISO 8601 creation timestamp in UTC.
        sequence: Positive monotonically ordered snapshot sequence used for
            newest-first selection.
        kind: Regular snapshot or final snapshot taken during terminal shutdown.
        label: Optional nonempty human-readable snapshot label.
        state: Saved runner execution state associated with this document.
        services: Service IDs mapped to experiment-relative state exports; None
            represents a stateless export.
        journal: Journal snapshot manifest identifying schema, boundary,
            generation, and checksum.
        inventory: Validated file/directory inventory whose filesystem integrity
            is checked separately.
        encoded_document: Original manifest JSON retained for exact public
            serialization; excluded from model dumps. Defaults to None.
    """

    state: SavedRunnerState
    services: dict[str, Text | None]
    journal: JournalSnapshotManifest
    inventory: SnapshotInventory
    encoded_document: str | None = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def validate_state_references(self) -> Self:
        """Return the payload after checking the saved cursor and result references.

        Raises:
            ValueError: Experiment identity, attempt state, DAG cursor, or accepted
                result references are inconsistent with a restorable snapshot.
        """
        state = self.state
        if (
            state.experiment_id != self.experiment_id
            or state.active_attempt is not None
        ):
            raise ValueError("Snapshot runner state is inconsistent.")
        if state.cycle_number > state.template.cycles or state.stage_position > len(
            state.template.stages
        ):
            raise ValueError("Snapshot cursor is outside its DAG.")
        stage_ids = {item.stage_id for item in state.template.stages}
        if state.stage_result_ids.keys() - stage_ids:
            raise ValueError("Snapshot result belongs to an unknown DAG node.")
        if (
            state.last_result_id is not None
            and state.last_result_id not in state.stage_result_ids.values()
            and (
                state.pending_input is None
                or state.last_result_id != state.pending_input.request_id
            )
        ):
            raise ValueError(
                "Snapshot retained result has no matching journal reference."
            )
        if state.last_result_id is None and state.last_result is not None:
            raise ValueError("Snapshot retained data has no journal reference.")
        return self

    @model_validator(mode="after")
    def validate_service_exports(self) -> Self:
        """Return the payload after checking complete, idle service restoration data.

        Raises:
            ValueError: Services or definitions disagree, work remains unresolved,
                or an export required by a service definition is absent.
        """
        state = self.state
        if set(state.services) != {item.service_id for item in state.template.services}:
            raise ValueError("Snapshot does not describe every service.")
        if self.services.keys() - state.services.keys():
            raise ValueError("Snapshot exports an unknown service.")
        definitions = {item.service_id: item for item in state.template.services}
        for service_id, instance in state.services.items():
            if instance.definition != definitions[service_id]:
                raise ValueError(
                    "Snapshot service settings differ from the applied template."
                )
            if (
                instance.active_request
                or instance.pending_requests
                or instance.freeze_id
                or instance.prepared_freeze_id
            ):
                raise ValueError("Snapshot contains unresolved service work.")
            if (
                self.services.get(service_id) is None
                and instance.definition.state_required
            ):
                raise ValueError("Required service export is missing.")
        return self

    def document(self) -> JsonObject:
        """Return original manifest JSON when retained, otherwise serialize typed data.

        Returns:
            Original manifest JSON when retained, otherwise serialize typed data.
        """
        if self.encoded_document is not None:
            return copy_json_object(
                json.loads(self.encoded_document), "snapshot manifest"
            )
        return {
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "experiment_id": self.experiment_id,
            "experiment_folder": self.experiment_folder,
            "created_at": self.created_at,
            "sequence": self.sequence,
            "kind": self.kind,
            "label": self.label,
            "state": self.state.model_dump(exclude_unset=True),
            "services": copy_json_object(self.services, "snapshot service exports"),
            "journal": self.journal.model_dump(),
            "directories": list(self.inventory.directories),
            "files": {
                name: item.model_dump() for name, item in self.inventory.files.items()
            },
        }


class RestoreTransaction(BaseModel):
    """Persisted restoration phase, source/target identity, and stopped checkpoint.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        restoration_id: UUID identifying one resumable restoration transaction.
        experiment_id: Experiment identifier associating this document with its
            execution history.
        target_folder: Single folder name identifying the restoration target
            under the project store.
        source_folder: Single folder name identifying the source experiment
            under the project store.
        snapshot_id: UUID of the snapshot selected or referenced by this
            document.
        run_id: Logical run identity within experiment history, retained across
            the relevant execution scope.
        clone: Whether restoration creates a continuation from another
            experiment.
        phase: Persisted restoration step; ambiguous/interrupted service loading
            requires recovery handling.
        preserve_diagnostics: Whether failed-rebuild audit exports/workspace are
            retained after restoration.
        stopped_state: Validated pre-restoration runner state proving the
            participant shutdown barrier.
        owner: Complete OS identity of the process allowed to advance this
            restoration.
    """
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    schema_version: Annotated[int, Field(ge=2, le=2)]
    restoration_id: UUIDText
    experiment_id: Text
    target_folder: FolderName
    source_folder: FolderName
    snapshot_id: UUIDText
    run_id: Text
    clone: Boolean
    phase: Literal[
        "staging",
        "prepared",
        "files_installed",
        "journal_restored",
        "services_starting",
        "complete",
        "failed",
    ]
    preserve_diagnostics: Boolean
    stopped_state: SavedRunnerState
    owner: ProcessIdentity

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of a restoration transaction marker.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "restore transaction")


class RestoredServiceObservation(BaseModel):
    """Only the historical ownership fields consumed during interrupted restore.

    Missing fields and their JSON values remain permissive until the operation
    compares them with its expected identity and validates the process record.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history. Defaults to None.
        participant_id: UUID of the stage or service represented by the
            participant. Defaults to None.
        participant_instance_id: UUID distinguishing this particular participant
            process/attempt from replacements. Defaults to None.
        process: Full OS identity of the observed process, or None when no
            process is known. Defaults to None.
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="ignore")

    experiment_id: JsonValue = None
    participant_id: JsonValue = None
    participant_instance_id: JsonValue = None
    process: JsonValue = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of an interrupted-restore service observation.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "restored service observation")
