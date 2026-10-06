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
    """Validate the outer document without claiming file or payload integrity."""

    state: JsonValue
    services: JsonValue
    journal: JsonValue
    directories: JsonValue
    files: JsonValue

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "snapshot manifest")


class SnapshotRetentionHeader(_SnapshotHeader):
    """Sort stored candidates without asserting their payload or file integrity."""

    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)


class SnapshotMetadata(BaseModel):
    """The legacy inspection subset, without full restoration/inventory requirements."""

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
        return copy_json_object(document, "snapshot metadata")


class SnapshotFile(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    size_bytes: NonnegativeInteger
    sha256: str


class SnapshotInventory(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    directories: list[SnapshotMember]
    files: dict[SnapshotMember, SnapshotFile]

    @model_validator(mode="before")
    @classmethod
    def validate_collisions(cls, document: object) -> object:
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
    """

    state: SavedRunnerState
    services: dict[str, Text | None]
    journal: JournalSnapshotManifest
    inventory: SnapshotInventory
    encoded_document: str | None = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def validate_state_references(self) -> Self:
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
        return copy_json_object(document, "restore transaction")


class RestoredServiceObservation(BaseModel):
    """Only the historical ownership fields consumed during interrupted restore.

    Missing fields and their JSON values remain permissive until the operation
    compares them with its expected identity and validates the process record.
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="ignore")

    experiment_id: JsonValue = None
    participant_id: JsonValue = None
    participant_instance_id: JsonValue = None
    process: JsonValue = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "restored service observation")
