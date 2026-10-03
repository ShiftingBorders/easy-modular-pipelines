"""Snapshot metadata, inventory and restoration marker data contracts."""

from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

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


class SnapshotManifest(BaseModel):
    """Validate metadata without starting a runner or claiming payload integrity."""

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
    state: JsonValue
    services: JsonValue
    journal: JsonValue
    directories: JsonValue
    files: JsonValue

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "snapshot manifest")


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
