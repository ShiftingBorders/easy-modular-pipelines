"""Validated local writer policy and the finite journal request protocol."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from core.models.journal_cache import FilteredPublication
from core.models.journal_records import JournalEvent
from core.models.process_identity import ProcessIdentity
from core.models.values import AbsolutePath, Text, UUIDText, _absolute_path
from core.primitives.json_values import JsonObject, JsonValue


class _Document(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        hide_input_in_errors=True,
        allow_inf_nan=False,
    )


class JournalWriterSettings(_Document):
    """Seconds for lifecycle budgets; queue_capacity counts waiting requests."""

    queue_capacity: int = Field(default=100, ge=1, le=10000)
    heartbeat_interval_seconds: float = Field(default=0.25, gt=0, le=1)
    stall_timeout_seconds: float = Field(default=10, gt=0)
    recovery_timeout_seconds: float = Field(default=60, gt=0, le=60)
    restart_delays_seconds: list[Annotated[float, Field(ge=0, le=5)]] = Field(
        default_factory=lambda: [0.1, 0.25, 0.5, 1.0]
    )
    idle_timeout_seconds: float = Field(default=300, gt=0)
    max_connections: int = Field(default=256, ge=2, le=4096)


class JournalIdentity(_Document):
    journal_id: UUIDText
    generation: UUIDText


class JournalInformation(JournalIdentity):
    schema_version: Literal[2]


class WriterDiscovery(_Document):
    runtime_directory: AbsolutePath
    managed: bool


class WriterRecoveryClock(_Document):
    model_config = ConfigDict(extra="ignore")
    clock_id: UUIDText | None = None
    recovery_seconds: float = Field(default=0, ge=0)
    recovery_started_at: float | None = Field(default=None, ge=0)

    def elapsed(self, now: float) -> float:
        ongoing = (
            0
            if self.recovery_started_at is None
            else max(0, now - self.recovery_started_at)
        )
        return self.recovery_seconds + ongoing


class WriterReadStatus(_Document):
    command: Text
    started_monotonic: float = Field(ge=0)
    age_seconds: float = Field(ge=0)
    diagnostic_id: UUIDText | None = None


class WriterProcessStatus(_Document):
    state: Literal["ready", "storage_unavailable"]
    process: ProcessIdentity
    writer_instance_id: Text
    queue_depth: int = Field(ge=0)
    queue_capacity: int = Field(gt=0)
    busy: bool
    read_queue_depth: int = Field(default=0, ge=0)
    active_read: WriterReadStatus | None = None
    progress_at: float = Field(ge=0)
    db_path: AbsolutePath
    journal: JournalInformation
    error: JsonObject | None = None


class JournalAppend(_Document):
    command: Literal["append", "append_command_result"]
    event: JournalEvent


class JournalReadEvents(_Document):
    command: Literal["read_events"]
    checkpoint: JsonObject | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    view: Literal["raw", "effective"] = "raw"


class JournalReadChanges(_Document):
    command: Literal["read_changes"]
    checkpoint: JsonObject | None = None
    limit: int = Field(default=100, ge=1, le=1000)


class JournalReadBatch(_Document):
    command: Literal["read_event_batch"]
    event_ids: list[str] | None = None
    limit: int = Field(default=1000, ge=1, le=1000)
    before: int | None = Field(default=None, gt=0)


class JournalReadResult(_Document):
    command: Literal["read_command_result"]
    request_id: str = Field(min_length=1)


class JournalSimpleCall(_Document):
    command: Literal["open", "get_journal_info", "status", "shutdown_standalone"]


class JournalExport(_Document):
    command: Literal["export_snapshot", "export_diagnostics"]
    path: str = Field(min_length=1)
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    operation_ids: list[str] = Field(default_factory=list)
    min_free_bytes: int = Field(default=0, ge=0)

    @field_validator("path")
    @classmethod
    def absolute_path(cls, value: str, info: ValidationInfo) -> str:
        return str(_absolute_path(value, info))


class JournalExportReceipt(_Document):
    call: JournalExport
    result: JsonObject | None = None


class JournalFilteredSource(_Document):
    command: Literal["export_filtered_source"]
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    previous: FilteredPublication | None = None


JournalCall = Annotated[
    JournalAppend
    | JournalReadEvents
    | JournalReadChanges
    | JournalReadBatch
    | JournalReadResult
    | JournalSimpleCall
    | JournalExport
    | JournalFilteredSource,
    Field(discriminator="command"),
]


class JournalRequest(_Document):
    token: str
    diagnostic_id: UUIDText | None = None
    expected_journal: JournalIdentity | None = None
    read_only: bool = False
    min_free_bytes: int = Field(default=0, ge=0)
    call: JournalCall


class JournalReply(_Document):
    ok: bool
    value: JsonValue = None
    error: JsonObject | None = None
    retryable: bool = False


class JournalEndpoint(_Document):
    host: Literal["127.0.0.1"]
    port: int = Field(gt=0, le=65535)
    token_file: AbsolutePath


class WriterEndpoint(_Document):
    protocol_version: Literal[1] = 1
    writer_instance_id: str
    process: ProcessIdentity
    endpoint: JournalEndpoint
    db_path: AbsolutePath
    journal: JournalIdentity
    managed: bool
