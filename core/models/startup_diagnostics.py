"""Safe, bounded metadata for internal startup diagnostics (never command results)."""

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.models.process_identity import ProcessIdentity
from core.models.values import AbsolutePath, Text


class StartupContext(BaseModel):
    """Allowlisted identifiers; settings, tokens and environment are not retained."""

    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    experiment_id: str | None = None
    run_id: str | None = None
    stage_id: str | None = None
    stage_execution_id: str | None = None
    attempt_id: str | None = None
    request_id: str | None = None
    participant_id: str | None = None
    participant_instance_id: str | None = None
    service_id: str | None = None
    service_instance_id: str | None = None
    template_revision_id: str | None = None
    module_name: str | None = None
    module_version: str | None = None
    module_hash: str | None = None
    cycle_number: int | None = None
    attempt_number: int | None = None

    @model_validator(mode="before")
    @classmethod
    def keep_identifiers(cls, value: object) -> object:
        """Optional diagnostics cannot narrow the logger's arbitrary JSON context."""
        if not isinstance(value, dict):
            return value
        counters = {"cycle_number", "attempt_number"}
        return {
            name: item
            for name, item in value.items()
            if name in cls.model_fields
            and (item is None or type(item) is (int if name in counters else str))
        }


class StartupError(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    error_type: Text
    message: str
    traceback: str


class StartupDetails(BaseModel):
    """Explicit metadata only; no arbitrary application payloads or SQL arguments."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    duration_seconds: float | None = Field(default=None, ge=0)
    queue_wait_seconds: float | None = Field(default=None, ge=0)
    wall_elapsed_seconds: float | None = Field(default=None, ge=0)
    recovery_seconds: float | None = Field(default=None, ge=0)
    charged_elapsed_seconds: float | None = Field(default=None, ge=0)
    timeout_seconds: float | None = Field(default=None, gt=0)
    timeout_source: str | None = None
    deadline_monotonic: float | None = None
    writer_instance_id: str | None = None
    diagnostic_id: str | None = None
    journal_id: str | None = None
    generation: str | None = None
    command: str | None = None
    stream: str | None = None
    dropped_bytes: int | None = Field(default=None, ge=0)
    limit_bytes: int | None = Field(default=None, ge=0)
    process_id: int | None = Field(default=None, gt=0)
    exit_code: int | None = None
    stopped: bool | None = None
    error: StartupError | None = None
    error_id: str | None = None
    last_phase: str | None = None
    diagnostic_path: str | None = None


class StartupRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    phase: Text
    occurred_at: Text
    monotonic: float
    process_id: int
    process: ProcessIdentity | None = None
    context: StartupContext
    details: StartupDetails


class StartupDestination(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    path: AbsolutePath
    max_bytes: int = Field(default=262144, ge=1024)
