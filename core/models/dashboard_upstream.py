"""Live fields consumed by the dashboard; runtime ownership stays upstream."""

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.models.values import NonnegativeInteger, UUIDText
from core.primitives.json_values import JsonObject, copy_json_object


class _Document(BaseModel):
    """Detached live-state document preserving additional upstream fields."""
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of upstream live state.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "live state")


class LiveServiceState(_Document):
    """Service identity and readiness flags reported by the runtime.

    Args:
        service_id: Stable UUID of a declared service.
        service_instance_id: UUID distinguishing one launch of a service from
            its previous instances. Defaults to None.
        module: Recorded module metadata for this live service. Defaults to a
            new empty dict.
        stopped: Whether shutdown of this instance has been confirmed. Defaults
            to False.
        stopping: Whether a shutdown operation is currently in progress.
            Defaults to False.
        ready: Whether this instance has confirmed readiness through its current
            observation. Defaults to False.
    """
    service_id: str
    service_instance_id: str | None = None
    module: JsonObject = Field(default_factory=dict)
    stopped: bool = False
    stopping: bool = False
    ready: bool = False


class LiveState(_Document):
    """Runtime experiment coordinates and service observations for the dashboard.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        server_instance_id: UUID of the HTTP runtime instance that owns these
            receipts or observations. Defaults to None.
        fresh: Whether this observation reflects current data rather than
            retained stale history. Defaults to False.
        phase: Observed experiment lifecycle phase; it does not independently
            prove process liveness. Defaults to None.
        mode: Running or paused scheduler mode. Defaults to None.
        cycle_number: Current runtime cycle, or None before one is available;
            sparse reads allow zero. Defaults to None.
        stage_position: Current runtime cursor, or None before one is available;
            sparse reads allow zero. Defaults to None.
        services: Latest runtime observations of declared service instances.
            Defaults to a new empty list.
    """
    experiment_id: str | None
    server_instance_id: UUIDText | None = None
    fresh: bool = False
    phase: str | None = None
    mode: str | None = None
    cycle_number: NonnegativeInteger | None = None
    stage_position: NonnegativeInteger | None = None
    services: list[LiveServiceState] = Field(default_factory=list)


class UpstreamError(BaseModel):
    """Optional upstream error text, ignoring malformed diagnostic values.

    Args:
        message: Human-readable diagnostic message. Defaults to None.
        code: Machine-readable diagnostic code used to classify the failure.
            Defaults to None.
    """
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    message: str | None = None
    code: str | None = None

    @field_validator("message", "code", mode="before")
    @classmethod
    def optional_text(cls, value: object) -> str | None:
        # Failed HTTP responses historically ignore malformed diagnostic fields.
        """Return string diagnostic text, or None for other input types.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            String diagnostic text, or None for other input types.
        """
        return value if isinstance(value, str) else None


class UpstreamFailure(BaseModel):
    """Optional structured error extracted from a failed upstream response."""
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    error: UpstreamError | None = None

    @field_validator("error", mode="before")
    @classmethod
    def optional_error(cls, value: object) -> object:
        """Return an error dictionary, or None for an unstructured error.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            An error dictionary, or None for an unstructured error.
        """
        return value if isinstance(value, dict) else None
