"""Live fields consumed by the dashboard; runtime ownership stays upstream."""

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.models.values import NonnegativeInteger, UUIDText
from core.primitives.json_values import JsonObject, copy_json_object


class _Document(BaseModel):
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "live state")


class LiveServiceState(_Document):
    service_id: str
    service_instance_id: str | None = None
    module: JsonObject = Field(default_factory=dict)
    stopped: bool = False
    stopping: bool = False
    ready: bool = False


class LiveState(_Document):
    experiment_id: str | None
    server_instance_id: UUIDText | None = None
    fresh: bool = False
    phase: str | None = None
    mode: str | None = None
    cycle_number: NonnegativeInteger | None = None
    stage_position: NonnegativeInteger | None = None
    services: list[LiveServiceState] = Field(default_factory=list)


class UpstreamError(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    message: str | None = None
    code: str | None = None

    @field_validator("message", "code", mode="before")
    @classmethod
    def optional_text(cls, value: object) -> str | None:
        # Failed HTTP responses historically ignore malformed diagnostic fields.
        return value if isinstance(value, str) else None


class UpstreamFailure(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    error: UpstreamError | None = None

    @field_validator("error", mode="before")
    @classmethod
    def optional_error(cls, value: object) -> object:
        return value if isinstance(value, dict) else None
