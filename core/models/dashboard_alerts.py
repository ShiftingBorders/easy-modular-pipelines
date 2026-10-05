"""Alert input and saved documents; incident transitions belong to the monitor."""

from typing import Annotated, Literal, Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.models.values import Boolean, Number
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


class _Document(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "alert document")


class AlertRule(_Document):
    id: Annotated[str, Field(min_length=1, max_length=100)] = Field(
        default_factory=lambda: str(uuid4())
    )
    name: str
    enabled: Boolean
    kind: Literal["resource", "errors"]
    threshold: Number
    duration_seconds: Annotated[Number, Field(le=86400)] = 0
    window_seconds: Annotated[Number, Field(ge=1, le=86400)] = 60
    experiment_id: Annotated[str, Field(max_length=512)] | None = None
    # Error rules historically preserve these fields without interpreting them.
    metric: JsonValue = None
    operator: JsonValue = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        if not isinstance(document, dict) or document.keys() - cls.model_fields.keys():
            raise ValueError("Unsupported Alert rule fields.")
        document = dict(document)
        if "id" in document:
            document["id"] = str(document["id"] or uuid4())
        return copy_json_object(document, "alert rule")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not 1 <= len(value.strip()) <= 100:
            raise ValueError("An Alert name is required (up to 100 characters).")
        return value

    @model_validator(mode="after")
    def validate_condition(self) -> Self:
        if self.kind == "resource":
            if self.metric not in (
                "cpu", "ram", "disk", "disk_free_gib",
                "internet_receive", "internet_transmit",
            ):
                raise ValueError("Choose a supported resource metric.")
            if self.operator not in ("above", "below"):
                raise ValueError("Choose above or below for the threshold.")
        elif type(self.threshold) is not int or self.threshold < 1:
            raise ValueError("An error rule needs a positive integer event count.")
        return self

    def document(self) -> JsonObject:
        """Preserve optional-field absence and the existing normalized defaults."""
        document = self.model_dump(exclude_unset=True)
        document.update(
            id=self.id,
            duration_seconds=self.duration_seconds,
            window_seconds=self.window_seconds,
        )
        return document


class DeliveryChannels(_Document):
    model_config = ConfigDict(extra="allow")

    desktop: Boolean = False
    sound: Boolean = False


class NotificationChannels(DeliveryChannels):
    model_config = ConfigDict(extra="forbid")

    desktop: Boolean = Field(...)
    sound: Boolean = Field(...)
    on_recovery: Boolean
    repeat_seconds: Annotated[int, Field(ge=10, le=86400)]


class AlertIncident(_Document):
    model_config = ConfigDict(extra="allow")

    # Saved incidents have a string contract, including legacy sources/timestamps.
    id: str
    source: str
    status: Literal["active", "resolved", "closed"]
    name: str
    started_at: str
    rule_id: JsonValue = None
    fresh: Boolean = False
    ended_at: JsonValue = None
    resolution: JsonValue = None
    value: JsonValue = None
    last_notification: JsonValue = None
    delivery: JsonValue = None

    @model_validator(mode="after")
    def validate_rule_reference(self) -> Self:
        if self.source == "system" and not isinstance(self.rule_id, str):
            raise ValueError("Saved incident has no rule ID.")
        return self


class SavedAlertIncident(AlertIncident):
    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        if type(document) is not dict:
            raise ValueError("Invalid saved incident.")
        return copy_json_object({**document, "fresh": False}, "saved incident")


class SavedAlertState(BaseModel):
    model_config = ConfigDict(
        extra="ignore", strict=True, frozen=True, hide_input_in_errors=True
    )

    rules: Annotated[list[AlertRule], Field(max_length=100)]
    channels: NotificationChannels
    incidents: list[SavedAlertIncident]


class AlertConfiguration(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    rule: AlertRule | None = None
    channels: NotificationChannels | None = None
    delete: str | None = None
