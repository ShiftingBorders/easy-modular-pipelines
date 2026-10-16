"""Alert input and saved documents; incident transitions belong to the monitor."""

from typing import Annotated, Literal, Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.models.values import Boolean, Number
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


class _Document(BaseModel):
    """Strict alert document with detached JSON input."""
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of the alert document.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "alert document")


class AlertRule(_Document):
    """Resource-threshold or error-count rule evaluated by the alert monitor.

    Args:
        id: Stable rule identifier; a UUID is generated when omitted or
            explicitly empty.
        name: Display name with a trimmed length of 1 through 100 characters.
        enabled: Whether this rule or probe is enabled.
        kind: Resource threshold rule or journal error-count rule.
        threshold: Resource threshold or, for error rules, a positive integer
            event count.
        duration_seconds: Required continuous threshold duration in seconds
            before raising an incident. Defaults to 0.
        window_seconds: Error-count observation window in seconds, from 1
            through 86400. Defaults to 60.
        experiment_id: Optional experiment filter for error observations.
            Defaults to None.
        metric: Resource metric selected by the rule; error rules retain this
            field without interpreting it. Defaults to None.
        operator: Resource threshold direction, above or below; opaque for
            legacy error rules. Defaults to None.
    """
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
        """Copy supported rule fields and normalize an explicitly supplied ID.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        if not isinstance(document, dict) or document.keys() - cls.model_fields.keys():
            raise ValueError("Unsupported Alert rule fields.")
        document = dict(document)
        if "id" in document:
            document["id"] = str(document["id"] or uuid4())
        return copy_json_object(document, "alert rule")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Return the original name if its trimmed length is between 1 and 100.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            The original name if its trimmed length is between 1 and 100.
        """
        if not 1 <= len(value.strip()) <= 100:
            raise ValueError("An Alert name is required (up to 100 characters).")
        return value

    @model_validator(mode="after")
    def validate_condition(self) -> Self:
        """Return the rule after checking the condition required by its kind.

        Raises:
            ValueError: A resource metric/operator is unsupported or an error
                threshold is not a positive integer.
        """
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
    """Desktop and sound delivery flags with optional saved metadata.

    Args:
        desktop: Whether to deliver desktop notifications on the dashboard host.
            Defaults to False.
        sound: Whether to play notification sounds on the dashboard host.
            Defaults to False.
    """
    model_config = ConfigDict(extra="allow")

    desktop: Boolean = False
    sound: Boolean = False


class NotificationChannels(DeliveryChannels):
    """Required delivery flags, recovery policy, and repeat interval in seconds.

    Args:
        desktop: Whether to deliver desktop notifications on the dashboard host.
        sound: Whether to play notification sounds on the dashboard host.
        on_recovery: Whether to notify when an active incident resolves.
        repeat_seconds: Minimum seconds between repeated incident notifications,
            from 10 through 86400.
    """
    model_config = ConfigDict(extra="forbid")

    desktop: Boolean = Field(...)
    sound: Boolean = Field(...)
    on_recovery: Boolean
    repeat_seconds: Annotated[int, Field(ge=10, le=86400)]


class AlertIncident(_Document):
    """Alert lifecycle observation retaining source-specific metadata.

    Args:
        id: Stable incident identifier used to update the same incident across
            observations.
        source: Origin of the incident; system incidents require a rule_id.
        status: Active, resolved, or administratively closed incident state.
        name: Display name captured when the incident was created.
        started_at: Recorded wall-clock start time.
        rule_id: Associated rule ID; system incidents require a string
            reference. Defaults to None.
        fresh: Whether this observation reflects current data rather than
            retained stale history. Defaults to False.
        ended_at: Recorded incident end time, or None while not ended. Defaults
            to None.
        resolution: Reason or metadata explaining how the incident ended.
            Defaults to None.
        value: Observed metric value or diagnostic payload that caused the
            incident. Defaults to None.
        last_notification: Retained notification timing metadata, if available.
            Defaults to None.
        delivery: Retained notification delivery outcome/metadata, if available.
            Defaults to None.
    """
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
        """Return the incident, requiring a string rule ID for system alerts."""
        if self.source == "system" and not isinstance(self.rule_id, str):
            raise ValueError("Saved incident has no rule ID.")
        return self


class SavedAlertIncident(AlertIncident):
    """Persisted incident whose freshness is reset when loaded.

    Args:
        id: Stable incident identifier used to update the same incident across
            observations.
        source: Origin of the incident; system incidents require a rule_id.
        status: Active, resolved, or administratively closed incident state.
        name: Display name captured when the incident was created.
        started_at: Recorded wall-clock start time.
        rule_id: Associated rule ID; system incidents require a string
            reference. Defaults to None.
        fresh: Always reset to False when loading a saved incident, regardless
            of the supplied value. Defaults to False.
        ended_at: Recorded incident end time, or None while not ended. Defaults
            to None.
        resolution: Reason or metadata explaining how the incident ended.
            Defaults to None.
        value: Observed metric value or diagnostic payload that caused the
            incident. Defaults to None.
        last_notification: Retained notification timing metadata, if available.
            Defaults to None.
        delivery: Retained notification delivery outcome/metadata, if available.
            Defaults to None.
    """
    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Copy a saved incident and mark it stale pending a new observation.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        if type(document) is not dict:
            raise ValueError("Invalid saved incident.")
        return copy_json_object({**document, "fresh": False}, "saved incident")


class SavedAlertState(BaseModel):
    """Persisted alert rules, notification settings, and incident history.

    Args:
        rules: Configured alert rules; persisted state permits at most 100.
        channels: Desktop/sound delivery and repeat/recovery notification
            settings.
        incidents: Retained incident history; loaded incidents become stale
            until observed again.
    """
    model_config = ConfigDict(
        extra="ignore", strict=True, frozen=True, hide_input_in_errors=True
    )

    rules: Annotated[list[AlertRule], Field(max_length=100)]
    channels: NotificationChannels
    incidents: list[SavedAlertIncident]


class AlertConfiguration(BaseModel):
    """Request to update a rule, change channels, or delete a rule.

    Args:
        rule: Rule to create or replace; None leaves rules unchanged unless
            delete is supplied. Defaults to None.
        channels: Desktop/sound delivery and repeat/recovery notification
            settings. Defaults to None.
        delete: Rule ID to remove, or None when no removal is requested.
            Defaults to None.
    """
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    rule: AlertRule | None = None
    channels: NotificationChannels | None = None
    delete: str | None = None
