"""ICMP data contracts; DNS, Echo Reply and worker ownership remain operations."""

import ipaddress
import re
from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from core.models.values import Number
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


def _host(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("host must be an IPv4 address or hostname.")
    host = value.strip()
    if not host:
        return host
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        try:
            host = host.encode("idna").decode("ascii").lower().rstrip(".")
        except UnicodeError as error:
            raise ValueError("Invalid hostname.") from error
        if len(host) > 253 or any(
            not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", part)
            for part in host.split(".")
        ):
            raise ValueError("Use a hostname without a URL, port, spaces or options.")
        return host
    if (
        address.version != 4
        or address.is_multicast
        or address.is_unspecified
        or str(address) == "255.255.255.255"
    ):
        raise ValueError("Use a unicast IPv4 address or hostname.")
    return str(address)


def _aware_timestamp(value: str) -> str:
    if datetime.fromisoformat(value).utcoffset() is None:
        raise ValueError("Saved ICMP timestamps must include a timezone.")
    return value


AwareTimestamp = Annotated[str, AfterValidator(_aware_timestamp)]
RTT = Annotated[Number, Field(le=3600000)]
type ProbeStatus = Literal["reply", "no_reply", "error"]


class _Document(BaseModel):
    """Strict ICMP document detached from caller-owned JSON containers."""
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of the ICMP document.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "ICMP document")


class ICMPSettings(_Document):
    """ICMP target and probe timing in seconds, with an explicit enabled flag.

    Args:
        enabled: Whether this rule or probe is enabled.
        host: Normalized DNS name or unicast IPv4 address; required when
            enabled.
        timeout_seconds: Probe response timeout in seconds, from 0.1 through 60.
        interval_seconds: Seconds between probes, from 1 through 600.
    """
    enabled: bool
    host: str
    timeout_seconds: Annotated[Number, Field(ge=0.1, le=60)]
    interval_seconds: Annotated[Number, Field(ge=1, le=600)]

    @field_validator("host", mode="before")
    @classmethod
    def normalize_host(cls, value: object) -> str:
        """Return a normalized DNS name or unicast IPv4 address, allowing empty text.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            A normalized DNS name or unicast IPv4 address, allowing empty text.
        """
        return _host(value)

    @model_validator(mode="after")
    def require_enabled_host(self) -> Self:
        """Return settings after requiring a target for enabled monitoring."""
        if self.enabled and not self.host:
            raise ValueError("A target host is required to enable ICMP monitoring.")
        return self


class ICMPWorkerResult(_Document):
    """Probe outcome with optional round-trip time in milliseconds.

    Args:
        status: Probe outcome: reply, no_reply, or error.
        rtt_ms: Round-trip time in milliseconds, or None when no timing is
            available. Defaults to None.
        reason: Failure or unavailability reason when the probe cannot supply a
            normal reply. Defaults to None.
        address: Resolved probe destination address, if available. Defaults to
            None.
        rtt_upper_bound: Whether rtt_ms is an upper bound rather than an exact
            measured value. Defaults to None.
    """
    model_config = ConfigDict(extra="allow")

    status: ProbeStatus
    rtt_ms: RTT | None = None
    reason: str | None = None
    address: str | None = None
    rtt_upper_bound: bool | None = None


class ICMPObservation(_Document):
    """Timestamped probe result associated with a monitoring session.

    Args:
        status: Probe outcome: reply, no_reply, or error.
        host: Configured probe target recorded with this observation or
            incident.
        probe_host: Normalized host/address actually used by the probe.
        observed_at: Wall-clock timestamp at which this observation was
            recorded.
        session_id: Identity of the monitor session that produced the
            observation.
        rtt_ms: Round-trip time in milliseconds, or None when no timing is
            available. Defaults to None.
        revision: Settings revision retained with the observation to detect
            obsolete probe results. Defaults to None.
    """
    model_config = ConfigDict(extra="allow")

    status: ProbeStatus
    host: str
    probe_host: str
    observed_at: AwareTimestamp
    session_id: str
    rtt_ms: RTT | None = None
    revision: JsonValue = None


class ICMPIncident(_Document):
    """Connectivity incident with timezone-aware start and optional end times.

    Args:
        id: Stable incident identifier for one period of connectivity failure.
        host: Configured probe target recorded with this observation or
            incident.
        probe_host: Normalized host/address actually used by the probe.
        status: Active, resolved, or administratively closed connectivity
            incident.
        started_at: Recorded wall-clock start time.
        ended_at: Recorded incident end time, or None while not ended. Defaults
            to None.
        resolution: Reason or metadata explaining how the incident ended.
            Defaults to None.
    """
    model_config = ConfigDict(extra="allow")

    id: str
    host: str
    probe_host: str
    status: Literal["active", "resolved", "closed"]
    started_at: AwareTimestamp
    ended_at: AwareTimestamp | None = None
    resolution: JsonValue = None


class SavedICMPState(_Document):
    """Persisted ICMP settings, observations, and incident history.

    Args:
        settings: Validated probe target, enabled flag, interval, and timeout.
        history: Retained ICMP observations in recorded order.
        incidents: Connectivity incident history; at most one incident may
            remain active.
    """
    settings: ICMPSettings
    history: list[ICMPObservation]
    incidents: list[ICMPIncident]

    @model_validator(mode="after")
    def one_active_incident(self) -> Self:
        """Return saved state after rejecting multiple active incidents."""
        if sum(item.status == "active" for item in self.incidents) > 1:
            raise ValueError("Multiple active incidents for one ICMP rule.")
        return self
