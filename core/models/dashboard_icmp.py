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
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "ICMP document")


class ICMPSettings(_Document):
    enabled: bool
    host: str
    timeout_seconds: Annotated[Number, Field(ge=0.1, le=60)]
    interval_seconds: Annotated[Number, Field(ge=1, le=600)]

    @field_validator("host", mode="before")
    @classmethod
    def normalize_host(cls, value: object) -> str:
        return _host(value)

    @model_validator(mode="after")
    def require_enabled_host(self) -> Self:
        if self.enabled and not self.host:
            raise ValueError("A target host is required to enable ICMP monitoring.")
        return self


class ICMPWorkerResult(_Document):
    model_config = ConfigDict(extra="allow")

    status: ProbeStatus
    rtt_ms: RTT | None = None
    reason: str | None = None
    address: str | None = None
    rtt_upper_bound: bool | None = None


class ICMPObservation(_Document):
    model_config = ConfigDict(extra="allow")

    status: ProbeStatus
    host: str
    probe_host: str
    observed_at: AwareTimestamp
    session_id: str
    rtt_ms: RTT | None = None
    revision: JsonValue = None


class ICMPIncident(_Document):
    model_config = ConfigDict(extra="allow")

    id: str
    host: str
    probe_host: str
    status: Literal["active", "resolved", "closed"]
    started_at: AwareTimestamp
    ended_at: AwareTimestamp | None = None
    resolution: JsonValue = None


class SavedICMPState(_Document):
    settings: ICMPSettings
    history: list[ICMPObservation]
    incidents: list[ICMPIncident]

    @model_validator(mode="after")
    def one_active_incident(self) -> Self:
        if sum(item.status == "active" for item in self.incidents) > 1:
            raise ValueError("Multiple active incidents for one ICMP rule.")
        return self
