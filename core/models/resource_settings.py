"""Collector configuration data; path anchoring belongs to its file loader."""

import ipaddress
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.models.values import PositiveInteger, PositiveNumber, Text


class CollectorConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    sample_interval_seconds: PositiveNumber = Field(ge=0.1)
    history_seconds: PositiveNumber
    max_buffer_bytes: PositiveInteger = Field(ge=4096)
    stale_after_intervals: PositiveInteger
    status_interval_seconds: PositiveNumber
    startup_timeout_seconds: PositiveNumber
    heartbeat_timeout_seconds: PositiveNumber
    shutdown_timeout_seconds: PositiveNumber
    restart_delays_seconds: list[PositiveNumber] = Field(strict=True, min_length=1)
    stable_reset_seconds: PositiveNumber
    logging_busy_timeout_seconds: PositiveNumber = Field(le=60)
    logging_retry_seconds: PositiveNumber
    disk_path: Text
    network_interface: Text | None
    network_reference_address: Text
    gpu_interval_seconds: PositiveNumber = 5  # Legacy no-op configuration field.

    @field_validator("network_reference_address")
    @classmethod
    def validate_reference_address(cls, value: str) -> str:
        ipaddress.IPv4Address(value)
        return value

    @model_validator(mode="after")
    def validate_intervals(self) -> Self:
        if self.restart_delays_seconds != sorted(self.restart_delays_seconds):
            raise ValueError("Restart delays must be nondecreasing.")
        if self.heartbeat_timeout_seconds <= self.status_interval_seconds:
            raise ValueError("Heartbeat timeout must exceed the status interval.")
        return self
