"""Collector configuration data; path anchoring belongs to its file loader."""

import ipaddress
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.models.values import PositiveInteger, PositiveNumber, Text


class CollectorConfiguration(BaseModel):
    """Sampling, history, supervision, and journal settings for resource collection.

    Args:
        sample_interval_seconds: Seconds between resource samples; must be at
            least 0.1.
        history_seconds: Maximum age in seconds of in-memory resource history.
        max_buffer_bytes: Memory byte budget for encoded resource history and
            bookkeeping.
        stale_after_intervals: Number of missed sample intervals after which a
            metric becomes stale.
        status_interval_seconds: Seconds between collector status publications.
        startup_timeout_seconds: Positive seconds allowed for initial process
            readiness.
        heartbeat_timeout_seconds: Seconds without collector status before
            restart; must exceed the status interval.
        shutdown_timeout_seconds: Positive seconds allowed for graceful owned-
            process shutdown.
        restart_delays_seconds: Nonempty, nondecreasing list of positive restart
            delays in seconds.
        stable_reset_seconds: Seconds of stable operation before resetting the
            restart backoff.
        logging_busy_timeout_seconds: Collector journal lock-wait timeout in
            seconds, at most 60.
        logging_retry_seconds: Seconds before attempting to reopen the journal
            after telemetry delivery failure.
        disk_path: Filesystem path whose capacity is sampled; relative values
            use the settings file's directory.
        network_interface: Explicit network interface name, or None to select
            the route to the reference address.
        network_reference_address: IPv4 address used to select a local route
            without sending a probe packet.
        gpu_interval_seconds: Legacy configuration field retained for
            compatibility; currently has no effect. Defaults to 5.
    """
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
        """Return the network reference address after validating IPv4 syntax.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            The network reference address after validating IPv4 syntax.
        """
        ipaddress.IPv4Address(value)
        return value

    @model_validator(mode="after")
    def validate_intervals(self) -> Self:
        """Return settings after checking restart order and heartbeat timeout.

        Raises:
            ValueError: Restart delays decrease or heartbeat timeout does not exceed
                the status publication interval.
        """
        if self.restart_delays_seconds != sorted(self.restart_delays_seconds):
            raise ValueError("Restart delays must be nondecreasing.")
        if self.heartbeat_timeout_seconds <= self.status_interval_seconds:
            raise ValueError("Heartbeat timeout must exceed the status interval.")
        return self
