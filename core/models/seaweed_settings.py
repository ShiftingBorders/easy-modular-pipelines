"""Seaweed config operations."""

import ipaddress

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ProcessArgs(BaseModel):
    """Managed SeaweedFS startup/shutdown timeout and archive size limit in GB.

    Args:
        start_stop_sec: Startup/shutdown timeout in seconds, greater than 1 and
            at most 3600.
        max_archive_gb: Maximum archive size in binary GB, using 1024**3 bytes
            per unit.
    """
    model_config = ConfigDict(extra="allow")  # unknown fields tolerated

    start_stop_sec: int = Field(gt=1, le=3600)
    max_archive_gb: float = Field(gt=0, le=5)


class StartArgs(BaseModel):
    """SeaweedFS server addresses, ports, Filer flag, and replication options.

    Args:
        ip: IP address advertised by the managed SeaweedFS server.
        ip_bind: IP address on which the managed server binds. Input alias:
            ip.bind.
        master_port: Master HTTP port; derived gRPC ports must also remain
            available. Input alias: master.port.
        volume_port: Volume-server HTTP port; derived gRPC ports must also
            remain available. Input alias: volume.port.
        filer: Whether the Filer is enabled; this integration requires true.
        filer_port: Filer HTTP port; derived gRPC ports must also remain
            available. Input alias: filer.port.
        master_default_replication: SeaweedFS replication-placement code passed
            to the master. Input alias: master.defaultReplication.
        master_telemetry: Whether managed SeaweedFS telemetry is enabled. Input
            alias: master.telemetry.
    """
    model_config = ConfigDict(extra="allow")

    ip: str
    ip_bind: str = Field(alias="ip.bind")
    master_port: int = Field(
        alias="master.port", gt=0, lt=55536
    )  # SeaweedFS HTTP and derived gRPC ports must not overlap
    volume_port: int = Field(alias="volume.port", gt=0, lt=55536)
    filer: bool
    filer_port: int = Field(alias="filer.port", gt=0, lt=55536)
    master_default_replication: str = Field(alias="master.defaultReplication")
    master_telemetry: bool = Field(alias="master.telemetry")

    @field_validator("filer", "master_telemetry", mode="before")
    @classmethod
    def coerce_string_bool(cls, v):
        """Convert string true to True and other strings to False, ignoring case.

        Args:
            v: Input field value before this validator's checks or normalization.

        Returns:
            True for case-insensitive trimmed true text, False for other strings, or
            the original nonstring value for subsequent validation.
        """
        if isinstance(v, str):
            return v.strip().lower() == "true"
        return v

    @field_validator("ip", "ip_bind")
    @classmethod
    def validate_ip_address(cls, v: str) -> str:
        """Return a valid IPv4 or IPv6 address string or raise ValueError.

        Args:
            v: Input field value before this validator's checks or normalization.

        Returns:
            A valid IPv4 or IPv6 address string or raise ValueError.
        """
        try:
            ipaddress.ip_address(v)
        except ValueError as error:
            raise ValueError(f"{v!r} is not a valid IP address") from error
        return v

    @field_validator("filer")
    @classmethod
    def check_filer(cls, v):
        """Require the Filer option to be enabled and return True.

        Args:
            v: Input field value before this validator's checks or normalization.

        Returns:
            True after checking the Filer option is enabled.
        """
        if v != True:
            raise ValueError("Filer argument is required to be set to true")
        return True


class SeaWeedConfig(BaseModel):
    """Managed SeaweedFS process limits and server command-line settings.

    Args:
        process_args: Managed process timeout and maximum archive size settings.
        start_args: Validated server command-line options using SeaweedFS
            aliases.
    """
    model_config = ConfigDict(extra="allow")

    process_args: ProcessArgs
    start_args: StartArgs
