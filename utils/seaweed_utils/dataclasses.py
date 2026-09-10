import ipaddress

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ProcessArgs(BaseModel):
    model_config = ConfigDict(extra="allow")  # unknown fields tolerated

    start_stop_sec: int = Field(gt=1,le=3600)
    max_archive_gb: float = Field(gt=0,le=5)

class StartArgs(BaseModel):
    model_config = ConfigDict(extra="allow")

    ip: str
    ip_bind: str = Field(alias="ip.bind")
    master_port: int = Field(alias="master.port", gt=0, lt=55536) # SeaweedFS HTTP and derived gRPC ports must not overlap
    volume_port: int = Field(alias="volume.port", gt=0, lt=55536)
    filer: bool
    filer_port: int = Field(alias="filer.port", gt=0, lt=55536)
    master_default_replication: str = Field(alias="master.defaultReplication")
    master_telemetry: bool = Field(alias="master.telemetry")

    @field_validator("filer", "master_telemetry", mode="before")
    @classmethod
    def coerce_string_bool(cls, v):
        if isinstance(v, str):
            return v.strip().lower() == "true"
        return v

    @field_validator("ip", "ip_bind")
    @classmethod
    def validate_ip_address(cls, v: str) -> str:
        try:
            ipaddress.ip_address(v)
        except ValueError as error:
            raise ValueError(f"{v!r} is not a valid IP address") from error
        return v

    @field_validator("filer")
    @classmethod
    def check_filer(cls,v):
        if v != True:
            raise ValueError("Filer argument is required to be set to true")
        return True

class SeaWeedConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    process_args: ProcessArgs
    start_args: StartArgs
