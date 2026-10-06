"""Complete server configuration after per-file path resolution."""

from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.models.module_settings import ModuleHashingSettings
from core.models.values import (
    AbsolutePath,
    PositiveInteger,
    PositiveNumber,
    SchemaVersionOne,
    Text,
)


class ServerConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: SchemaVersionOne
    project_root: AbsolutePath
    hash_config_path: AbsolutePath | None
    server_mode: Literal["run", "maintenance"]
    filer_url: Text | None
    seaweed_config_path: AbsolutePath
    seaweed_min_free_gb: PositiveNumber
    resource_config_path: AbsolutePath
    archive_config_path: AbsolutePath
    host: Text
    port: PositiveInteger = Field(le=65535)
    token_env: Text | None
    startup_timeout_seconds: PositiveNumber
    shutdown_timeout_seconds: PositiveNumber
    read_timeout_seconds: PositiveNumber
    result_ttl_seconds: PositiveNumber
    max_pending_commands: PositiveInteger
    max_read_requests: PositiveInteger
    max_command_records: PositiveInteger
    max_request_bytes: PositiveInteger
    max_response_bytes: PositiveInteger
    max_cached_result_bytes: PositiveInteger
    module_hashing: ModuleHashingSettings = Field(default_factory=ModuleHashingSettings)

    @field_validator("filer_url")
    @classmethod
    def validate_filer_url(cls, value: str | None) -> str | None:
        url = urlsplit(value or "http://127.0.0.1")
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "filer_url must be an HTTP(S) URL without credentials, query or fragment."
            )
        if url.port is not None and not 1 <= url.port <= 65535:
            raise ValueError("Invalid filer_url port.")
        return value

    @model_validator(mode="after")
    def validate_capacity(self) -> Self:
        if self.max_command_records <= self.max_pending_commands:
            raise ValueError("max_command_records must leave room for a priority stop.")
        if self.max_cached_result_bytes < self.max_response_bytes:
            raise ValueError("max_cached_result_bytes must be >= max_response_bytes.")
        return self
