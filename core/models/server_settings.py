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
    """Resolved server paths, runtime mode, connection settings, and capacity limits.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        project_root: Absolute root of the project containing registered modules
            and experiments.
        hash_config_path: Absolute hash-store config path; None selects the
            project-local default.
        server_mode: Maintenance permits module changes; run permits experiment
            execution.
        filer_url: HTTP(S) base URL of the existing SeaweedFS Filer.
        seaweed_config_path: Absolute managed SeaweedFS settings path.
        seaweed_min_free_gb: Minimum managed storage free-space reserve in GB.
        resource_config_path: Absolute resource-collector configuration path.
        archive_config_path: Absolute experiment-archive limits configuration
            path.
        host: Network address on which the HTTP listener binds.
        port: TCP listener port.
        token_env: Environment-variable name holding the bearer token; None
            disables token lookup.
        startup_timeout_seconds: Positive seconds allowed for initial process
            readiness.
        shutdown_timeout_seconds: Positive seconds allowed for graceful owned-
            process shutdown.
        read_timeout_seconds: Maximum seconds to await a fresh controller read.
        result_ttl_seconds: Seconds completed command receipts remain eligible
            for retention.
        max_pending_commands: Maximum ordinary pending commands; standalone
            stop/lifecycle admission has reserved capacity.
        max_read_requests: Maximum concurrent outstanding controller reads.
        max_command_records: Maximum retained command records, including room
            beyond ordinary pending commands.
        max_request_bytes: Maximum encoded API request bytes.
        max_response_bytes: Maximum permitted encoded response size in bytes.
        max_cached_result_bytes: Total retained result-cache byte budget; at
            least max_response_bytes.
        module_hashing: Validated hashing buffer sizes and worker limits.
    """
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
        """Return an optional HTTP(S) Filer URL after validating host and components.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            An optional HTTP(S) Filer URL after validating host and components.
        """
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
        """Return settings after reserving stop capacity and one full cached response.

        Raises:
            ValueError: Command records cannot reserve a priority stop, or result
                cache capacity is smaller than the maximum response size.
        """
        if self.max_command_records <= self.max_pending_commands:
            raise ValueError("max_command_records must leave room for a priority stop.")
        if self.max_cached_result_bytes < self.max_response_bytes:
            raise ValueError("max_cached_result_bytes must be >= max_response_bytes.")
        return self
