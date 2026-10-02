"""CLI settings shared by configuration loading and direct client construction."""

from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, field_validator

from core.models.values import PositiveInteger, PositiveNumber, SchemaVersionOne, Text


class ClientConnectionConfiguration(BaseModel):
    # Explicit CLI overrides historically preserve extra fields in the returned dict.
    model_config = ConfigDict(extra="allow", frozen=True, hide_input_in_errors=True)

    server_url: Text
    token_env: Text | None
    request_timeout_seconds: PositiveNumber
    wait_timeout_seconds: PositiveNumber
    poll_interval_seconds: PositiveNumber
    max_response_bytes: PositiveInteger

    @field_validator("server_url")
    @classmethod
    def validate_server_url(cls, value: str) -> str:
        url = urlsplit(value)
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "server_url must be an HTTP(S) base URL without credentials, query or fragment."
            )
        if url.port is not None and not 1 <= url.port <= 65535:
            raise ValueError("Invalid server_url port.")
        return value


class ClientConfiguration(ClientConnectionConfiguration):
    """The on-disk configuration adds a required version to connection values."""

    schema_version: SchemaVersionOne
