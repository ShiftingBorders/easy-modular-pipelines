"""Dashboard configuration without filesystem or environment access."""

import re
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.models.values import AbsolutePath, Number, PositiveInteger


class DashboardConnectionConfiguration(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, hide_input_in_errors=True)

    system_api_url: str | None = Field(strict=True)
    request_timeout_seconds: Annotated[Number, Field(ge=0.1, le=60)]
    max_response_bytes: PositiveInteger = Field(ge=1024, le=67108864)
    system_api_token_env: str | None = Field(default=None, strict=True)

    @field_validator("system_api_token_env")
    @classmethod
    def validate_token_environment(cls, value: str | None) -> str | None:
        if value is not None and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
            raise ValueError(
                "system_api_token_env must name an environment variable or be null."
            )
        return value

    @field_validator("system_api_url")
    @classmethod
    def validate_system_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError(
                "system_api_url must not contain whitespace or control characters."
            )
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.port == 0
        ):
            raise ValueError(
                "system_api_url must be an HTTP(S) base URL without credentials or query."
            )
        return value.rstrip("/") + "/"


class DashboardConfiguration(DashboardConnectionConfiguration):
    model_config = ConfigDict(extra="forbid")

    host: str = Field(strict=True)
    port: PositiveInteger = Field(le=65535)
    state_directory: str = Field(strict=True)
    refresh_seconds: Annotated[Number, Field(ge=1, le=600)]
    project_root: str | None = Field(default=None, strict=True)
    history_max_events: PositiveInteger = Field(default=100000, le=10000000)
    history_max_bytes: PositiveInteger = Field(default=67108864, le=2147483648)
    history_window_events: PositiveInteger = Field(default=1000, le=100000)
    cache_workers: PositiveInteger = Field(default=2, le=32)

    @field_validator("host", "state_directory", "project_root")
    @classmethod
    def validate_nonempty_text(cls, value: str | Path | None) -> str | Path | None:
        if isinstance(value, str) and not value.strip():
            raise ValueError("Configured hosts and paths must be nonempty.")
        return value


class DashboardRuntimeConfiguration(DashboardConfiguration):
    """The already resolved, native-path configuration used by runtime consumers."""

    state_directory: AbsolutePath
    project_root: AbsolutePath | None = None
