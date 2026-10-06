"""Dashboard configuration without filesystem or environment access."""

import re
from typing import Annotated
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.models.values import AbsolutePath, Number, PositiveInteger


class DashboardConnectionConfiguration(BaseModel):
    """Optional system API connection with request and response limits.

    Args:
        system_api_url: Optional HTTP(S) runtime API URL, normalized to one
            trailing slash.
        request_timeout_seconds: Positive HTTP request timeout in seconds.
        max_response_bytes: Maximum permitted encoded response size in bytes.
        system_api_token_env: Environment-variable name containing the API
            token; never the token value itself. Defaults to None.
    """
    model_config = ConfigDict(extra="ignore", frozen=True, hide_input_in_errors=True)

    system_api_url: str | None = Field(strict=True)
    request_timeout_seconds: Annotated[Number, Field(ge=0.1, le=60)]
    max_response_bytes: PositiveInteger = Field(ge=1024, le=67108864)
    system_api_token_env: str | None = Field(default=None, strict=True)

    @field_validator("system_api_token_env")
    @classmethod
    def validate_token_environment(cls, value: str | None) -> str | None:
        """Return an environment-variable name or null, rejecting invalid names.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            An environment-variable name or null, rejecting invalid names.
        """
        if value is not None and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
            raise ValueError(
                "system_api_token_env must name an environment variable or be null."
            )
        return value

    @field_validator("system_api_url")
    @classmethod
    def validate_system_url(cls, value: str | None) -> str | None:
        """Validate an HTTP(S) base URL and normalize its trailing slash.

        Args:
            value: Configured API URL, or None for an unconfigured connection.

        Returns:
            The URL with one trailing slash, or None.

        Raises:
            ValueError: The URL has an invalid scheme, host, port, or component.
        """
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


class _DashboardOptions(DashboardConnectionConfiguration):
    """Dashboard listener, refresh, and cache limits before path resolution.

    Args:
        system_api_url: Optional HTTP(S) runtime API URL, normalized to one
            trailing slash.
        request_timeout_seconds: Positive HTTP request timeout in seconds.
        max_response_bytes: Maximum permitted encoded response size in bytes.
        system_api_token_env: Environment-variable name containing the API
            token; never the token value itself. Defaults to None.
        host: Network address on which the HTTP listener binds.
        port: TCP listener port.
        refresh_seconds: Dashboard refresh interval in seconds.
        history_max_events: Maximum projection input count for one scope, not a
            total journal retention limit. Defaults to 100000.
        history_max_bytes: Byte budget for the active event window and per-scope
            projection data. Defaults to 67108864.
        history_window_events: Maximum original events retained in the
            dashboard's active RAM window. Defaults to 1000.
        cache_workers: Maximum number of independent history-cache worker
            processes. Defaults to 2.
    """
    model_config = ConfigDict(extra="forbid")

    host: str = Field(strict=True)
    port: PositiveInteger = Field(le=65535)
    refresh_seconds: Annotated[Number, Field(ge=1, le=600)]
    history_max_events: PositiveInteger = Field(default=100000, le=10000000)
    history_max_bytes: PositiveInteger = Field(default=67108864, le=2147483648)
    history_window_events: PositiveInteger = Field(default=1000, le=100000)
    cache_workers: PositiveInteger = Field(default=2, le=32)

    @field_validator("host")
    @classmethod
    def validate_nonempty_text(cls, value: str) -> str:
        """Return configured text after rejecting whitespace-only values.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            Configured text after rejecting whitespace-only values.
        """
        if not value.strip():
            raise ValueError("Configured hosts and paths must be nonempty.")
        return value


class DashboardConfiguration(_DashboardOptions):
    """Dashboard settings with paths resolved later by the configuration loader.

    Args:
        system_api_url: Optional HTTP(S) runtime API URL, normalized to one
            trailing slash.
        request_timeout_seconds: Positive HTTP request timeout in seconds.
        max_response_bytes: Maximum permitted encoded response size in bytes.
        system_api_token_env: Environment-variable name containing the API
            token; never the token value itself. Defaults to None.
        host: Network address on which the HTTP listener binds.
        port: TCP listener port.
        refresh_seconds: Dashboard refresh interval in seconds.
        history_max_events: Maximum projection input count for one scope, not a
            total journal retention limit. Defaults to 100000.
        history_max_bytes: Byte budget for the active event window and per-scope
            projection data. Defaults to 67108864.
        history_window_events: Maximum original events retained in the
            dashboard's active RAM window. Defaults to 1000.
        cache_workers: Maximum number of independent history-cache worker
            processes. Defaults to 2.
        state_directory: Directory for dashboard-owned state/cache; relative
            configuration values use the config directory.
        project_root: Optional locally readable project path; relative values
            resolve from the config file. Defaults to None.
    """
    state_directory: str = Field(strict=True)
    project_root: str | None = Field(default=None, strict=True)

    @field_validator("state_directory", "project_root")
    @classmethod
    def validate_nonempty_paths(cls, value: str | None) -> str | None:
        """Return a configured path or null, rejecting whitespace-only paths.

        Args:
            value: Input field/document value before this validator's checks or
                normalization.

        Returns:
            A configured path or null, rejecting whitespace-only paths.
        """
        if isinstance(value, str) and not value.strip():
            raise ValueError("Configured hosts and paths must be nonempty.")
        return value


class DashboardRuntimeConfiguration(_DashboardOptions):
    """The already resolved, native-path configuration used by runtime consumers.

    Args:
        system_api_url: Optional HTTP(S) runtime API URL, normalized to one
            trailing slash.
        request_timeout_seconds: Positive HTTP request timeout in seconds.
        max_response_bytes: Maximum permitted encoded response size in bytes.
        system_api_token_env: Environment-variable name containing the API
            token; never the token value itself. Defaults to None.
        host: Network address on which the HTTP listener binds.
        port: TCP listener port.
        refresh_seconds: Dashboard refresh interval in seconds.
        history_max_events: Maximum projection input count for one scope, not a
            total journal retention limit. Defaults to 100000.
        history_max_bytes: Byte budget for the active event window and per-scope
            projection data. Defaults to 67108864.
        history_window_events: Maximum original events retained in the
            dashboard's active RAM window. Defaults to 1000.
        cache_workers: Maximum number of independent history-cache worker
            processes. Defaults to 2.
        state_directory: Resolved absolute path for dashboard-owned state and
            disposable caches.
        project_root: Resolved absolute locally readable project root, or None
            while unconfigured. Defaults to None.
    """

    state_directory: AbsolutePath
    project_root: AbsolutePath | None = None
