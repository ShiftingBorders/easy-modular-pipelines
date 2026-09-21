"""Explicit dashboard configuration, resolved against its containing file."""

import json
import math
import re
from pathlib import Path
from urllib.parse import urlsplit


def number(value: object, name: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number.")
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}.")
    return value


def load_settings(path: Path, overrides: dict | None = None) -> dict:
    if not path.is_absolute():
        raise ValueError("The dashboard configuration path must be absolute.")
    path = path.resolve(strict=True)
    settings = json.loads(path.read_text(encoding="utf-8-sig"))
    if overrides:
        if not isinstance(settings, dict):
            raise ValueError("Dashboard settings must be an object.")
        settings.update(overrides)
    required = {
        "host",
        "port",
        "system_api_url",
        "request_timeout_seconds",
        "max_response_bytes",
        "state_directory",
        "refresh_seconds",
    }
    if (
        not isinstance(settings, dict)
        or not required.issubset(settings)
        or settings.keys()
        - required
        - {
            "system_api_token_env",
            "project_root",
            "history_max_events",
            "history_max_bytes",
            "history_window_events",
        }
    ):
        raise ValueError(
            f"Dashboard settings require exactly: {', '.join(sorted(required))}."
        )
    if not isinstance(settings["host"], str) or not settings["host"].strip():
        raise ValueError("host must be a nonempty string.")
    for field, lower, upper in [
        ("port", 1, 65535),
        ("max_response_bytes", 1024, 67108864),
    ]:
        if type(settings[field]) is not int:
            raise ValueError(f"{field} must be an integer.")
        number(settings[field], field, lower, upper)
    number(settings["request_timeout_seconds"], "request_timeout_seconds", 0.1, 60)
    number(settings["refresh_seconds"], "refresh_seconds", 1, 600)
    token_env = settings.get("system_api_token_env")
    if token_env is not None and (
        not isinstance(token_env, str)
        or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token_env) is None
    ):
        raise ValueError(
            "system_api_token_env must name an environment variable or be null."
        )
    base_url = settings["system_api_url"]
    if base_url is not None:
        if not isinstance(base_url, str):
            raise ValueError("system_api_url must be a URL or null.")
        if any(
            char.isspace() or ord(char) < 32 or ord(char) == 127 for char in base_url
        ):
            raise ValueError(
                "system_api_url must not contain whitespace or control characters."
            )
        parsed = urlsplit(base_url)
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
        settings["system_api_url"] = base_url.rstrip("/") + "/"
    directory = settings["state_directory"]
    if not isinstance(directory, str) or not directory.strip():
        raise ValueError("state_directory must be a nonempty path.")
    state_path = Path(directory)
    settings["state_directory"] = (
        state_path if state_path.is_absolute() else path.parent / state_path
    )
    project = settings.get("project_root")
    if project is not None:
        if not isinstance(project, str) or not project.strip():
            raise ValueError("project_root must be a path or null.")
        project = Path(project)
        settings["project_root"] = (
            project if project.is_absolute() else path.parent / project
        ).resolve()
    else:
        settings["project_root"] = None
    for name, default, maximum in (
        ("history_window_events", 1000, 100000),
        ("history_max_events", 100000, 10000000),
        ("history_max_bytes", 67108864, 2147483648),
    ):
        value = settings.get(name, default)
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError(f"{name} must be an integer between 1 and {maximum}.")
        settings[name] = value
    return settings
