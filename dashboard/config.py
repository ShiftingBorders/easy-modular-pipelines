"""Explicit dashboard configuration, resolved against its containing file."""

import json
import math
from pathlib import Path

from core.models.dashboard_settings import DashboardConfiguration


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
    validated = DashboardConfiguration.model_validate(settings)
    settings = validated.model_dump()
    # Optional token_env historically appears only when explicitly configured.
    if "system_api_token_env" not in validated.model_fields_set:
        settings.pop("system_api_token_env")
    directory = settings["state_directory"]
    state_path = Path(directory)
    settings["state_directory"] = (
        state_path if state_path.is_absolute() else path.parent / state_path
    )
    project = settings.get("project_root")
    if project is not None:
        project = Path(project)
        settings["project_root"] = (
            project if project.is_absolute() else path.parent / project
        ).resolve()
    else:
        settings["project_root"] = None
    return settings
