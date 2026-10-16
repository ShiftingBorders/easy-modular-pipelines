"""Explicit dashboard configuration, resolved against its containing file."""

import json
import math
from pathlib import Path

from core.models.dashboard_settings import (
    DashboardConfiguration,
    DashboardRuntimeConfiguration,
)


def number(value: object, name: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number.")
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}.")
    return value


def load_settings(path: Path, overrides: dict | None = None) -> dict:
    validated = _load_settings(path, overrides)
    settings = validated.model_dump()
    # Optional token_env historically appears only when explicitly configured.
    if "system_api_token_env" not in validated.model_fields_set:
        settings.pop("system_api_token_env")
    return settings


def _load_settings(
    path: Path, overrides: dict | None = None
) -> DashboardRuntimeConfiguration:
    if not path.is_absolute():
        raise ValueError("The dashboard configuration path must be absolute.")
    path = path.resolve(strict=True)
    settings = json.loads(path.read_text(encoding="utf-8-sig"))
    if overrides:
        if not isinstance(settings, dict):
            raise ValueError("Dashboard settings must be an object.")
        settings.update(overrides)
    validated = DashboardConfiguration.model_validate(settings)
    state_path = Path(validated.state_directory)
    state_path = state_path if state_path.is_absolute() else path.parent / state_path
    project = None
    if validated.project_root is not None:
        configured_project = Path(validated.project_root)
        project = (
            configured_project
            if configured_project.is_absolute()
            else path.parent / configured_project
        ).resolve()
    values = {
        name: getattr(validated, name)
        for name in DashboardConfiguration.model_fields
        if name != "system_api_token_env" or name in validated.model_fields_set
    }
    values.update(state_directory=state_path, project_root=project)
    return DashboardRuntimeConfiguration.model_validate(values)
