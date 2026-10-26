"""Shared validated inputs for module reads in both controller modes."""

from core.models.server_arguments import (
    ModuleCoordinates,
    NoArguments,
    TemplatePathArguments,
)
from core.primitives.json_values import JsonObject


def _validate_module_read_args(
    name: str, args: JsonObject
) -> NoArguments | ModuleCoordinates | TemplatePathArguments:
    if name == "stats.modules":
        return NoArguments.model_validate(args)
    if name == "stats.module":
        return ModuleCoordinates.model_validate(args)
    if name == "stats.template":
        return TemplatePathArguments.model_validate(args)
    raise NotImplementedError(f"Unsupported module read: {name}")
