"""Read the module contract before packaging or assembling executable code."""

from pathlib import Path

import yaml

from core.models.module_manifest import ModuleManifest
from core.primitives.json_values import JsonObject


def read_module_manifest(module_directory: Path) -> JsonObject:
    return _read_module_manifest(module_directory).model_dump(exclude_unset=True)


def _read_module_manifest(module_directory: Path) -> ModuleManifest:
    directory = Path(module_directory)
    if not directory.is_absolute():
        raise ValueError("module_directory must be absolute.")
    for path in (directory, *directory.parents):
        if path.is_symlink() or path.is_junction():
            raise ValueError("Module paths must not traverse filesystem links.")
    if not directory.is_dir():
        raise NotADirectoryError(f"Module folder does not exist: {directory}")
    for path in directory.rglob("*"):
        if path.is_symlink() or path.is_junction():
            raise ValueError(f"Module code must not contain filesystem links: {path}")
    manifest = directory / "module.yaml"
    try:
        return ModuleManifest.model_validate(
            yaml.safe_load(manifest.read_text(encoding="utf-8"))
        )
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid YAML in {manifest}: {error}") from error
