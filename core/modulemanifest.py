"""Read the module contract before packaging or assembling executable code."""

from pathlib import Path, PureWindowsPath

import yaml

from core.logger_utils.events import JsonObject, copy_json_object, require_text
from utils.seaweed_utils.utils import check_input_metadata


def read_module_manifest(module_directory: Path) -> JsonObject:
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
        module = copy_json_object(
            yaml.safe_load(manifest.read_text(encoding="utf-8")), "module"
        )
    except yaml.YAMLError as error:
        raise ValueError(f"Invalid YAML in {manifest}: {error}") from error
    required = {
        "schema_version",
        "name",
        "version",
        "role",
        "implementation",
        "commands",
        "defaults",
    }
    if (
        module.keys() != required
        or type(module["schema_version"]) is not int
        or module["schema_version"] != 2
    ):
        raise ValueError(
            f"Invalid module schema in {manifest}; expected schema_version 2."
        )
    name = require_text(module["name"], "module.name")
    version = require_text(module["version"], "module.version")
    check_input_metadata(name, version)
    for label, value in (("name", name), ("version", version)):
        if (
            value != value.strip()
            or value in (".", "..")
            or value.endswith(".")
            or PureWindowsPath(value).is_reserved()
            or len(value) > 255
        ):
            raise ValueError(f"module.{label} must be a portable folder name.")
    if module["role"] not in ("stage", "service") or module["implementation"] not in (
        "full",
        "action",
    ):
        raise NotImplementedError(
            "Modules require a stage/service role and full/action implementation."
        )
    commands = copy_json_object(module["commands"], "commands")
    if commands.keys() != {"start"}:
        raise ValueError("Module commands must contain exactly commands.start.")
    argv = commands["start"]
    if not isinstance(argv, list) or not argv:
        raise ValueError("commands.start must be a nonempty argv array.")
    for argument in argv:
        require_text(argument, "command argument")
    copy_json_object(module["defaults"], "module defaults")
    return module
