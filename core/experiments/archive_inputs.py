"""Archive paths, members and reader configuration."""

from __future__ import annotations

import tarfile
import tempfile
from pathlib import Path, PurePosixPath, PureWindowsPath

from core.primitives.json_files import write_json
from core.primitives.json_values import JsonObject, copy_json_object, require_text


def _reader_config(settings: JsonObject, identity: JsonObject, folder: Path) -> Path:
    settings.update(
        {
            "open_mode": "existing",
            "expected_journal": {
                key: identity[key] for key in ("journal_id", "generation")
            },
        }
    )
    reader_config = folder / "reader.json"
    write_json(
        reader_config,
        {
            "logging": settings,
            "operation_context": {"source": "experiment_archiver"},
        },
    )
    return reader_config


def _objects(value: object, field: str) -> list[JsonObject]:
    if not isinstance(value, list):
        raise TypeError(f"{field} must be an array of objects.")
    return [copy_json_object(item, field) for item in value]


def _path(value: Path) -> Path:
    if not isinstance(value, (str, Path)):
        raise TypeError("A filesystem path must be a string or Path.")
    path = Path(value)
    if not path.is_absolute() or "\x00" in str(path):
        raise ValueError("An absolute filesystem path is required.")
    for ancestor in (path, *path.parents):
        if ancestor.is_symlink() or ancestor.is_junction():
            raise ValueError(f"Filesystem links are not allowed: {ancestor}")
    return path.resolve()


def _member(value: object) -> str:
    name = require_text(value, "archive member")
    parts = name.split("/")
    if (
        PurePosixPath(name).is_absolute()
        or any(part in ("", ".", "..") for part in parts)
        or any(c in '\\:*?"<>|' or ord(c) < 32 or ord(c) == 127 for c in name)
        or any(
            part.endswith((".", " ")) or PureWindowsPath(part).is_reserved()
            for part in parts
        )
    ):
        raise ValueError(f"Unsafe archive member: {name!r}")
    return name


def _validate_member_header(header: bytes, member: tarfile.TarInfo) -> None:
    if header[257:265] != b"ustar\x0000" or member.type not in (
        tarfile.REGTYPE,
        tarfile.AREGTYPE,
        tarfile.DIRTYPE,
    ):
        raise ValueError("Archive links, sparse and special files are forbidden.")
    if member.size < 0 or (member.isdir() and member.size != 0):
        raise ValueError("Invalid archive member size.")


def _cleanup(
    temporary: tempfile.TemporaryDirectory,
    parent: Path,
    failure: BaseException | None,
) -> None:
    try:
        target = _path(Path(temporary.name))
        if target.parent != parent.resolve():
            raise ValueError("Archive cleanup escaped its workspace.")
        temporary.cleanup()
    except (OSError, ValueError) as error:
        note = f"Archive cleanup failed at {temporary.name}: {error}"
        if failure is not None:
            failure.add_note(note)
        else:
            error.add_note(f"Operation completed before cleanup failed. {note}")
            raise


def _modules(template: JsonObject) -> list[dict[str, str]]:
    modules: dict[tuple[str, str], dict[str, str]] = {}
    for role in ("stage", "service"):
        for definition in _objects(template[f"{role}s"], role):
            if role == "stage" and "service_id" in definition:
                continue
            module = copy_json_object(definition["module"], "module")
            name = _member(module["name"])
            version = _member(module["version"])
            if "/" in name or "/" in version:
                raise ValueError("Module identities must be single path components.")
            item = {
                "name": name,
                "version": version,
                "hash": require_text(module["hash"], "module.hash").lower(),
                "role": role,
            }
            key = (name, version)
            if key in modules and modules[key] != item:
                raise ValueError("Conflicting definitions of the same module version.")
            modules[key] = item
    return [modules[key] for key in sorted(modules)]
