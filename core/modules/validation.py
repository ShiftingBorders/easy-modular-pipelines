"""Validation operations."""

import tarfile
from pathlib import Path, PurePosixPath

from core.primitives.json_values import JsonObject
from core.storage.module_identity import HEX_DIGITS, INVALID_MODULE_VERSION_CHARACTERS


def _normalize_path(path: str | Path) -> Path:
    """Convert a string path without resolving it or accessing the filesystem."""
    if not isinstance(path, (str, Path)):
        raise TypeError("Path must be a string or pathlib.Path.")
    if isinstance(path, str) and not path.strip():
        raise ValueError("Path must not be empty.")
    if "\x00" in str(path):
        raise ValueError("Path must not contain a null character.")
    return Path(path)


def _validate_version(module_version: str) -> None:
    if not isinstance(module_version, str):
        raise TypeError("module_version must be a string.")
    if not module_version.strip() or module_version.strip() in {".", ".."}:
        raise ValueError("module_version must not be empty or a dot segment.")
    if any(
        character in INVALID_MODULE_VERSION_CHARACTERS for character in module_version
    ):
        raise ValueError("module_version contains invalid characters.")


def _validate_module_hash(digest: str) -> None:
    if not isinstance(digest, str):
        raise TypeError("Module hash must be a string.")
    if len(digest) != 64 or any(character not in HEX_DIGITS for character in digest):
        raise ValueError("Module hash must contain exactly 64 hexadecimal characters.")


def _validate_archive_members(
    members: list[tarfile.TarInfo], target: Path, package: bool
) -> None:
    seen_paths = set()
    for member in members:
        member_path = PurePosixPath(member.name)
        if (
            not member.name
            or member_path.is_absolute()
            or ".." in member_path.parts
            or "\\" in member.name
            or ":" in member.name
            or not (member.isfile() or member.isdir())
        ):
            raise ValueError(f"Unsupported archive entry: {member.name!r}")
        # Compare normalized paths, including case aliases on Windows.
        destination = target / member_path.as_posix()
        if destination in seen_paths:
            raise ValueError(f"Duplicate archive path: {member.name!r}")
        seen_paths.add(destination)
        if package and (
            not member.isfile()
            or len(member_path.parts) != 1
            or member.name != member_path.name
        ):
            raise ValueError("Package entries must be files at its root.")
    if package:
        names = [member.name for member in members]
        if (
            len(names) != 2
            or "hash.txt" not in names
            or sum(name.endswith(".tar.xz") for name in names) != 1
        ):
            raise ValueError(
                "Package must contain only hash.txt and one tar.xz archive."
            )


def _validate_registration_manifest(
    manifest: JsonObject, module_name: str, module_version: str
) -> None:
    if (manifest["name"], manifest["version"]) != (module_name, module_version):
        raise ValueError("module.yaml name/version differ from registration arguments.")


def _optional_module_folder(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    folder = _normalize_path(value)
    if not folder.is_absolute():
        raise ValueError("Module folder path must be absolute.")
    if folder.is_symlink() or folder.is_junction():
        raise ValueError("Module folder must not be a filesystem link.")
    return folder


def _validate_folder_path(folder_path: str | Path) -> Path:
    """Return a resolved existing folder, rejecting relative paths and links."""
    folder = _normalize_path(folder_path)
    if not folder.is_absolute():
        raise ValueError(f"Folder path must be absolute: {folder}")
    if folder.is_symlink() or folder.is_junction():
        raise ValueError(f"Folder must not be a filesystem link: {folder}")
    folder = folder.resolve(strict=True)
    if not folder.is_dir():
        raise NotADirectoryError(f"Path is not a folder: {folder}")
    return folder


def _uncompression_paths(
    archive_path: str | Path, target_path: str | Path
) -> tuple[Path, Path]:
    archive_path = _normalize_path(archive_path)
    target = _normalize_path(target_path)
    if not archive_path.is_absolute() or not target.is_absolute():
        raise ValueError("Archive and target paths must be absolute.")
    if target.is_symlink() or target.is_junction():
        raise ValueError("The target folder must not be a filesystem link.")
    archive_path = archive_path.resolve(strict=True)
    target = target.resolve()
    if not archive_path.is_file():
        raise ValueError(f"Archive is not a regular file: {archive_path}")
    if target == archive_path or target in archive_path.parents:
        raise ValueError("The archive must be outside the target folder.")
    if target.exists() and not target.is_dir():
        raise NotADirectoryError(f"Target is not a folder: {target}")
    return archive_path, target
