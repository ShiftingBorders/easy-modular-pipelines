"""Archive paths, members and reader configuration."""

from __future__ import annotations

import tarfile
import tempfile
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, Literal

from core.primitives.json_files import write_json
from core.primitives.json_values import require_text

if TYPE_CHECKING:
    from core.models.archive_documents import ArchiveModule
    from core.models.experiment_template import ExperimentTemplate
    from core.models.journal_records import JournalIdentity
    from core.models.journal_settings import LoggingConfiguration


def _reader_config(
    settings: LoggingConfiguration, identity: JournalIdentity, folder: Path
) -> Path:
    from core.models.updates import _update_model

    settings = _update_model(
        settings,
        open_mode="existing",
        expected_journal={
            "journal_id": identity.journal_id,
            "generation": identity.generation,
        },
    )
    reader_config = folder / "reader.json"
    write_json(
        reader_config,
        {
            "logging": settings.model_dump(mode="json"),
            "operation_context": {"source": "experiment_archiver"},
        },
    )
    return reader_config


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


def _modules(template: ExperimentTemplate) -> list[ArchiveModule]:
    from core.models.archive_documents import ArchiveModule
    from core.models.experiment_template import StageDefinition

    modules: dict[tuple[str, str], ArchiveModule] = {}
    roles: tuple[Literal["stage", "service"], ...] = ("stage", "service")
    for role in roles:
        definitions = template.stages if role == "stage" else template.services
        for definition in definitions:
            if role == "stage" and not isinstance(definition, StageDefinition):
                continue
            module = template.module_reference(definition)
            name = _member(module.name)
            version = _member(module.version)
            if "/" in name or "/" in version:
                raise ValueError("Module identities must be single path components.")
            item = ArchiveModule(
                name=name,
                version=version,
                hash=require_text(module.hash, "module.hash").lower(),
                role=role,
            )
            key = (name, version)
            if key in modules and modules[key] != item:
                raise ValueError("Conflicting definitions of the same module version.")
            modules[key] = item
    return [modules[key] for key in sorted(modules)]
