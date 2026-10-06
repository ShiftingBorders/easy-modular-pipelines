"""Preparation workspaces and reversible module filesystem operations."""

import tempfile
import time
from pathlib import Path
from types import TracebackType


def _raise_walk_error(error: OSError) -> None:
    """Preserve traversal failures instead of hashing an incomplete tree."""
    raise error


def _backup_existing_folder(target: Path) -> Path | None:
    backup_folder = None
    if target.exists():
        # A failed restore must leave the backup outside automatic cleanup.
        backup_folder = Path(tempfile.mkdtemp(prefix=".backup-", dir=target.parent))
        try:
            target.rename(backup_folder / "previous")
        except OSError as error:
            try:
                backup_folder.rmdir()
            except OSError as cleanup_error:
                error.add_note(f"Cleanup failed at {backup_folder}: {cleanup_error}")
            raise
    return backup_folder


def _restore_previous_folder(backup_folder: Path, target: Path, error: OSError) -> None:
    try:
        (backup_folder / "previous").rename(target)
    except OSError as restore_error:
        error.add_note(
            f"Restore failed: {restore_error}. Previous folder remains at "
            f"{backup_folder / 'previous'}."
        )
    else:
        try:
            backup_folder.rmdir()
        except OSError as cleanup_error:
            error.add_note(
                f"Previous folder restored; cleanup failed at "
                f"{backup_folder}: {cleanup_error}"
            )


def _cleanup_registration(
    temporary: tempfile.TemporaryDirectory,
    work_root: Path,
    failure: BaseException | None,
) -> None:
    work = Path(temporary.name)
    try:
        for attempt in range(3):
            if (
                work.is_symlink()
                or work.is_junction()
                or not work.resolve().is_relative_to(work_root)
            ):
                raise ValueError("Temporary cleanup target escaped its workspace.")
            try:
                temporary.cleanup()
                break
            except OSError as error:
                if getattr(error, "winerror", None) != 145 or attempt == 2:
                    raise
                time.sleep(0.02 * (attempt + 1))
    except OSError as error:
        note = f"Temporary cleanup failed at {work}: {error}"
        if failure is not None:
            failure.add_note(note)
        else:
            error.add_note(f"Module is already registered. {note}")
            raise


class _ModuleWorkspace:
    """Clean a preparation workspace without replacing the operation's error."""

    def __init__(
        self,
        temporary: tempfile.TemporaryDirectory[str],
        completion: str,
        destination: Path,
    ) -> None:
        self._temporary = temporary
        self._work = temporary.name
        self._completion = completion
        self._destination = destination

    def __enter__(self) -> str:
        return self._work

    def __exit__(
        self,
        error_type: type[BaseException] | None,
        failure: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            for attempt in range(3):
                try:
                    self._temporary.cleanup()
                    break
                except OSError as cleanup_error:
                    # Windows may still report a directory as nonempty while
                    # its last deleted entry is disappearing from the filesystem.
                    if getattr(cleanup_error, "winerror", None) != 145 or attempt == 2:
                        raise
                    time.sleep(0.02 * (attempt + 1))
        except OSError as cleanup_error:
            note = f"Temporary cleanup failed at {self._work}: {cleanup_error}"
            if failure is not None:
                failure.add_note(note)
            else:
                cleanup_error.add_note(
                    f"{self._completion} {self._destination}. {note}"
                )
                raise
