"""Manager operations."""

import asyncio
import lzma
import os
import shutil
import tarfile
import tempfile
from pathlib import Path, PureWindowsPath
from threading import Event, Lock

from core.models.module_settings import ModuleHashingSettings
from core.modules.errors import HashMismatch
from core.modules.filesystem import (
    _backup_existing_folder,
    _cleanup_registration,
    _ModuleWorkspace,
    _raise_walk_error,
    _restore_previous_folder,
)
from core.modules.hashing import _check_hash_cancelled, _hash_files
from core.modules.manifest import read_module_manifest
from core.modules.validation import (
    _normalize_path,
    _optional_module_folder,
    _uncompression_paths,
    _validate_archive_members,
    _validate_folder_path,
    _validate_module_hash,
    _validate_registration_manifest,
    _validate_version,
)
from core.primitives.json_values import JsonObject, require_text
from core.primitives.paths import repository_root
from core.primitives.tasks import _await_outcome
from core.storage.contracts import HashDatabase, ModuleAddResult, ModuleDatabase
from core.storage.errors import StorageConflict, StorageError, StoredObjectNotFound
from core.storage.module_identity import (
    HEX_DIGITS,
    INVALID_MODULE_NAME_CHARACTERS,
)

# Storage may have committed; finish before the owner closes it.


class ModuleManager:
    """Coordinate module operations through caller-owned storage contracts.

    Expected storage failures use core.storage.errors. The caller creates and
    closes both stores; this manager never starts or stops their services.
    Local module preparation retains its filesystem and validation exceptions.
    """

    def __init__(
        self,
        module_storage_path: str | Path,
        hash_db: HashDatabase,
        module_db: ModuleDatabase,
        temp_folder: str | Path,
        ignore_folders: set[str | Path] | None = None,
        *,
        hashing_settings: ModuleHashingSettings | None = None,
    ) -> None:
        """Bind caller-owned stores and validate local module workspace paths.

        Args:
            module_storage_path: Absolute directory for installed modules.
            hash_db: Open hash database owned by the caller.
            module_db: Archive store owned by the caller.
            temp_folder: Absolute directory for temporary preparation work.
            ignore_folders: Optional set of paths excluded from module packaging.
            hashing_settings: Validated hashing limits, or None for defaults.

        Raises:
            ValueError: A workspace path is relative or a filesystem link.
            NotADirectoryError: An existing workspace path is not a directory.
            TypeError: Ignore paths or storage dependencies violate their contracts.
        """
        storage_path = _normalize_path(module_storage_path)
        temporary_path = _normalize_path(temp_folder)
        for folder in (storage_path, temporary_path):
            if not folder.is_absolute():
                raise ValueError(f"Folder path must be absolute: {folder}")
            if folder.is_symlink() or folder.is_junction():
                raise ValueError(f"Folder must not be a filesystem link: {folder}")
            if folder.exists() and not folder.is_dir():
                raise NotADirectoryError(f"Path is not a folder: {folder}")
        if ignore_folders is not None and not isinstance(ignore_folders, set):
            raise TypeError("ignore_folders must be a set or None.")
        ignored_paths = {
            _normalize_path(folder) for folder in (ignore_folders or set())
        }
        for dependency_name, dependency, contract in (
            ("hash_db", hash_db, HashDatabase),
            ("module_db", module_db, ModuleDatabase),
        ):
            if isinstance(dependency, type):
                raise TypeError(f"{dependency_name} must be an instance, not a class.")
            for method_name, method in vars(contract).items():
                if (
                    not method_name.startswith("_")
                    and callable(method)
                    and not callable(getattr(dependency, method_name, None))
                ):
                    raise TypeError(
                        f"{dependency_name}.{method_name} must be callable."
                    )
        self.module_storage_path = storage_path.resolve()
        self.ignore_folders = ignored_paths
        self.hash_db = hash_db
        self.module_db = module_db
        self.temp_folder = temporary_path.resolve()
        self.hashing_settings = (
            ModuleHashingSettings()
            if hashing_settings is None
            else ModuleHashingSettings.model_validate(hashing_settings)
        )
        # One file pool at a time per manager, including callers in other workers.
        self._hash_lock = Lock()

    def list_modules(self) -> JsonObject:
        """Return registered module references under the items key."""
        return {"items": self.hash_db.list_module_hashes()}

    async def inspect_module(self, name: str, version: str) -> JsonObject:
        """Inspect registration, archive availability, and local installation.

        Args:
            name: Module name; surrounding whitespace is removed.
            version: Module version; surrounding whitespace is removed.

        Returns:
            Registration and availability metadata with integrity marked not_checked.

        Raises:
            StoredObjectNotFound: No hash is registered for this identity.
            ValueError: Name or version is unsafe.
            StorageError: A backing store cannot complete the lookup.
        """
        name = require_text(name, "module name").strip()
        version = require_text(version, "module version").strip()
        for field, value in (("name", name), ("version", version)):
            if value in (".", "..") or any(
                character in INVALID_MODULE_NAME_CHARACTERS for character in value
            ):
                raise ValueError(f"Unsafe module {field}.")
        digest = self.hash_db.get_module_hash(name, version)
        if not digest:
            raise StoredObjectNotFound(f"No hash registered for {name}/{version}.")
        # Keep SQLite on its owning thread; the archive client may block on HTTP.
        operation = asyncio.create_task(
            asyncio.to_thread(self.module_db.check_module_stored, name, version)
        )
        cancelled = False
        try:
            while True:
                try:
                    archive_available = await asyncio.shield(operation)
                    break
                except asyncio.CancelledError:
                    cancelled = True
                    if operation.cancelled():
                        raise
        finally:
            # Finish the storage call before its owner closes the client, then
            # propagate cancellation so validation starts no further checks.
            if cancelled:
                raise asyncio.CancelledError
        directory = self.module_storage_path / name / version
        return {
            "module": {"name": name, "version": version, "hash": digest},
            "archive_available": archive_available,
            "installed": directory.is_dir(),
            "installation_path": str(directory),
            "integrity": "not_checked",
        }

    def _get_temp_folder(self, temp_folder: str | Path | None) -> Path:
        """Return the configured workspace or normalize an explicit override."""
        return self.temp_folder if temp_folder is None else _normalize_path(temp_folder)

    def _ensure_folder(self, temp_folder: str | Path | None) -> bool:
        """Return whether the selected temporary path exists as a directory."""
        target_path = self._get_temp_folder(temp_folder)
        return bool(target_path.exists() and target_path.is_dir())

    def _validate_folder_path(self, folder_path: str | Path) -> Path:
        """Return a validated absolute, existing, unlinked source directory."""
        return _validate_folder_path(folder_path)

    def _replace_folder(self, target_location: str | Path, source_location: str | Path):
        """Replace a folder with a copy of the source, retaining the source.

        Args:
            target_location: Absolute destination directory to replace after
                successful preparation.
            source_location: Absolute existing directory supplying replacement
                contents.
        """
        source, target = self._replacement_paths(target_location, source_location)

        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix=".replace-", dir=target.parent)
        work = temporary.name
        failure = None
        installed = False
        try:
            staging = Path(work) / "new"
            shutil.copytree(source, staging, symlinks=True)
            backup_folder = _backup_existing_folder(target)
            try:
                staging.rename(target)
                installed = True
            except OSError as error:
                if backup_folder is not None:
                    _restore_previous_folder(backup_folder, target, error)
                raise
            if backup_folder is not None:
                try:
                    shutil.rmtree(backup_folder)
                except OSError as error:
                    error.add_note(
                        f"Folder already installed at {target}; backup cleanup failed "
                        f"at {backup_folder}."
                    )
                    raise
        except BaseException as error:
            failure = error
            raise
        finally:
            try:
                temporary.cleanup()
            except OSError as cleanup_error:
                note = f"Temporary cleanup failed at {work}: {cleanup_error}"
                if failure is not None:
                    failure.add_note(note)
                else:
                    if installed:
                        cleanup_error.add_note(f"Folder already installed at {target}.")
                    cleanup_error.add_note(note)
                    raise

    def _replacement_paths(
        self, target_location: str | Path, source_location: str | Path
    ) -> tuple[Path, Path]:
        """Validate disjoint replacement paths and inspect the source tree.

        Args:
            target_location: Absolute destination directory to replace.
            source_location: Existing absolute source directory.

        Returns:
            Resolved source and target paths, in that order.

        Raises:
            ValueError: Paths overlap, traverse links, target a protected directory,
                or the source contains special files.
            NotADirectoryError: An existing path is not a directory.
        """
        source = self._validate_folder_path(source_location)
        target = _normalize_path(target_location)
        if not target.is_absolute():
            raise ValueError("Target path must be absolute.")
        if target.is_symlink() or target.is_junction():
            raise ValueError("Target folder must not be a filesystem link.")
        target = target.resolve()
        if target == source or target in source.parents or source in target.parents:
            raise ValueError("Source and target folders must not overlap.")
        if target.exists() and not target.is_dir():
            raise NotADirectoryError(f"Target is not a folder: {target}")
        if target in {repository_root(), Path.home().resolve()}:
            raise ValueError("Cannot replace the project or home folder.")

        folders = [source]
        while folders:
            for entry in folders.pop().iterdir():
                if entry.is_symlink() or entry.is_junction():
                    raise ValueError(f"Source contains a filesystem link: {entry}")
                if entry.is_dir():
                    folders.append(entry)
                elif not entry.is_file():
                    raise ValueError(f"Source contains a special file: {entry}")
        return source, target

    def _validate_module_folder(
        self, module_name: str, *, require_exists: bool = True
    ) -> Path:
        """Validate the module location and return its resolved folder path.

        Args:
            module_name: Registered module name.
            require_exists: Whether the module directory must already exist.

        Returns:
            Resolved confined module directory, with existence checked when
            requested.
        """
        if not isinstance(module_name, str):
            raise TypeError("module_name must be a string.")
        module_name = module_name.strip()
        storage_path = self.module_storage_path
        if not storage_path.is_absolute():
            raise ValueError("module_storage_path must be an absolute path.")
        if (
            not module_name
            or module_name in {".", ".."}
            or any(
                character in INVALID_MODULE_NAME_CHARACTERS for character in module_name
            )
            or module_name.endswith(".")
            or PureWindowsPath(module_name).is_reserved()
        ):
            raise ValueError("module_name must be a single folder name.")

        folder = storage_path / module_name
        if require_exists:
            return self._validate_folder_path(folder)
        return folder

    def _collect_module_files(
        self, folder_path: str | Path, *, apply_ignores: bool = True
    ) -> list[Path]:
        """Return sorted relative file paths, skipping links and ignored folders.

        Relative ignored folder paths are interpreted from folder_path.

        Args:
            folder_path: Absolute module source directory.
            apply_ignores: Whether configured ignored folders are excluded from the
                file set.

        Returns:
            Sorted relative file paths, skipping links and ignored folders.
        """
        folder_path = self._validate_folder_path(folder_path)
        ignored_folders = set()
        for ignored_folder in self.ignore_folders if apply_ignores else ():
            ignored_path = _normalize_path(ignored_folder)
            if not ignored_path.is_absolute():
                ignored_path = folder_path / ignored_path
            ignored_folders.add(ignored_path.resolve())

        files: list[Path] = []
        for folder_name, children, filenames in os.walk(
            folder_path, followlinks=False, onerror=_raise_walk_error
        ):
            folder = Path(folder_name)
            if any(
                ignored == folder or ignored in folder.parents
                for ignored in ignored_folders
            ):
                children.clear()
                continue
            children[:] = [
                name for name in children
                if not (folder / name).is_symlink()
                and not (folder / name).is_junction()
                and (folder / name).resolve() not in ignored_folders
            ]
            for name in filenames:
                entry = folder / name
                if entry.is_symlink() or entry.is_junction():
                    continue
                if entry.is_file():
                    files.append(entry.relative_to(folder_path))
        return sorted(files, key=lambda path: path.as_posix())

    def _compress_folder(
        self,
        folder_path: str | Path,
        temp_folder: str | Path,
        *,
        apply_ignores: bool = True,
    ) -> Path:
        """Archive folder contents as tar.xz using maximum LZMA compression.

        Args:
            folder_path: Absolute module source directory.
            temp_folder: Temporary directory override, or None for the manager's
                configured workspace.
            apply_ignores: Whether configured ignored folders are excluded from the
                file set.

        Returns:
            Path to the published tar.xz archive containing the selected source
            files.
        """
        folder = self._validate_folder_path(folder_path)
        output = _normalize_path(temp_folder)
        if not output.is_absolute():
            raise ValueError("Archive directory path must be absolute.")
        output = output.resolve()
        if output == folder or folder in output.parents:
            raise ValueError("Archive directory must be outside the source folder.")

        files = self._collect_module_files(folder, apply_ignores=apply_ignores)

        output.mkdir(parents=True, exist_ok=True)
        archive_path = output / f"{folder.name}.tar.xz"
        temporary = tempfile.TemporaryDirectory(prefix=".compress-", dir=output)
        with _ModuleWorkspace(
            temporary, "Archive already saved at", archive_path
        ) as work:
            staging = Path(work) / "archive.tar.xz"
            with tarfile.open(
                staging, "w:xz", preset=9 | lzma.PRESET_EXTREME, dereference=True
            ) as archive:
                for relative_path in files:
                    archive.add(
                        folder / relative_path,
                        arcname=relative_path.as_posix(),
                        recursive=False,
                    )
            staging.replace(archive_path)
        return archive_path

    def _uncompress_folder(
        self,
        archive_path: str | Path,
        target_path: str | Path,
        *,
        package: bool = False,
    ) -> Path:
        """Extract tar.xz into a staged folder, then replace the destination.

        Checks every member before extraction into a temporary sibling. Only a fully
        extracted staged directory replaces the destination; cleanup failures
        preserve the primary operation error.

        Args:
            archive_path: Absolute archive file path on the server filesystem.
            target_path: Absolute destination directory for extraction.
            package: Whether the archive must satisfy the outer package layout
                rather than the code archive.

        Returns:
            Extraction directory after member-path/layout validation and extraction.
        """
        archive_path, target = _uncompression_paths(archive_path, target_path)

        with tarfile.open(archive_path, "r:xz") as archive:
            members = archive.getmembers()
            _validate_archive_members(members, target, package)
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = tempfile.TemporaryDirectory(
                prefix=".extract-", dir=target.parent
            )
            with _ModuleWorkspace(
                temporary, "Folder already installed at", target
            ) as work:
                staging = Path(work) / "contents"
                staging.mkdir()
                archive.extractall(staging, members=members, filter="data")
                self._replace_folder(target, staging)
        return target

    def _add_hash_file(self, hash: str, module_temp_folder: str | Path):
        """Write a SHA-256 digest to hash.txt in the module staging folder.

        Args:
            hash: Validated hexadecimal SHA-256 digest written to package metadata.
            module_temp_folder: Absolute staging directory receiving hash.txt.
        """
        _validate_module_hash(hash)
        folder = self._validate_folder_path(module_temp_folder)
        temporary = tempfile.TemporaryDirectory(prefix=".hash-", dir=folder)
        with _ModuleWorkspace(temporary, "hash.txt already saved in", folder) as work:
            staging = Path(work) / "hash.txt"
            staging.write_text(hash.lower() + "\n", encoding="UTF-8", newline="\n")
            staging.replace(folder / "hash.txt")

    def module_hash(
        self, module_name: str, target_folder: str | Path | None = None
    ) -> str:
        """Hash relative file names and contents, excluding links and empty folders.

        The storage path must be absolute. Relative ignored folder paths are
        interpreted from the module folder. Files must remain unchanged while
        the hash is being computed.

        Args:
            module_name: Registered module name.
            target_folder: Explicit source folder for hashing, or None to use
                installed module storage.

        Returns:
            Deterministic hexadecimal content hash of included module files.
        """
        return self._module_hash(module_name, target_folder)

    def _module_hash(
        self,
        module_name: str,
        target_folder: str | Path | None = None,
        cancelled: Event | None = None,
    ) -> str:
        """Hash in a caller's worker, joining every file reader before returning.

        Args:
            module_name: Registered module name.
            target_folder: Explicit source folder for hashing, or None to use
                installed module storage.
            cancelled: Thread-safe cooperative cancellation event; callers await the
                worker before cleanup.

        Returns:
            Deterministic hexadecimal module hash after the cooperative file-hashing
            workers complete.
        """
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        target_folder = _optional_module_folder(target_folder)
        if target_folder is not None and self._ensure_folder(target_folder):
            module_folder = self._validate_folder_path(target_folder)
        else:
            module_folder = self._validate_module_folder(module_name)

        cancellation = cancelled if cancelled is not None else Event()
        while not self._hash_lock.acquire(timeout=0.05):
            _check_hash_cancelled(cancellation)
        try:
            _check_hash_cancelled(cancellation)
            files = self._collect_module_files(module_folder)
            return _hash_files(module_folder, files, self.hashing_settings, cancellation)
        finally:
            self._hash_lock.release()

    def register_module(
        self,
        module_name: str,
        module_version: str,
        module_folder: str | Path | None = None,
    ) -> bool:
        """Return True for a new registration, False for an identical stored module.

        A different hash or an incomplete stored pair raises StorageConflict.
        Upload failures trigger an attempt to roll back the newly added hash.
        Database operations are not a shared transaction. An upload error may
        still leave remote archive data; concurrent removal is not coordinated.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            module_folder: Explicit source directory, or None to use the module's
                configured location.

        Returns:
            True for a new registration, False for an identical stored module.
        """
        module_name, source_folder, work_root = self._registration_source(
            module_name, module_version, module_folder
        )
        module_hash = self._registration_hash(module_name, module_version, source_folder)
        archive_exists = self.module_db.check_module_stored(module_name, module_version)
        if self._registration_exists(
            module_name, module_version, module_hash, archive_exists
        ):
            return False
        work_root.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(
            prefix="register-module-", dir=work_root
        )
        failure = None
        try:
            archive_path = self._registration_package(
                source_folder, Path(temporary.name), module_hash
            )
            result = self.hash_db.add_module_hash(
                module_name, module_version, module_hash
            )
            if result != ModuleAddResult.module_added:
                archive_exists = self.module_db.check_module_stored(
                    module_name, module_version
                )
                self._validate_registration_result(
                    result, module_name, module_version, module_hash, archive_exists
                )
                return False
            try:
                self.module_db.save_module(module_name, module_version, archive_path)
            except StorageError as upload_error:
                self._rollback_registration_hash(
                    module_name, module_version, upload_error
                )
                raise
        except BaseException as error:
            failure = error
            raise
        finally:
            _cleanup_registration(temporary, work_root, failure)
        return True

    async def register_module_async(
        self,
        module_name: str,
        module_version: str,
        module_folder: str | Path | None = None,
    ) -> bool:
        """Register without blocking the event loop on compression or network I/O.

        Hash storage stays on the calling thread. Archive storage must support
        worker-thread calls (as SeaweedDB does). Serialize registrations just as
        for register_module. Cancellation waits for the registration's outcome.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            module_folder: Explicit source directory, or None to use the module's
                configured location.

        Returns:
            True for new registration or False for an identical existing
            hash/archive pair.
        """
        operation = asyncio.create_task(
            self._register_module_async(module_name, module_version, module_folder)
        )
        return await _await_outcome(operation)

    async def _register_module_async(
        self, module_name: str, module_version: str, module_folder: str | Path | None
    ) -> bool:
        """Package and register a module while offloading archive I/O to threads.

        Args:
            module_name: Module name to register.
            module_version: Version to register without replacement.
            module_folder: Explicit source directory, or None for the module folder.

        Returns:
            True for a new registration, False for an identical existing one.

        Raises:
            StorageConflict: Existing hash/archive data conflicts or is incomplete.
            StorageError: Registration fails; upload failure triggers hash rollback.
        """
        module_name, source_folder, work_root = self._registration_source(
            module_name, module_version, module_folder
        )
        module_hash = await asyncio.to_thread(
            self._registration_hash, module_name, module_version, source_folder
        )
        archive_exists = await asyncio.to_thread(
            self.module_db.check_module_stored, module_name, module_version
        )
        if self._registration_exists(
            module_name, module_version, module_hash, archive_exists
        ):
            return False
        work_root.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(
            prefix="register-module-", dir=work_root
        )
        failure = None
        try:
            archive_path = await asyncio.to_thread(
                self._registration_package,
                source_folder,
                Path(temporary.name),
                module_hash,
            )
            result = self.hash_db.add_module_hash(
                module_name, module_version, module_hash
            )
            if result != ModuleAddResult.module_added:
                archive_exists = await asyncio.to_thread(
                    self.module_db.check_module_stored, module_name, module_version
                )
                self._validate_registration_result(
                    result, module_name, module_version, module_hash, archive_exists
                )
                return False
            try:
                await asyncio.to_thread(
                    self.module_db.save_module,
                    module_name,
                    module_version,
                    archive_path,
                )
            except StorageError as upload_error:
                self._rollback_registration_hash(
                    module_name, module_version, upload_error
                )
                raise
        except BaseException as error:
            failure = error
            raise
        finally:
            await asyncio.to_thread(
                _cleanup_registration, temporary, work_root, failure
            )
        return True

    def _registration_hash(self, name: str, version: str, source: Path) -> str:
        """Check the manifest identity before hashing the registration's source.

        Args:
            name: Operation or module name used by the selected action.
            version: Module version associated with the supplied name.
            source: Validated immutable module source directory.

        Returns:
            Computed digest after checking source manifest name/version against
            registration coordinates.
        """
        manifest = read_module_manifest(source)
        _validate_registration_manifest(manifest, name, version)
        return self.module_hash(name, source)

    def _validate_registration_result(
        self,
        result: ModuleAddResult,
        module_name: str,
        module_version: str,
        module_hash: str,
        archive_exists: bool,
    ) -> None:
        """Accept a duplicate insert only when the stored pair is identical.

        Args:
            result: Hash store's insertion outcome to reconcile with existing
                archive/hash state.
            module_name: Registered module name.
            module_version: Registered module version.
            module_hash: Expected module content hash.
            archive_exists: Previously observed presence of the corresponding stored
                module archive.
        """
        if result == ModuleAddResult.module_exists_err and self._registration_exists(
            module_name, module_version, module_hash, archive_exists
        ):
            return
        raise StorageConflict("The hash database did not add the module record.")

    def _registration_source(
        self, module_name: str, module_version: str, module_folder: str | Path | None
    ) -> tuple[str, Path, Path]:
        """Return normalized name, source, and a workspace outside the source tree.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            module_folder: Explicit source directory, or None to use the module's
                configured location.

        Returns:
            Normalized name, source, and a workspace outside the source tree.
        """
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        _validate_version(module_version)
        source_folder = (
            self._validate_module_folder(module_name)
            if module_folder is None
            else self._validate_folder_path(module_folder)
        )
        work_root = Path(self.temp_folder)
        if not work_root.is_absolute():
            raise ValueError("Temporary folder path must be absolute.")
        work_root = work_root.resolve()
        if work_root == source_folder or source_folder in work_root.parents:
            raise ValueError(
                "Temporary folder must be outside the module source folder."
            )

        return module_name, source_folder, work_root

    def _registration_exists(
        self,
        module_name: str,
        module_version: str,
        module_hash: str,
        archive_exists: bool,
    ) -> bool:
        """Return whether the identical hash/archive pair exists, raising on conflicts.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            module_hash: Expected module content hash.
            archive_exists: Previously observed presence of the corresponding stored
                module archive.

        Returns:
            Whether the identical hash/archive pair exists, raising on conflicts.
        """
        stored_hash = self.hash_db.get_module_hash(module_name, module_version)
        if stored_hash == "" and not archive_exists:
            return False
        if stored_hash != "" and archive_exists and stored_hash.lower() == module_hash:
            return True
        raise StorageConflict(
            f"Cannot register module {module_name!r}, version {module_version!r}: "
            "the stored hash differs or the hash/archive pair is incomplete."
        )

    def _registration_package(self, source: Path, work: Path, digest: str) -> Path:
        """Create a nested module archive with its hash file and return its path.

        Args:
            source: Validated immutable module source directory to package with
                hash.txt.
            work: Owned temporary workspace for downloaded or prepared files.
            digest: Expected lowercase SHA-256 module digest.

        Returns:
            Outer tar.xz archive path containing the code archive and expected
            hash.txt.
        """
        package = work / "package"
        self._compress_folder(source, package)
        self._add_hash_file(digest, package)
        return self._compress_folder(package, work / "archive", apply_ignores=False)

    def _rollback_registration_hash(
        self, name: str, version: str, error: StorageError
    ) -> None:
        """Remove a failed upload's hash record, annotating the original storage error.

        Args:
            name: Operation or module name used by the selected action.
            version: Module version associated with the supplied name.
            error: Primary failure retained while cleanup or error translation
                proceeds.
        """
        try:
            if not self.hash_db.remove_module_hash(name, version):
                error.add_note("Hash rollback found no record to remove.")
        except StorageError as rollback_error:
            error.add_note(f"Hash rollback also failed: {rollback_error}")

    def _download_verified_module(
        self, name: str, version: str, digest: str, work: Path
    ) -> Path:
        """Verify the stored package into an isolated directory without installing it.

        Args:
            name: Operation or module name used by the selected action.
            version: Module version associated with the supplied name.
            digest: Expected lowercase SHA-256 module digest.
            work: Owned temporary workspace for downloaded or prepared files.

        Returns:
            Extracted code directory after validating both package hash metadata and
            actual module contents.
        """
        if not self.module_db.check_module_stored(name, version):
            raise StoredObjectNotFound(f"No archive stored for {name}/{version}.")
        archive = work / "package.tar.xz"
        if not self.module_db.retrieve_module(name, version, archive):
            raise StorageError(f"Failed to download {name}/{version}.")
        package = self._uncompress_folder(archive, work / "package", package=True)
        package_hash = (
            (package / "hash.txt").read_text(encoding="utf-8").strip().lower()
        )
        if package_hash != digest:
            raise HashMismatch("The package hash does not match the database hash.")
        code_archive = next(package.glob("*.tar.xz"))
        code = self._uncompress_folder(code_archive, work / "code")
        if self.module_hash(name, code) != digest:
            raise HashMismatch("The extracted module does not match the database hash.")
        return code

    def _verify_stored_module(self, name: str, version: str, digest: str) -> JsonObject:
        """Download and verify a package and manifest, then return its module reference.

        Args:
            name: Operation or module name used by the selected action.
            version: Module version associated with the supplied name.
            digest: Expected lowercase SHA-256 module digest.

        Returns:
            Verified name/version/hash reference after temporary download,
            extraction, and manifest checks.
        """
        self.temp_folder.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="validate-module-", dir=self.temp_folder
        ) as work:
            code = self._download_verified_module(name, version, digest, Path(work))
            manifest = read_module_manifest(code)
            if (manifest["name"], manifest["version"]) != (name, version):
                raise ValueError(
                    "Stored module.yaml identity differs from its registration."
                )
        return {"name": name, "version": version, "hash": digest}

    async def validate_stored_module_async(self, name: str, version: str) -> JsonObject:
        """Return a verified template reference, reading HashDB on its owning thread.

        Args:
            name: Operation or module name used by the selected action.
            version: Module version associated with the supplied name.

        Returns:
            A verified template reference, reading HashDB on its owning thread.
        """
        digest = self.hash_db.get_module_hash(name, version)
        if not digest:
            raise StoredObjectNotFound(f"No hash registered for {name}/{version}.")
        if len(digest) != 64 or any(
            character not in HEX_DIGITS for character in digest
        ):
            raise ValueError("The registered module hash is not a SHA-256 digest.")
        operation = asyncio.create_task(
            asyncio.to_thread(self._verify_stored_module, name, version, digest.lower())
        )
        # Never release the databases while a worker still uses their clients.
        return await _await_outcome(operation)

    def _copy_module_source(self, source: Path, destination: Path) -> None:
        """Copy included source files into a newly created staging directory.

        Args:
            source: Validated immutable module source directory.
            destination: New temporary source-copy directory.
        """
        destination.mkdir()
        for relative in self._collect_module_files(source):
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source / relative, target)

    async def _prepare_install_copy(
        self, staged: Path, publish: Path, name: str, digest: str
    ) -> None:
        """Copy staged code for publication and reject any changed content hash.

        Args:
            staged: Prepared immutable module source-copy directory.
            publish: New directory on the destination filesystem used for final
                atomic publication.
            name: Operation or module name used by the selected action.
            digest: Expected lowercase SHA-256 module digest.
        """
        await asyncio.to_thread(shutil.copytree, staged, publish)
        if await asyncio.to_thread(self.module_hash, name, publish) != digest:
            raise HashMismatch("Installation changed during copying.")

    async def register_and_install_module_async(
        self, module_folder: Path
    ) -> JsonObject:
        """Register an arbitrary source and publish an immutable version in this project.

        Args:
            module_folder: Explicit source directory, or None to use the module's
                configured location.

        Returns:
            Registered module reference, registration status, installation path, and
            whether local code was newly installed.
        """
        operation = asyncio.create_task(
            self._register_and_install_module(module_folder)
        )
        return await _await_outcome(operation)

    async def _register_and_install_module(self, module_folder: Path) -> JsonObject:
        """Stage, register, verify, and publish module code into local storage.

        Args:
            module_folder: Absolute module source directory.

        Returns:
            Module reference, registration status, installation path, and whether
            this call installed the code.

        Raises:
            HashMismatch: Staged, registered, or copied contents disagree.
            StorageConflict: Different code exists or the destination appears mid-copy.
            StorageError: Registration or package verification fails.
        """
        manifest = await asyncio.to_thread(read_module_manifest, Path(module_folder))
        source = self._validate_folder_path(module_folder)
        name, version = manifest["name"], manifest["version"]
        target, work_root = self._installation_paths(source, name, version)
        work_root.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="install-module-", dir=work_root)
        failure = None
        registered = None
        installed = False
        try:
            staged = Path(temporary.name) / "source"
            digest = await self._prepare_install_source(source, staged, manifest, name)
            target_exists = await self._check_existing_installation(
                target, name, digest
            )
            registered = await self.register_module_async(name, version, staged)
            reference = await self.validate_stored_module_async(name, version)
            if reference["hash"] != digest:
                raise HashMismatch(
                    "Registered module differs from the prepared installation."
                )
            if not target_exists:
                target.parent.mkdir(parents=True, exist_ok=True)
                # Stage on the destination filesystem so publication is one rename.
                with tempfile.TemporaryDirectory(
                    prefix=".install-", dir=target.parent
                ) as local:
                    publish = Path(local) / "code"
                    await self._prepare_install_copy(staged, publish, name, digest)
                    if target.exists():
                        raise StorageConflict(
                            f"Installation destination appeared: {target}."
                        )
                    publish.rename(target)
                    installed = True
            return {
                "module": reference,
                "status": "registered" if registered else "already_registered",
                "installation_path": str(target),
                "installed": installed,
            }
        except BaseException as error:
            failure = error
            if registered is not None:
                error.add_note(
                    f"Registration exists for {name}/{version}; installation={installed}. "
                    "Retry module add with the same content and a new command_id."
                )
            raise
        finally:
            await asyncio.to_thread(
                _cleanup_registration, temporary, work_root, failure
            )

    def _installation_paths(
        self, source: Path, name: str, version: str
    ) -> tuple[Path, Path]:
        """Return confined installation and workspace paths after checking links/overlap.

        Args:
            source: Resolved module source directory that must not contain the
                temporary workspace.
            name: Operation or module name used by the selected action.
            version: Module version associated with the supplied name.

        Returns:
            Confined installation and workspace paths after checking links/overlap.
        """
        target = self.module_storage_path / name / version
        for path in (target, *target.parents):
            if path.is_symlink() or path.is_junction():
                raise ValueError(
                    "Installation path must not traverse filesystem links."
                )
        if not target.resolve().is_relative_to(self.module_storage_path):
            raise ValueError("Installation path escapes module storage.")
        work_root = self.temp_folder
        if work_root == source or source in work_root.parents:
            raise ValueError(
                "Temporary folder must be outside the module source folder."
            )
        return target, work_root

    async def _prepare_install_source(
        self, source: Path, staged: Path, manifest: JsonObject, name: str
    ) -> str:
        """Copy module source, verify its manifest stayed fixed, and return its hash.

        Args:
            source: Validated module source directory.
            staged: New staging directory receiving an isolated source copy.
            manifest: Original module manifest JSON compared with the copied
                manifest.
            name: Operation or module name used by the selected action.

        Returns:
            Hash of the copied immutable source after confirming its manifest did
            not change during copying.
        """
        await asyncio.to_thread(self._copy_module_source, source, staged)
        copied = await asyncio.to_thread(read_module_manifest, staged)
        if copied != manifest:
            raise ValueError("Module manifest changed while copying the source.")
        digest = await asyncio.to_thread(self.module_hash, name, staged)
        return digest

    async def _check_existing_installation(
        self, target: Path, name: str, digest: str
    ) -> bool:
        """Return whether matching code exists, raising StorageConflict for other content.

        Args:
            target: Destination path or target record selected for this operation.
            name: Operation or module name used by the selected action.
            digest: Expected lowercase SHA-256 module digest.

        Returns:
            Whether matching code exists, raising StorageConflict for other content.
        """
        target_exists = target.exists()
        if target_exists:
            await asyncio.to_thread(read_module_manifest, target)
            if await asyncio.to_thread(self.module_hash, name, target) != digest:
                raise StorageConflict(f"A different module is installed at {target}.")
        return target_exists

    def _remove_archive_if_present(self, name: str, version: str) -> bool:
        # Filer can acknowledge DELETE with 2xx even when the entry is absent.
        """Delete a confirmed existing archive and return whether deletion occurred.

        Args:
            name: Operation or module name used by the selected action.
            version: Module version associated with the supplied name.

        Returns:
            False for confirmed absence, otherwise the archive store's deletion
            result.
        """
        if not self.module_db.check_module_stored(name, version):
            return False
        return self.module_db.delete_module(name, version)

    async def unregister_module_async(self, name: str, version: str) -> bool:
        """Remove the archive in a worker; keep SQLite on the calling thread.

        Args:
            name: Operation or module name used by the selected action.
            version: Module version associated with the supplied name.

        Returns:
            Whether either the hash record or stored archive was removed; partial
            removal failures propagate with diagnostics.
        """
        # Validate both identifiers before deleting anything remotely.
        self.hash_db.get_module_hash(name, version)
        operation = asyncio.create_task(
            asyncio.to_thread(self._remove_archive_if_present, name, version)
        )
        removed = await _await_outcome(operation)
        try:
            hash_removed = self.hash_db.remove_module_hash(name, version)
        except StorageError as error:
            if removed:
                error.add_note(
                    "Archive removed; hash removal failed. Retry module remove."
                )
            raise
        return removed or hash_removed

    def unregister_module(self, module_name: str, module_version: str) -> bool:
        """Remove the archive and hash, returning whether either was removed.

        False means both were already absent. Database errors propagate;
        an archive deletion error leaves the hash untouched. A hash deletion
        error after archive removal leaves a partial result for a later retry.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.

        Returns:
            Whether the module's hash/archive registration was removed; False for
            already absent storage.
        """
        # 1. Add silent/forced deletion attempt
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        _validate_version(module_version)
        module_removed = self.module_db.delete_module(module_name, module_version)
        try:
            hash_removed = self.hash_db.remove_module_hash(module_name, module_version)
        except StorageError as error:
            if module_removed:
                error.add_note(
                    f"Archive for module {module_name!r}, version {module_version!r}, "
                    "was removed, but hash deletion failed. Retry unregistration."
                )
            raise
        return module_removed or hash_removed

    def upload_module(
        self, module_name: str, module_version: str, compressed_module_path: str | Path
    ) -> bool:
        """Upload a regular archive file for a validated module identity.

        Args:
            module_name: Module name to store.
            module_version: Module version to store.
            compressed_module_path: Absolute archive file path.

        Returns:
            True after the archive store accepts the upload.

        Raises:
            ValueError: The archive path is relative, linked, or not a regular file.
            FileNotFoundError: The archive does not exist.
            StorageError: The archive store rejects or fails the upload.
        """
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        _validate_version(module_version)
        archive = _normalize_path(compressed_module_path)
        if not archive.is_absolute():
            raise ValueError("Archive path must be absolute.")
        if archive.is_symlink():
            raise ValueError("Archive must not be a filesystem link.")
        archive = archive.resolve(strict=True)
        if not archive.is_file():
            raise ValueError(f"Archive is not a regular file: {archive}")
        self.module_db.save_module(module_name, module_version, archive)
        return True

    def extract_module(
        self,
        module_name: str,
        module_version: str,
        temp_folder: str | Path | None = None,
    ) -> Path:
        """Download, verify and install a module, returning its absolute folder.

        The package must contain only hash.txt and one tar.xz code archive at
        its root. The existing module is replaced only after both
        the package hash and the extracted code match the database hash.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            temp_folder: Temporary directory override, or None for the manager's
                configured workspace.

        Returns:
            Published local module directory after downloaded contents match the
            registered hash.
        """
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        _validate_version(module_version)
        target, work_root = self._extraction_paths(module_name, temp_folder)
        stored_hash = self._extraction_hash(module_name, module_version)

        work_root.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="extract-module-", dir=work_root)
        with _ModuleWorkspace(temporary, "Module already installed at", target) as work:
            work_path = Path(work)
            code_folder = self._download_verified_module(
                module_name, module_version, stored_hash, work_path
            )

            self._replace_folder(target, code_folder)
        return target

    def _extraction_paths(
        self, module_name: str, temp_folder: str | Path | None
    ) -> tuple[Path, Path]:
        """Return module and temporary paths, rejecting linked or overlapping workspaces.

        Args:
            module_name: Registered module name.
            temp_folder: Temporary directory override, or None for the manager's
                configured workspace.

        Returns:
            Module and temporary paths, rejecting linked or overlapping workspaces.
        """
        try:
            target = self._validate_module_folder(module_name)
        except FileNotFoundError:
            # A first installation has no module folder to validate yet.
            target = (Path(self.module_storage_path) / module_name).resolve()

        work_root = self._get_temp_folder(temp_folder)
        if not work_root.is_absolute():
            raise ValueError("Temporary folder path must be absolute.")
        if work_root.is_symlink() or work_root.is_junction():
            raise ValueError("Temporary folder must not be a filesystem link.")
        work_root = work_root.resolve()
        if work_root == target or target in work_root.parents:
            raise ValueError("Temporary folder must be outside the module folder.")
        return target, work_root

    def _extraction_hash(self, module_name: str, module_version: str) -> str:
        """Return the lowercase registered SHA-256 hash after confirming archive presence.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.

        Returns:
            The lowercase registered SHA-256 hash after confirming archive presence.
        """
        stored_hash = self.hash_db.get_module_hash(module_name, module_version)
        if stored_hash == "":
            raise StoredObjectNotFound(
                f"No hash registered for module {module_name!r}, version {module_version!r}."
            )
        if (
            not isinstance(stored_hash, str)
            or len(stored_hash) != 64
            or any(character not in HEX_DIGITS for character in stored_hash)
        ):
            raise ValueError("The registered module hash is not a SHA-256 digest.")
        stored_hash = stored_hash.lower()
        if not self.module_db.check_module_stored(module_name, module_version):
            raise StoredObjectNotFound(
                f"No archive stored for module {module_name!r}, version {module_version!r}."
            )
        return stored_hash

    def validate_module(
        self,
        module_name: str,
        module_version: str,
        module_folder: str | Path | None = None,
        module_hash: None | str = None,
    ) -> bool:
        """Compare a supplied or computed SHA-256 digest with the registered hash.

        A supplied hash takes precedence over module_folder. Otherwise hash
        that folder, or the installed module when no folder is supplied.
        Missing registration or a mismatch returns False; storage, filesystem
        and invalid-input errors propagate to the caller.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            module_folder: Explicit source directory, or None to use the module's
                configured location.
            module_hash: Expected module content hash.

        Returns:
            True when the supplied digest or computed local hash matches the
            registered digest; False for missing registration or mismatched content.

        Raises:
            ValueError: Module selection or a supplied/stored hash is invalid.
            StorageError: Hash registration lookup fails.
            OSError: Module files cannot be inspected or hashed.
        """
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        _validate_version(module_version)
        module_folder = _optional_module_folder(module_folder)
        if module_hash is not None:
            _validate_module_hash(module_hash)

        module_stored_hash = self.hash_db.get_module_hash(module_name, module_version)
        if module_stored_hash == "":
            return False
        if (
            not isinstance(module_stored_hash, str)
            or len(module_stored_hash) != 64
            or any(character not in HEX_DIGITS for character in module_stored_hash)
        ):
            raise ValueError("The registered module hash is not a SHA-256 digest.")
        if module_hash is not None:
            return module_stored_hash.lower() == module_hash.lower()
        if module_folder is None:
            module_hash = self.module_hash(module_name)
        else:
            folder = self._validate_folder_path(module_folder)
            module_hash = self.module_hash(module_name, target_folder=folder)

        return module_stored_hash.lower() == module_hash.lower()
