import asyncio
import hashlib
import lzma
import shutil
import tarfile
import tempfile
import time
from pathlib import Path, PurePosixPath, PureWindowsPath

from core.logger_utils.events import JsonObject
from core.modulemanifest import read_module_manifest
from core.storage_contracts import HashDatabase, ModuleAddResult, ModuleDatabase
from core.storage_errors import StorageConflict, StorageError, StoredObjectNotFound
from core.validation_constants import (
    HEX_DIGITS,
    INVALID_MODULE_NAME_CHARACTERS,
    INVALID_MODULE_VERSION_CHARACTERS,
)
from utils.modulemanager_utils.modulemanager_errors import HashMismatch


class ModuleManager:
    """Coordinate module operations through caller-owned storage contracts.

    Expected storage failures use core.storage_errors. The caller creates and
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
    ) -> None:
        storage_path = self._normalize_path(module_storage_path)
        temporary_path = self._normalize_path(temp_folder)
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
            self._normalize_path(folder) for folder in (ignore_folders or set())
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

    def _normalize_path(self, path: str | Path) -> Path:
        """Convert a string path without resolving it or accessing the filesystem."""
        if not isinstance(path, (str, Path)):
            raise TypeError("Path must be a string or pathlib.Path.")
        if isinstance(path, str) and not path.strip():
            raise ValueError("Path must not be empty.")
        if "\x00" in str(path):
            raise ValueError("Path must not contain a null character.")
        return Path(path)

    def _get_temp_folder(self, temp_folder: str | Path | None) -> Path:
        return (
            self.temp_folder
            if temp_folder is None
            else self._normalize_path(temp_folder)
        )

    def _ensure_folder(self, temp_folder: str | Path | None) -> bool:
        target_path = self._get_temp_folder(temp_folder)
        return bool(target_path.exists() and target_path.is_dir())

    def _validate_folder_path(self, folder_path: str | Path) -> Path:
        """Return a resolved existing folder, rejecting relative paths and links."""
        folder = self._normalize_path(folder_path)
        if not folder.is_absolute():
            raise ValueError(f"Folder path must be absolute: {folder}")
        if folder.is_symlink() or folder.is_junction():
            raise ValueError(f"Folder must not be a filesystem link: {folder}")
        folder = folder.resolve(strict=True)
        if not folder.is_dir():
            raise NotADirectoryError(f"Path is not a folder: {folder}")
        return folder

    def _replace_folder(self, target_location: str | Path, source_location: str | Path):
        """Replace a folder with a copy of the source, retaining the source."""
        source = self._validate_folder_path(source_location)
        target = self._normalize_path(target_location)
        if not target.is_absolute():
            raise ValueError("Target path must be absolute.")
        if target.is_symlink() or target.is_junction():
            raise ValueError("Target folder must not be a filesystem link.")
        target = target.resolve()
        if target == source or target in source.parents or source in target.parents:
            raise ValueError("Source and target folders must not overlap.")
        if target.exists() and not target.is_dir():
            raise NotADirectoryError(f"Target is not a folder: {target}")
        if target in {Path(__file__).resolve().parent.parent, Path.home().resolve()}:
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

        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix=".replace-", dir=target.parent)
        work = temporary.name
        failure = None
        installed = False
        try:
            staging = Path(work) / "new"
            shutil.copytree(source, staging, symlinks=True)
            backup_folder = None
            if target.exists():
                # A failed restore must leave the backup outside automatic cleanup.
                backup_folder = Path(
                    tempfile.mkdtemp(prefix=".backup-", dir=target.parent)
                )
                try:
                    target.rename(backup_folder / "previous")
                except OSError as error:
                    try:
                        backup_folder.rmdir()
                    except OSError as cleanup_error:
                        error.add_note(
                            f"Cleanup failed at {backup_folder}: {cleanup_error}"
                        )
                    raise
            try:
                staging.rename(target)
                installed = True
            except OSError as error:
                if backup_folder is not None:
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

    def _validate_module_folder(
        self, module_name: str, *, require_exists: bool = True
    ) -> Path:
        """Validate the module location and return its resolved folder path."""
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
        """
        folder_path = self._validate_folder_path(folder_path)
        ignored_folders = set()
        for ignored_folder in self.ignore_folders if apply_ignores else ():
            ignored_path = self._normalize_path(ignored_folder)
            if not ignored_path.is_absolute():
                ignored_path = folder_path / ignored_path
            ignored_folders.add(ignored_path.resolve())

        files: list[Path] = []
        folders = [folder_path]
        while folders:
            folder = folders.pop()
            if any(
                ignored == folder or ignored in folder.parents
                for ignored in ignored_folders
            ):
                continue
            for entry in folder.iterdir():
                if entry.is_symlink() or entry.is_junction():
                    continue
                if entry.is_dir():
                    folders.append(entry)
                elif entry.is_file():
                    files.append(entry.relative_to(folder_path))
        return sorted(files, key=lambda path: path.as_posix())

    def _compress_folder(
        self,
        folder_path: str | Path,
        temp_folder: str | Path,
        *,
        apply_ignores: bool = True,
    ) -> Path:
        """Archive folder contents as tar.xz using maximum LZMA compression."""
        folder = self._validate_folder_path(folder_path)
        output = self._normalize_path(temp_folder)
        if not output.is_absolute():
            raise ValueError("Archive directory path must be absolute.")
        output = output.resolve()
        if output == folder or folder in output.parents:
            raise ValueError("Archive directory must be outside the source folder.")

        files = self._collect_module_files(folder, apply_ignores=apply_ignores)

        output.mkdir(parents=True, exist_ok=True)
        archive_path = output / f"{folder.name}.tar.xz"
        temporary = tempfile.TemporaryDirectory(prefix=".compress-", dir=output)
        work = temporary.name
        failure = None
        try:
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
                    cleanup_error.add_note(
                        f"Archive already saved at {archive_path}. {note}"
                    )
                    raise
        return archive_path

    def _uncompress_folder(
        self,
        archive_path: str | Path,
        target_path: str | Path,
        *,
        package: bool = False,
    ) -> Path:
        """Extract tar.xz into a staged folder, then replace the destination."""
        archive_path = self._normalize_path(archive_path)
        target = self._normalize_path(target_path)
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

        with tarfile.open(archive_path, "r:xz") as archive:
            members = archive.getmembers()
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
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = tempfile.TemporaryDirectory(
                prefix=".extract-", dir=target.parent
            )
            work = temporary.name
            failure = None
            try:
                staging = Path(work) / "contents"
                staging.mkdir()
                archive.extractall(staging, members=members, filter="data")
                self._replace_folder(target, staging)
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
                        cleanup_error.add_note(
                            f"Folder already installed at {target}. {note}"
                        )
                        raise
        return target

    def _add_hash_file(self, hash: str, module_temp_folder: str | Path):
        """Write a SHA-256 digest to hash.txt in the module staging folder."""
        if not isinstance(hash, str):
            raise TypeError("Module hash must be a string.")
        if len(hash) != 64 or any(character not in HEX_DIGITS for character in hash):
            raise ValueError(
                "Module hash must contain exactly 64 hexadecimal characters."
            )
        folder = self._validate_folder_path(module_temp_folder)
        temporary = tempfile.TemporaryDirectory(prefix=".hash-", dir=folder)
        work = temporary.name
        failure = None
        try:
            staging = Path(work) / "hash.txt"
            staging.write_text(hash.lower() + "\n", encoding="UTF-8", newline="\n")
            staging.replace(folder / "hash.txt")
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
                    cleanup_error.add_note(
                        f"hash.txt already saved in {folder}. {note}"
                    )
                    raise

    def module_hash(
        self, module_name: str, target_folder: str | Path | None = None
    ) -> str:
        """Hash relative file names and contents, excluding links and empty folders.

        The storage path must be absolute. Relative ignored folder paths are
        interpreted from the module folder. Files must remain unchanged while
        the hash is being computed.
        """
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        if target_folder is not None:
            target_folder = self._normalize_path(target_folder)
            if not target_folder.is_absolute():
                raise ValueError("Module folder path must be absolute.")
            if target_folder.is_symlink() or target_folder.is_junction():
                raise ValueError("Module folder must not be a filesystem link.")
        if target_folder is not None and self._ensure_folder(target_folder):
            module_folder = self._validate_folder_path(target_folder)
        else:
            module_folder = self._validate_module_folder(module_name)

        files = self._collect_module_files(module_folder)

        hasher = hashlib.sha256()
        for relative_path in files:
            encoded_path = relative_path.as_posix().encode("UTF-8")
            file_hasher = hashlib.sha256()
            with (module_folder / relative_path).open("rb") as file:
                while chunk := file.read(65536):
                    file_hasher.update(chunk)

            # Length-prefixed names and fixed-size digests delimit each file.
            hasher.update(len(encoded_path).to_bytes(8, "big"))
            hasher.update(encoded_path)
            hasher.update(file_hasher.digest())
        return hasher.hexdigest()

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
        """
        module_name, source_folder, work_root = self._registration_source(
            module_name, module_version, module_folder
        )
        manifest = read_module_manifest(source_folder)
        if (manifest["name"], manifest["version"]) != (module_name, module_version):
            raise ValueError(
                "module.yaml name/version differ from registration arguments."
            )
        module_hash = self.module_hash(module_name, source_folder)
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
                if (
                    result == ModuleAddResult.module_exists_err
                    and self._registration_exists(
                        module_name, module_version, module_hash, archive_exists
                    )
                ):
                    return False
                raise StorageConflict(
                    "The hash database did not add the module record."
                )
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
            self._cleanup_registration(temporary, work_root, failure)
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
        """
        operation = asyncio.create_task(
            self._register_module_async(module_name, module_version, module_folder)
        )
        while True:
            try:
                return await asyncio.shield(operation)
            except asyncio.CancelledError:
                if operation.cancelled():
                    raise
                # An upload may already have committed; report its actual result.
                continue

    async def _register_module_async(
        self, module_name: str, module_version: str, module_folder: str | Path | None
    ) -> bool:
        module_name, source_folder, work_root = self._registration_source(
            module_name, module_version, module_folder
        )
        manifest = await asyncio.to_thread(read_module_manifest, source_folder)
        if (manifest["name"], manifest["version"]) != (module_name, module_version):
            raise ValueError(
                "module.yaml name/version differ from registration arguments."
            )
        module_hash = await asyncio.to_thread(
            self.module_hash, module_name, source_folder
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
                if (
                    result == ModuleAddResult.module_exists_err
                    and self._registration_exists(
                        module_name, module_version, module_hash, archive_exists
                    )
                ):
                    return False
                raise StorageConflict(
                    "The hash database did not add the module record."
                )
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
                self._cleanup_registration, temporary, work_root, failure
            )
        return True

    def _registration_source(
        self, module_name: str, module_version: str, module_folder: str | Path | None
    ) -> tuple[str, Path, Path]:
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        if not isinstance(module_version, str):
            raise TypeError("module_version must be a string.")
        if not module_version.strip() or module_version.strip() in {".", ".."}:
            raise ValueError("module_version must not be empty or a dot segment.")
        if any(
            character in INVALID_MODULE_VERSION_CHARACTERS
            for character in module_version
        ):
            raise ValueError("module_version contains invalid characters.")
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
        stored_hash = self.hash_db.get_module_hash(module_name, module_version)
        if stored_hash != "" or archive_exists:
            if (
                stored_hash != ""
                and archive_exists
                and stored_hash.lower() == module_hash
            ):
                return True
            raise StorageConflict(
                f"Cannot register module {module_name!r}, version {module_version!r}: "
                "the stored hash differs or the hash/archive pair is incomplete."
            )
        return False

    def _registration_package(self, source: Path, work: Path, digest: str) -> Path:
        package = work / "package"
        self._compress_folder(source, package)
        self._add_hash_file(digest, package)
        return self._compress_folder(package, work / "archive", apply_ignores=False)

    def _rollback_registration_hash(
        self, name: str, version: str, error: StorageError
    ) -> None:
        try:
            if not self.hash_db.remove_module_hash(name, version):
                error.add_note("Hash rollback found no record to remove.")
        except StorageError as rollback_error:
            error.add_note(f"Hash rollback also failed: {rollback_error}")

    def _cleanup_registration(
        self,
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

    def _download_verified_module(
        self, name: str, version: str, digest: str, work: Path
    ) -> Path:
        """Verify the stored package into an isolated directory without installing it."""
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
        """Return a verified template reference, reading HashDB on its owning thread."""
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
        while True:
            try:
                return await asyncio.shield(operation)
            except asyncio.CancelledError:
                if operation.cancelled():
                    raise

    def _copy_module_source(self, source: Path, destination: Path) -> None:
        destination.mkdir()
        for relative in self._collect_module_files(source):
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source / relative, target)

    async def register_and_install_module_async(
        self, module_folder: Path
    ) -> JsonObject:
        """Register an arbitrary source and publish an immutable version in this project."""
        operation = asyncio.create_task(
            self._register_and_install_module(module_folder)
        )
        while True:
            try:
                return await asyncio.shield(operation)
            except asyncio.CancelledError:
                if operation.cancelled():
                    raise

    async def _register_and_install_module(self, module_folder: Path) -> JsonObject:
        manifest = await asyncio.to_thread(read_module_manifest, Path(module_folder))
        source = self._validate_folder_path(module_folder)
        name, version = manifest["name"], manifest["version"]
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
        work_root.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="install-module-", dir=work_root)
        failure = None
        registered = None
        installed = False
        try:
            staged = Path(temporary.name) / "source"
            await asyncio.to_thread(self._copy_module_source, source, staged)
            copied = await asyncio.to_thread(read_module_manifest, staged)
            if copied != manifest:
                raise ValueError("Module manifest changed while copying the source.")
            digest = await asyncio.to_thread(self.module_hash, name, staged)
            target_exists = target.exists()
            if target_exists:
                await asyncio.to_thread(read_module_manifest, target)
                if await asyncio.to_thread(self.module_hash, name, target) != digest:
                    raise StorageConflict(
                        f"A different module is installed at {target}."
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
                    await asyncio.to_thread(shutil.copytree, staged, publish)
                    if (
                        await asyncio.to_thread(self.module_hash, name, publish)
                        != digest
                    ):
                        raise HashMismatch("Installation changed during copying.")
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
                self._cleanup_registration, temporary, work_root, failure
            )

    def _remove_archive_if_present(self, name: str, version: str) -> bool:
        # Filer can acknowledge DELETE with 2xx even when the entry is absent.
        if not self.module_db.check_module_stored(name, version):
            return False
        return self.module_db.delete_module(name, version)

    async def unregister_module_async(self, name: str, version: str) -> bool:
        """Remove the archive in a worker; keep SQLite on the calling thread."""
        # Validate both identifiers before deleting anything remotely.
        self.hash_db.get_module_hash(name, version)
        operation = asyncio.create_task(
            asyncio.to_thread(self._remove_archive_if_present, name, version)
        )
        while True:
            try:
                removed = await asyncio.shield(operation)
                break
            except asyncio.CancelledError:
                if operation.cancelled():
                    raise
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
        """
        # 1. Add silent/forced deletion attempt
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        if not isinstance(module_version, str):
            raise TypeError("module_version must be a string.")
        if not module_version.strip() or module_version.strip() in {".", ".."}:
            raise ValueError("module_version must not be empty or a dot segment.")
        if any(
            character in INVALID_MODULE_VERSION_CHARACTERS
            for character in module_version
        ):
            raise ValueError("module_version contains invalid characters.")
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
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        if not isinstance(module_version, str):
            raise TypeError("module_version must be a string.")
        if not module_version.strip() or module_version.strip() in {".", ".."}:
            raise ValueError("module_version must not be empty or a dot segment.")
        if any(
            character in INVALID_MODULE_VERSION_CHARACTERS
            for character in module_version
        ):
            raise ValueError("module_version contains invalid characters.")
        archive = self._normalize_path(compressed_module_path)
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
        """
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        if not isinstance(module_version, str):
            raise TypeError("module_version must be a string.")
        if not module_version.strip() or module_version.strip() in {".", ".."}:
            raise ValueError("module_version must not be empty or a dot segment.")
        if any(
            character in INVALID_MODULE_VERSION_CHARACTERS
            for character in module_version
        ):
            raise ValueError("module_version contains invalid characters.")
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

        work_root.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(prefix="extract-module-", dir=work_root)
        work = temporary.name
        failure = None
        try:
            work_path = Path(work)
            code_folder = self._download_verified_module(
                module_name, module_version, stored_hash, work_path
            )

            self._replace_folder(target, code_folder)
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
                    cleanup_error.add_note(
                        f"Module already installed at {target}. {note}"
                    )
                    raise
        return target

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
        """
        module_name = self._validate_module_folder(
            module_name, require_exists=False
        ).name
        if not isinstance(module_version, str):
            raise TypeError("module_version must be a string.")
        if not module_version.strip() or module_version.strip() in {".", ".."}:
            raise ValueError("module_version must not be empty or a dot segment.")
        if any(
            character in INVALID_MODULE_VERSION_CHARACTERS
            for character in module_version
        ):
            raise ValueError("module_version contains invalid characters.")
        if module_folder is not None:
            module_folder = self._normalize_path(module_folder)
            if not module_folder.is_absolute():
                raise ValueError("Module folder path must be absolute.")
            if module_folder.is_symlink() or module_folder.is_junction():
                raise ValueError("Module folder must not be a filesystem link.")
        if module_hash is not None:
            if not isinstance(module_hash, str):
                raise TypeError("Module hash must be a string.")
            if len(module_hash) != 64 or any(
                character not in HEX_DIGITS for character in module_hash
            ):
                raise ValueError(
                    "Module hash must contain exactly 64 hexadecimal characters."
                )

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
