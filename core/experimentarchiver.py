"""Portable, checked experiment inputs and installation into a local project."""

from __future__ import annotations

import asyncio
import hashlib
import json
import lzma
import os
import shutil
import tarfile
import tempfile
from collections.abc import Coroutine
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from uuid import UUID, uuid4

import psutil
import yaml

from core.experimentassembler import ExperimentAssembler, find_experiment
from core.logger import OperationLogger
from core.logger_utils.events import copy_json_object, require_text
from core.modulemanager import ModuleManager
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from core.runner_utils.state import JsonObject, RunnerState
from core.storage_errors import StorageCapacityError, StorageConflict


class ExperimentArchiver:
    """Exchange code, applied templates and static resources; never start code.

    Call operations on the thread owning ModuleManager's hash database. File
    and archive-storage I/O runs in workers. The caller serializes writes from
    other instances/processes to the same module identities and destinations.
    Cancellation waits for completion and returns the actual operation result.
    """

    def __init__(
        self,
        project_root: Path,
        module_manager: ModuleManager,
        *,
        config_path: Path | None = None,
    ) -> None:
        self._project_root = self._path(project_root)
        self._manager = module_manager
        self._assembler = ExperimentAssembler(self._project_root, module_manager)
        config = self._path(
            Path(__file__).resolve().parents[1]
            / "default_settings/experiment_archiver.json"
            if config_path is None
            else config_path
        )
        settings = read_json(config)
        if settings.keys() != {
            "schema_version",
            "max_archive_bytes",
            "max_unpacked_bytes",
            "max_members",
            "max_manifest_bytes",
            "max_decompression_memory_bytes",
            "min_free_bytes",
            "compression_preset",
        }:
            raise ValueError("Invalid experiment archiver settings fields.")
        for name, value in settings.items():
            minimum = 0 if name in ("min_free_bytes", "compression_preset") else 1
            if type(value) is not int or value < minimum:
                raise ValueError(f"Invalid archiver setting: {name}")
        self._settings: dict[str, int] = {
            key: value for key, value in settings.items() if isinstance(value, int)
        }
        if (
            self._settings["schema_version"] != 1
            or self._settings["compression_preset"] > 9
        ):
            raise ValueError(
                "Unsupported archiver settings version or compression preset."
            )
        self._busy = False

    async def create(self, state: RunnerState, archive_path: Path) -> JsonObject:
        """Publish a new tar.xz from a stopped or completed registered experiment."""
        return await self._execute(self._create(state, archive_path), "create")

    async def inspect(self, archive_path: Path) -> JsonObject:
        """Fully validate in disposable storage; leave the archive and stores intact."""
        return await self._execute(self._inspect(archive_path), "inspect")

    async def install(self, archive_path: Path, destination: Path) -> JsonObject:
        """Register missing modules and publish template/resources in a new folder.

        Existing identical modules are reused. Conflicts are never overwritten.
        On failure, exception notes report completed registrations/publications;
        remote registration and local publication are not a shared transaction.
        """
        return await self._execute(self._install(archive_path, destination), "install")

    async def _execute(
        self, operation: Coroutine[None, None, JsonObject], name: str
    ) -> JsonObject:
        if self._busy:
            operation.close()
            raise RuntimeError("An archive operation is already in progress.")
        self._busy = True
        task = asyncio.create_task(self._logged_operation(operation, name))
        try:
            while True:
                try:
                    return await asyncio.shield(task)
                except asyncio.CancelledError:
                    if task.cancelled():
                        raise
                    # A cancelled worker await cannot undo publication or upload.
                    continue
        finally:
            self._busy = False

    async def _logged_operation(
        self,
        operation: Coroutine[None, None, JsonObject],
        name: str,
    ) -> JsonObject:
        logger = None
        failure = None
        config = None
        try:
            operation_id = str(uuid4())
            folder = self._path(
                self._project_root / "controller/archives" / operation_id
            )
            folder.mkdir(parents=True, exist_ok=False)
            settings = read_json(
                Path(__file__).resolve().parents[1] / "default_settings/logging.json"
            )
            settings.update(
                {
                    "db_path": str(folder / "events.sqlite"),
                    "open_mode": "create",
                    "expected_journal": None,
                }
            )
            config = folder / "logging.json"
            write_json(
                config,
                {
                    "logging": settings,
                    "operation_context": {"source": "experiment_archiver"},
                },
            )
            logger = OperationLogger(config)
            await asyncio.to_thread(logger.open)
            identity = logger.get_journal_info()
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
            config = reader_config
            await asyncio.to_thread(
                logger.record_event,
                "archive.started",
                {"operation_id": operation_id, "action": name},
            )
            try:
                result = await operation
            except Exception as error:
                try:
                    await asyncio.to_thread(
                        logger.record_error, error, include_traceback=True
                    )
                except Exception as logging_error:  # noqa: BLE001 - Preserve the operation's primary failure.
                    error.add_note(
                        f"Recording the archive failure also failed: {logging_error}"
                    )
                raise
            result["operation_id"] = operation_id
            result["logging_config_path"] = str(config)
            try:
                await asyncio.to_thread(
                    logger.record_event, "archive.completed", result
                )
            except Exception as error:
                error.add_note(
                    f"Archive operation completed before logging failed: {result}"
                )
                raise
            return result
        except BaseException as error:
            failure = error
            if config is not None:
                error.add_note(f"Archive operation logging configuration: {config}")
            raise
        finally:
            operation.close()
            if logger is not None:
                try:
                    await asyncio.to_thread(logger.close)
                except Exception as close_error:
                    if failure is None:
                        close_error.add_note(
                            "Archive operation completed before logger close failed."
                        )
                        close_error.add_note(
                            f"Archive operation logging configuration: {config}"
                        )
                        raise
                    failure.add_note(
                        f"Closing the archive logger also failed: {close_error}"
                    )

    def _objects(self, value: object, field: str) -> list[JsonObject]:
        if not isinstance(value, list):
            raise TypeError(f"{field} must be an array of objects.")
        return [copy_json_object(item, field) for item in value]

    def _path(self, value: Path) -> Path:
        if not isinstance(value, (str, Path)):
            raise TypeError("A filesystem path must be a string or Path.")
        path = Path(value)
        if not path.is_absolute() or "\x00" in str(path):
            raise ValueError("An absolute filesystem path is required.")
        for ancestor in (path, *path.parents):
            if ancestor.is_symlink() or ancestor.is_junction():
                raise ValueError(f"Filesystem links are not allowed: {ancestor}")
        return path.resolve()

    def _member(self, value: object) -> str:
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

    def _space(self, folder: Path, required: int = 0) -> None:
        if shutil.disk_usage(folder).free < required + self._settings["min_free_bytes"]:
            raise StorageCapacityError(f"Insufficient free space at {folder}.")

    def _workspace(self, parent: Path) -> tempfile.TemporaryDirectory:
        parent = self._path(parent)
        parent.mkdir(parents=True, exist_ok=True)
        self._space(parent)
        return tempfile.TemporaryDirectory(prefix="experiment-archive-", dir=parent)

    def _cleanup(
        self,
        temporary: tempfile.TemporaryDirectory,
        parent: Path,
        failure: BaseException | None,
    ) -> None:
        try:
            target = self._path(Path(temporary.name))
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

    def _assert_stopped(self, state: RunnerState) -> None:
        root = self._path(state.experiment_directory)
        if root != find_experiment(self._project_root, state.experiment_id):
            raise ValueError("Experiment identity differs from the project registry.")
        if (
            state.phase not in ("stopped", "completed")
            or state.active_attempt is not None
        ):
            raise RuntimeError(
                "Archive creation requires a stopped or completed experiment."
            )
        if (
            state.owner_identity is not None
            and state.owner_identity != process_identity(os.getpid())
        ):
            self._assert_exited(state.owner_identity)
        transaction = (
            self._project_root / "controller/restore_transactions" / f"{root.name}.json"
        )
        if (
            transaction.exists()
            and read_json(self._path(transaction)).get("phase") != "complete"
        ):
            raise RuntimeError("Resolve the restoration transaction before archiving.")
        for instance in state.services.values():
            if (
                not instance.stopped
                or instance.pending_requests
                or instance.active_request
            ):
                raise RuntimeError("Service shutdown has not been confirmed.")
            if instance.process_identity is not None:
                self._assert_exited(instance.process_identity)
        lock = self._path(root / "executor.lock.json")
        if lock.exists():
            self._assert_exited(
                copy_json_object(read_json(lock).get("executor"), "executor")
            )
        # Earlier executors can briefly own writers after publishing their result.
        attempts = root / "shared_artifacts"
        if attempts.exists():
            self._path(attempts)
            for record in attempts.glob("epoch_*/*/*/attempt_*/process.json"):
                data = read_json(self._path(record))
                for key in ("executor", "stage"):
                    if data.get(key) is not None:
                        self._assert_exited(copy_json_object(data[key], key))

    def _assert_exited(self, identity: JsonObject) -> None:
        if identity.keys() != {"pid", "created_at_os", "host_id", "boot_id"}:
            raise ValueError("Incomplete process identity.")
        pid = identity["pid"]
        if type(pid) is not int or pid <= 0:
            raise ValueError("Invalid process PID.")
        created = identity["created_at_os"]
        if type(created) is not int or created <= 0:
            raise ValueError("Invalid process creation identity.")
        require_text(identity["host_id"], "process host_id")
        require_text(identity["boot_id"], "process boot_id")
        local = process_identity(os.getpid())
        if identity["host_id"] != local["host_id"]:
            raise RuntimeError("Cannot confirm shutdown of a process on another host.")
        if identity["boot_id"] != local["boot_id"]:
            return
        try:
            process = psutil.Process(pid)
            if process_identity(pid) != identity:
                return
            process.wait(timeout=0)
        except (psutil.NoSuchProcess, ProcessLookupError, FileNotFoundError):
            return
        except psutil.TimeoutExpired as error:
            raise RuntimeError(f"Experiment process is still alive: {pid}") from error

    def _inventory(self, root: Path) -> tuple[list[str], JsonObject]:
        root = self._path(root)
        directories: list[str] = []
        files: JsonObject = {}
        names: set[str] = set()
        pending = [root]
        total = 0
        while pending:
            folder = pending.pop()
            for entry in sorted(folder.iterdir()):
                self._path(entry)
                name = self._member(entry.relative_to(root).as_posix())
                if name.casefold() in names:
                    raise ValueError("Archive member names collide ignoring case.")
                names.add(name.casefold())
                if len(names) > self._settings["max_members"]:
                    raise StorageCapacityError("Too many archive members.")
                if entry.is_dir():
                    directories.append(name)
                    pending.append(entry)
                elif entry.is_file():
                    size = entry.stat().st_size
                    total += size
                    if total > self._settings["max_unpacked_bytes"]:
                        raise StorageCapacityError(
                            "Unpacked archive size exceeds its limit."
                        )
                    with entry.open("rb") as stream:
                        digest = hashlib.file_digest(stream, "sha256").hexdigest()
                        if os.fstat(stream.fileno()).st_size != size:
                            raise ValueError("A source file changed during archiving.")
                    files[name] = {"size": size, "sha256": digest}
                else:
                    raise ValueError(f"Archive source is not a regular file: {entry}")
        return sorted(directories), files

    def _copy(self, source: Path, target: Path) -> None:
        source = self._path(source)
        target = self._path(target)
        if source.is_dir():
            directories, files = self._inventory(source)
            target.mkdir(parents=True, exist_ok=False)
            for name in directories:
                (target / name).mkdir(parents=True, exist_ok=True)
            for name in files:
                self._space(target, (source / name).stat().st_size)
                shutil.copy2(self._path(source / name), target / name)
            if self._inventory(target) != (directories, files):
                raise ValueError(
                    "Source files changed while copying the archive payload."
                )
        elif source.is_file():
            if source.stat().st_size > self._settings["max_unpacked_bytes"]:
                raise StorageCapacityError(
                    "Source file exceeds the unpacked size limit."
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            self._space(target.parent, source.stat().st_size)
            with source.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            shutil.copy2(source, target)
            with target.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
                    raise ValueError(
                        "Source file changed while copying the archive payload."
                    )
        else:
            raise FileNotFoundError(source)

    def _modules(self, template: JsonObject) -> list[dict[str, str]]:
        modules: dict[tuple[str, str], dict[str, str]] = {}
        for role in ("stage", "service"):
            for definition in self._objects(template[f"{role}s"], role):
                if role == "stage" and "service_id" in definition:
                    continue
                module = copy_json_object(definition["module"], "module")
                name = self._member(module["name"])
                version = self._member(module["version"])
                if "/" in name or "/" in version:
                    raise ValueError(
                        "Module identities must be single path components."
                    )
                item = {
                    "name": name,
                    "version": version,
                    "hash": require_text(module["hash"], "module.hash").lower(),
                    "role": role,
                }
                key = (name, version)
                if key in modules and modules[key] != item:
                    raise ValueError(
                        "Conflicting definitions of the same module version."
                    )
                modules[key] = item
        return [modules[key] for key in sorted(modules)]

    async def _create(self, state: RunnerState, archive_path: Path) -> JsonObject:
        archive = self._path(archive_path)
        root = self._path(state.experiment_directory)
        if archive.exists():
            raise FileExistsError(archive)
        if archive.is_relative_to(root):
            raise ValueError(
                "Archive destination must be outside the source experiment."
            )
        await asyncio.to_thread(self._assert_stopped, state)
        _, template = self._assembler.load_template(
            state.template_path, template_yaml=state.template_yaml
        )
        if template != state.template:
            raise ValueError(
                "Applied template differs from the runner's normalized template."
            )
        modules = self._modules(template)
        # SQLite hash lookups must stay on their owner thread.
        for module in modules:
            registered = self._manager.hash_db.get_module_hash(
                module["name"], module["version"]
            )
            if registered.lower() != module["hash"]:
                raise ValueError("A module hash differs from the registered hash.")
        temporary = self._workspace(archive.parent)
        work = Path(temporary.name)
        failure = None
        try:
            payload = work / "payload"
            payload.mkdir()
            for module in modules:
                relative = Path("modules") / module["name"] / module["version"]
                await asyncio.to_thread(self._copy, root / relative, payload / relative)
            resources = self._objects(template["resources"], "resources")
            for resource in resources:
                name = self._member(resource["name"])
                await asyncio.to_thread(
                    self._copy,
                    root / "shared_data/resources" / name,
                    payload / "resources" / name,
                )
                resource["path"] = f"resources/{name}"
            template["resources"] = copy_json_object({"items": resources}, "resources")[
                "items"
            ]
            (payload / "experiment.yaml").write_text(
                yaml.safe_dump(template, allow_unicode=True, sort_keys=False),
                encoding="utf-8",
            )
            directories, files = await asyncio.to_thread(self._inventory, payload)
            manifest = copy_json_object(
                {
                    "schema_version": 2,
                    "archive_id": str(uuid4()),
                    "created_at": datetime.now(UTC).isoformat(),
                    "source_experiment_id": state.experiment_id,
                    "template": "experiment.yaml",
                    "modules": modules,
                    "directories": directories,
                    "files": files,
                },
                "archive manifest",
            )
            await asyncio.to_thread(self._validate_payload, payload, manifest)
            encoded = json.dumps(manifest, ensure_ascii=False, allow_nan=False).encode(
                "utf-8"
            )
            if len(encoded) > self._settings["max_manifest_bytes"]:
                raise StorageCapacityError("Archive manifest exceeds its size limit.")
            (payload / "manifest.json").write_bytes(encoded)
            packed = work / "archive.tar.xz"
            await asyncio.to_thread(self._pack, payload, packed)
            await asyncio.to_thread(self._assert_stopped, state)
            # A same-volume hard link publishes the complete file without replacement.
            self._path(archive)
            os.link(packed, archive)
            return {"archive_path": str(archive), "manifest": manifest}
        except BaseException as error:
            failure = error
            raise
        finally:
            await asyncio.to_thread(self._cleanup, temporary, archive.parent, failure)

    def _pack(self, payload: Path, archive: Path) -> None:
        directories, files = self._inventory(payload)
        if len(directories) + len(files) > self._settings["max_members"]:
            raise StorageCapacityError("Too many archive members including manifest.")
        unpacked = archive.with_name("uncompressed.tar")
        with tarfile.open(
            unpacked,
            "x",
            format=tarfile.USTAR_FORMAT,
            dereference=True,
            encoding="utf-8",
            errors="strict",
        ) as stream:
            for name in [*directories, *files]:
                source = self._path(payload / name)
                # Reserve padding as well as data before TarFile copies a member.
                self._space(
                    archive.parent,
                    (source.stat().st_size if source.is_file() else 0) + 10240,
                )
                stream.add(source, arcname=name, recursive=False)
        compressor = lzma.LZMACompressor(preset=self._settings["compression_preset"])
        written = 0
        with unpacked.open("rb") as source, archive.open("xb") as destination:
            while True:
                block = source.read(1024 * 1024)
                data = compressor.compress(block) if block else compressor.flush()
                written += len(data)
                if written > self._settings["max_archive_bytes"]:
                    raise StorageCapacityError(
                        "Compressed archive size exceeds its limit."
                    )
                self._space(archive.parent, len(data))
                destination.write(data)
                if not block:
                    break
        unpacked.unlink()

    def _decompress(self, archive: Path, target: Path) -> None:
        """Bound both decoder memory and disk use before parsing any tar metadata."""
        decoder = lzma.LZMADecompressor(
            format=lzma.FORMAT_XZ,
            memlimit=self._settings["max_decompression_memory_bytes"],
        )
        limit = (
            self._settings["max_unpacked_bytes"]
            + self._settings["max_members"] * 1024
            + 10240
        )
        compressed = unpacked = 0
        with archive.open("rb") as source, target.open("xb") as destination:
            while not decoder.eof:
                block = source.read(1024 * 1024) if decoder.needs_input else b""
                if decoder.needs_input and not block:
                    raise ValueError("Truncated XZ stream.")
                compressed += len(block)
                if compressed > self._settings["max_archive_bytes"]:
                    raise StorageCapacityError(
                        "Compressed archive size exceeds its limit."
                    )
                data = decoder.decompress(block, max_length=1024 * 1024)
                unpacked += len(data)
                if unpacked > limit:
                    raise StorageCapacityError(
                        "Decompressed tar stream exceeds its limit."
                    )
                self._space(target.parent, len(data))
                destination.write(data)
            if decoder.unused_data or source.read(1):
                raise ValueError("Trailing data or multiple XZ streams are forbidden.")

    def _unpack(self, archive_path: Path, target: Path) -> JsonObject:
        archive = self._path(archive_path)
        if not archive.is_file():
            raise FileNotFoundError(archive)
        if archive.stat().st_size > self._settings["max_archive_bytes"]:
            raise StorageCapacityError("Compressed archive size exceeds its limit.")
        target.mkdir()
        unpacked_tar = target.parent / "archive.tar"
        self._decompress(archive, unpacked_tar)
        names: set[str] = set()
        total = 0
        # Parse fixed USTAR headers before reading data. TarFile's automatic
        # PAX/GNU processing would allocate metadata before our size checks.
        with unpacked_tar.open("rb") as stream:
            while True:
                header = stream.read(512)
                if len(header) != 512:
                    raise ValueError("Truncated archive header.")
                if header == bytes(512):
                    if stream.read(512) != bytes(512):
                        raise ValueError("Invalid archive terminator.")
                    padding = 0
                    while block := stream.read(1024 * 1024):
                        padding += len(block)
                        if any(block) or padding > 10240:
                            raise ValueError(
                                "Unexpected data after the archive terminator."
                            )
                    break
                member = tarfile.TarInfo.frombuf(header, "utf-8", "strict")
                name = self._member(member.name)
                if name.casefold() in names:
                    raise ValueError("Duplicate or case-colliding archive member.")
                names.add(name.casefold())
                if len(names) > self._settings["max_members"]:
                    raise StorageCapacityError("Too many archive members.")
                if header[257:265] != b"ustar\x0000" or member.type not in (
                    tarfile.REGTYPE,
                    tarfile.AREGTYPE,
                    tarfile.DIRTYPE,
                ):
                    raise ValueError(
                        "Archive links, sparse and special files are forbidden."
                    )
                if member.size < 0 or (member.isdir() and member.size != 0):
                    raise ValueError("Invalid archive member size.")
                total += member.size
                if total > self._settings["max_unpacked_bytes"]:
                    raise StorageCapacityError(
                        "Unpacked archive size exceeds its limit."
                    )
                if (
                    name == "manifest.json"
                    and member.size > self._settings["max_manifest_bytes"]
                ):
                    raise StorageCapacityError(
                        "Archive manifest exceeds its size limit."
                    )
                output = target / name
                self._path(output)
                self._space(target, member.size)
                if member.isdir():
                    output.mkdir(parents=True, exist_ok=True)
                else:
                    output.parent.mkdir(parents=True, exist_ok=True)
                    with output.open("xb") as destination:
                        remaining = member.size
                        while remaining:
                            block = stream.read(min(1024 * 1024, remaining))
                            if not block:
                                raise ValueError("Truncated archive member.")
                            self._space(target, len(block))
                            destination.write(block)
                            remaining -= len(block)
                    padding_size = (-member.size) % 512
                    if stream.read(padding_size) != bytes(padding_size):
                        raise ValueError("Invalid archive file padding.")
                    # Preserve executability without granting special permissions.
                    output.chmod((member.mode & 0o111) | 0o600)
        manifest = read_json(target / "manifest.json")
        expected = self._validate_payload(target, manifest) | {"manifest.json"}
        if {name.casefold() for name in expected} != names:
            raise ValueError("Archive entries differ from its manifest inventory.")
        unpacked_tar.unlink()
        return manifest

    def _validate_payload(self, payload: Path, manifest: JsonObject) -> set[str]:
        if (
            manifest.keys()
            != {
                "schema_version",
                "archive_id",
                "created_at",
                "source_experiment_id",
                "template",
                "modules",
                "directories",
                "files",
            }
            or type(manifest["schema_version"]) is not int
            or manifest["schema_version"] != 2
        ):
            raise ValueError("Unsupported experiment archive manifest.")
        UUID(require_text(manifest["archive_id"], "archive_id"))
        require_text(manifest["source_experiment_id"], "source_experiment_id")
        if datetime.fromisoformat(
            require_text(manifest["created_at"], "created_at")
        ).utcoffset() != UTC.utcoffset(None):
            raise ValueError("Archive creation time must use UTC.")
        if manifest["template"] != "experiment.yaml":
            raise ValueError("The archive template must be experiment.yaml.")
        directories = manifest["directories"]
        files = copy_json_object(manifest["files"], "archive files")
        if not isinstance(directories, list) or any(
            not isinstance(item, str) for item in directories
        ):
            raise ValueError("Archive directories must be an array of paths.")
        directories = [require_text(item, "directory") for item in directories]
        names = [self._member(name) for name in [*directories, *files]]
        if "manifest.json" in names or len({name.casefold() for name in names}) != len(
            names
        ):
            raise ValueError("Archive manifest contains colliding or reserved paths.")
        for info in files.values():
            info = copy_json_object(info, "file integrity")
            if (
                info.keys() != {"size", "sha256"}
                or type(info["size"]) is not int
                or info["size"] < 0
            ):
                raise ValueError("Invalid file integrity record.")
            digest = require_text(info["sha256"], "file sha256")
            if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("Invalid file SHA-256.")
        actual_dirs, actual_files = self._inventory(payload)
        actual_files.pop("manifest.json", None)
        if sorted(directories) != actual_dirs or files != actual_files:
            raise ValueError("Archive inventory, file size or SHA-256 differs.")
        template_path = payload / "experiment.yaml"
        raw_template = copy_json_object(
            yaml.safe_load(template_path.read_text(encoding="utf-8")), "template"
        )
        _, template = self._assembler.load_template(template_path)
        modules = self._modules(template)
        if manifest["modules"] != modules:
            raise ValueError("Archive modules differ from the applied template.")
        allowed_files = {"experiment.yaml"}
        roots: list[str] = []
        for module in modules:
            relative = f"modules/{module['name']}/{module['version']}"
            folder = payload / relative
            definition = self._assembler.read_module(folder)
            if any(
                definition[key] != module[key] for key in ("name", "version", "role")
            ):
                raise ValueError(
                    "module.yaml identity or role differs from the template."
                )
            if self._manager.module_hash(module["name"], folder) != module["hash"]:
                raise ValueError("Module content differs from its expected hash.")
            roots.append(relative)
        for resource in self._objects(raw_template["resources"], "resources"):
            name = self._member(resource["name"])
            relative = f"resources/{name}"
            if resource["path"] != relative:
                raise ValueError(
                    "Archive resources must use portable template-relative paths."
                )
            source = payload / relative
            if not source.exists():
                raise FileNotFoundError(source)
            if resource["hash"] is not None:
                if source.is_dir():
                    digest = self._manager.module_hash(name, source)
                else:
                    with source.open("rb") as stream:
                        digest = hashlib.file_digest(stream, "sha256").hexdigest()
                if digest != require_text(resource["hash"], "resource hash").lower():
                    raise ValueError("Static resource differs from its expected hash.")
            roots.append(relative)
        for name in names:
            if name in allowed_files:
                continue
            if not any(
                name == root
                or name.startswith(root + "/")
                or (name in directories and root.startswith(name + "/"))
                for root in roots
            ):
                raise ValueError(f"Unexpected archive payload: {name}")
        return set(names)

    async def _inspect(self, archive_path: Path) -> JsonObject:
        parent = self._path(self._project_root / "controller/archive_work")
        temporary = self._workspace(parent)
        failure = None
        try:
            manifest = await asyncio.to_thread(
                self._unpack, archive_path, Path(temporary.name) / "payload"
            )
            return {"archive_path": str(self._path(archive_path)), "manifest": manifest}
        except BaseException as error:
            failure = error
            raise
        finally:
            await asyncio.to_thread(self._cleanup, temporary, parent, failure)

    async def _install(self, archive_path: Path, destination: Path) -> JsonObject:
        destination = self._path(destination)
        if destination.exists():
            raise FileExistsError(destination)
        modules_root = self._path(self._project_root / "modules")
        for reserved in (
            modules_root,
            self._path(Path(self._manager.module_storage_path)),
            self._path(Path(self._manager.temp_folder)),
            self._project_root / "experiments",
            self._project_root / "controller",
            self._project_root / "snapshots",
        ):
            if destination.is_relative_to(reserved) or reserved.is_relative_to(
                destination
            ):
                raise ValueError(
                    "Installation destination overlaps project runtime storage."
                )
        temporary = self._workspace(destination.parent)
        work = Path(temporary.name)
        failure = None
        completed: list[JsonObject] = []
        try:
            payload = work / "payload"
            manifest = await asyncio.to_thread(self._unpack, archive_path, payload)
            # Reject known conflicts across the whole bundle before the first write.
            modules = [
                {key: require_text(value, key) for key, value in item.items()}
                for item in self._objects(manifest["modules"], "modules")
            ]
            for module in modules:
                name, version, digest = (
                    module["name"],
                    module["version"],
                    module["hash"],
                )
                stored = self._manager.hash_db.get_module_hash(name, version)
                exists = await asyncio.to_thread(
                    self._manager.module_db.check_module_stored, name, version
                )
                if (stored or exists) and (stored.lower() != digest or not exists):
                    raise StorageConflict(
                        f"Stored module conflicts with archive: {name}/{version}"
                    )
                target = self._path(modules_root / name / version)
                if target.exists():
                    if not target.is_dir():
                        raise StorageConflict(
                            f"Module destination is not a directory: {target}"
                        )
                    await asyncio.to_thread(self._inventory, target)
                    actual = await asyncio.to_thread(
                        self._manager.module_hash, name, target
                    )
                    if actual != digest:
                        raise StorageConflict(
                            f"Local module conflicts with archive: {name}/{version}"
                        )
            for module in modules:
                name, version = module["name"], module["version"]
                source = payload / "modules" / name / version
                registered = await self._manager.register_module_async(
                    name, version, source
                )
                entry: JsonObject = {
                    "name": name,
                    "version": version,
                    "registered": registered,
                    "installed": False,
                }
                completed.append(entry)
                target = self._path(modules_root / name / version)
                if not target.exists():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    local = self._workspace(target.parent)
                    local_failure = None
                    try:
                        staged = Path(local.name) / "module"
                        await asyncio.to_thread(self._copy, source, staged)
                        if (
                            await asyncio.to_thread(
                                self._manager.module_hash, name, staged
                            )
                            != module["hash"]
                        ):
                            raise ValueError(
                                "Installed module hash differs after copying."
                            )
                        if self._path(target).exists():
                            raise StorageConflict(
                                f"Module destination appeared during installation: {target}"
                            )
                        staged.rename(target)
                        entry["installed"] = True
                    except BaseException as error:
                        local_failure = error
                        raise
                    finally:
                        await asyncio.to_thread(
                            self._cleanup, local, target.parent, local_failure
                        )
            bundle = work / "installation"
            bundle.mkdir()
            await asyncio.to_thread(
                self._copy, payload / "experiment.yaml", bundle / "experiment.yaml"
            )
            if (payload / "resources").exists():
                await asyncio.to_thread(
                    self._copy, payload / "resources", bundle / "resources"
                )
            receipt = copy_json_object(
                {"archive_id": manifest["archive_id"], "modules": completed},
                "installation receipt",
            )
            write_json(bundle / "installation.json", receipt)
            if self._path(destination).exists():
                raise FileExistsError(destination)
            bundle.rename(destination)
            return {
                **receipt,
                "destination": str(destination),
                "template_path": str(destination / "experiment.yaml"),
            }
        except BaseException as error:
            failure = error
            error.add_note(
                f"Completed archive installation steps: {json.dumps(completed, ensure_ascii=False)}"
            )
            error.add_note(
                "An interrupted remote upload may have committed; inspect storage before retrying."
            )
            raise
        finally:
            await asyncio.to_thread(
                self._cleanup, temporary, destination.parent, failure
            )
