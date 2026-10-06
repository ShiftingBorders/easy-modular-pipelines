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
from pathlib import Path
from uuid import uuid4

import psutil
import yaml

from core.experiments.archive_inputs import (
    _cleanup,
    _member,
    _modules,
    _path,
    _reader_config,
    _validate_member_header,
)
from core.experiments.assembler import ExperimentAssembler, find_experiment
from core.experiments.state import RunnerState
from core.journal.logger import OperationLogger
from core.models.archive_documents import ArchiveFile, ArchiveManifest, ArchiveModule
from core.models.archive_settings import ArchiveConfiguration
from core.models.experiment_template import ExperimentTemplate, ResourceDefinition
from core.models.journal_records import JournalIdentity
from core.models.journal_settings import LoggingConfiguration
from core.models.module_manifest import ModuleManifest
from core.models.updates import _update_model
from core.modules.manager import ModuleManager
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import JsonObject, copy_json_object, require_text
from core.primitives.paths import repository_root
from core.primitives.processes import process_identity
from core.primitives.tasks import _await_outcome
from core.storage.errors import StorageCapacityError, StorageConflict


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
        """Bind project storage and read archive limits without starting module code.

        Args:
            project_root: Absolute project directory.
            module_manager: Caller-owned manager used on its hash database's owner thread.
            config_path: Absolute archive settings path, or None for repository defaults.
        """
        self._project_root = _path(project_root)
        self._manager = module_manager
        self._assembler = ExperimentAssembler(self._project_root, module_manager)
        config = _path(
            repository_root() / "default_settings/experiment_archiver.json"
            if config_path is None
            else config_path
        )
        self._settings = ArchiveConfiguration.model_validate(read_json(config))
        self._busy = False

    async def create(self, state: RunnerState, archive_path: Path) -> JsonObject:
        """Publish a new tar.xz from a stopped or completed registered experiment.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            archive_path: Absolute archive file path on the server filesystem.

        Returns:
            Archive path and validated manifest, extended with the independent
            archive operation ID and logger configuration path.
        """
        return await self._execute(self._create(state, archive_path), "create")

    async def inspect(self, archive_path: Path) -> JsonObject:
        """Fully validate in disposable storage; leave the archive and stores intact.

        Args:
            archive_path: Absolute archive file path on the server filesystem.

        Returns:
            Validated archive manifest and source path, plus operation ID and logger
            configuration.
        """
        return await self._execute(self._inspect(archive_path), "inspect")

    async def install(self, archive_path: Path, destination: Path) -> JsonObject:
        """Register missing modules and publish template/resources in a new folder.

        Existing identical modules are reused. Conflicts are never overwritten.
        On failure, exception notes report completed registrations/publications;
        remote registration and local publication are not a shared transaction.

        Args:
            archive_path: Absolute archive file path on the server filesystem.
            destination: Absolute output path for the requested file or directory.

        Returns:
            Installation receipt listing completed module steps, destination,
            template path, and operation journal location.
        """
        return await self._execute(self._install(archive_path, destination), "install")

    async def _execute(
        self, operation: Coroutine[None, None, JsonObject], name: str
    ) -> JsonObject:
        """Serialize one archive operation and await its actual outcome despite cancellation.

        Args:
            operation: Unstarted archive coroutine executed under exclusive
                maintenance and audit.
            name: Archive action name included in the independent operation journal.

        Returns:
            Logged operation result after the actual action completes, even if the
            caller was cancelled.
        """
        if self._busy:
            operation.close()
            raise RuntimeError("An archive operation is already in progress.")
        self._busy = True
        task = asyncio.create_task(self._logged_operation(operation, name))
        try:
            return await _await_outcome(task)
        finally:
            self._busy = False

    async def _logged_operation(
        self,
        operation: Coroutine[None, None, JsonObject],
        name: str,
    ) -> JsonObject:
        """Run an archive action with a dedicated journal and annotate cleanup failures.

        Creates a separate controller archive journal and records start/completion.
        A logging failure after the action can occur after files or registrations
        have already been published; exception notes retain that outcome and journal
        location. The coroutine and logger are closed on every exit path.

        Args:
            operation: Unstarted archive coroutine executed under exclusive
                maintenance and audit.
            name: Archive action name included in the independent operation journal.

        Returns:
            The operation's result with operation_id and logging_config_path added.
        """
        logger = None
        failure = None
        config = None
        try:
            operation_id = str(uuid4())
            folder = _path(self._project_root / "controller/archives" / operation_id)
            folder.mkdir(parents=True, exist_ok=False)
            settings = LoggingConfiguration.model_validate(
                {
                    **read_json(repository_root() / "default_settings/logging.json"),
                    "db_path": str(folder / "events.sqlite"),
                    "open_mode": "create",
                    "expected_journal": None,
                }
            )
            config = folder / "logging.json"
            write_json(
                config,
                {
                    "logging": settings.model_dump(mode="json"),
                    "operation_context": {"source": "experiment_archiver"},
                },
            )
            logger = OperationLogger(config)
            await asyncio.to_thread(logger.open)
            info = logger.get_journal_info()
            identity = JournalIdentity.model_validate(
                {"journal_id": info["journal_id"], "generation": info["generation"]}
            )
            reader_config = _reader_config(settings, identity, folder)
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

    def _space(self, folder: Path, required: int = 0) -> None:
        """Require free bytes for the operation plus the configured storage reserve.

        Args:
            folder: Directory on the filesystem whose available capacity is checked.
            required: Additional operation bytes that must fit beyond the configured
                reserve.
        """
        if shutil.disk_usage(folder).free < required + self._settings.min_free_bytes:
            raise StorageCapacityError(f"Insufficient free space at {folder}.")

    def _workspace(self, parent: Path) -> tempfile.TemporaryDirectory:
        """Create an owned temporary workspace beneath a checked parent with free space."""
        parent = _path(parent)
        parent.mkdir(parents=True, exist_ok=True)
        self._space(parent)
        return tempfile.TemporaryDirectory(prefix="experiment-archive-", dir=parent)

    def _assert_stopped(self, state: RunnerState) -> None:
        """Verify registered experiment state and confirm all known writer processes exited.

        Requires terminal runner state, no active attempt or unresolved service
        requests, and no incomplete restoration. Saved owner, service, stage, and
        executor identities must not still identify live writers.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.

        Raises:
            RuntimeError: Experiment state, unfinished restoration, unresolved
                service work, or a live writer prevents archiving.
            ValueError: Registered experiment identity or recorded process metadata
                is invalid.
        """
        root = _path(state.experiment_directory)
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
            and state.owner_identity.model_dump() != process_identity(os.getpid())
        ):
            self._assert_exited(state.owner_identity.model_dump())
        transaction = (
            self._project_root / "controller/restore_transactions" / f"{root.name}.json"
        )
        if (
            transaction.exists()
            and read_json(_path(transaction)).get("phase") != "complete"
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
                self._assert_exited(instance.process_identity.model_dump())
        lock = _path(root / "executor.lock.json")
        if lock.exists():
            self._assert_exited(
                copy_json_object(read_json(lock).get("executor"), "executor")
            )
        # Earlier executors can briefly own writers after publishing their result.
        attempts = root / "shared_artifacts"
        if attempts.exists():
            _path(attempts)
            for record in attempts.glob("epoch_*/*/*/attempt_*/process.json"):
                data = read_json(_path(record))
                for key in ("executor", "stage"):
                    if data.get(key) is not None:
                        self._assert_exited(copy_json_object(data[key], key))

    def _assert_exited(self, identity: JsonObject) -> None:
        """Confirm a complete local process identity has exited or belongs to an old boot.

        Args:
            identity: Expected complete process identity used to detect PID reuse.

        Raises:
            ValueError: The process identity is incomplete or invalid.
            RuntimeError: The same process is still alive or belongs to another host
                whose shutdown cannot be confirmed.
        """
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

    def _inventory(self, root: Path) -> tuple[list[str], dict[str, ArchiveFile]]:
        """Inventory portable directories and hashed files within member and byte limits.

        Args:
            root: Absolute payload tree whose member paths and file contents are
                inventoried.

        Returns:
            Sorted portable directory names and a mapping of file names to byte
            sizes and SHA-256 digests.

        Raises:
            ValueError: A path is unsafe, case-colliding, linked, or not a regular
                file/directory.
            StorageCapacityError: The member or total unpacked-byte limit is
                exceeded.
        """
        root = _path(root)
        directories: list[str] = []
        files: dict[str, ArchiveFile] = {}
        names: set[str] = set()
        pending = [root]
        total = 0
        while pending:
            folder = pending.pop()
            for entry in sorted(folder.iterdir()):
                _path(entry)
                name = _member(entry.relative_to(root).as_posix())
                if name.casefold() in names:
                    raise ValueError("Archive member names collide ignoring case.")
                names.add(name.casefold())
                if len(names) > self._settings.max_members:
                    raise StorageCapacityError("Too many archive members.")
                if entry.is_dir():
                    directories.append(name)
                    pending.append(entry)
                elif entry.is_file():
                    total, files[name] = self._file_inventory(entry, total)
                else:
                    raise ValueError(f"Archive source is not a regular file: {entry}")
        return sorted(directories), files

    def _file_inventory(self, entry: Path, total: int) -> tuple[int, ArchiveFile]:
        """Return updated byte total and file size/digest, rejecting oversized or changing input.

        Args:
            entry: Regular payload file whose size and SHA-256 are being recorded.
            total: Total uncompressed file bytes counted before this file.

        Returns:
            Updated byte total and file size/digest, rejecting oversized or changing
            input.
        """
        size = entry.stat().st_size
        total += size
        if total > self._settings.max_unpacked_bytes:
            raise StorageCapacityError("Unpacked archive size exceeds its limit.")
        with entry.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if os.fstat(stream.fileno()).st_size != size:
                raise ValueError("A source file changed during archiving.")
        return total, ArchiveFile(size=size, sha256=digest)

    def _copy(self, source: Path, target: Path) -> None:
        """Copy a checked regular file or directory and reject unavailable source paths.

        Args:
            source: Source path or input record selected for this operation.
            target: Destination path or target record selected for this operation.
        """
        source = _path(source)
        target = _path(target)
        if source.is_dir():
            self._copy_directory(source, target)
        elif source.is_file():
            self._copy_file(source, target)
        else:
            raise FileNotFoundError(source)

    def _copy_directory(self, source: Path, target: Path) -> None:
        """Copy an inventoried tree with space checks and compare the resulting inventory.

        Args:
            source: Source path or input record selected for this operation.
            target: Destination path or target record selected for this operation.
        """
        directories, files = self._inventory(source)
        target.mkdir(parents=True, exist_ok=False)
        for name in directories:
            (target / name).mkdir(parents=True, exist_ok=True)
        for name in files:
            self._space(target, (source / name).stat().st_size)
            shutil.copy2(_path(source / name), target / name)
        if self._inventory(target) != (directories, files):
            raise ValueError("Source files changed while copying the archive payload.")

    def _copy_file(self, source: Path, target: Path) -> None:
        """Copy a file within size/space limits and verify its SHA-256 stayed unchanged.

        Args:
            source: Regular source file whose size/hash is captured before copying.
            target: Destination file under the owned payload workspace.
        """
        if source.stat().st_size > self._settings.max_unpacked_bytes:
            raise StorageCapacityError("Source file exceeds the unpacked size limit.")
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

    async def _create(self, state: RunnerState, archive_path: Path) -> JsonObject:
        """Package a stopped experiment and publish a complete archive without replacement.

        Args:
            state: Stopped/completed experiment state with confirmed process shutdown.
            archive_path: Absolute new archive path outside the source experiment.

        Returns:
            Published archive path and manifest.

        Raises:
            FileExistsError: The destination exists.
            RuntimeError: Shutdown or restoration state prevents archiving.
            ValueError: Applied template, registered hashes, or payload contents disagree.
        """
        archive = _path(archive_path)
        root = _path(state.experiment_directory)
        if archive.exists():
            raise FileExistsError(archive)
        if archive.is_relative_to(root):
            raise ValueError(
                "Archive destination must be outside the source experiment."
            )
        await asyncio.to_thread(self._assert_stopped, state)
        _, document = self._assembler.load_template(
            state.template_path, template_yaml=state.template_yaml
        )
        if document != state.template.model_dump(exclude_unset=True):
            raise ValueError(
                "Applied template differs from the runner's normalized template."
            )
        template = ExperimentTemplate.model_validate(document)
        modules = _modules(template)
        # SQLite hash lookups must stay on their owner thread.
        for module in modules:
            registered = self._manager.hash_db.get_module_hash(
                module.name, module.version
            )
            if registered.lower() != module.hash:
                raise ValueError("A module hash differs from the registered hash.")
        temporary = self._workspace(archive.parent)
        work = Path(temporary.name)
        failure = None
        try:
            payload, manifest = await self._prepare_payload(
                state, root, work, template, modules
            )
            packed = work / "archive.tar.xz"
            await asyncio.to_thread(self._pack, payload, packed)
            await asyncio.to_thread(self._assert_stopped, state)
            # A same-volume hard link publishes the complete file without replacement.
            _path(archive)
            os.link(packed, archive)
            return {"archive_path": str(archive), "manifest": manifest.model_dump()}
        except BaseException as error:
            failure = error
            raise
        finally:
            await asyncio.to_thread(_cleanup, temporary, archive.parent, failure)

    async def _prepare_payload(
        self,
        state: RunnerState,
        root: Path,
        work: Path,
        template: ExperimentTemplate,
        modules: list[ArchiveModule],
    ) -> tuple[Path, ArchiveManifest]:
        """Stage modules/resources and a portable template, validate them, and write the manifest.

        Copies immutable module code and static resources, rewrites resource paths
        relative to the portable template, then verifies all contents before writing
        manifest.json.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            root: Absolute source experiment directory.
            work: Owned temporary workspace for downloaded or prepared files.
            template: Validated experiment template supplying definitions and
                policies.
            modules: Validated module references included in the archive.

        Returns:
            Staged payload directory and its validated archive manifest.
        """
        payload = work / "payload"
        payload.mkdir()
        for module in modules:
            relative = Path("modules") / module.name / module.version
            await asyncio.to_thread(self._copy, root / relative, payload / relative)
        resources = []
        for resource in template.resources:
            name = _member(resource.name)
            await asyncio.to_thread(
                self._copy,
                root / "shared_data/resources" / name,
                payload / "resources" / name,
            )
            resources.append(_update_model(resource, path=f"resources/{name}"))
        template = _update_model(template, resources=resources)
        (payload / "experiment.yaml").write_text(
            yaml.safe_dump(
                template.model_dump(exclude_unset=True),
                allow_unicode=True,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        directories, files = await asyncio.to_thread(self._inventory, payload)
        manifest = ArchiveManifest(
            schema_version=2,
            archive_id=str(uuid4()),
            created_at=datetime.now(UTC).isoformat(),
            source_experiment_id=state.experiment_id,
            template="experiment.yaml",
            modules=modules,
            directories=directories,
            files=files,
        )
        await asyncio.to_thread(self._check_payload, payload, manifest)
        encoded = json.dumps(
            manifest.model_dump(), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        if len(encoded) > self._settings.max_manifest_bytes:
            raise StorageCapacityError("Archive manifest exceeds its size limit.")
        (payload / "manifest.json").write_bytes(encoded)
        return payload, manifest

    def _pack(self, payload: Path, archive: Path) -> None:
        """Write a USTAR/XZ archive while enforcing member, compressed-size, and free-space limits.

        Uses a temporary uncompressed USTAR file before XZ compression. The
        destination must be new. Member count, free space, and compressed size are
        enforced while writing.

        Args:
            payload: Extracted/staged archive root containing template, modules, and
                resources.
            archive: Archive file path used by this operation.

        Raises:
            StorageCapacityError: Member count, compressed size, or free-space
                limits are exceeded.
            OSError: The archive or temporary TAR file cannot be written.
        """
        directories, files = self._inventory(payload)
        if len(directories) + len(files) > self._settings.max_members:
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
                source = _path(payload / name)
                # Reserve padding as well as data before TarFile copies a member.
                self._space(
                    archive.parent,
                    (source.stat().st_size if source.is_file() else 0) + 10240,
                )
                stream.add(source, arcname=name, recursive=False)
        compressor = lzma.LZMACompressor(preset=self._settings.compression_preset)
        written = 0
        with unpacked.open("rb") as source, archive.open("xb") as destination:
            while True:
                block = source.read(1024 * 1024)
                data = compressor.compress(block) if block else compressor.flush()
                written += len(data)
                if written > self._settings.max_archive_bytes:
                    raise StorageCapacityError(
                        "Compressed archive size exceeds its limit."
                    )
                self._space(archive.parent, len(data))
                destination.write(data)
                if not block:
                    break
        unpacked.unlink()

    def _decompress(self, archive: Path, target: Path) -> None:
        """Bound both decoder memory and disk use before parsing any tar metadata.

        Bounds decoder memory and uncompressed output. Trailing compressed
        streams/data are rejected; partially written temporary output remains owned
        by the caller's workspace cleanup.

        Args:
            archive: Existing XZ archive file.
            target: New temporary uncompressed TAR file.
        """
        decoder = lzma.LZMADecompressor(
            format=lzma.FORMAT_XZ,
            memlimit=self._settings.max_decompression_memory_bytes,
        )
        limit = (
            self._settings.max_unpacked_bytes
            + self._settings.max_members * 1024
            + 10240
        )
        compressed = unpacked = 0
        with archive.open("rb") as source, target.open("xb") as destination:
            while not decoder.eof:
                block = source.read(1024 * 1024) if decoder.needs_input else b""
                if decoder.needs_input and not block:
                    raise ValueError("Truncated XZ stream.")
                compressed += len(block)
                if compressed > self._settings.max_archive_bytes:
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

    def _unpack(self, archive_path: Path, target: Path) -> ArchiveManifest:
        """Extract bounded USTAR/XZ contents and verify their complete manifest inventory.

        Args:
            archive_path: Absolute existing archive file.
            target: New extraction directory within the caller-owned workspace.

        Returns:
            Validated archive manifest after checking every payload member.

        Raises:
            ValueError: Headers, paths, padding, inventory, or content checks fail.
            StorageCapacityError: Archive limits or free-space reserves are exceeded.
        """
        archive = _path(archive_path)
        if not archive.is_file():
            raise FileNotFoundError(archive)
        if archive.stat().st_size > self._settings.max_archive_bytes:
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
                name = _member(member.name)
                if name.casefold() in names:
                    raise ValueError("Duplicate or case-colliding archive member.")
                names.add(name.casefold())
                if len(names) > self._settings.max_members:
                    raise StorageCapacityError("Too many archive members.")
                _validate_member_header(header, member)
                total += member.size
                if total > self._settings.max_unpacked_bytes:
                    raise StorageCapacityError(
                        "Unpacked archive size exceeds its limit."
                    )
                if (
                    name == "manifest.json"
                    and member.size > self._settings.max_manifest_bytes
                ):
                    raise StorageCapacityError(
                        "Archive manifest exceeds its size limit."
                    )
                output = target / name
                _path(output)
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
        manifest = ArchiveManifest.model_validate(read_json(target / "manifest.json"))
        expected = self._check_payload(target, manifest) | {"manifest.json"}
        if {name.casefold() for name in expected} != names:
            raise ValueError("Archive entries differ from its manifest inventory.")
        unpacked_tar.unlink()
        return manifest

    def _check_payload(self, payload: Path, manifest: ArchiveManifest) -> set[str]:
        """Verify inventory/template/modules/resources and return the allowed payload member set.

        Args:
            payload: Extracted/staged archive root containing template, modules, and
                resources.
            manifest: Validated snapshot/archive manifest describing expected
                contents.

        Returns:
            Exactly the payload member names allowed by the validated template and
            inventory, excluding manifest.json.

        Raises:
            ValueError: Inventory, hashes, template references, or allowed member
                roots disagree.
            FileNotFoundError: A required payload resource or module file is
                unavailable.
        """
        directories = manifest.directories
        files = manifest.files
        names = [*directories, *files]
        actual_dirs, actual_files = self._inventory(payload)
        actual_files.pop("manifest.json", None)
        if sorted(directories) != actual_dirs or files != actual_files:
            raise ValueError("Archive inventory, file size or SHA-256 differs.")
        template_path = payload / "experiment.yaml"
        raw_template = copy_json_object(
            yaml.safe_load(template_path.read_text(encoding="utf-8")), "template"
        )
        _, template = self._assembler.load_template(template_path)
        modules = _modules(ExperimentTemplate.model_validate(template))
        if manifest.modules != modules:
            raise ValueError("Archive modules differ from the applied template.")
        allowed_files = {"experiment.yaml"}
        roots: list[str] = []
        self._validate_payload_modules(payload, modules, roots)
        self._validate_payload_resources(
            payload, ExperimentTemplate.model_validate(raw_template).resources, roots
        )
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

    def _validate_payload_modules(
        self, payload: Path, modules: list[ArchiveModule], roots: list[str]
    ) -> None:
        """Check packaged module identity, role, and hash and append allowed module roots.

        Args:
            payload: Extracted/staged archive root containing template, modules, and
                resources.
            modules: Validated module references included in the archive.
            roots: Mutable list receiving the portable paths of allowed payload
                roots.
        """
        for module in modules:
            relative = f"modules/{module.name}/{module.version}"
            folder = payload / relative
            definition = ModuleManifest.model_validate(
                self._assembler.read_module(folder)
            )
            if (definition.name, definition.version, definition.role) != (
                module.name,
                module.version,
                module.role,
            ):
                raise ValueError(
                    "module.yaml identity or role differs from the template."
                )
            if self._manager.module_hash(module.name, folder) != module.hash:
                raise ValueError("Module content differs from its expected hash.")
            roots.append(relative)

    def _validate_payload_resources(
        self, payload: Path, resources: list[ResourceDefinition], roots: list[str]
    ) -> None:
        """Check portable resource paths and optional hashes and append allowed resource roots.

        Args:
            payload: Extracted/staged archive root containing template, modules, and
                resources.
            resources: Validated static resource definitions whose optional hashes
                are checked.
            roots: Mutable list receiving the portable paths of allowed payload
                roots.
        """
        for resource in resources:
            name = _member(resource.name)
            relative = f"resources/{name}"
            if resource.path != relative:
                raise ValueError(
                    "Archive resources must use portable template-relative paths."
                )
            source = payload / relative
            if not source.exists():
                raise FileNotFoundError(source)
            if resource.hash is not None:
                if source.is_dir():
                    digest = self._manager.module_hash(name, source)
                else:
                    with source.open("rb") as stream:
                        digest = hashlib.file_digest(stream, "sha256").hexdigest()
                if digest != require_text(resource.hash, "resource hash").lower():
                    raise ValueError("Static resource differs from its expected hash.")
            roots.append(relative)

    async def _inspect(self, archive_path: Path) -> JsonObject:
        """Extract and fully validate an archive in a temporary workspace, returning its manifest.

        Args:
            archive_path: Absolute archive file path on the server filesystem.

        Returns:
            Absolute source archive path and the fully checked manifest document.
        """
        parent = _path(self._project_root / "controller/archive_work")
        temporary = self._workspace(parent)
        failure = None
        try:
            manifest = await asyncio.to_thread(
                self._unpack, archive_path, Path(temporary.name) / "payload"
            )
            return {
                "archive_path": str(_path(archive_path)),
                "manifest": manifest.model_dump(),
            }
        except BaseException as error:
            failure = error
            raise
        finally:
            await asyncio.to_thread(_cleanup, temporary, parent, failure)

    async def _install(self, archive_path: Path, destination: Path) -> JsonObject:
        """Validate a bundle, register/install its modules, and publish template/resources.

        Args:
            archive_path: Absolute source archive path.
            destination: New absolute directory disjoint from project runtime storage.

        Returns:
            Archive/module receipt, destination, and installed template path.

        Raises:
            FileExistsError: The destination already exists.
            StorageConflict: A registered or installed module has different content.
            ValueError: Destination or archive validation fails.

        Failures retain completed-step notes; prior registrations are not rolled back.
        """
        destination = _path(destination)
        if destination.exists():
            raise FileExistsError(destination)
        modules_root = _path(self._project_root / "modules")
        for reserved in (
            modules_root,
            _path(Path(self._manager.module_storage_path)),
            _path(Path(self._manager.temp_folder)),
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
            modules = manifest.modules
            for module in modules:
                name, version, digest = (
                    require_text(module.name, "name"),
                    require_text(module.version, "version"),
                    require_text(module.hash, "hash"),
                )
                stored = self._manager.hash_db.get_module_hash(name, version)
                exists = await asyncio.to_thread(
                    self._manager.module_db.check_module_stored, name, version
                )
                if (stored or exists) and (stored.lower() != digest or not exists):
                    raise StorageConflict(
                        f"Stored module conflicts with archive: {name}/{version}"
                    )
                target = _path(modules_root / name / version)
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
                await self._install_module(module, payload, modules_root, completed)
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
                {"archive_id": manifest.archive_id, "modules": completed},
                "installation receipt",
            )
            write_json(bundle / "installation.json", receipt)
            if _path(destination).exists():
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
            await asyncio.to_thread(_cleanup, temporary, destination.parent, failure)

    async def _install_module(
        self,
        module: ArchiveModule,
        payload: Path,
        modules_root: Path,
        completed: list[JsonObject],
    ) -> None:
        """Register one checked module and atomically publish missing local code with progress notes.

        Args:
            module: Validated archive module reference including its expected hash.
            payload: Extracted/staged archive root containing template, modules, and
                resources.
            modules_root: Absolute project directory containing installed versioned
                module code.
            completed: Mutable installation receipt list updated after each
                completed registration/publication step.
        """
        name, version = (
            require_text(module.name, "name"),
            require_text(module.version, "version"),
        )
        source = payload / "modules" / name / version
        registered = await self._manager.register_module_async(name, version, source)
        entry: JsonObject = {
            "name": name,
            "version": version,
            "registered": registered,
            "installed": False,
        }
        completed.append(entry)
        target = _path(modules_root / name / version)
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            local = self._workspace(target.parent)
            local_failure = None
            try:
                staged = Path(local.name) / "module"
                await asyncio.to_thread(self._copy, source, staged)
                if await asyncio.to_thread(
                    self._manager.module_hash, name, staged
                ) != require_text(module.hash, "hash"):
                    raise ValueError("Installed module hash differs after copying.")
                if _path(target).exists():
                    raise StorageConflict(
                        f"Module destination appeared during installation: {target}"
                    )
                staged.rename(target)
                entry["installed"] = True
            except BaseException as error:
                local_failure = error
                raise
            finally:
                await asyncio.to_thread(_cleanup, local, target.parent, local_failure)
