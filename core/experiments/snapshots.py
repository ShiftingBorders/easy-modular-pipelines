"""Consistent experiment snapshots and recoverable replacement of runtime files."""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import psutil

from core.experiments.assembler import ExperimentAssembler
from core.experiments.journal import RunnerJournal
from core.experiments.restore_inputs import (
    RestorePaths,
    _restored_saved_state,
    _snapshot_state,
)
from core.experiments.services import ServiceManager
from core.experiments.snapshot_validation import (
    _retention_candidates,
    _validate_journal_results,
    _validate_snapshot_exports,
    _validate_snapshot_manifest,
)
from core.experiments.stages import StageRunner
from core.experiments.state import (
    RunnerState,
    RunnerStateStore,
    ServiceInstance,
    _restore_state,
    state_to_document,
)
from core.journal.events import LoggingError
from core.journal.logger import OperationLogger
from core.journal.storage import SQLiteEventStore
from core.models.experiment_template import (
    ExperimentTemplate,
    ServiceCallDefinition,
)
from core.models.journal_diagnostics import JournalSnapshotManifest
from core.models.module_manifest import ModuleManifest
from core.models.process_identity import ProcessIdentity
from core.models.runner_state import SavedRunnerState
from core.models.snapshot_documents import (
    STAGE_CONTROL_FILES,
    RestoredServiceObservation,
    RestoreTransaction,
    SnapshotFile,
    SnapshotInventory,
    SnapshotPayload,
)
from core.models.updates import _update_model
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import JsonObject, copy_json_object, require_text
from core.primitives.processes import process_identity
from core.primitives.tasks import _await_read_task


class ExperimentSnapshots:
    """Own snapshot publication and restoration; the runner owns DAG decisions."""

    _STAGE_CONTROL_FILES = STAGE_CONTROL_FILES

    def __init__(
        self,
        project_root: Path,
        stages: StageRunner,
        services: ServiceManager,
        journal: RunnerJournal,
        state_store: RunnerStateStore,
        *,
        assembler: ExperimentAssembler,
        hash_module: Callable[[str, Path], str],
        notify_resources: Callable[[], None] | None = None,
    ) -> None:
        root = Path(project_root)
        if not root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._project_root = root.resolve()
        self._stages = stages
        self._services = services
        self._journal = journal
        self._state_store = state_store
        self._assembler = assembler
        self._hash_module = hash_module
        self._notify_resources = notify_resources
        self._lock = asyncio.Lock()

    async def create(self, state: RunnerState, label: str | None = None) -> JsonObject:
        if label is not None:
            require_text(label, "snapshot label")
        if state.active_attempt is not None:
            raise RuntimeError("A snapshot requires a stage-free boundary.")
        async with self._lock:
            await self._assembler._check_modules_async(state)
            if (
                shutil.disk_usage(state.experiment_directory).free
                < state.template.storage.min_snapshot_free_bytes
            ):
                raise OSError("Insufficient free space for an experiment snapshot.")
            # Result publication precedes executor cleanup. Reap its remaining
            # writers before enumerating artifacts or exporting the journal.
            await self._stages.close(state)
            snapshot_id = str(uuid4())
            exports = await self._services.save_states(state, snapshot_id)
            return await self._publish(state, snapshot_id, exports, "regular", label)

    async def finalize(
        self, state: RunnerState, *, terminal_phase: str = "stopped"
    ) -> JsonObject:
        """Export state before shutdown; only complete finalization is restorable."""
        if terminal_phase not in ("stopped", "completed"):
            raise ValueError("Finalization requires a stopped or completed outcome.")
        if state.active_attempt is not None:
            raise RuntimeError(
                "Interrupt and clear the active stage before finalization."
            )
        async with self._lock:
            snapshot_id = str(uuid4())
            failure = None
            shutdown_failed = False
            exports = {}
            try:
                exports = await self._services.save_states(state, snapshot_id)
            except BaseException as error:  # noqa: BLE001 - Even cancellation must attempt participant shutdown.
                failure = error
            try:
                stops = await self._services.stop_all(state)
                if any(
                    not result["stopped"] or result["error"]
                    for result in stops.values()
                ):
                    raise RuntimeError(f"Final service shutdown is incomplete: {stops}")
                await self._services.reset(state)
                await self._stages.close(state)
            except BaseException as error:  # noqa: BLE001 - Preserve export failure alongside shutdown failure.
                shutdown_failed = True
                if failure is None:
                    failure = error
                else:
                    failure.add_note(f"Final shutdown also failed: {error}")
            if failure is not None:
                return self._finalization_failure(
                    state, snapshot_id, failure, shutdown_failed, terminal_phase
                )
            state.phase = terminal_phase
            if self._notify_resources is not None:
                self._notify_resources()
            self._journal.client.record_event(
                "experiment.finalized",
                {"phase": state.phase, "stops": stops},
                context={"experiment_id": state.experiment_id, "run_id": state.run_id},
            )
            return await self._publish(state, snapshot_id, exports, "final", None)

    def _finalization_failure(
        self,
        state: RunnerState,
        snapshot_id: str,
        failure: BaseException,
        shutdown_failed: bool,
        terminal_phase: str,
    ) -> JsonObject:
        diagnostic = (
            self._project_root
            / "controller"
            / "snapshot_failures"
            / f"{snapshot_id}.json"
        )
        if not diagnostic.resolve().is_relative_to(self._project_root):
            raise ValueError(
                "Snapshot diagnostic path escapes the project."
            ) from failure
        write_json(
            diagnostic,
            {
                "snapshot_id": snapshot_id,
                "experiment_id": state.experiment_id,
                "valid": False,
                "error": f"{type(failure).__name__}: {failure}",
            },
        )
        if (
            isinstance(failure, Exception)
            and not isinstance(failure, LoggingError)
            and not shutdown_failed
        ):
            state.phase = terminal_phase
            if self._notify_resources is not None:
                self._notify_resources()
            result = {
                "snapshot_id": snapshot_id,
                "valid": False,
                "error": f"{type(failure).__name__}: {failure}",
            }
            self._journal.client.record_event(
                "snapshot.failed",
                result,
                context={
                    "experiment_id": state.experiment_id,
                    "run_id": state.run_id,
                },
            )
            return result
        raise failure

    def _build_snapshot(
        self,
        root: Path,
        directory: Path,
        document: SavedRunnerState,
        exports: dict[str, str],
        kind: str,
        label: str | None,
        logging_config: Path,
    ) -> SnapshotPayload:
        """Copy frozen files in a worker with its own client for SQLite backup."""
        reserve = document.template.storage.min_snapshot_free_bytes
        if directory.exists() or not directory.resolve().is_relative_to(
            self._project_root / "snapshots"
        ):
            raise ValueError(
                "Snapshot destination must be new and inside the snapshot store."
            )
        directory.mkdir(parents=True)
        with OperationLogger(logging_config) as logger:
            journal = JournalSnapshotManifest.model_validate(
                logger.export_snapshot(directory / "journal", min_free_bytes=reserve)
            )
        self._copy_snapshot_payload(root, directory, reserve, document.template_yaml)
        inventory = self._snapshot_inventory(directory)
        sequences = [0]
        for previous in directory.parent.glob("*/manifest.json"):
            try:
                sequence = read_json(previous).get("sequence")
                if type(sequence) is int and 0 < sequence < 9223372036854775807:
                    sequences.append(sequence)
            except (OSError, ValueError):
                continue
        manifest = SnapshotPayload(
            schema_version=2,
            snapshot_id=directory.name,
            experiment_id=document.experiment_id,
            experiment_folder=root.name,
            created_at=datetime.now(UTC).isoformat(),
            sequence=max(sequences) + 1,
            kind=kind,
            label=label,
            state=document,
            services=exports,
            journal=journal,
            inventory=inventory,
        )
        return self._load_snapshot(directory, manifest=manifest)

    def _copy_snapshot_payload(
        self, root: Path, directory: Path, reserve: int, template_yaml: str
    ) -> None:
        payload = directory / "files"
        payload.mkdir()
        snapshot_id = directory.name
        for name in (
            "modules",
            "module_data",
            "shared_settings",
            "shared_data",
            "shared_artifacts",
        ):
            source = root / name
            if not source.exists():
                continue
            if (
                source.is_symlink()
                or source.is_junction()
                or not source.resolve().is_relative_to(root)
            ):
                raise ValueError("Snapshot sources cannot contain filesystem links.")
            for folder, children, filenames in os.walk(source, followlinks=False):
                current = Path(folder)
                relative = current.relative_to(root)
                if relative == Path("shared_artifacts/services"):
                    children.clear()
                    continue
                if (
                    relative.parts[:2] == ("shared_data", "service_state")
                    and len(relative.parts) >= 4
                    and relative.parts[3] != snapshot_id
                ):
                    children.clear()
                    continue
                destination = payload / relative
                destination.mkdir(parents=True, exist_ok=True)
                for child in children:
                    path = current / child
                    if path.is_symlink() or path.is_junction():
                        raise ValueError(
                            "Snapshot sources cannot contain filesystem links."
                        )
                for filename in filenames:
                    path = current / filename
                    self._copy_snapshot_file(
                        path, relative, filename, destination, directory, reserve
                    )
        (payload / "experiment.yaml").write_text(template_yaml, encoding="utf-8")

    def _snapshot_inventory(self, directory: Path) -> SnapshotInventory:
        directories = []
        files: dict[str, SnapshotFile] = {}
        for top in (directory / "files", directory / "journal"):
            directories.append(top.relative_to(directory).as_posix())
            for item in sorted(top.rglob("*")):
                relative = item.relative_to(directory).as_posix()
                if item.is_symlink() or item.is_junction():
                    raise ValueError("Copied snapshot contains a filesystem link.")
                if item.is_dir():
                    directories.append(relative)
                elif item.is_file():
                    with item.open("rb") as stream:
                        digest = hashlib.file_digest(stream, "sha256").hexdigest()
                    files[relative] = SnapshotFile(
                        size_bytes=item.stat().st_size, sha256=digest
                    )
                else:
                    raise ValueError("Copied snapshot contains a special file.")
        return SnapshotInventory(directories=sorted(directories), files=files)

    def _copy_snapshot_file(
        self,
        path: Path,
        relative: Path,
        filename: str,
        destination: Path,
        directory: Path,
        reserve: int,
    ) -> None:
        if path.is_symlink() or path.is_junction() or not path.is_file():
            raise ValueError("Snapshot sources must be regular files.")
        if (
            len(relative.parts) == 5
            and relative.parts[0] == "shared_artifacts"
            and relative.parts[1].startswith("epoch_")
            and relative.parts[4].startswith("attempt_")
            and (
                filename in self._STAGE_CONTROL_FILES
                or filename.startswith("executor.lock.")
                and filename.endswith(".token")
            )
        ):
            return
        if shutil.disk_usage(directory).free < path.stat().st_size + reserve:
            raise OSError("Insufficient free space while copying a snapshot.")
        shutil.copy2(path, destination / filename)

    async def _publish(
        self,
        state: RunnerState,
        snapshot_id: str,
        exports: dict[str, Path],
        kind: str,
        label: str | None,
    ) -> JsonObject:
        root = state.experiment_directory.resolve()
        directory = self._project_root / "snapshots" / root.name / snapshot_id
        identities = {
            key: value.service_instance_id for key, value in state.services.items()
        }
        document = _snapshot_state(state, snapshot_id, kind)
        config = self._journal.write_client_config(
            state,
            {
                "source": "runner",
                "experiment_id": state.experiment_id,
                "run_id": state.run_id,
            },
        )
        build = asyncio.create_task(
            asyncio.to_thread(
                self._build_snapshot,
                root,
                directory,
                document,
                {
                    key: path.resolve().relative_to(root).as_posix()
                    for key, path in exports.items()
                },
                kind,
                label,
                config,
            )
        )
        failure = None
        manifest = None
        observer = (
            asyncio.create_task(self._services.monitor(state))
            if kind == "regular" and state.services
            else None
        )
        try:
            if observer is not None:
                await asyncio.wait(
                    (build, observer), return_when=asyncio.FIRST_COMPLETED
                )
                if observer.done():
                    observer.result()
                    observer = asyncio.create_task(self._services.monitor(state))
                    raise RuntimeError(
                        "A service policy invalidated snapshot preparation."
                    )
            manifest = await asyncio.shield(build)
            if kind == "regular" and any(
                key not in state.services
                or state.services[key].service_instance_id != value
                or not state.services[key].ready
                or state.services[key].freeze_id != snapshot_id
                for key, value in identities.items()
            ):
                raise RuntimeError("Service state changed while copying the snapshot.")
        except BaseException as error:  # noqa: BLE001 - Reap the worker and unfreeze before propagating failure.
            failure = error
            # Reap copying threads before releasing their frozen source.
            await asyncio.gather(build, return_exceptions=True)
        finally:
            if observer is not None:
                observer.cancel()
                await asyncio.gather(observer, return_exceptions=True)
        if kind == "regular":
            try:
                await self._services.unfreeze(state, snapshot_id)
            except BaseException as error:
                if failure is not None:
                    error.add_note(f"Snapshot preparation failed: {failure}")
                raise RuntimeError(
                    "Snapshot write resumption is unconfirmed; stop the experiment."
                ) from error
        if failure is not None:
            raise failure
        self._journal.client.record_event(
            "snapshot.prepared",
            {"snapshot_id": snapshot_id, "kind": kind, "label": label},
            context={"experiment_id": state.experiment_id, "run_id": state.run_id},
        )
        write_json(directory / "manifest.json", manifest.document())
        try:
            self._journal.client.record_event(
                "snapshot.created",
                {"snapshot_id": snapshot_id, "kind": kind, "label": label},
                context={"experiment_id": state.experiment_id, "run_id": state.run_id},
            )
        except LoggingError:
            (directory / "manifest.json").unlink()
            raise
        state.stable_snapshot_id = snapshot_id
        try:
            self._state_store.save(state)
        except OSError as error:
            self._journal.client.record_error(
                error, context={"experiment_id": state.experiment_id}
            )
        await self._retain_snapshots(directory, state, manifest)
        return {
            "valid": True,
            "snapshot_id": manifest.snapshot_id,
            "created_at": manifest.created_at,
            "sequence": manifest.sequence,
            "kind": manifest.kind,
            "label": manifest.label,
        }

    async def _retain_snapshots(
        self, directory: Path, state: RunnerState, published: SnapshotPayload
    ) -> None:
        """Validate retained candidates only; delete other owned UUID directories."""
        candidates, owned = await _await_read_task(asyncio.create_task(asyncio.to_thread(
            _retention_candidates, directory.parent, published.experiment_id,
            published.experiment_folder,
        )))
        retained = {directory}
        for _, path in candidates:
            if len(retained) >= state.template.snapshots.keep:
                break
            if path == directory:
                continue
            try:
                payload = await self._read_snapshot(path)
                if (
                    payload.snapshot_id == path.name
                    and payload.experiment_id == published.experiment_id
                    and payload.experiment_folder == published.experiment_folder
                ):
                    retained.add(path)
            except (OSError, ValueError, TypeError, KeyError, LoggingError):
                continue
        for path in owned:
            if path in retained:
                continue
            if (
                path.is_symlink()
                or path.is_junction()
                or not path.resolve().is_relative_to(directory.parent.resolve())
            ):
                raise ValueError("Unsafe snapshot retention target.")
            await asyncio.to_thread(shutil.rmtree, path)

    def _validate_snapshot(
        self, directory: Path, *, manifest: JsonObject | None = None
    ) -> JsonObject:
        return self._load_snapshot(directory, manifest=manifest).document()

    async def _read_snapshot(self, directory: Path) -> SnapshotPayload:
        reading = asyncio.create_task(asyncio.to_thread(self._load_snapshot, directory))
        return await _await_read_task(reading)

    def _load_snapshot(
        self, directory: Path, *, manifest: SnapshotPayload | JsonObject | None = None
    ) -> SnapshotPayload:
        directory = Path(directory)
        if (
            not directory.is_absolute()
            or directory.is_symlink()
            or directory.is_junction()
            or not directory.resolve().is_relative_to(self._project_root)
        ):
            raise ValueError("Snapshot directory escapes the project.")
        directory = directory.resolve()
        manifest_path = directory / "manifest.json"
        if (
            manifest_path.is_symlink()
            or manifest_path.is_junction()
            or not manifest_path.resolve().is_relative_to(directory)
        ):
            raise ValueError("Snapshot manifest escapes its directory.")
        if isinstance(manifest, SnapshotPayload):
            # Preserve the full-document JSON bound at the persistence boundary.
            copy_json_object(manifest.document(), "snapshot manifest")
            payload = manifest
        else:
            document = read_json(manifest_path) if manifest is None else manifest
            payload = _validate_snapshot_manifest(document)
        self._check_snapshot_inventory(directory, payload.inventory)
        state = _restore_state(directory / "files", payload.state.model_copy(deep=True))
        if state.template_path != directory / "files" / "experiment.yaml":
            raise ValueError("Snapshot runner state is inconsistent.")
        yaml_text, template = self._assembler.load_template(state.template_path)
        if yaml_text != state.template_yaml or template != state.template.model_dump(
            exclude_unset=True
        ):
            raise ValueError("Snapshot template differs from its applied revision.")
        self._validate_snapshot_modules(directory, ExperimentTemplate.model_validate(template))
        for relative in state.retained_artifacts:
            if not (directory / "files" / relative).exists():
                raise ValueError("Snapshot is missing a retained conditional artifact.")
        _validate_snapshot_exports(directory, payload, state)
        if (
            read_json(directory / "journal/manifest.json")
            != payload.journal.model_dump()
        ):
            raise ValueError("Journal manifest differs from the experiment snapshot.")
        self._validate_snapshot_journal(directory, payload, state)
        return payload

    def _validate_snapshot_journal(
        self, directory: Path, payload: SnapshotPayload, state: RunnerState
    ) -> None:
        settings = state.template.logging
        store = SQLiteEventStore(
            directory / "journal/journal.sqlite",
            busy_timeout_seconds=settings.busy_timeout_seconds,
            max_event_bytes=settings.max_event_bytes,
            min_free_bytes=0,
            open_mode="existing",
            expected_journal={
                "journal_id": payload.journal.journal_id,
                "generation": payload.journal.generation,
            },
            read_only=True,
        )
        try:
            store._open_snapshot()
            store._check_snapshot_contents(payload.journal)
            _validate_journal_results(state, store)
        finally:
            store.close()

    def _check_snapshot_inventory(
        self,
        directory: Path,
        inventory: SnapshotInventory,
    ) -> None:
        files, directories = inventory.files, inventory.directories
        for name in (*directories, *files):
            path = directory / name
            if (
                path.is_symlink()
                or path.is_junction()
                or not path.resolve().is_relative_to(directory)
            ):
                raise ValueError("Snapshot member escapes its directory.")
        observed_files, observed_directories = set(), set()
        for top in (directory / "files", directory / "journal"):
            if top.is_symlink() or top.is_junction() or not top.is_dir():
                raise ValueError("Snapshot payload is missing or linked.")
            for folder_path, children, filenames in os.walk(top, followlinks=False):
                current = Path(folder_path)
                observed_directories.add(current.relative_to(directory).as_posix())
                for name in children:
                    child = current / name
                    if child.is_symlink() or child.is_junction():
                        raise ValueError("Snapshot directories cannot be links.")
                for name in filenames:
                    path = current / name
                    if path.is_symlink() or path.is_junction() or not path.is_file():
                        raise ValueError("Snapshot files must be regular files.")
                    relative = path.relative_to(directory).as_posix()
                    observed_files.add(relative)
                    expected = files.get(relative)
                    if expected is None:
                        raise ValueError("Invalid snapshot file inventory.")
                    with path.open("rb") as stream:
                        digest = hashlib.file_digest(stream, "sha256").hexdigest()
                    if (
                        path.stat().st_size != expected.size_bytes
                        or digest != expected.sha256
                    ):
                        raise ValueError(f"Snapshot file checksum mismatch: {relative}")
        if observed_files != set(files) or observed_directories != set(directories):
            raise ValueError("Snapshot members differ from the inventory.")

    def _validate_snapshot_modules(
        self, directory: Path, template: ExperimentTemplate
    ) -> None:
        checked_modules: dict[tuple[str, str], tuple[ModuleManifest, str]] = {}
        for role in ("stage", "service"):
            definitions = template.stages if role == "stage" else template.services
            for definition in definitions:
                reference = template.module_reference(definition)
                key = (reference.name, reference.version)
                if key not in checked_modules:
                    module_path = directory / "files/modules" / key[0] / key[1]
                    checked_modules[key] = (
                        ModuleManifest.model_validate(
                            self._assembler.read_module(module_path)
                        ),
                        self._hash_module(key[0], module_path),
                    )
                metadata, digest = checked_modules[key]
                if (
                    metadata.role
                    != (
                        "service"
                        if isinstance(definition, ServiceCallDefinition)
                        else role
                    )
                    or (metadata.stage_kind == "conditional")
                    != ("returns_data" in definition.model_fields_set)
                    or (metadata.name, metadata.version) != key
                    or digest != reference.hash
                ):
                    raise ValueError(
                        "Snapshot module identity or contents differ from its template."
                    )

    def latest_valid(self, experiment_directory: Path) -> JsonObject:
        return self._latest_valid(experiment_directory).document()

    def _latest_valid(self, experiment_directory: Path) -> SnapshotPayload:
        root = Path(experiment_directory)
        if not root.is_absolute() or not root.resolve().is_relative_to(
            self._project_root
        ):
            raise ValueError("Experiment directory escapes the project.")
        parent = self._project_root / "snapshots" / root.name
        if not parent.resolve().is_relative_to(self._project_root):
            raise ValueError("Snapshot store escapes the project.")
        candidates = []
        for path in parent.glob("*/manifest.json"):
            try:
                document = read_json(path)
                if type(document.get("sequence")) is int:
                    candidates.append((document["sequence"], path.parent))
            except (OSError, ValueError):
                continue
        for _, directory in sorted(candidates, reverse=True):
            try:
                document = self._load_snapshot(directory)
                if (
                    document.experiment_folder != root.name
                    or document.snapshot_id != directory.name
                ):
                    continue
                return document
            except (OSError, ValueError, TypeError, KeyError, LoggingError):
                continue
        raise FileNotFoundError("No complete valid experiment snapshot is available.")

    async def restore(
        self,
        state: RunnerState,
        snapshot_id: str,
        *,
        source_directory: Path | None = None,
        preserve_rebuild_diagnostics: bool = False,
        suspend_resources: Callable[[], Awaitable[None]] | None = None,
        resume_transaction: bool = False,
    ) -> RunnerState:
        snapshot_id = str(UUID(require_text(snapshot_id, "snapshot_id")))
        target = state.experiment_directory.resolve()
        if target.parent != self._project_root / "experiments":
            raise ValueError(
                "Restoration requires an experiment folder inside the project."
            )
        source = (
            target if source_directory is None else Path(source_directory).resolve()
        )
        if source.parent != self._project_root / "experiments":
            raise ValueError("Snapshot source is outside the experiment store.")
        marker = (
            self._project_root
            / "controller/restore_transactions"
            / f"{target.name}.json"
        )
        if not marker.resolve().is_relative_to(self._project_root):
            raise ValueError("Restore marker escapes the project.")
        async with self._lock:
            if marker.exists():
                transaction = RestoreTransaction.model_validate(read_json(marker))
                await self._finish_restore(
                    state, transaction, marker, validate_only=True
                )
                if transaction.phase == "failed" and resume_transaction:
                    if suspend_resources is not None:
                        await suspend_resources()
                    return await self._finish_restore(state, transaction, marker)
                if transaction.phase == "failed":
                    if suspend_resources is not None:
                        await suspend_resources()
                    await self._finish_restore(
                        state, transaction, marker, retry_failed=True
                    )
                if transaction.phase in ("complete", "failed"):
                    abandoned = (
                        self._project_root
                        / "controller/restores"
                        / str(UUID(transaction.restoration_id))
                        / "previous-transaction.json"
                    )
                    if not abandoned.resolve().is_relative_to(self._project_root):
                        raise ValueError("Restore diagnostic path escapes the project.")
                    write_json(abandoned, transaction.model_dump(exclude_unset=True))
                    marker.unlink()
                elif (
                    transaction.snapshot_id == snapshot_id
                    and transaction.source_folder == source.name
                ):
                    if suspend_resources is not None:
                        await suspend_resources()
                    return await self._finish_restore(state, transaction, marker)
                else:
                    raise RuntimeError(
                        "Resolve the existing restoration transaction first."
                    )
            archive = self._project_root / "snapshots" / source.name / snapshot_id
            manifest = await self._read_snapshot(archive)
            if (
                manifest.snapshot_id != snapshot_id
                or manifest.experiment_folder != source.name
            ):
                raise ValueError(
                    "Snapshot identity differs from its selected directory."
                )
            registry = read_json(self._project_root / "experiments.json")
            if registry.get(manifest.experiment_id) != source.name:
                raise ValueError("Snapshot belongs to another registered experiment.")
            if (
                source_directory is None
                and manifest.experiment_id != state.experiment_id
            ):
                raise ValueError("Rollback snapshot belongs to another experiment.")
            checked = _restore_state(
                archive / "files", manifest.state.model_copy(deep=True)
            )
            await self._assembler._check_modules_async(checked)
            required = (
                3 * sum(item.size_bytes for item in manifest.inventory.files.values())
                + state.template.storage.min_snapshot_free_bytes
            )
            if shutil.disk_usage(self._project_root).free < required:
                raise OSError(
                    "Insufficient free space to stage and validate restoration."
                )
            if suspend_resources is not None:
                await suspend_resources()
            if not await self._stages.interrupt(state, "rollback"):
                raise RuntimeError("Previous stage termination is unconfirmed.")
            await self._stages.close(state)
            state.active_attempt = None
            stops = await self._services.stop_all(state)
            if any(not item["stopped"] or item["error"] for item in stops.values()):
                raise RuntimeError(
                    f"Previous services have not stopped cleanly: {stops}"
                )
            await self._services.reset(state)
            restoration_id = str(uuid4())
            work = self._project_root / "controller/restores" / restoration_id
            if not work.resolve().is_relative_to(self._project_root):
                raise ValueError("Restoration workspace escapes the project.")
            work.mkdir(parents=True)
            # Bootstrap a clone's private journal to record its own restore intent.
            # The source experiment and archived journal are never opened for writing.
            if source_directory is not None:
                self._bootstrap_clone_journal(state, archive, manifest, target)
            operation = self._journal.client.start_operation(
                "restore_prepare",
                "experiment.restore_intent",
                context={"experiment_id": state.experiment_id, "run_id": state.run_id},
            )
            self._journal.client.record_event(
                "control.intent",
                {
                    "action": "restore_experiment",
                    "snapshot_id": snapshot_id,
                    "restoration_id": restoration_id,
                    "source_experiment_id": manifest.experiment_id,
                },
                operation=operation,
            )
            self._journal.client.finish_operation(
                operation, attributes={"phase": "prepared"}
            )
            operations = [operation.get_operation_id()]
            if preserve_rebuild_diagnostics and state.pending_rebuild is not None:
                operations.append(state.pending_rebuild.operation_id)
            self._journal.client.export_diagnostics(operations, work / "diagnostics")
            self._journal.close()
            transaction = RestoreTransaction.model_validate(
                {
                    "schema_version": 2,
                    "restoration_id": restoration_id,
                    "experiment_id": state.experiment_id,
                    "target_folder": target.name,
                    "source_folder": source.name,
                    "snapshot_id": snapshot_id,
                    "run_id": state.run_id
                    if source_directory is not None
                    else checked.run_id,
                    "clone": source_directory is not None,
                    "phase": "staging",
                    "preserve_diagnostics": preserve_rebuild_diagnostics,
                    "stopped_state": state_to_document(state),
                    "owner": process_identity(os.getpid()),
                }
            )
            write_json(marker, transaction.model_dump(exclude_unset=True))
            return await self._finish_restore(state, transaction, marker)

    def _bootstrap_clone_journal(
        self, state: RunnerState, archive: Path, manifest: SnapshotPayload, target: Path
    ) -> None:
        target.mkdir(parents=True, exist_ok=True)
        (target / "journals").mkdir(exist_ok=True)
        shutil.copyfile(
            archive / "journal/journal.sqlite",
            target / "journals/events.sqlite",
        )
        write_json(
            target / "runner/journal.json",
            {
                "journal_id": manifest.journal.journal_id,
                "generation": manifest.journal.generation,
            },
        )
        self._journal.open(state, create=False)

    async def _finish_restore(
        self,
        state: RunnerState,
        transaction: RestoreTransaction,
        marker: Path,
        *,
        validate_only: bool = False,
        retry_failed: bool = False,
    ) -> RunnerState:
        restoration_id = str(UUID(transaction.restoration_id))
        snapshot_id = str(UUID(transaction.snapshot_id))
        paths = self._check_restore_paths(state, transaction, marker)
        if validate_only:
            return state
        if transaction.phase in ("services_starting", "failed"):
            return await self._stop_interrupted_restore(
                state, transaction, marker, paths.target, retry_failed
            )
        target = paths.target
        stopped = _restore_state(
            target, transaction.stopped_state.model_copy(deep=True)
        )
        if (
            stopped.experiment_id != transaction.experiment_id
            or stopped.active_attempt is not None
            or any(not item.stopped for item in stopped.services.values())
        ):
            raise ValueError("Restore marker does not confirm the participant barrier.")
        if transaction.phase != "complete":
            await self._services.reset(stopped)
            transaction = _update_model(
                transaction, owner=process_identity(os.getpid())
            )
            write_json(marker, transaction.model_dump(exclude_unset=True))
        if transaction.phase == "staging":
            transaction = await self._stage_restore_snapshot(
                state, transaction, marker, paths, snapshot_id
            )
        manifest = await self._cached_restore_manifest(
            transaction, paths.cached, snapshot_id
        )
        if transaction.phase == "prepared":
            transaction = await self._install_restore_files(transaction, marker, paths)
        transaction = self._bind_restored_journal(
            state, transaction, marker, paths, manifest, restoration_id
        )
        if transaction.phase == "complete":
            return state
        transaction = await self._start_restore_services(
            state, transaction, marker, manifest, snapshot_id, restoration_id
        )
        if not transaction.preserve_diagnostics:
            await self._cleanup_completed_restore(paths)
        return state

    def _check_restore_paths(
        self, state: RunnerState, validated: RestoreTransaction, marker: Path
    ) -> RestorePaths:
        owner = validated.owner
        if owner.pid != os.getpid():
            try:
                if process_identity(owner.pid) == owner.model_dump():
                    try:
                        psutil.Process(owner.pid).wait(timeout=0)
                    except psutil.TimeoutExpired as error:
                        raise RuntimeError(
                            "Another live process owns this restoration."
                        ) from error
            except psutil.NoSuchProcess:
                pass
            except OSError as error:
                if not isinstance(
                    error, (FileNotFoundError, ProcessLookupError)
                ) and getattr(error, "winerror", None) not in (87, 1168):
                    raise
        target = self._project_root / "experiments" / validated.target_folder
        work = (
            self._project_root
            / "controller/restores"
            / str(UUID(validated.restoration_id))
        )
        if (
            target.resolve() != state.experiment_directory.resolve()
            or validated.experiment_id != state.experiment_id
            or not work.resolve().is_relative_to(self._project_root)
        ):
            raise ValueError("Restore transaction belongs to another experiment.")
        for path in (
            marker,
            target,
            work,
            work / "snapshot",
            work / "replacement",
            work / "previous",
        ):
            if (
                path.is_symlink()
                or path.is_junction()
                or not path.resolve().is_relative_to(self._project_root)
            ):
                raise ValueError("Restoration paths cannot escape the project.")
        return RestorePaths(
            target, work, work / "snapshot", work / "replacement", work / "previous"
        )

    async def _stop_interrupted_restore(
        self,
        state: RunnerState,
        transaction: RestoreTransaction,
        marker: Path,
        target: Path,
        retry_failed: bool,
    ) -> RunnerState:
        # An interrupted load is not evidence that it was never executed.
        # Stop its participants, retain diagnostics, and require a new rollback.
        if not (target / "runner/state.json").is_file():
            raise RuntimeError(
                "Restored participant state is missing; termination cannot be confirmed."
            )
        recovered = self._state_store.load(target)
        if recovered.experiment_id != state.experiment_id:
            raise ValueError(
                "Restored participant state belongs to another experiment."
            )
        vars(state).update(vars(recovered))
        self._journal.close()
        self._journal.open(state, create=False)
        self._restore_announced_services(state, target)
        stops = await self._services.stop_all(state)
        transaction = _update_model(transaction, stopped_state=state_to_document(state))
        if any(not item["stopped"] or item["error"] for item in stops.values()):
            transaction = _update_model(transaction, phase="failed")
            write_json(marker, transaction.model_dump(exclude_unset=True))
            raise RuntimeError(
                "Interrupted restoration participants have not stopped cleanly."
            )
        await self._services.reset(state)
        transaction = _update_model(transaction, phase="failed")
        write_json(marker, transaction.model_dump(exclude_unset=True))
        if retry_failed:
            return state
        raise RuntimeError(
            "Service restoration was interrupted; request a fresh rollback."
        )

    def _restore_announced_services(self, state: RunnerState, target: Path) -> None:
        for definition in state.template.services:
            service_id = definition.service_id
            endpoint = target / "runner/endpoints" / f"{service_id}.json"
            if not endpoint.is_file():
                continue
            announced = RestoredServiceObservation.model_validate(read_json(endpoint))
            current = state.services.get(service_id)
            if (
                current is not None
                and announced.participant_instance_id == current.service_instance_id
            ):
                continue
            instance_id = str(UUID(announced.participant_instance_id))
            artifacts = target / "shared_artifacts/services" / service_id / instance_id
            if (
                not artifacts.resolve().is_relative_to(target)
                or not (artifacts / "process.json").is_file()
            ):
                raise RuntimeError(
                    "An unaccounted-for restored service cannot be confirmed stopped."
                )
            recorded = RestoredServiceObservation.model_validate(
                read_json(artifacts / "process.json")
            )
            expected = (state.experiment_id, service_id, instance_id)
            if (
                any(
                    (
                        observation.experiment_id,
                        observation.participant_id,
                        observation.participant_instance_id,
                    )
                    != expected
                    for observation in (announced, recorded)
                )
                or recorded.process != announced.process
            ):
                raise RuntimeError("Restored service ownership cannot be verified.")
            instance = ServiceInstance(service_id, instance_id, definition)
            instance.process_identity = ProcessIdentity.model_validate(recorded.process)
            instance.endpoint_path = endpoint
            instance.artifacts_directory = artifacts
            metadata = ModuleManifest.model_validate(
                self._assembler.read_module(
                    target
                    / "modules"
                    / definition.module.name
                    / definition.module.version
                )
            )
            instance.implementation = metadata.implementation
            state.services[service_id] = instance

    async def _stage_restore_snapshot(
        self,
        state: RunnerState,
        transaction: RestoreTransaction,
        marker: Path,
        paths: RestorePaths,
        snapshot_id: str,
    ) -> RestoreTransaction:
        work, cached, replacement = paths.work, paths.cached, paths.replacement
        archive = (
            self._project_root / "snapshots" / transaction.source_folder / snapshot_id
        )
        manifest = await self._read_snapshot(archive)
        for path in (cached, replacement):
            if path.exists():
                if (
                    path.is_symlink()
                    or path.is_junction()
                    or not path.resolve().is_relative_to(work.resolve())
                ):
                    raise ValueError("Unsafe incomplete restoration cleanup.")
                await asyncio.to_thread(shutil.rmtree, path)
        copy_task = asyncio.create_task(
            asyncio.to_thread(shutil.copytree, archive, cached)
        )
        try:
            await asyncio.shield(copy_task)
        except BaseException:
            await asyncio.gather(copy_task, return_exceptions=True)
            raise
        await self._read_snapshot(cached)
        copy_task = asyncio.create_task(
            asyncio.to_thread(shutil.copytree, cached / "files", replacement)
        )
        try:
            await asyncio.shield(copy_task)
        except BaseException:
            await asyncio.gather(copy_task, return_exceptions=True)
            raise
        (replacement / "journals").mkdir()
        shutil.copyfile(
            cached / "journal/journal.sqlite",
            replacement / "journals/events.sqlite",
        )
        document = _restored_saved_state(
            manifest, transaction.stopped_state, state.experiment_id, transaction.run_id
        )
        restored = _restore_state(replacement, document)
        self._state_store.save(restored)
        write_json(
            replacement / "runner/journal.json",
            {
                "journal_id": manifest.journal.journal_id,
                "generation": manifest.journal.generation,
            },
        )
        transaction = _update_model(transaction, phase="prepared")
        write_json(marker, transaction.model_dump(exclude_unset=True))
        return transaction

    async def _cached_restore_manifest(
        self, transaction: RestoreTransaction, cached: Path, snapshot_id: str
    ) -> SnapshotPayload:
        manifest = await self._read_snapshot(cached)
        if (
            manifest.snapshot_id != snapshot_id
            or manifest.experiment_folder != transaction.source_folder
            or (
                not transaction.clone
                and manifest.experiment_id != transaction.experiment_id
            )
        ):
            raise ValueError("Cached restoration snapshot has a different identity.")
        return manifest

    async def _install_restore_files(
        self, transaction: RestoreTransaction, marker: Path, paths: RestorePaths
    ) -> RestoreTransaction:
        target, previous, replacement = paths.target, paths.previous, paths.replacement
        if not previous.exists():
            if not target.is_dir() or not replacement.is_dir():
                raise RuntimeError(
                    "Prepared restoration is missing its source or replacement."
                )
            # Read-only dashboard clients open the source briefly. Give them
            # a bounded opportunity to release it, without bypassing the
            # barrier for a persistently open external journal on Windows.
            await self._replace_restore_directory(target, previous)
        if not target.exists():
            if not replacement.is_dir():
                raise RuntimeError(
                    "Restoration replacement is missing after displacement."
                )
            await self._replace_restore_directory(replacement, target)
        elif replacement.exists():
            raise RuntimeError(
                "Ambiguous restoration directories; no files were overwritten."
            )
        transaction = _update_model(transaction, phase="files_installed")
        write_json(marker, transaction.model_dump(exclude_unset=True))
        return transaction

    def _bind_restored_journal(
        self,
        state: RunnerState,
        transaction: RestoreTransaction,
        marker: Path,
        paths: RestorePaths,
        manifest: SnapshotPayload,
        restoration_id: str,
    ) -> RestoreTransaction:
        target, work = paths.target, paths.work
        restored = self._state_store.load(target)
        if restored.experiment_id != transaction.experiment_id:
            raise ValueError("Installed state has another experiment identity.")
        vars(state).update(vars(restored))
        if transaction.phase == "files_installed":
            self._journal.complete_restore(
                state,
                manifest.journal,
                restoration_id,
                work / "diagnostics",
            )
            transaction = _update_model(transaction, phase="journal_restored")
            write_json(marker, transaction.model_dump(exclude_unset=True))
        elif transaction.phase == "journal_restored":
            self._journal.open(state, create=False)
        return transaction

    async def _start_restore_services(
        self,
        state: RunnerState,
        transaction: RestoreTransaction,
        marker: Path,
        manifest: SnapshotPayload,
        snapshot_id: str,
        restoration_id: str,
    ) -> RestoreTransaction:
        transaction = _update_model(transaction, phase="services_starting")
        write_json(marker, transaction.model_dump(exclude_unset=True))
        try:
            if await self._services.reconcile(state, state.template) != "ready":
                raise RuntimeError("Restored services did not become ready.")
            exports: dict[str, Path] = {}
            for key, value in manifest.services.items():
                if value is None:
                    raise TypeError(
                        "expected str, bytes or os.PathLike object, not NoneType"
                    )
                exports[key] = Path(value)
            await self._services.load_states(state, exports)
            state.phase, state.mode = "waiting", "paused"
            self._journal.client.record_event(
                "experiment.restored",
                {
                    "snapshot_id": snapshot_id,
                    "restoration_id": restoration_id,
                    "source_experiment_id": manifest.experiment_id,
                },
                context={"experiment_id": state.experiment_id, "run_id": state.run_id},
            )
            self._state_store.save(state)
            transaction = _update_model(transaction, phase="complete")
            write_json(marker, transaction.model_dump(exclude_unset=True))
        except BaseException as error:
            try:
                await self._services.stop_all(state)
            except Exception as shutdown_error:  # noqa: BLE001 - Preserve the restore failure and shutdown diagnostics.
                error.add_note(f"Restored service shutdown failed: {shutdown_error}")
            transaction = _update_model(transaction, phase="failed")
            write_json(marker, transaction.model_dump(exclude_unset=True))
            raise
        return transaction

    async def _cleanup_completed_restore(self, paths: RestorePaths) -> None:
        previous, cached, work = paths.previous, paths.cached, paths.work
        for path in (previous, cached):
            if (
                path.is_symlink()
                or path.is_junction()
                or not path.resolve().is_relative_to(work.resolve())
            ):
                raise ValueError("Unsafe completed restoration cleanup.")
            if any(
                item.is_symlink() or item.is_junction() for item in path.rglob("*")
            ):
                continue
            await asyncio.to_thread(shutil.rmtree, path)


    async def _replace_restore_directory(self, source: Path, target: Path) -> None:
        deadline = time.monotonic() + 1
        while True:
            try:
                source.replace(target)
                break
            except OSError as error:
                if (
                    os.name != "nt"
                    or getattr(error, "winerror", None) not in (5, 32, 33)
                    or time.monotonic() >= deadline
                ):
                    raise
                await asyncio.sleep(0.01)
