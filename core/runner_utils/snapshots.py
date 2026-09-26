"""Consistent experiment snapshots and recoverable replacement of runtime files."""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import tempfile
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from uuid import UUID, uuid4

import psutil

from core.experimentassembler import ExperimentAssembler
from core.logger import OperationLogger
from core.logger_utils.events import LoggingError, copy_json_object, require_text
from core.logger_utils.storage import SQLiteEventStore
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.results import read_result
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from core.runner_utils.services import ServiceManager
from core.runner_utils.stages import StageRunner
from core.runner_utils.state import (
    JsonObject,
    RunnerState,
    RunnerStateStore,
    ServiceInstance,
    state_from_document,
    state_to_document,
)


class ExperimentSnapshots:
    """Own snapshot publication and restoration; the runner owns DAG decisions."""

    _STAGE_CONTROL_FILES = frozenset(
        {
            "launch.json",
            "context.json",
            "process.json",
            "ready.json",
            "executor.token",
            "executor.lock.json",
            "stop.emergency.json",
        }
    )

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
            self._assembler.check_modules(state)
            if (
                shutil.disk_usage(state.experiment_directory).free
                < state.template["storage"]["min_snapshot_free_bytes"]
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
            state.phase = terminal_phase
            if self._notify_resources is not None:
                self._notify_resources()
            self._journal.client.record_event(
                "experiment.finalized",
                {"phase": state.phase, "stops": stops},
                context={"experiment_id": state.experiment_id, "run_id": state.run_id},
            )
            return await self._publish(state, snapshot_id, exports, "final", None)

    def _build_snapshot(
        self,
        root: Path,
        directory: Path,
        document: JsonObject,
        exports: dict[str, str],
        kind: str,
        label: str | None,
        logging_config: Path,
    ) -> JsonObject:
        """Copy frozen files in a worker with its own client for SQLite backup."""
        reserve = document["template"]["storage"]["min_snapshot_free_bytes"]
        if directory.exists() or not directory.resolve().is_relative_to(
            self._project_root / "snapshots"
        ):
            raise ValueError(
                "Snapshot destination must be new and inside the snapshot store."
            )
        directory.mkdir(parents=True)
        with OperationLogger(logging_config) as logger:
            journal = logger.export_snapshot(
                directory / "journal", min_free_bytes=reserve
            )
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
                        continue
                    if (
                        shutil.disk_usage(directory).free
                        < path.stat().st_size + reserve
                    ):
                        raise OSError(
                            "Insufficient free space while copying a snapshot."
                        )
                    shutil.copy2(path, destination / filename)
        (payload / "experiment.yaml").write_text(
            document["template_yaml"], encoding="utf-8"
        )
        directories = []
        files = {}
        for top in (payload, directory / "journal"):
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
                    files[relative] = {
                        "size_bytes": item.stat().st_size,
                        "sha256": digest,
                    }
                else:
                    raise ValueError("Copied snapshot contains a special file.")
        sequences = [0]
        for previous in directory.parent.glob("*/manifest.json"):
            try:
                sequence = read_json(previous).get("sequence")
                if type(sequence) is int and 0 < sequence < 9223372036854775807:
                    sequences.append(sequence)
            except (OSError, ValueError):
                continue
        manifest = {
            "schema_version": 2,
            "snapshot_id": snapshot_id,
            "experiment_id": document["experiment_id"],
            "experiment_folder": root.name,
            "created_at": datetime.now(UTC).isoformat(),
            "sequence": max(sequences) + 1,
            "kind": kind,
            "label": label,
            "state": document,
            "services": exports,
            "journal": journal,
            "directories": sorted(directories),
            "files": files,
        }
        self._validate_snapshot(directory, manifest=manifest)
        return manifest

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
        document = state_to_document(state)
        document["phase"] = state.phase if kind == "final" else "waiting"
        document["mode"] = "paused"
        document["pause_requested"] = False
        document["stable_snapshot_id"] = snapshot_id
        for service in document["services"].values():
            service["freeze_id"] = service["prepared_freeze_id"] = None
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
        write_json(directory / "manifest.json", manifest)
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
        valid = []
        for path in directory.parent.glob("*/manifest.json"):
            try:
                entry = await asyncio.to_thread(self._validate_snapshot, path.parent)
                valid.append((entry["sequence"], path.parent))
            except (OSError, ValueError, TypeError, KeyError, LoggingError):
                continue
        valid.sort(reverse=True)
        retained = {directory}
        for _, path in valid:
            if len(retained) < state.template["snapshots"]["keep"]:
                retained.add(path)
        for _, path in valid:
            if path in retained:
                continue
            if (
                path.is_symlink()
                or path.is_junction()
                or not path.resolve().is_relative_to(directory.parent.resolve())
            ):
                raise ValueError("Unsafe snapshot retention target.")
            await asyncio.to_thread(shutil.rmtree, path)
        return {
            "valid": True,
            **{
                key: manifest[key]
                for key in ("snapshot_id", "created_at", "sequence", "kind", "label")
            },
        }

    def _validate_snapshot(
        self, directory: Path, *, manifest: JsonObject | None = None
    ) -> JsonObject:
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
        document = (
            read_json(directory / "manifest.json")
            if manifest is None
            else copy_json_object(manifest, "snapshot manifest")
        )
        required = {
            "schema_version",
            "snapshot_id",
            "experiment_id",
            "experiment_folder",
            "created_at",
            "sequence",
            "kind",
            "label",
            "state",
            "services",
            "journal",
            "directories",
            "files",
        }
        if (
            document.keys() != required
            or type(document["schema_version"]) is not int
            or document["schema_version"] != 2
        ):
            raise ValueError("Unsupported experiment snapshot manifest.")
        UUID(require_text(document["snapshot_id"], "snapshot_id"))
        require_text(document["experiment_id"], "experiment_id")
        folder = require_text(document["experiment_folder"], "experiment folder")
        if (
            folder in (".", "..")
            or Path(folder).name != folder
            or any(character in folder for character in '/\\:*?"<>|')
        ):
            raise ValueError("Invalid experiment folder in snapshot.")
        if (
            type(document["sequence"]) is not int
            or not 0 < document["sequence"] <= 9223372036854775807
            or document["kind"] not in ("regular", "final")
        ):
            raise ValueError("Invalid snapshot sequence or kind.")
        if datetime.fromisoformat(document["created_at"]).utcoffset() != UTC.utcoffset(
            None
        ):
            raise ValueError("Snapshot time must be UTC.")
        if document["label"] is not None:
            require_text(document["label"], "snapshot label")
        files = copy_json_object(document["files"], "snapshot files")
        directories = document["directories"]
        if type(directories) is not list or any(
            type(item) is not str for item in directories
        ):
            raise TypeError("Snapshot directories must be a string array.")
        names = [*directories, *files]
        if len({name.casefold() for name in names}) != len(names):
            raise ValueError("Snapshot paths collide.")
        for name in names:
            relative = PurePosixPath(name)
            if (
                not name
                or "\\" in name
                or ":" in name
                or PureWindowsPath(name).anchor
                or relative.is_absolute()
                or any(
                    part in (".", "..") or part.rstrip(" .") != part
                    for part in name.split("/")
                )
                or relative.parts[0] not in ("files", "journal")
            ):
                raise ValueError("Unsafe snapshot member path.")
            parts = relative.parts
            if (
                parts[0] == "files"
                and len(parts) > 1
                and parts[1]
                not in {
                    "modules",
                    "module_data",
                    "shared_settings",
                    "shared_data",
                    "shared_artifacts",
                    "experiment.yaml",
                }
            ):
                raise ValueError("Snapshot contains a runtime control file.")
            if parts[:3] == ("files", "shared_artifacts", "services"):
                raise ValueError("Snapshot contains live service control artifacts.")
            if (
                len(parts) == 7
                and parts[:2] == ("files", "shared_artifacts")
                and parts[2].startswith("epoch_")
                and parts[5].startswith("attempt_")
                and (
                    parts[6] in self._STAGE_CONTROL_FILES
                    or parts[6].startswith("executor.lock.")
                    and parts[6].endswith(".token")
                )
            ):
                raise ValueError("Snapshot contains live stage control artifacts.")
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
                    if (
                        type(expected) is not dict
                        or expected.keys() != {"size_bytes", "sha256"}
                        or type(expected["size_bytes"]) is not int
                        or expected["size_bytes"] < 0
                    ):
                        raise ValueError("Invalid snapshot file inventory.")
                    with path.open("rb") as stream:
                        digest = hashlib.file_digest(stream, "sha256").hexdigest()
                    if (
                        path.stat().st_size != expected["size_bytes"]
                        or digest != expected["sha256"]
                    ):
                        raise ValueError(f"Snapshot file checksum mismatch: {relative}")
        if observed_files != set(files) or observed_directories != set(directories):
            raise ValueError("Snapshot members differ from the inventory.")
        state = state_from_document(directory / "files", document["state"])
        if (
            state.experiment_id != document["experiment_id"]
            or state.active_attempt is not None
            or state.template_path != directory / "files" / "experiment.yaml"
        ):
            raise ValueError("Snapshot runner state is inconsistent.")
        yaml_text, template = self._assembler.load_template(state.template_path)
        if yaml_text != state.template_yaml or template != state.template:
            raise ValueError("Snapshot template differs from its applied revision.")
        checked_modules = {}
        for role in ("stage", "service"):
            for definition in template[f"{role}s"]:
                reference = self._assembler.module_reference(template, definition)
                key = (reference["name"], reference["version"])
                if key not in checked_modules:
                    module_path = directory / "files/modules" / key[0] / key[1]
                    checked_modules[key] = (
                        self._assembler.read_module(module_path),
                        self._hash_module(key[0], module_path),
                    )
                metadata, digest = checked_modules[key]
                if (
                    metadata["role"]
                    != ("service" if "service_id" in definition else role)
                    or (metadata["name"], metadata["version"]) != key
                    or digest != reference["hash"]
                ):
                    raise ValueError(
                        "Snapshot module identity or contents differ from its template."
                    )
        if state.cycle_number > state.template["cycles"] or state.stage_position > len(
            state.template["stages"]
        ):
            raise ValueError("Snapshot cursor is outside its DAG.")
        stage_ids = {item["stage_id"] for item in state.template["stages"]}
        for stage_id, request_id in state.stage_result_ids.items():
            if stage_id not in stage_ids:
                raise ValueError("Snapshot result belongs to an unknown DAG node.")
            UUID(require_text(request_id, "result request ID"))
        if (
            state.last_result_id is not None
            and state.last_result_id not in state.stage_result_ids.values()
        ):
            raise ValueError(
                "Snapshot retained result has no matching journal reference."
            )
        if state.last_result_id is None and state.last_result is not None:
            raise ValueError("Snapshot retained data has no journal reference.")
        if set(state.services) != {
            item["service_id"] for item in state.template["services"]
        }:
            raise ValueError("Snapshot does not describe every service.")
        exports = copy_json_object(document["services"], "snapshot service exports")
        if exports.keys() - state.services.keys():
            raise ValueError("Snapshot exports an unknown service.")
        for service_id, instance in state.services.items():
            definition = next(
                item
                for item in state.template["services"]
                if item["service_id"] == service_id
            )
            if instance.definition != definition:
                raise ValueError(
                    "Snapshot service settings differ from the applied template."
                )
            path = exports.get(service_id)
            if (
                instance.active_request
                or instance.pending_requests
                or instance.freeze_id
                or instance.prepared_freeze_id
            ):
                raise ValueError("Snapshot contains unresolved service work.")
            if path is None:
                if instance.definition["state_required"]:
                    raise ValueError("Required service export is missing.")
                continue
            name = require_text(path, "service state path")
            member = directory / "files" / name
            allocated = (
                directory
                / "files/shared_data/service_state"
                / service_id
                / document["snapshot_id"]
            )
            if (
                PureWindowsPath(name).anchor
                or ".." in PurePosixPath(name).parts
                or not member.resolve().is_relative_to(allocated.resolve())
                or not member.exists()
            ):
                raise ValueError(
                    "Service export escapes its allocated snapshot directory."
                )
        if read_json(directory / "journal/manifest.json") != document["journal"]:
            raise ValueError("Journal manifest differs from the experiment snapshot.")
        # Reuse the journal's public full-content validator on a disposable copy.
        # Never open a journal client against the immutable archived database.
        scratch = self._project_root / "controller/snapshot_validation"
        if not scratch.resolve().is_relative_to(self._project_root):
            raise ValueError("Snapshot validation path escapes the project.")
        scratch.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=scratch)
        try:
            database = Path(temporary.name) / "journal.sqlite"
            shutil.copyfile(directory / "journal/journal.sqlite", database)
            settings = state.template["logging"]
            store = SQLiteEventStore(
                database,
                busy_timeout_seconds=settings["busy_timeout_seconds"],
                max_event_bytes=settings["max_event_bytes"],
                min_free_bytes=0,
                open_mode="existing",
                expected_journal={
                    key: document["journal"][key]
                    for key in ("journal_id", "generation")
                },
            )
            try:
                store.open()
                try:
                    for stage_id, request_id in state.stage_result_ids.items():
                        record = read_result(
                            store,
                            request_id,
                            expected={
                                "experiment_id": state.stage_result_origins.get(
                                    stage_id, state.experiment_id
                                ),
                                "stage_id": stage_id,
                            },
                            accepted=True,
                        )
                        if (
                            record is None
                            or record["outcome"] != "succeeded"
                            or record["response"]["result"] != "success"
                        ):
                            raise ValueError(
                                "Snapshot result is missing or unsuccessful in its journal."
                            )
                        if (
                            request_id == state.last_result_id
                            and record["response"]["data"] != state.last_result
                        ):
                            raise ValueError(
                                "Snapshot retained data differs from its journal result."
                            )
                finally:
                    store.close()
                store.complete_restore(
                    document["journal"],
                    restoration_id=str(uuid4()),
                    new_generation=str(uuid4()),
                )
            finally:
                store.close()
        finally:
            for attempt in range(10):
                try:
                    temporary.cleanup()
                    break
                except OSError as error:
                    if (
                        getattr(error, "winerror", None) not in (32, 145)
                        or attempt == 9
                    ):
                        raise
                    time.sleep(0.1)
        return document

    def latest_valid(self, experiment_directory: Path) -> JsonObject:
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
                document = self._validate_snapshot(directory)
                if (
                    document["experiment_folder"] != root.name
                    or document["snapshot_id"] != directory.name
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
                transaction = read_json(marker)
                await self._finish_restore(
                    state, transaction, marker, validate_only=True
                )
                if transaction.get("phase") == "failed" and resume_transaction:
                    if suspend_resources is not None:
                        await suspend_resources()
                    return await self._finish_restore(state, transaction, marker)
                if transaction["phase"] == "failed":
                    if suspend_resources is not None:
                        await suspend_resources()
                    await self._finish_restore(
                        state, transaction, marker, retry_failed=True
                    )
                if transaction.get("phase") in ("complete", "failed"):
                    abandoned = (
                        self._project_root
                        / "controller/restores"
                        / str(UUID(transaction["restoration_id"]))
                        / "previous-transaction.json"
                    )
                    if not abandoned.resolve().is_relative_to(self._project_root):
                        raise ValueError("Restore diagnostic path escapes the project.")
                    write_json(abandoned, transaction)
                    marker.unlink()
                elif (
                    transaction.get("snapshot_id") == snapshot_id
                    and transaction.get("source_folder") == source.name
                ):
                    if suspend_resources is not None:
                        await suspend_resources()
                    return await self._finish_restore(state, transaction, marker)
                else:
                    raise RuntimeError(
                        "Resolve the existing restoration transaction first."
                    )
            archive = self._project_root / "snapshots" / source.name / snapshot_id
            manifest = await asyncio.to_thread(self._validate_snapshot, archive)
            if (
                manifest["snapshot_id"] != snapshot_id
                or manifest["experiment_folder"] != source.name
            ):
                raise ValueError(
                    "Snapshot identity differs from its selected directory."
                )
            registry = read_json(self._project_root / "experiments.json")
            if registry.get(manifest["experiment_id"]) != source.name:
                raise ValueError("Snapshot belongs to another registered experiment.")
            if (
                source_directory is None
                and manifest["experiment_id"] != state.experiment_id
            ):
                raise ValueError("Rollback snapshot belongs to another experiment.")
            checked = state_from_document(archive / "files", manifest["state"])
            self._assembler.check_modules(checked)
            required = (
                3 * sum(item["size_bytes"] for item in manifest["files"].values())
                + state.template["storage"]["min_snapshot_free_bytes"]
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
                target.mkdir(parents=True, exist_ok=True)
                (target / "journals").mkdir(exist_ok=True)
                shutil.copyfile(
                    archive / "journal/journal.sqlite",
                    target / "journals/events.sqlite",
                )
                write_json(
                    target / "runner/journal.json",
                    {
                        key: manifest["journal"][key]
                        for key in ("journal_id", "generation")
                    },
                )
                self._journal.open(state, create=False)
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
                    "source_experiment_id": manifest["experiment_id"],
                },
                operation=operation,
            )
            self._journal.client.finish_operation(
                operation, attributes={"phase": "prepared"}
            )
            operations = [operation.get_operation_id()]
            if preserve_rebuild_diagnostics and state.pending_rebuild is not None:
                operations.append(state.pending_rebuild["operation_id"])
            self._journal.client.export_diagnostics(operations, work / "diagnostics")
            self._journal.close()
            transaction = {
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
            write_json(marker, transaction)
            return await self._finish_restore(state, transaction, marker)

    async def _finish_restore(
        self,
        state: RunnerState,
        transaction: JsonObject,
        marker: Path,
        *,
        validate_only: bool = False,
        retry_failed: bool = False,
    ) -> RunnerState:
        transaction = copy_json_object(transaction, "restore transaction")
        if (
            transaction.keys()
            != {
                "schema_version",
                "restoration_id",
                "experiment_id",
                "target_folder",
                "source_folder",
                "snapshot_id",
                "run_id",
                "clone",
                "phase",
                "preserve_diagnostics",
                "stopped_state",
                "owner",
            }
            or type(transaction["schema_version"]) is not int
            or transaction["schema_version"] != 2
        ):
            raise ValueError("Invalid restore transaction marker.")
        restoration_id = str(UUID(transaction["restoration_id"]))
        snapshot_id = str(UUID(transaction["snapshot_id"]))
        owner = copy_json_object(transaction["owner"], "restore owner")
        if (
            owner.keys() != {"pid", "created_at_os", "host_id", "boot_id"}
            or type(owner["pid"]) is not int
            or owner["pid"] < 1
            or type(owner["created_at_os"]) is not int
            or owner["created_at_os"] < 0
        ):
            raise ValueError("Invalid restoration owner identity.")
        require_text(owner["host_id"], "restore owner host")
        require_text(owner["boot_id"], "restore owner boot")
        if owner.get("pid") != os.getpid():
            try:
                if process_identity(owner["pid"]) == owner:
                    try:
                        psutil.Process(owner["pid"]).wait(timeout=0)
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
        if (
            type(transaction["clone"]) is not bool
            or type(transaction["preserve_diagnostics"]) is not bool
        ):
            raise TypeError("Restore transaction flags must be booleans.")
        for key in ("target_folder", "source_folder"):
            name = require_text(transaction[key], key)
            if (
                name in (".", "..")
                or Path(name).name != name
                or any(character in name for character in '/\\:*?"<>|')
            ):
                raise ValueError("Unsafe transaction folder.")
        target = self._project_root / "experiments" / transaction["target_folder"]
        work = self._project_root / "controller/restores" / restoration_id
        if (
            target.resolve() != state.experiment_directory.resolve()
            or transaction["experiment_id"] != state.experiment_id
            or not work.resolve().is_relative_to(self._project_root)
        ):
            raise ValueError("Restore transaction belongs to another experiment.")
        if transaction["phase"] not in (
            "staging",
            "prepared",
            "files_installed",
            "journal_restored",
            "services_starting",
            "complete",
            "failed",
        ):
            raise ValueError("Unknown restoration phase.")
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
        if validate_only:
            return state
        if transaction["phase"] in ("services_starting", "failed"):
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
            for definition in state.template["services"]:
                service_id = definition["service_id"]
                endpoint = target / "runner/endpoints" / f"{service_id}.json"
                if not endpoint.is_file():
                    continue
                announced = read_json(endpoint)
                current = state.services.get(service_id)
                if (
                    current is not None
                    and announced.get("participant_instance_id")
                    == current.service_instance_id
                ):
                    continue
                instance_id = str(UUID(announced["participant_instance_id"]))
                artifacts = (
                    target / "shared_artifacts/services" / service_id / instance_id
                )
                if (
                    not artifacts.resolve().is_relative_to(target)
                    or not (artifacts / "process.json").is_file()
                ):
                    raise RuntimeError(
                        "An unaccounted-for restored service cannot be confirmed stopped."
                    )
                recorded = read_json(artifacts / "process.json")
                identity = {
                    "experiment_id": state.experiment_id,
                    "participant_id": service_id,
                    "participant_instance_id": instance_id,
                }
                if any(
                    announced.get(key) != value or recorded.get(key) != value
                    for key, value in identity.items()
                ) or recorded.get("process") != announced.get("process"):
                    raise RuntimeError("Restored service ownership cannot be verified.")
                instance = ServiceInstance(service_id, instance_id, definition)
                instance.process_identity = recorded["process"]
                instance.endpoint_path = endpoint
                instance.artifacts_directory = artifacts
                metadata = self._assembler.read_module(
                    target
                    / "modules"
                    / definition["module"]["name"]
                    / definition["module"]["version"]
                )
                instance.implementation = metadata["implementation"]
                state.services[service_id] = instance
            stops = await self._services.stop_all(state)
            transaction["stopped_state"] = state_to_document(state)
            if any(not item["stopped"] or item["error"] for item in stops.values()):
                transaction["phase"] = "failed"
                write_json(marker, transaction)
                raise RuntimeError(
                    "Interrupted restoration participants have not stopped cleanly."
                )
            await self._services.reset(state)
            transaction["phase"] = "failed"
            write_json(marker, transaction)
            if retry_failed:
                return state
            raise RuntimeError(
                "Service restoration was interrupted; request a fresh rollback."
            )
        stopped = state_from_document(target, transaction["stopped_state"])
        if (
            stopped.experiment_id != transaction["experiment_id"]
            or stopped.active_attempt is not None
            or any(not item.stopped for item in stopped.services.values())
        ):
            raise ValueError("Restore marker does not confirm the participant barrier.")
        if transaction["phase"] != "complete":
            await self._services.reset(stopped)
            transaction["owner"] = process_identity(os.getpid())
            write_json(marker, transaction)
        cached = work / "snapshot"
        replacement = work / "replacement"
        previous = work / "previous"
        if transaction["phase"] == "staging":
            archive = (
                self._project_root
                / "snapshots"
                / transaction["source_folder"]
                / snapshot_id
            )
            manifest = await asyncio.to_thread(self._validate_snapshot, archive)
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
            await asyncio.to_thread(self._validate_snapshot, cached)
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
            document = copy_json_object(manifest["state"], "restored state")
            document["experiment_id"] = state.experiment_id
            document["run_id"] = transaction["run_id"]
            document["template_path"] = "experiment.yaml"
            document["mode"], document["phase"], document["pause_requested"] = (
                "paused",
                "restoring",
                False,
            )
            document["checkpoint_id"] = None
            document["owner_identity"] = None
            document["used_request_ids"] = sorted(
                set(document["used_request_ids"])
                | set(transaction["stopped_state"]["used_request_ids"])
            )
            for stage_id in document["stage_result_ids"]:
                document["stage_result_origins"].setdefault(
                    stage_id, manifest["experiment_id"]
                )
            for instance in document["services"].values():
                instance.update(
                    process_identity=None,
                    ready=False,
                    ever_ready=False,
                    started_at=None,
                    start_deadline=None,
                    last_status=None,
                    stopping=False,
                    stopped=True,
                    blocked_action=None,
                    failure=None,
                    freeze_id=None,
                    prepared_freeze_id=None,
                    active_request=None,
                    pending_requests=[],
                )
            restored = state_from_document(replacement, document)
            self._state_store.save(restored)
            write_json(
                replacement / "runner/journal.json",
                {key: manifest["journal"][key] for key in ("journal_id", "generation")},
            )
            transaction["phase"] = "prepared"
            write_json(marker, transaction)
        manifest = await asyncio.to_thread(self._validate_snapshot, cached)
        if (
            manifest["snapshot_id"] != snapshot_id
            or manifest["experiment_folder"] != transaction["source_folder"]
            or (
                not transaction["clone"]
                and manifest["experiment_id"] != transaction["experiment_id"]
            )
        ):
            raise ValueError("Cached restoration snapshot has a different identity.")
        if transaction["phase"] == "prepared":
            if not previous.exists():
                if not target.is_dir() or not replacement.is_dir():
                    raise RuntimeError(
                        "Prepared restoration is missing its source or replacement."
                    )
                # Read-only dashboard clients open the source briefly. Give them
                # a bounded opportunity to release it, without bypassing the
                # barrier for a persistently open external journal on Windows.
                deadline = time.monotonic() + 1
                while True:
                    try:
                        target.replace(previous)
                        break
                    except OSError as error:
                        if (
                            os.name != "nt"
                            or getattr(error, "winerror", None) not in (5, 32, 33)
                            or time.monotonic() >= deadline
                        ):
                            raise
                        await asyncio.sleep(0.01)
            if not target.exists():
                if not replacement.is_dir():
                    raise RuntimeError(
                        "Restoration replacement is missing after displacement."
                    )
                deadline = time.monotonic() + 1
                while True:
                    try:
                        replacement.replace(target)
                        break
                    except OSError as error:
                        if (
                            os.name != "nt"
                            or getattr(error, "winerror", None) not in (5, 32, 33)
                            or time.monotonic() >= deadline
                        ):
                            raise
                        await asyncio.sleep(0.01)
            elif replacement.exists():
                raise RuntimeError(
                    "Ambiguous restoration directories; no files were overwritten."
                )
            transaction["phase"] = "files_installed"
            write_json(marker, transaction)
        restored = self._state_store.load(target)
        if restored.experiment_id != transaction["experiment_id"]:
            raise ValueError("Installed state has another experiment identity.")
        vars(state).update(vars(restored))
        if transaction["phase"] == "files_installed":
            self._journal.complete_restore(
                state, manifest["journal"], restoration_id, work / "diagnostics"
            )
            transaction["phase"] = "journal_restored"
            write_json(marker, transaction)
        elif transaction["phase"] == "journal_restored":
            self._journal.open(state, create=False)
        if transaction["phase"] == "complete":
            return state
        transaction["phase"] = "services_starting"
        write_json(marker, transaction)
        try:
            if await self._services.reconcile(state, state.template) != "ready":
                raise RuntimeError("Restored services did not become ready.")
            await self._services.load_states(
                state, {key: Path(value) for key, value in manifest["services"].items()}
            )
            state.phase, state.mode = "waiting", "paused"
            self._journal.client.record_event(
                "experiment.restored",
                {
                    "snapshot_id": snapshot_id,
                    "restoration_id": restoration_id,
                    "source_experiment_id": manifest["experiment_id"],
                },
                context={"experiment_id": state.experiment_id, "run_id": state.run_id},
            )
            self._state_store.save(state)
            transaction["phase"] = "complete"
            write_json(marker, transaction)
        except BaseException as error:
            try:
                await self._services.stop_all(state)
            except Exception as shutdown_error:  # noqa: BLE001 - Preserve the restore failure and shutdown diagnostics.
                error.add_note(f"Restored service shutdown failed: {shutdown_error}")
            transaction["phase"] = "failed"
            write_json(marker, transaction)
            raise
        if not transaction["preserve_diagnostics"]:
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
        return state
