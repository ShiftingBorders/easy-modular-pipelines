"""One controller process and two queues behind the independent HTTP server."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import multiprocessing
import os
import signal
import tempfile
import threading
import time
import tomllib
from collections import OrderedDict
from contextlib import ExitStack
from datetime import UTC, datetime
from multiprocessing.process import BaseProcess
from multiprocessing.queues import Queue
from pathlib import Path
from queue import Empty, Full
from typing import BinaryIO, Self
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from core.logger_utils.events import copy_json_object, require_number, require_text
from core.runner_utils.runtimeio import read_json, write_json
from core.runner_utils.state import JsonObject

DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "default_settings/webserver.json"
SETTING_FIELDS = frozenset(
    {
        "schema_version",
        "project_root",
        "hash_config_path",
        "server_mode",
        "filer_url",
        "seaweed_config_path",
        "seaweed_min_free_gb",
        "resource_config_path",
        "archive_config_path",
        "host",
        "port",
        "token_env",
        "startup_timeout_seconds",
        "shutdown_timeout_seconds",
        "read_timeout_seconds",
        "result_ttl_seconds",
        "max_pending_commands",
        "max_read_requests",
        "max_command_records",
        "max_request_bytes",
        "max_response_bytes",
        "max_cached_result_bytes",
    }
)


class ServerError(Exception):
    """A transport/lifecycle failure, distinct from a completed controller response."""

    def __init__(self, code: str, message: str, status: int = 503) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


def integer_setting(document: JsonObject, name: str, minimum: int = 1) -> int:
    value = document[name]
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}.")
    return value


class ServerSettings:
    """Validated startup values; construction never starts runtime components."""

    def __init__(self, document: JsonObject) -> None:
        if document.keys() != SETTING_FIELDS:
            raise ValueError("Server settings have missing or unknown fields.")
        self.project_root = Path(
            require_text(document["project_root"], "project_root (required)")
        )
        self.default_hash_config = document["hash_config_path"] is None
        self.hash_config_path = (
            self.project_root / "hash_db/config.json"
            if self.default_hash_config
            else Path(require_text(document["hash_config_path"], "hash_config_path"))
        )
        self.server_mode = require_text(document["server_mode"], "server_mode")
        if self.server_mode not in ("run", "maintenance"):
            raise ValueError("server_mode must be run or maintenance.")
        self.seaweed_config_path = Path(
            require_text(document["seaweed_config_path"], "seaweed_config_path")
        )
        self.seaweed_min_free_gb = require_number(
            document["seaweed_min_free_gb"], "seaweed_min_free_gb"
        )
        if self.seaweed_min_free_gb <= 0:
            raise ValueError("seaweed_min_free_gb must be positive.")
        self.resource_config_path = Path(
            require_text(document["resource_config_path"], "resource_config_path")
        )
        self.archive_config_path = Path(
            require_text(document["archive_config_path"], "archive_config_path")
        )
        for path in (
            self.project_root,
            self.hash_config_path,
            self.resource_config_path,
            self.archive_config_path,
            self.seaweed_config_path,
        ):
            if not path.is_absolute():
                raise ValueError(
                    "Server settings paths must be absolute after resolution."
                )
        self.filer_url = (
            None
            if document["filer_url"] is None
            else require_text(document["filer_url"], "filer_url")
        )
        url = urlsplit(self.filer_url or "http://127.0.0.1")
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "filer_url must be an HTTP(S) URL without credentials, query or fragment."
            )
        self.host = require_text(document["host"], "host")
        if url.port is not None and not 1 <= url.port <= 65535:
            raise ValueError("Invalid filer_url port.")
        self.port = integer_setting(document, "port")
        if self.port > 65535:
            raise ValueError("port must not exceed 65535.")
        self.token_env = (
            None
            if document["token_env"] is None
            else require_text(document["token_env"], "token_env")
        )
        self.startup_timeout = require_number(
            document["startup_timeout_seconds"], "startup_timeout_seconds"
        )
        self.shutdown_timeout = require_number(
            document["shutdown_timeout_seconds"], "shutdown_timeout_seconds"
        )
        self.read_timeout = require_number(
            document["read_timeout_seconds"], "read_timeout_seconds"
        )
        self.result_ttl = require_number(
            document["result_ttl_seconds"], "result_ttl_seconds"
        )
        if (
            min(
                self.startup_timeout,
                self.shutdown_timeout,
                self.read_timeout,
                self.result_ttl,
            )
            <= 0
        ):
            raise ValueError("Server timeouts and result TTL must be positive.")
        self.max_pending = integer_setting(document, "max_pending_commands")
        self.max_reads = integer_setting(document, "max_read_requests")
        self.max_records = integer_setting(document, "max_command_records")
        self.max_request_bytes = integer_setting(document, "max_request_bytes")
        self.max_response_bytes = integer_setting(document, "max_response_bytes")
        self.max_cache_bytes = integer_setting(document, "max_cached_result_bytes")
        if self.max_records <= self.max_pending:
            raise ValueError("max_command_records must leave room for a priority stop.")
        if self.max_cache_bytes < self.max_response_bytes:
            raise ValueError("max_cached_result_bytes must be >= max_response_bytes.")
        if integer_setting(document, "schema_version") != 1:
            raise ValueError("Only server settings schema_version 1 is supported.")


def load_server_settings(
    config_path: Path | None = None, overrides: JsonObject | None = None
) -> ServerSettings:
    """Resolve each configured path against the file which actually supplied it."""
    files = [DEFAULT_CONFIG]
    if config_path is not None:
        config_path = Path(config_path)
        if not config_path.is_absolute():
            raise ValueError("config_path must be absolute.")
        if config_path.resolve() != DEFAULT_CONFIG:
            files.append(config_path)
    settings: JsonObject = {}
    fields = SETTING_FIELDS
    path_fields = {
        "project_root",
        "hash_config_path",
        "resource_config_path",
        "archive_config_path",
        "seaweed_config_path",
    }
    for path in files:
        values = read_json(path)
        if values.keys() - fields:
            raise ValueError(
                f"Unknown server settings: {sorted(values.keys() - fields)}"
            )
        for name in path_fields & values.keys():
            if values[name] is not None:
                configured = Path(require_text(values[name], name))
                if not configured.is_absolute() and (
                    configured.drive or configured.root
                ):
                    raise ValueError(f"Ambiguous configured path: {name}")
                values[name] = str((path.parent / configured).resolve())
        settings.update(values)
    if overrides:
        if overrides.keys() - fields:
            raise ValueError("Unknown server setting override.")
        for name in path_fields & overrides.keys():
            if not Path(require_text(overrides[name], name)).is_absolute():
                raise ValueError("Explicit path overrides must be absolute.")
        settings.update(overrides)
    return ServerSettings(settings)


class ProjectLock:
    """Hold an OS lock in the controller process, including during parent failure."""

    def __init__(self, root: Path) -> None:
        self.path = root / "controller/server.lock"
        self.stream: BinaryIO | None = None

    def __enter__(self) -> Self:
        for path in (self.path, *self.path.parents):
            if path.is_symlink() or path.is_junction():
                raise ValueError("The server lock must not traverse filesystem links.")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        stream = self.path.open("a+b")
        try:
            if stream.seek(0, os.SEEK_END) == 0:
                stream.write(b"\0")
                stream.flush()
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            stream.close()
            raise
        self.stream = stream
        return self

    def __exit__(self, *_error: object) -> None:
        if self.stream is not None:
            # Closing releases the OS lock even when cleanup raised an exception.
            self.stream.close()
            self.stream = None


def write_initial_config(path: Path, text: str) -> None:
    """Publish a complete initial file without replacing a user's existing file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".prepare-", dir=path.parent) as work:
        staged = Path(work) / "config"
        with staged.open("w", encoding="utf-8", newline="\n") as output:
            output.write(text)
            output.flush()
            os.fsync(output.fileno())
        os.link(staged, path)


def prepare_work_directory(settings: ServerSettings) -> tuple[Path, Path | None]:
    """Prepare missing project files under the caller's ProjectLock, before opening DBs."""
    from utils.hashdb_utils.dataclasses import HashDBConfig
    from utils.seaweed_utils.dataclasses import SeaWeedConfig

    expected_schema = read_json(DEFAULT_CONFIG.with_name("hash_db_schema.json"))
    if list(expected_schema.items()) != [
        ("Mname", "VARCHAR(255) NOT NULL"),
        ("MVersion", "VARCHAR(255) NOT NULL"),
        ("MHash", "VARCHAR(255) NOT NULL"),
    ]:
        raise ValueError("The bundled HashDB schema is invalid.")
    config_path = settings.hash_config_path
    new_config = not config_path.exists()
    if new_config:
        if not settings.default_hash_config:
            raise FileNotFoundError(
                f"Explicit HashDB configuration does not exist: {config_path}"
            )
        db_path = config_path.parent / "modules.db"
        schema_path = config_path.parent / "schema.json"
        if db_path.exists():
            raise ValueError(
                "HashDB exists without its configuration; restore config.json first."
            )
    else:
        document = read_json(config_path)
        for key in ("schema_path", "db_path"):
            configured = config_path.parent / Path(require_text(document.get(key), key))
            for path in (configured, *configured.parents):
                if path.is_symlink() or path.is_junction():
                    raise ValueError(
                        f"HashDB paths must not traverse filesystem links: {path}"
                    )
            document[key] = str(configured.resolve())
        validated = HashDBConfig.model_validate(document)
        db_path, schema_path = validated.db_path, validated.schema_path
    if schema_path.exists() and list(read_json(schema_path).items()) != list(
        expected_schema.items()
    ):
        raise ValueError(f"Incompatible HashDB schema: {schema_path}")
    if not new_config and not schema_path.is_file():
        raise FileNotFoundError(f"Missing HashDB schema: {schema_path}")
    seaweed_root = None
    seaweed_document = None
    if settings.filer_url is None:
        seaweed_root = db_path.parent.parent / "seaweedfs"
        if (
            seaweed_root == db_path.parent
            or seaweed_root in db_path.parent.parents
            or db_path.parent in seaweed_root.parents
        ):
            raise ValueError(
                "HashDB and SeaweedFS must use distinct sibling directories."
            )
        seaweed_config = seaweed_root / "config/seaweed.json"
        if seaweed_config.exists():
            seaweed_document = read_json(seaweed_config)
        else:
            seaweed_document = read_json(settings.seaweed_config_path)
            for key in ("dir", "master.dir", "volume.dir.idx"):
                if key in seaweed_document["start_args"]:
                    raise ValueError(
                        f"Local storage manages {key}; remove it from SeaweedFS defaults."
                    )
            seaweed_document["start_args"]["master.dir"] = "../master"
        SeaWeedConfig.model_validate(seaweed_document)
        if seaweed_document["start_args"].get("master.dir") != "../master":
            raise ValueError("Local SeaweedFS requires master.dir=../master.")
        if any(
            key in seaweed_document["start_args"] for key in ("dir", "volume.dir.idx")
        ):
            raise ValueError("Local SeaweedFS volume paths are managed by the server.")
        filer_config = seaweed_root / "config/filer.toml"
        if filer_config.exists():
            filer = tomllib.loads(filer_config.read_text(encoding="utf-8"))
            if filer != {"leveldb2": {"enabled": True, "dir": "../filer"}}:
                raise ValueError(
                    "Local filer.toml must use only leveldb2 with dir=../filer."
                )
    paths = [
        config_path,
        schema_path,
        db_path,
        settings.project_root / "modules",
        settings.project_root / "experiments",
        settings.project_root / "controller/module_work",
    ]
    if seaweed_root is not None:
        paths.extend(
            seaweed_root / name
            for name in (
                "master",
                "volume",
                "filer",
                "config/seaweed.json",
                "config/filer.toml",
            )
        )
    for target in paths:
        for path in (target, *target.parents):
            if path.is_symlink() or path.is_junction():
                raise ValueError(
                    f"Project storage must not traverse filesystem links: {path}"
                )
    # All configuration checks precede new persistent configuration/data files.
    for name in ("modules", "experiments", "controller/module_work"):
        (settings.project_root / name).mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if new_config:
        if not schema_path.exists():
            write_initial_config(
                schema_path, json.dumps(expected_schema, indent=2) + "\n"
            )
        write_initial_config(
            config_path,
            json.dumps(
                {
                    "schema_path": "schema.json",
                    "db_path": "modules.db",
                },
                indent=2,
            )
            + "\n",
        )
    if seaweed_root is not None:
        for name in ("config", "master", "volume", "filer"):
            (seaweed_root / name).mkdir(parents=True, exist_ok=True)
        if not seaweed_config.exists():
            write_initial_config(
                seaweed_config, json.dumps(seaweed_document, indent=2) + "\n"
            )
        if not filer_config.exists():
            write_initial_config(
                filer_config, '[leveldb2]\nenabled = true\ndir = "../filer"\n'
            )
    return db_path, seaweed_root


def recovery_candidates(project_root: Path) -> list[str]:
    """A new owner must explicitly recover unfinished or unreadable previous runs."""
    import psutil

    from core.experimentassembler import find_experiment
    from core.runner_utils.runtimeio import process_identity
    from core.runner_utils.state import RunnerStateStore

    registry = project_root / "experiments.json"
    if not registry.exists():
        return []
    candidates = []
    local = process_identity(os.getpid())
    for experiment_id in read_json(registry):
        try:
            root = find_experiment(project_root, experiment_id)
            transaction = (
                project_root / "controller/restore_transactions" / f"{root.name}.json"
            )
            if transaction.exists() and read_json(transaction).get("phase") not in (
                "complete",
                "failed",
            ):
                candidates.append(experiment_id)
                continue
            if (
                not (root / "runner/state.json").exists()
                and not (root / "journals/events.sqlite").exists()
                and not (root / "executor.lock.json").exists()
                and not any((root / "shared_artifacts").rglob("process.json"))
            ):
                # Assembler-only drafts have never started an executor or services.
                continue
            state = RunnerStateStore().load(root)
            if (
                state.phase == "idle"
                and state.owner_identity is None
                and state.active_attempt is None
                and not state.services
            ):
                continue
            if (
                state.phase in ("stopped", "completed", "failed")
                and state.active_attempt is None
                and all(instance.stopped for instance in state.services.values())
            ):
                identities = [
                    state.owner_identity,
                    *(
                        instance.process_identity
                        for instance in state.services.values()
                    ),
                ]
                lock = root / "executor.lock.json"
                if lock.exists():
                    identities.append(
                        copy_json_object(
                            read_json(lock).get("executor"), "executor identity"
                        )
                    )
                alive = False
                for identity in identities:
                    if identity is None:
                        continue
                    if identity["host_id"] != local["host_id"]:
                        alive = True
                        break
                    if identity["boot_id"] != local["boot_id"]:
                        continue
                    pid = identity["pid"]
                    if type(pid) is not int or pid <= 0:
                        raise ValueError("Invalid saved process identity.")
                    try:
                        process = psutil.Process(pid)
                        if process_identity(pid) == identity:
                            process.wait(timeout=0)
                    except (
                        psutil.NoSuchProcess,
                        ProcessLookupError,
                        FileNotFoundError,
                    ):
                        continue
                    except psutil.TimeoutExpired:
                        alive = True
                        break
                if not alive:
                    continue
        except (OSError, ValueError, TypeError, KeyError, psutil.Error):
            pass
        candidates.append(experiment_id)
    return candidates


async def controller_main(
    settings: ServerSettings, requests: Queue, responses: Queue, instance_id: str
) -> None:
    """Create all thread-bound dependencies inside their owning process/event loop."""
    from core.experimentcontroller import ExperimentController
    from core.hashdb import HashDB
    from core.logger import OperationLogger
    from core.maintenancecontroller import MaintenanceController
    from core.modulemanager import ModuleManager
    from core.runner_utils.experimentrunner import ExperimentRunner
    from core.runner_utils.runtimeio import process_identity
    from core.seaweed import SeaweedDB
    from core.seaweed_process import SeaweedProcess

    with ExitStack() as stack:
        stack.enter_context(ProjectLock(settings.project_root))
        db_path, seaweed_root = prepare_work_directory(settings)
        directory = settings.project_root / "controller/server" / instance_id
        logging_settings = read_json(DEFAULT_CONFIG.with_name("logging.json"))
        logging_settings.update(
            {
                "db_path": str(directory / "events.sqlite"),
                "open_mode": "create",
                "expected_journal": None,
            }
        )
        logging_config = directory / "logging.json"
        write_json(
            logging_config,
            {
                "logging": logging_settings,
                "operation_context": {"source": "server_controller"},
            },
        )
        logger = stack.enter_context(OperationLogger(logging_config))
        identity = logger.get_journal_info()
        logging_settings.update(
            {
                "open_mode": "existing",
                "expected_journal": {
                    key: identity[key] for key in ("journal_id", "generation")
                },
            }
        )
        write_json(
            directory / "reader.json",
            {
                "logging": logging_settings,
                "operation_context": {"source": "server_controller"},
            },
        )
        try:
            process_resources = stack.enter_context(ExitStack())
            hashes = HashDB(settings.hash_config_path)
            stack.callback(hashes.close_connection)
            if seaweed_root is not None:
                seaweed = SeaweedProcess(
                    seaweed_root / "volume",
                    settings.seaweed_min_free_gb,
                    seaweed_root / "config/seaweed.json",
                )
                process_resources.callback(seaweed.stop)
                await asyncio.to_thread(seaweed.start)
                storage = SeaweedDB(
                    seaweed.filer_url,
                    max_archive_gb=seaweed.max_archive_gb,
                    before_upload=seaweed.check_upload_space,
                )
            else:
                storage = SeaweedDB(settings.filer_url)
            stack.callback(storage.close)
            # A reachable storage service may legitimately have no modules yet.
            await asyncio.to_thread(storage.check_module_stored, "startup_probe", "1")
            manager = ModuleManager(
                settings.project_root / "modules",
                hashes,
                storage,
                settings.project_root / "controller/module_work",
            )
            shutdown_requested = asyncio.Event()
            runner = None
            if settings.server_mode == "maintenance":
                controller = MaintenanceController(
                    manager,
                    logger,
                    requests,
                    responses,
                    project_root=settings.project_root,
                    shutdown_requested=shutdown_requested,
                    recovery_required=recovery_candidates(settings.project_root),
                )
            else:
                runner = ExperimentRunner(
                    settings.project_root,
                    manager,
                    archive_config_path=settings.archive_config_path,
                )
                controller = ExperimentController(
                    settings.project_root,
                    runner,
                    requests,
                    responses,
                    module_manager=manager,
                    resource_config_path=settings.resource_config_path,
                    shutdown_requested=shutdown_requested,
                    recovery_required=recovery_candidates(settings.project_root),
                )
            serving = asyncio.create_task(controller.serve())
            try:
                await asyncio.sleep(0)
                if serving.done():
                    await serving
                    raise RuntimeError("Controller stopped during startup.")
                identity = process_identity(os.getpid())
                logger.record_event(
                    "server.controller_started",
                    {"server_instance_id": instance_id, "process": identity},
                )
                responses.put_nowait(
                    {
                        "_runtime": "ready",
                        "process": identity,
                        "storage": {
                            "initialized": True,
                            "hash_db_path": str(db_path),
                            "seaweed_path": None
                            if seaweed_root is None
                            else str(seaweed_root),
                            "filer_url": seaweed.filer_url
                            if seaweed_root is not None
                            else settings.filer_url,
                            "checked_at": datetime.now(UTC).isoformat(),
                        },
                    }
                )
                parent = multiprocessing.parent_process()
                while not shutdown_requested.is_set():
                    if serving.done():
                        await serving
                        raise RuntimeError("Controller stopped unexpectedly.")
                    if parent is not None and not parent.is_alive():
                        logger.record_event("server.parent_lost", {})
                        break
                    await asyncio.sleep(0.1)
            finally:
                try:
                    await controller.close()
                finally:
                    try:
                        if runner is not None:
                            await runner.stop()
                    finally:
                        if runner is not None:
                            await runner.close()
                        await asyncio.gather(serving, return_exceptions=True)
            logger.record_event("server.controller_stopped", {})
            responses.put_nowait({"_runtime": "stopped"})
        except Exception as error:
            try:
                logger.record_error(error, include_traceback=True)
            except Exception as logging_error:  # noqa: BLE001 - Preserve the startup/shutdown failure.
                error.add_note(f"Server lifecycle logging also failed: {logging_error}")
            raise


def controller_process(
    settings: ServerSettings, requests: Queue, responses: Queue, instance_id: str
) -> None:
    """Spawn entry point; only the two queues cross the application boundary."""
    # The HTTP owner handles console Ctrl+C and requests an orderly shutdown
    # through the queue; the child must not race that request on Windows.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        asyncio.run(controller_main(settings, requests, responses, instance_id))
    except BaseException as error:
        try:
            responses.put(
                {"_runtime": "error", "message": f"{type(error).__name__}: {error}"},
                timeout=1,
            )
        except Full:
            pass
        raise
    finally:
        requests.close()
        responses.close()
        parent = multiprocessing.parent_process()
        if parent is not None and parent.is_alive():
            responses.join_thread()
        else:
            responses.cancel_join_thread()


class CommandRecord:
    def __init__(self, command: JsonObject) -> None:
        encoded = json.dumps(
            command, sort_keys=True, ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        self.fingerprint = hashlib.sha256(encoded).hexdigest()
        self.chain_id = command.get("chain_id")
        self.command = command["command"]
        self.is_stop = command["command"] == "stop" and self.chain_id is None
        self.submitted_at = datetime.now(UTC).isoformat()
        self.finished_at: float | None = None
        self.response: JsonObject | None = None
        self.size = 0


class ServerRuntime:
    """Server-owned process and bounded command results; clients never own either."""

    def __init__(self, settings: ServerSettings) -> None:
        self.settings = settings
        self.instance_id = str(uuid4())
        self._process: BaseProcess | None = None
        self._requests: Queue | None = None
        self._responses: Queue | None = None
        self._watcher: asyncio.Task | None = None
        self._reader_thread: threading.Thread | None = None
        self._reader_stop = threading.Event()
        self._ready: asyncio.Future | None = None
        self._records: OrderedDict[str, CommandRecord] = OrderedDict()
        self._chains: dict[str, list[str]] = {}
        self._reads: dict[str, asyncio.Future[JsonObject]] = {}
        self._cache_bytes = 0
        self._state = "new"
        self._error: str | None = None
        self._closing = False
        self._last_response_at: str | None = None
        self._identity: JsonObject | None = None
        self._storage: JsonObject | None = None

    async def start(self) -> None:
        if self._state != "new":
            raise RuntimeError("Server runtime has already started.")
        self._state = "starting"
        context = multiprocessing.get_context("spawn")
        capacity = self.settings.max_pending + self.settings.max_reads + 4
        self._requests = context.Queue(capacity)
        self._responses = context.Queue(capacity)
        self._ready = asyncio.get_running_loop().create_future()
        self._process = context.Process(
            target=controller_process,
            args=(self.settings, self._requests, self._responses, self.instance_id),
            name="experiment-controller",
        )
        try:
            self._process.start()
            # A killed queue writer can leave get() stuck inside a partial frame.
            # Keep that reader out of asyncio's executor so it cannot hold HTTP
            # shutdown hostage; the independent watchdog still observes exit.
            self._reader_thread = threading.Thread(
                target=self._read_responses,
                args=(asyncio.get_running_loop(),),
                name="controller-response-reader",
                daemon=True,
            )
            self._reader_thread.start()
            self._watcher = asyncio.create_task(self._watch_process())
            await asyncio.wait_for(
                asyncio.shield(self._ready), self.settings.startup_timeout
            )
        except BaseException as error:
            if not self._ready.done():
                self._ready.cancel()
            try:
                await self.close()
            except Exception as cleanup_error:  # noqa: BLE001 - Preserve the startup failure.
                error.add_note(f"Runtime cleanup also failed: {cleanup_error}")
            raise

    def health(self) -> JsonObject:
        alive = self._process is not None and self._process.is_alive()
        return {
            "server_instance_id": self.instance_id,
            "server_mode": self.settings.server_mode,
            "storage_initialization": self._storage,
            "state": self._state,
            "controller_alive": alive,
            "controller": self._identity,
            "last_response_at": self._last_response_at,
            "pending_commands": sum(
                record.response is None for record in self._records.values()
            ),
            "error": self._error,
        }

    def _require_ready(self) -> Queue:
        if (
            self._closing
            or self._state != "ready"
            or self._process is None
            or not self._process.is_alive()
            or self._requests is None
        ):
            raise ServerError(
                "controller_unavailable", self._error or "Controller is not available."
            )
        return self._requests

    def _command(self, value: object, *, chain_id: str | None = None) -> JsonObject:
        command = copy_json_object(value, "command")
        if command.keys() - {"api_version", "command_id", "command", "args", "target"}:
            raise ValueError("Unknown command fields.")
        version = command.get("api_version", 1)
        if type(version) is not int or version != 1:
            raise ValueError("Only api_version 1 is supported.")
        name = require_text(command.get("command"), "command")
        if name.startswith("module.") and self.settings.server_mode != "maintenance":
            raise ServerError(
                "invalid_mode", "Module commands require --mode maintenance.", 409
            )
        if name.startswith("server."):
            raise ValueError("Server lifecycle messages are not public commands.")
        command["api_version"] = 1
        command["command_id"] = str(
            UUID(require_text(command.get("command_id", str(uuid4())), "command_id"))
        )
        command["args"] = copy_json_object(command.get("args", {}), "args")
        target = copy_json_object(command.get("target", {}), "target")
        if target:
            if target.keys() != {"kind", "position"} or target["kind"] not in (
                "stage",
                "service",
            ):
                raise ValueError("target requires kind=stage|service and position.")
            integer_setting(target, "position")
            command["target"] = target
        else:
            command.pop("target", None)
        if chain_id is not None:
            command["chain_id"] = chain_id
        return command

    def submit(self, document: object, *, chain: bool = False) -> JsonObject:
        """Validate and enqueue without awaiting: disconnect cannot split admission."""
        requests = self._require_ready()
        self._prune()
        chain_id = None
        if chain:
            value = copy_json_object(document, "chain")
            if value.keys() - {"api_version", "chain_id", "commands"}:
                raise ValueError("Unknown chain fields.")
            version = value.get("api_version", 1)
            if type(version) is not int or version != 1:
                raise ValueError("Only api_version 1 is supported.")
            chain_id = str(
                UUID(require_text(value.get("chain_id", str(uuid4())), "chain_id"))
            )
            entries = value.get("commands")
            if not isinstance(entries, list) or not entries:
                raise ValueError("commands must be a nonempty array.")
            commands = [self._command(item, chain_id=chain_id) for item in entries]
            message = copy_json_object(
                {"api_version": 1, "chain_id": chain_id, "commands": commands}, "chain"
            )
        else:
            message = self._command(document)
            commands = [message]
        if (
            len(json.dumps(message, ensure_ascii=False).encode("utf-8"))
            > self.settings.max_request_bytes
        ):
            raise ServerError(
                "request_too_large",
                "Command envelope exceeds its configured limit.",
                413,
            )
        identifiers = [
            require_text(command["command_id"], "command_id") for command in commands
        ]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("Command IDs in a chain must be distinct.")
        if any(identifier in self._reads for identifier in identifiers):
            raise ServerError(
                "command_id_conflict",
                "A command ID is already used by an active read request.",
                409,
            )
        if (
            chain_id is not None
            and chain_id in self._chains
            and self._chains[chain_id] != identifiers
        ):
            raise ServerError(
                "chain_id_conflict",
                "The retained chain ID has a different command order or membership.",
                409,
            )
        records = [CommandRecord(command) for command in commands]
        existing = [identifier in self._records for identifier in identifiers]
        if any(existing):
            if not all(existing) or any(
                self._records[key].fingerprint != record.fingerprint
                for key, record in zip(identifiers, records, strict=True)
            ):
                raise ServerError(
                    "command_id_conflict",
                    "A retained command ID has a different request.",
                    409,
                )
            return self._receipt(identifiers, chain_id)
        if chain_id is not None and chain_id in self._chains:
            raise ServerError(
                "chain_id_conflict",
                "The retained chain ID already identifies another chain.",
                409,
            )
        pending = [
            record for record in self._records.values() if record.response is None
        ]
        priority_stop = not chain and commands[0]["command"] == "stop"
        if priority_stop and any(record.is_stop for record in pending):
            raise ServerError(
                "stop_pending", "A standalone stop is already pending.", 409
            )
        if len(pending) + len(commands) > self.settings.max_pending + int(
            priority_stop
        ):
            raise ServerError(
                "queue_full",
                "Too many pending commands; a standalone stop remains available.",
                429,
            )
        self._prune(required=len(commands))
        if len(self._records) + len(commands) > self.settings.max_records:
            raise ServerError(
                "queue_full", "The command record limit has been reached.", 429
            )
        try:
            requests.put_nowait(message)
        except Full as error:
            raise ServerError(
                "queue_full", "Controller request queue is full.", 429
            ) from error
        for identifier, record in zip(identifiers, records, strict=True):
            self._records[identifier] = record
        if chain_id is not None:
            self._chains[chain_id] = identifiers
        return self._receipt(identifiers, chain_id)

    def _receipt(self, identifiers: list[str], chain_id: str | None) -> JsonObject:
        if chain_id is None:
            return self.result(identifiers[0])
        return copy_json_object(
            {
                "server_instance_id": self.instance_id,
                "chain_id": chain_id,
                "commands": [self.result(key) for key in identifiers],
            },
            "receipt",
        )

    def result(self, command_id: str) -> JsonObject:
        identifier = str(UUID(command_id))
        self._prune()
        record = self._records.get(identifier)
        if record is None:
            raise ServerError(
                "unknown_command",
                "Command is unknown or expired in this server instance; do not infer that it never executed.",
                404,
            )
        response = record.response or {
            "command_id": identifier,
            "chain_id": record.chain_id,
            "state": "pending",
            "result": None,
            "experiment_id": None,
            "data": None,
            "error": None,
        }
        return {
            **response,
            "server_instance_id": self.instance_id,
            "submitted_at": record.submitted_at,
        }

    def list_commands(
        self,
        *,
        after: str | None = None,
        limit: int = 100,
        state: str | None = None,
        command: str | None = None,
    ) -> JsonObject:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000.")
        if state is not None and state not in (
            "pending",
            "succeeded",
            "failed",
            "cancelled",
            "unknown",
            "unavailable",
        ):
            raise ValueError("Unknown command state filter.")
        if command is not None:
            require_text(command, "command")
        self._prune()
        entries = list(self._records.items())
        if after is not None:
            after = str(UUID(after))
            identifiers = [identifier for identifier, _ in entries]
            if after not in identifiers:
                raise ServerError(
                    "unknown_command",
                    "List cursor is unknown or expired; restart pagination.",
                    404,
                )
            entries = entries[identifiers.index(after) + 1 :]
        items = []
        for identifier, record in entries:
            response = record.response or {}
            current_state = response.get("state", "pending")
            if state is not None and current_state != state:
                continue
            if command is not None and record.command != command:
                continue
            items.append(
                {
                    "command_id": identifier,
                    "command": record.command,
                    "state": current_state,
                    "chain_id": record.chain_id,
                    "submitted_at": record.submitted_at,
                    "experiment_id": response.get("experiment_id"),
                }
            )
            if len(items) > limit:
                break
        has_more = len(items) > limit
        items = items[:limit]
        return {
            "items": items,
            "has_more": has_more,
            "next_after": items[-1]["command_id"] if has_more else None,
            "server_instance_id": self.instance_id,
        }

    async def read(self, name: str, args: JsonObject | None = None) -> JsonObject:
        requests = self._require_ready()
        if not name.startswith(("stats.", "logs.")):
            raise ValueError("Only controller read commands use this channel.")
        if len(self._reads) >= self.settings.max_reads:
            raise ServerError(
                "too_many_reads", "Too many concurrent controller reads.", 429
            )
        identifier = str(uuid4())
        while identifier in self._records or identifier in self._reads:
            identifier = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        self._reads[identifier] = future
        try:
            requests.put_nowait(
                {
                    "api_version": 1,
                    "command_id": identifier,
                    "command": name,
                    "args": args or {},
                }
            )
            return await asyncio.wait_for(
                asyncio.shield(future), self.settings.read_timeout
            )
        except Full as error:
            raise ServerError(
                "queue_full", "Controller request queue is full.", 429
            ) from error
        except TimeoutError as error:
            raise ServerError(
                "controller_timeout",
                "Controller did not provide a fresh response before the deadline.",
                504,
            ) from error
        finally:
            self._reads.pop(identifier, None)
            if not future.done():
                future.cancel()

    def _read_responses(self, loop: asyncio.AbstractEventLoop) -> None:
        responses = self._responses
        if responses is None:
            return
        try:
            while not self._reader_stop.is_set():
                try:
                    message = responses.get(timeout=0.1)
                except Empty:
                    continue
                loop.call_soon_threadsafe(self._accept_response, message)
        except Exception as error:  # noqa: BLE001 - A broken IPC reader makes pending outcomes unknown.
            if not self._reader_stop.is_set():
                try:
                    loop.call_soon_threadsafe(
                        self._unavailable,
                        f"Controller response channel failed: {error}",
                    )
                except RuntimeError:
                    pass

    async def _watch_process(self) -> None:
        process = self._process
        if process is None:
            return
        while process.is_alive():
            await asyncio.sleep(0.1)
        self._unavailable(f"Controller exited with code {process.exitcode}.")

    def _accept_response(self, message: object) -> None:
        if self._state == "closed":
            return
        try:
            response = copy_json_object(message, "controller response")
            self._last_response_at = datetime.now(UTC).isoformat()
            kind = response.get("_runtime")
            if kind == "ready":
                if (
                    self._closing
                    or self._process is None
                    or not self._process.is_alive()
                ):
                    return
                self._identity = copy_json_object(
                    response.get("process"), "process identity"
                )
                self._storage = copy_json_object(
                    response.get("storage", {}), "storage initialization"
                )
                self._state = "ready"
                if self._ready is not None and not self._ready.done():
                    self._ready.set_result(None)
                return
            if kind in ("stopped", "error"):
                self._unavailable(str(response.get("message", "Controller stopped.")))
                if kind == "stopped":
                    self._state = "stopped"
                return
            identifier = str(
                UUID(require_text(response.get("command_id"), "command_id"))
            )
            state = response.get("state")
            expected = "success" if state == "succeeded" else "fail"
            if (
                state not in ("succeeded", "failed", "cancelled")
                or response.get("result") != expected
            ):
                raise ValueError("Invalid controller command outcome.")
            encoded_size = len(
                json.dumps(response, ensure_ascii=False, allow_nan=False).encode(
                    "utf-8"
                )
            )
            if encoded_size > self.settings.max_response_bytes:
                response = {
                    "command_id": identifier,
                    "chain_id": response.get("chain_id"),
                    "state": "unavailable",
                    "result": None,
                    "data": None,
                    "error": {
                        "code": "response_too_large",
                        "message": "Controller response exceeded its configured limit.",
                        "details": {
                            "command_state": response.get("state"),
                            "command_result": response.get("result"),
                        },
                    },
                }
                encoded_size = len(json.dumps(response).encode("utf-8"))
            waiter = self._reads.get(identifier)
            if waiter is not None and not waiter.done():
                waiter.set_result(response)
                return
            record = self._records.get(identifier)
            if record is not None and (
                record.response is None or record.response.get("state") == "unknown"
            ):
                # A complete queued reply is stronger evidence than an earlier
                # process-exit observation. Never replace an already known outcome.
                self._cache_bytes -= record.size
                record.response = response
                record.finished_at = time.monotonic()
                record.size = encoded_size
                self._cache_bytes += encoded_size
                self._prune()
        except Exception as error:  # noqa: BLE001 - Do not leave a failed response consumer reporting readiness.
            self._unavailable(f"Invalid controller response: {error}")

    def _unavailable(self, message: str) -> None:
        if self._state == "closed":
            return
        self._state = "unavailable"
        self._error = message
        if self._ready is not None and not self._ready.done():
            self._ready.set_exception(ServerError("controller_unavailable", message))
        for future in self._reads.values():
            if not future.done():
                future.set_exception(ServerError("controller_unavailable", message))
        for identifier, record in self._records.items():
            if record.response is None:
                record.response = {
                    "command_id": identifier,
                    "chain_id": record.chain_id,
                    "state": "unknown",
                    "result": None,
                    "data": None,
                    "error": {
                        "code": "command_outcome_unknown",
                        "message": message,
                        "details": {},
                    },
                }
                record.finished_at = time.monotonic()
                record.size = len(json.dumps(record.response).encode("utf-8"))
                self._cache_bytes += record.size
        self._prune()

    def _prune(self, *, required: int = 0) -> None:
        now = time.monotonic()
        finished = sorted(
            (
                (record.finished_at, identifier, record)
                for identifier, record in self._records.items()
                if record.finished_at is not None
            ),
            key=lambda item: item[0],
        )
        for finished_at, identifier, record in finished:
            if (
                now >= finished_at + self.settings.result_ttl
                or len(self._records) + required > self.settings.max_records
                or self._cache_bytes > self.settings.max_cache_bytes
            ):
                self._cache_bytes -= record.size
                del self._records[identifier]
        for chain_id, identifiers in list(self._chains.items()):
            if not any(identifier in self._records for identifier in identifiers):
                del self._chains[chain_id]

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        process = self._process
        forced = False
        exit_code = None
        try:
            if process is not None and process.pid is not None:
                if process.is_alive() and self._requests is not None:
                    try:
                        self._requests.put_nowait(
                            {
                                "api_version": 1,
                                "command_id": str(uuid4()),
                                "command": "server.shutdown",
                                "args": {},
                            }
                        )
                    except Full:
                        logging.getLogger(__name__).error(
                            "Controller queue is full during server shutdown."
                        )
                    await asyncio.to_thread(
                        process.join, self.settings.shutdown_timeout
                    )
                if process.is_alive():
                    forced = True
                    process.terminate()
                    await asyncio.to_thread(process.join, 5)
                    if process.is_alive():
                        process.kill()
                        await asyncio.to_thread(process.join, 5)
                else:
                    await asyncio.to_thread(process.join)
                exit_code = process.exitcode
            self._unavailable(
                "Server runtime is closed; unfinished command outcomes are unknown."
            )
        finally:
            if self._watcher is not None:
                self._watcher.cancel()
                await asyncio.gather(self._watcher, return_exceptions=True)
            self._reader_stop.set()
            if self._reader_thread is not None:
                await asyncio.to_thread(self._reader_thread.join, 1)
                if self._reader_thread.is_alive():
                    logging.getLogger(__name__).error(
                        "A corrupted IPC reader will be released when the HTTP process exits."
                    )
            for queue in (self._requests, self._responses):
                if queue is not None:
                    queue.cancel_join_thread()
                    queue.close()
            if (
                process is not None
                and process.pid is not None
                and not process.is_alive()
            ):
                process.close()
            self._process = None
            self._state = "closed"
        if forced:
            raise RuntimeError(
                "Controller shutdown timed out; inspect and recover unfinished experiments before continuing."
            )
        if exit_code not in (None, 0):
            raise RuntimeError(
                f"Controller exited with code {exit_code}; inspect its journal before continuing."
            )
