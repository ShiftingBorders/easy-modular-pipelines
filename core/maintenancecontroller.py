"""Serial module maintenance over the existing controller queues, without a runner."""

import asyncio
import multiprocessing
import threading
from multiprocessing.queues import Queue
from pathlib import Path
from queue import Empty, Full

from core.experimentassembler import ExperimentAssembler
from core.experimentreader import ExperimentReader
from core.logger import OperationLogger
from core.logger_utils.events import (
    JsonObject,
    LoggingError,
    copy_json_object,
    require_text,
)
from core.modulemanager import ModuleManager
from core.modulemanifest import read_module_manifest
from core.storage_errors import StorageConflict, StorageError, StoredObjectNotFound


class MaintenanceController:
    def __init__(
        self,
        manager: ModuleManager,
        logger: OperationLogger,
        requests: Queue,
        responses: Queue,
        *,
        shutdown_requested: asyncio.Event,
        recovery_required: list[str],
        project_root: Path,
    ) -> None:
        self._manager = manager
        self._experiment_reader = ExperimentReader(project_root)
        self._assembler = ExperimentAssembler(project_root, manager)
        self._logger = logger
        self._requests = requests
        self._responses = responses
        self._shutdown_requested = shutdown_requested
        self._recovery_required = recovery_required
        self._incoming: asyncio.Queue = asyncio.Queue()
        self._commands: asyncio.Queue = asyncio.Queue()
        self._read_commands: asyncio.Queue = asyncio.Queue(maxsize=64)
        self._read_tasks: list[asyncio.Task] = []
        self._intake_stop = threading.Event()
        self._intake_thread: threading.Thread | None = None
        self._tasks: list[asyncio.Task] = []
        self._active_task: asyncio.Task | None = None
        self._current_command: JsonObject | None = None
        self._closing = False
        self._idle = asyncio.Event()
        self._idle.set()

    def _read_requests(self, loop: asyncio.AbstractEventLoop) -> None:
        # A partial IPC frame must not hold event-loop shutdown hostage.
        try:
            while not self._intake_stop.is_set():
                try:
                    request = self._requests.get(True, 0.1)
                except Empty:
                    continue
                loop.call_soon_threadsafe(self._incoming.put_nowait, request)
        except Exception as error:  # noqa: BLE001 - Wake the lifecycle owner on IPC failure.
            if not self._intake_stop.is_set():
                loop.call_soon_threadsafe(self._incoming.put_nowait, error)

    async def serve(self) -> None:
        if self._tasks or self._closing:
            raise RuntimeError("Maintenance controller is already running or closed.")
        self._intake_thread = threading.Thread(
            target=self._read_requests,
            args=(asyncio.get_running_loop(),),
            name="maintenance-intake",
            daemon=True,
        )
        self._intake_thread.start()
        self._read_tasks = [
            asyncio.create_task(self._work(read_only=True)) for _ in range(4)
        ]
        self._tasks = [
            asyncio.create_task(self._receive()),
            asyncio.create_task(self._work()),
            *self._read_tasks,
        ]
        await asyncio.gather(*self._tasks)

    async def _receive(self) -> None:
        while not self._closing:
            request = await self._incoming.get()
            if isinstance(request, Exception):
                raise RuntimeError("Maintenance command channel failed.") from request  # noqa: TRY004 - This is a transport failure, not a type error.
            request = copy_json_object(request, "request")
            if request.get("command") == "server.shutdown":
                self._shutdown_requested.set()
                return
            if "commands" in request:
                commands = [
                    {**command, "chain_id": request["chain_id"]}
                    for command in request["commands"]
                ]
            else:
                if request["command"].startswith(("stats.", "logs.")):
                    if request["command"] == "stats.state":
                        await self._publish(await self._execute(request))
                    else:
                        try:
                            self._read_commands.put_nowait(request)
                        except asyncio.QueueFull:
                            await self._publish(
                                self._failure(
                                    request,
                                    "too_many_reads",
                                    "Too many queued maintenance reads.",
                                )
                            )
                    continue
                commands = [request]
            await self._commands.put(commands)

    async def _work(self, *, read_only: bool = False) -> None:
        if read_only:
            while not self._closing and not self._shutdown_requested.is_set():
                command = await self._read_commands.get()
                if self._shutdown_requested.is_set():
                    return
                await self._publish(await self._execute(command))
            return
        while not self._closing:
            commands = await self._commands.get()
            failed = False
            for command in commands:
                if self._closing or self._shutdown_requested.is_set() or failed:
                    await self._publish(
                        self._failure(
                            command,
                            "command_cancelled",
                            "Server is stopping or a previous command failed.",
                        )
                    )
                    continue
                self._current_command = command
                self._idle.clear()
                self._active_task = asyncio.create_task(self._execute(command))
                try:
                    response = await self._active_task
                    await self._publish(response)
                    failed = response["result"] == "fail"
                finally:
                    self._active_task = None
                    self._current_command = None
                    self._idle.set()

    async def _execute(self, command: JsonObject) -> JsonObject:
        try:
            args = copy_json_object(command.get("args", {}), "args")
            name = require_text(command.get("command"), "command")
            if command.get("target"):
                raise ValueError("Maintenance commands do not accept target.")
            if name in (
                "stats.experiments",
                "stats.experiment",
                "stats.snapshots",
                "stats.snapshot",
                "stats.artifacts",
                "stats.artifact",
            ):
                data = await asyncio.to_thread(
                    self._experiment_reader.read, name, args, None
                )
            elif name == "stats.modules":
                if args:
                    raise ValueError("Module list does not accept arguments.")
                data = self._manager.list_modules()
            elif name == "stats.module":
                if args.keys() != {"name", "version"}:
                    raise ValueError("Module inspection requires name/version.")
                data = await self._manager.inspect_module(**args)
            elif name == "stats.template":
                if args.keys() != {"template_path"}:
                    raise ValueError("Template validation requires template_path.")
                data = await self._assembler.validate_template(
                    Path(require_text(args["template_path"], "template_path"))
                )
            elif name == "stats.state":
                if args.keys() - {"experiment_id"}:
                    raise ValueError("Unknown stats.state arguments.")
                if args.get("experiment_id") is not None:
                    raise FileNotFoundError("Maintenance has no selected experiment.")
                data = {
                    "server_mode": "maintenance",
                    "experiment_id": None,
                    "current_command": self._current_command,
                    "recovery_required": self._recovery_required,
                }
            elif name in ("module.add", "module.validate", "module.remove"):
                if name != "module.validate" and self._recovery_required:
                    return self._failure(
                        command,
                        "invalid_state",
                        "Recover and stop unfinished experiments in run mode before changing modules: "
                        + ", ".join(self._recovery_required),
                    )
                self._logger.record_event(
                    "module.command_started", {"command": command}
                )
                if name == "module.add":
                    if args.keys() != {"folder"}:
                        raise ValueError("module.add requires only folder.")
                    data = await self._manager.register_and_install_module_async(
                        Path(require_text(args["folder"], "folder"))
                    )
                elif name == "module.validate" and "folder" in args:
                    if args.keys() != {"folder"}:
                        raise ValueError("Source validation requires only folder.")
                    manifest = await asyncio.to_thread(
                        read_module_manifest,
                        Path(require_text(args["folder"], "folder")),
                    )
                    data = {"scope": "source", "valid": True, "manifest": manifest}
                else:
                    if args.keys() != {"name", "version"}:
                        raise ValueError(
                            "Stored module operations require name and version."
                        )
                    module_name = require_text(args["name"], "name")
                    version = require_text(args["version"], "version")
                    if name == "module.validate":
                        reference = await self._manager.validate_stored_module_async(
                            module_name, version
                        )
                        data = {"scope": "stored", "valid": True, "module": reference}
                    else:
                        removed = await self._manager.unregister_module_async(
                            module_name, version
                        )
                        data = {"status": "removed" if removed else "already_absent"}
                self._logger.record_event(
                    "module.command_completed",
                    {
                        "command_id": command["command_id"],
                        "data": data,
                    },
                )
            else:
                return self._failure(
                    command,
                    "invalid_mode",
                    "This command is unavailable in maintenance mode. Restart with --mode run.",
                )
            return {
                "command_id": command["command_id"],
                "chain_id": command.get("chain_id"),
                "experiment_id": None,
                "state": "succeeded",
                "result": "success",
                "data": data,
                "error": None,
            }
        except Exception as error:  # noqa: BLE001 - Report operation failures through the command protocol.
            code = "operation_failed"
            if isinstance(error, LoggingError):
                code = "journal_unavailable"
            elif isinstance(error, (FileNotFoundError, StoredObjectNotFound)):
                code = "not_found"
            elif isinstance(error, StorageConflict):
                code = "storage_conflict"
            elif isinstance(error, StorageError):
                code = "storage_error"
            elif isinstance(
                error, (TypeError, ValueError, KeyError, NotImplementedError)
            ):
                code = "invalid_request"
            if not isinstance(error, LoggingError):
                try:
                    self._logger.record_error(
                        error, context={"command_id": command["command_id"]}
                    )
                except Exception as logging_error:  # noqa: BLE001 - Preserve both failures.
                    error.add_note(f"Logging the failure also failed: {logging_error}")
            return self._failure(command, code, str(error), error)

    def _failure(
        self,
        command: JsonObject,
        code: str,
        message: str,
        error: Exception | None = None,
    ) -> JsonObject:
        return {
            "command_id": command.get("command_id"),
            "chain_id": command.get("chain_id"),
            "experiment_id": None,
            "state": "cancelled" if code == "command_cancelled" else "failed",
            "result": "fail",
            "data": None,
            "error": {
                "code": code,
                "message": message,
                "details": {
                    "notes": list(getattr(error, "__notes__", [])),
                },
            },
        }

    async def _publish(self, response: JsonObject) -> None:
        while not self._closing:
            try:
                await asyncio.to_thread(self._responses.put, response, True, 0.1)
                return
            except Full:
                parent = multiprocessing.parent_process()
                if parent is not None and not parent.is_alive():
                    return
                continue

    async def close(self) -> None:
        if self._closing:
            return
        self._shutdown_requested.set()
        self._intake_stop.set()
        # Cancel reads immediately, while serial mutations finish safely.
        # Storage inspection waits for its in-flight call before propagating
        # cancellation, so the storage owner can close after these tasks finish.
        for task in self._read_tasks:
            task.cancel()
        await asyncio.gather(*self._read_tasks, return_exceptions=True)
        await self._idle.wait()
        self._closing = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        while not self._read_commands.empty():
            self._read_commands.get_nowait()
        if self._intake_thread is not None:
            await asyncio.to_thread(self._intake_thread.join, 1)
