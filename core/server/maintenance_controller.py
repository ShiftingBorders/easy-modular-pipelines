"""Serial module maintenance over the existing controller queues, without a runner."""

import asyncio
import multiprocessing
import threading
from multiprocessing.queues import Queue
from pathlib import Path
from queue import Empty, Full

from core.experiments.assembler import ExperimentAssembler
from core.experiments.reader import ExperimentReader
from core.journal.events import LoggingError
from core.journal.logger import OperationLogger
from core.models.server_arguments import (
    MaintenanceInvocation,
    ModuleCoordinates,
    ModuleSource,
    NoArguments,
    StateQueryArguments,
)
from core.models.server_commands import (
    ControllerChain,
    ControllerCommand,
    ControllerOutcome,
)
from core.modules.manager import ModuleManager
from core.modules.manifest import read_module_manifest
from core.primitives.json_values import JsonObject, copy_json_object
from core.server.module_reads import _validate_module_read_args
from core.storage.errors import StorageConflict, StorageError, StoredObjectNotFound


class MaintenanceController:
    """Serialize module mutations while serving bounded concurrent maintenance reads."""
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
        """Bind caller-owned storage, logger, IPC queues, and shutdown coordination.

        Args:
            manager: Module manager used for registration and inspection.
            logger: Open controller logger owned by the caller.
            requests: Incoming multiprocessing command queue.
            responses: Outgoing multiprocessing response queue.
            shutdown_requested: Event shared with the controller lifecycle owner.
            recovery_required: Experiments that must be recovered before module mutation.
            project_root: Absolute project path for experiment and template reads.
        """
        self._manager = manager
        self._experiment_reader = ExperimentReader(project_root)
        self._assembler = ExperimentAssembler(project_root, manager)
        self._logger = logger
        self._requests = requests
        self._responses = responses
        self._shutdown_requested = shutdown_requested
        self._recovery_required = recovery_required
        self._incoming: asyncio.Queue = asyncio.Queue()
        self._commands: asyncio.Queue[list[ControllerCommand]] = asyncio.Queue()
        self._read_commands: asyncio.Queue[ControllerCommand] = asyncio.Queue(
            maxsize=64
        )
        self._read_tasks: list[asyncio.Task] = []
        self._intake_stop = threading.Event()
        self._intake_thread: threading.Thread | None = None
        self._tasks: list[asyncio.Task] = []
        self._active_task: asyncio.Task | None = None
        self._current_command: ControllerCommand | None = None
        self._closing = False
        self._idle = asyncio.Event()
        self._idle.set()

    def _read_requests(self, loop: asyncio.AbstractEventLoop) -> None:
        # A partial IPC frame must not hold event-loop shutdown hostage.
        """Forward blocking IPC reads to the event loop from a dedicated thread.

        Args:
            loop: Owning asyncio event loop receiving thread-safe IPC callbacks.
        """
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
        """Start intake, serial mutation, and read workers and await their lifetimes.

        Starts one serial mutation worker and four read workers alongside intake. It
        awaits their lifetimes; the lifecycle owner calls close to cancel reads and
        wait for an active mutation safely.
        """
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
        """Validate incoming commands and route shutdown, reads, and mutation chains.

        Routes stats.state directly, queues other reads under their own capacity
        bound, and admits ordered mutation chains. Private shutdown signals the
        owner and ends intake.
        """
        while not self._closing:
            request = await self._incoming.get()
            if isinstance(request, Exception):
                raise RuntimeError("Maintenance command channel failed.") from request  # noqa: TRY004 - This is a transport failure, not a type error.
            request = copy_json_object(request, "request")
            if "commands" in request:
                chain = ControllerChain.model_validate(request)
                commands = chain.commands
            else:
                command = ControllerCommand.model_validate(request)
                if command.command == "server.shutdown":
                    self._shutdown_requested.set()
                    return
                if command.command.startswith(("stats.", "logs.")):
                    await self._admit_command_read(command)
                    continue
                commands = [command]
            await self._commands.put(commands)

    async def _admit_command_read(self, command: ControllerCommand) -> None:
        """Answer state immediately or enqueue another maintenance read."""
        if command.command == "stats.state":
            await self._publish(await self._execute(command))
        else:
            await self._admit_read(command)

    async def _admit_read(self, request: ControllerCommand) -> None:
        """Enqueue a read without blocking, publishing a failure when capacity is exhausted.

        Args:
            request: Validated maintenance read envelope awaiting bounded-queue
                admission.
        """
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

    async def _work(self, *, read_only: bool = False) -> None:
        """Execute serial command chains, cancelling their remaining commands after failure.

        Args:
            read_only: Use the independent read queue rather than the serial
                mutation queue.
        """
        if read_only:
            return await self._work_reads()
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
                    failed = response.result == "fail"
                finally:
                    self._active_task = None
                    self._current_command = None
                    self._idle.set()

    async def _work_reads(self) -> None:
        """Consume admitted reads until controller shutdown is requested."""
        while not self._closing and not self._shutdown_requested.is_set():
            command = await self._read_commands.get()
            if self._shutdown_requested.is_set():
                return
            await self._publish(await self._execute(command))
        return

    async def _execute(self, command: ControllerCommand) -> ControllerOutcome:
        """Validate and execute a command, translating operation errors into outcomes.

        Args:
            command: Validated admitted maintenance command with ID and chain
                identity.

        Returns:
            Correlated typed outcome after translating validation, storage, and
            journal failures to protocol error codes.
        """
        try:
            invocation = MaintenanceInvocation.model_validate(
                {
                    "command": command.command,
                    "args": command.args,
                    "target": command.target,
                }
            )
            return await self._execute_request(command, invocation)
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
                        error, context={"command_id": command.command_id}
                    )
                except Exception as logging_error:  # noqa: BLE001 - Preserve both failures.
                    error.add_note(f"Logging the failure also failed: {logging_error}")
            return self._failure(command, code, str(error), error)

    async def _execute_request(
        self, command: ControllerCommand, invocation: MaintenanceInvocation
    ) -> ControllerOutcome:
        """Dispatch a validated maintenance invocation and construct its command outcome.

        Args:
            command: Validated admitted maintenance command with ID and chain
                identity.
            invocation: Validated command arguments and target after boundary
                validation.

        Returns:
            Succeeded outcome with read/module data, or a failed outcome for
            mode/recovery restrictions.
        """
        name, args = invocation.command, invocation.args
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
        elif name in ("stats.modules", "stats.module", "stats.template"):
            arguments = _validate_module_read_args(name, args)
            if isinstance(arguments, NoArguments):
                data = self._manager.list_modules()
            elif isinstance(arguments, ModuleCoordinates):
                data = await self._manager.inspect_module(
                    name=arguments.name, version=arguments.version
                )
            else:
                data = await self._assembler.validate_template(arguments.template_path)
        elif name == "stats.state":
            arguments = StateQueryArguments.model_validate(args)
            if arguments.experiment_id is not None:
                raise FileNotFoundError("Maintenance has no selected experiment.")
            data = {
                "server_mode": "maintenance",
                "experiment_id": None,
                "current_command": None
                if self._current_command is None
                else self._current_command.model_dump(exclude_unset=True),
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
            data = await self._execute_module_command(command, name, args)
        else:
            return self._failure(
                command,
                "invalid_mode",
                "This command is unavailable in maintenance mode. Restart with --mode run.",
            )
        return ControllerOutcome.model_validate(
            {
                "command_id": command.command_id,
                "chain_id": command.chain_id,
                "experiment_id": None,
                "state": "succeeded",
                "result": "success",
                "data": data,
                "error": None,
            }
        )

    async def _execute_module_command(
        self, command: ControllerCommand, name: str, args: JsonObject
    ) -> JsonObject:
        """Journal and perform module registration, validation, or removal.

        Args:
            command: Validated admitted maintenance command with ID and chain
                identity.
            name: Maintenance command name selecting module add, validation, or
                removal.
            args: JSON command arguments; mutable inputs are detached at the
                validation boundary.

        Returns:
            Module reference/installation status, validation metadata, or removal
            status, depending on the admitted action.
        """
        self._logger.record_event(
            "module.command_started",
            {"command": command.model_dump(exclude_unset=True)},
        )
        data: JsonObject
        if name == "module.add":
            arguments = ModuleSource.model_validate(args)
            data = await self._manager.register_and_install_module_async(
                arguments.folder
            )
        elif name == "module.validate" and "folder" in args:
            arguments = ModuleSource.model_validate(args)
            manifest = await asyncio.to_thread(
                read_module_manifest,
                arguments.folder,
            )
            data = {"scope": "source", "valid": True, "manifest": manifest}
        else:
            arguments = ModuleCoordinates.model_validate(args)
            if name == "module.validate":
                reference = await self._manager.validate_stored_module_async(
                    arguments.name, arguments.version
                )
                data = {"scope": "stored", "valid": True, "module": reference}
            else:
                removed = await self._manager.unregister_module_async(
                    arguments.name, arguments.version
                )
                data = {"status": "removed" if removed else "already_absent"}
        self._logger.record_event(
            "module.command_completed",
            {
                "command_id": command.command_id,
                "data": data,
            },
        )
        return data

    def _failure(
        self,
        command: ControllerCommand,
        code: str,
        message: str,
        error: Exception | None = None,
    ) -> ControllerOutcome:
        """Build a correlated failed/cancelled outcome including exception notes.

        Args:
            command: Validated admitted maintenance command with ID and chain
                identity.
            code: Machine-readable failure category used in the returned outcome.
            message: Human-readable diagnostic to expose when the operation is
                rejected.
            error: Primary failure retained while cleanup or error translation
                proceeds.

        Returns:
            Failed or cancelled outcome retaining command/chain identity, error
            code, message, and exception notes.
        """
        return ControllerOutcome.model_validate(
            {
                "command_id": command.command_id,
                "chain_id": command.chain_id,
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
        )

    async def _publish(self, response: ControllerOutcome) -> None:
        """Send an outcome through bounded IPC until accepted, closed, or parent exit.

        Args:
            response: Participant/controller result envelope being processed.
        """
        document = response.model_dump(exclude_unset=True)
        while not self._closing:
            try:
                await asyncio.to_thread(self._responses.put, document, True, 0.1)
                return
            except Full:
                parent = multiprocessing.parent_process()
                if parent is not None and not parent.is_alive():
                    return
                continue

    async def close(self) -> None:
        """Cancel reads, let active mutation finish, and stop intake and worker tasks.

        Requests shutdown, cancels and awaits read workers, then waits for the
        active serial mutation to become idle before cancelling remaining tasks.
        Storage ownership stays with the caller until these operations finish.
        """
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
