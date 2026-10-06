"""Responsive command intake, serial control chains, and independent read requests."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from multiprocessing.queues import Queue
from pathlib import Path
from queue import Empty, Full

from core.experiments.assembler import ExperimentAssembler
from core.experiments.reader import ExperimentReader
from core.experiments.runner import ExperimentRunner
from core.journal.events import LoggingError
from core.models.server_arguments import (
    ArchiveArguments,
    ArchiveInstall,
    ArchiveSelection,
    ControlInvocation,
    EventReadArguments,
    ExperimentReference,
    ModuleCoordinates,
    NoArguments,
    PositionArguments,
    RerunArguments,
    ResetRetriesArguments,
    ResourceHistoryArguments,
    RunArguments,
    SnapshotArguments,
    SnapshotReference,
    StateQueryArguments,
    TemplateArguments,
)
from core.models.server_commands import (
    ControllerChain,
    ControllerCommand,
    ControllerOutcome,
    ControllerRejection,
)
from core.modules.manager import ModuleManager
from core.primitives.json_values import JsonObject, copy_json_object
from core.primitives.paths import repository_root
from core.resources.collector import ResourceCollector
from core.server.module_reads import _validate_module_read_args
from core.storage.errors import (
    StorageCapacityError,
    StorageConflict,
    StorageError,
    StoredObjectNotFound,
)


class ExperimentController:
    """Coordinate run-mode commands, independent reads, and resource monitoring."""
    def __init__(
        self,
        project_root: Path,
        runner: ExperimentRunner,
        requests: Queue,
        responses: Queue,
        *,
        resource_config_path: Path | None = None,
        shutdown_requested: asyncio.Event | None = None,
        recovery_required: list[str] | None = None,
        module_manager: ModuleManager | None = None,
    ) -> None:
        """Bind runner, project readers, IPC queues, and collector supervision.

        Args:
            project_root: Absolute project directory.
            runner: Experiment runner controlled by admitted commands.
            requests: Incoming multiprocessing command queue.
            responses: Outgoing multiprocessing outcome queue.
            resource_config_path: Collector config path, or None for repository defaults.
            shutdown_requested: Optional lifecycle event for private shutdown requests.
            recovery_required: Experiment IDs restricting admission until recovered.
            module_manager: Optional manager for module/template reads.

        Raises:
            ValueError: The project root is relative.
        """
        self._project_root = Path(project_root)
        if not self._project_root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._runner = runner
        self._experiment_reader = ExperimentReader(self._project_root)
        self._module_manager = module_manager
        self._requests = requests
        self._responses = responses
        self._control_queue: asyncio.Queue[list[ControllerCommand]] = asyncio.Queue()
        self._incoming: asyncio.Queue[object] = asyncio.Queue()
        self._intake_stop = threading.Event()
        self._intake_thread: threading.Thread | None = None
        self._intake_error: Exception | None = None
        self._outbox: asyncio.Queue[ControllerOutcome | ControllerRejection] = (
            asyncio.Queue()
        )
        self._current_command: ControllerCommand | None = None
        self._active_tail: list[ControllerCommand] = []
        self._active_task = None
        self._loops: list[asyncio.Task] = []
        self._reads: set[asyncio.Task] = set()
        self._closing = False
        self._shutdown_requested = shutdown_requested
        self._recovery_required = set(recovery_required or [])
        self.resources = ResourceCollector(
            repository_root() / "default_settings" / "resource_collector.json"
            if resource_config_path is None
            else resource_config_path
        )

    async def serve(self) -> None:
        """Start intake, command, response, and collector loops, closing on exit.

        Starts a dedicated IPC reader thread plus command, response, and resource-
        collector tasks. Registers the runner's resource observer before serving,
        and closes controller-owned tasks/collector if any loop exits.
        """
        if self._loops or self._closing:
            raise RuntimeError("Controller is already running or closed.")
        # A parent can die halfway through a multiprocessing queue frame. A
        # dedicated daemon reader keeps that partial read out of the event loop
        # and its executor, allowing the controller to stop its participants.
        self._intake_thread = threading.Thread(
            target=self._read_request_queue,
            args=(asyncio.get_running_loop(),),
            name="controller-command-reader",
            daemon=True,
        )
        self._intake_thread.start()
        self._runner.set_resource_observer(
            self.resources.update,
            suspend=self.resources.suspend_experiment,
            resume=self.resources.resume_experiment,
        )
        self._loops = [
            asyncio.create_task(self._receive_requests()),
            asyncio.create_task(self._execute_commands()),
            asyncio.create_task(self._write_responses()),
            asyncio.create_task(self.resources.serve()),
        ]
        try:
            await asyncio.gather(*self._loops)
        finally:
            await self.close()

    def _read_request_queue(self, loop: asyncio.AbstractEventLoop) -> None:
        """Forward blocking queue input from a dedicated thread to the event loop.

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
        except Exception as error:  # noqa: BLE001 - Wake the owner when the IPC transport fails.
            if not self._intake_stop.is_set():
                try:
                    loop.call_soon_threadsafe(self._intake_failed, error)
                except RuntimeError:
                    pass

    def _intake_failed(self, error: Exception) -> None:
        """Retain an IPC failure and wake the async intake loop to surface it."""
        self._intake_error = error
        self._incoming.put_nowait(None)

    async def _receive_requests(self) -> None:
        """Validate queue input and admit commands/chains or publish intake failures.

        Validates single commands and chains at the IPC boundary. Private shutdown
        signals the lifecycle owner, reads run independently, and a standalone stop
        cancels queued control work. Invalid intake produces a rejection even when
        no valid command ID is available.
        """
        while not self._closing:
            request = await self._incoming.get()
            if self._intake_error is not None:
                raise RuntimeError(
                    "Controller command channel failed."
                ) from self._intake_error
            try:
                request = copy_json_object(request, "request")
                if "commands" in request:
                    batch = self._read_chain(request)
                else:
                    command = ControllerCommand.model_validate(request)
                    if command.command == "server.shutdown" and self._shutdown_requested is not None:
                        # Only the server owner sends this private queue message.
                        self._shutdown_requested.set()
                        return
                    batch = await self._admit_command(command)
                    if batch is None:
                        continue
                await self._control_queue.put(batch)
            except (TypeError, ValueError, KeyError) as error:
                command = request if isinstance(request, dict) else {}
                await self._publish_response(
                    self._failure(command, "invalid_request", str(error))
                )

    def _read_chain(self, request: JsonObject) -> list[ControllerCommand]:
        """Validate a controller chain and return its ordered commands."""
        chain = ControllerChain.model_validate(request)
        return chain.commands

    async def _admit_command(
        self, command: ControllerCommand
    ) -> list[ControllerCommand] | None:
        """Start independent reads or return queued control work, prioritizing stop.

        Args:
            command: Validated admitted command envelope with ID, arguments, and
                optional chain identity.

        Returns:
            None when a read was launched independently; otherwise a one-command
            list for serial control. Standalone stop first cancels earlier queued
            work.
        """
        if command.command.startswith(("stats.", "logs.")):
            task = asyncio.create_task(self._answer_read(command))
            self._reads.add(task)
            task.add_done_callback(self._reads.discard)
            return None
        if command.command == "stop":
            await self._cancel_queued_commands()
        return [command]

    async def _cancel_queued_commands(self) -> None:
        """Cancel active control and queued chain tails for a standalone stop.

        Clears the current chain tail and queued chains and cancels the active
        control task. Every removed command receives a cancelled outcome instead of
        silently disappearing from receipt tracking.
        """
        cancelled = list(self._active_tail)
        self._active_tail.clear()
        while not self._control_queue.empty():
            cancelled.extend(self._control_queue.get_nowait())
        if self._active_task is not None:
            self._active_task.cancel()
        for command in cancelled:
            await self._publish_response(
                self._failure(
                    command,
                    "command_cancelled",
                    "Cancelled by standalone stop.",
                )
            )

    async def _execute_commands(self) -> None:
        """Run admitted command chains serially and cancel each failed chain's tail.

        Executes one admitted control command at a time. A failed command cancels
        the rest of its chain; independent reads are not serialized through this
        loop.
        """
        while not self._closing:
            self._active_tail = await self._control_queue.get()
            while self._active_tail and not self._closing:
                command = self._active_tail.pop(0)
                response = await self._execute_queued_command(command)
                await self._publish_response(response)
                if response.result == "fail":
                    cancelled, self._active_tail = self._active_tail, []
                    for remaining in cancelled:
                        await self._publish_response(
                            self._failure(
                                remaining,
                                "command_cancelled",
                                "Previous command failed.",
                            )
                        )

    async def _execute_queued_command(
        self, command: ControllerCommand
    ) -> ControllerOutcome | ControllerRejection:
        """Track the current command and translate standalone-stop cancellation.

        Args:
            command: Validated admitted command envelope with ID, arguments, and
                optional chain identity.

        Returns:
            Correlated outcome, including a cancellation result when standalone stop
            interrupts the active command.
        """
        self._current_command = command
        self._active_task = asyncio.create_task(self._execute_command(command))
        try:
            response = await self._active_task
        except asyncio.CancelledError:
            if self._closing:
                raise
            response = self._failure(
                command, "command_cancelled", "Interrupted by standalone stop."
            )
        finally:
            self._active_task = None
            self._current_command = None
        return response

    async def _execute_command(
        self, command: ControllerCommand
    ) -> ControllerOutcome | ControllerRejection:
        """Execute a command and convert expected failures into correlated API outcomes.

        Args:
            command: Validated admitted command envelope with ID, arguments, and
                optional chain identity.

        Returns:
            Successful controller outcome or an error-classified rejection/outcome.
            Mandatory journal failures may first enter the runner's failure path.
        """
        name = command.command
        try:
            return await self._perform_command(command)
        except LoggingError as error:
            if (
                name not in ("stats.artifacts", "stats.artifact")
                and not name.startswith("archive.")
                and (
                    not name.startswith(("stats.", "logs."))
                    or getattr(error, "journal_failed", False)
                )
            ):
                await self._runner._fail(error, {"command_id": command.command_id})
            return self._failure(command, "journal_unavailable", str(error), error)
        except NotImplementedError as error:
            return self._failure(command, "unsupported_feature", str(error))
        except FileExistsError as error:
            return self._failure(
                command,
                "archive_conflict"
                if name.startswith("archive.")
                else "experiment_id_conflict",
                str(error),
                error,
            )
        except StorageConflict as error:
            return self._failure(command, "storage_conflict", str(error), error)
        except StorageCapacityError as error:
            return self._failure(command, "storage_capacity", str(error), error)
        except StoredObjectNotFound as error:
            return self._failure(command, "not_found", str(error), error)
        except StorageError as error:
            return self._failure(command, "storage_error", str(error), error)
        except FileNotFoundError as error:
            return self._failure(command, "not_found", str(error), error)
        except (TypeError, ValueError, KeyError) as error:
            return self._failure(command, "invalid_request", str(error), error)
        except RuntimeError as error:
            return self._failure(command, "invalid_state", str(error), error)
        except Exception as error:  # noqa: BLE001 - Convert operation failures at the API boundary.
            return self._failure(
                command, "operation_failed", f"{type(error).__name__}: {error}", error
            )

    async def _perform_command(self, command: ControllerCommand) -> ControllerOutcome:
        """Dispatch reads/controls and update recovery admission after confirmed changes.

        Args:
            command: Validated admitted command envelope with ID, arguments, and
                optional chain identity.

        Returns:
            Succeeded outcome containing read/control data and the selected
            experiment identity after the action.
        """
        name = command.command
        if name.startswith(("stats.", "logs.")):
            data = await self._read_request(command)
        else:
            if self._recovery_required and name not in (
                "recover",
                "stop",
                "archive.inspect",
            ):
                raise RuntimeError(
                    "Recover unfinished experiments before issuing control commands: "
                    f"{sorted(self._recovery_required)}"
                )
            data = await self._execute_control_command(name, command.args, command)
            if name == "recover":
                self._recovery_required.discard(command.args["experiment_id"])
            if (
                name == "stop"
                and isinstance(data, dict)
                and data.get("termination_confirmed")
            ):
                identifier = data.get("experiment_id")
                if isinstance(identifier, str):
                    # Confirmed shutdown permits rollback after an incomplete restoration.
                    if data.get("pending_rebuild") is not None:
                        self._recovery_required.add(identifier)
                    else:
                        self._recovery_required.discard(identifier)
            if data is None:
                data = {}
        return ControllerOutcome.model_validate(
            {
                "command_id": command.command_id,
                "chain_id": command.chain_id,
                "state": "succeeded",
                "result": "success",
                "experiment_id": self._runner.get_state()["experiment_id"],
                "data": data,
                "error": None,
            }
        )

    async def _execute_control_command(
        self, name: str, args: JsonObject, command: ControllerCommand
    ) -> JsonObject | None:
        """Validate command arguments, bind journal context, and invoke the runner.

        Args:
            name: Command name used to select a runner control method.
            args: JSON command arguments; mutable inputs are detached at the
                validation boundary.
            command: Validated admitted command envelope with ID, arguments, and
                optional chain identity.

        Returns:
            The runner method's JSON result, or None for control methods without a
            payload.
        """
        invocation = ControlInvocation.model_validate(
            {"command": name, "args": args, "target": command.target}
        )
        args = invocation.arguments()
        handlers = {
            "run": (self._runner.run, RunArguments),
            "pause": (self._runner.pause, NoArguments),
            "resume": (self._runner.resume, NoArguments),
            "stop": (self._runner.stop, NoArguments),
            "step": (self._runner.step, NoArguments),
            "rerun": (self._runner.rerun, RerunArguments),
            "retry": (self._runner.retry, PositionArguments),
            "service.start": (self._runner.start_service, PositionArguments),
            "service.stop": (self._runner.stop_service, PositionArguments),
            "move": (self._runner.move, PositionArguments),
            "reset_retries": (self._runner.reset_retries, ResetRetriesArguments),
            "replace": (self._runner.replace, None),
            "reload_template": (self._runner.reload_template, TemplateArguments),
            "snapshot": (self._runner.snapshot, SnapshotArguments),
            "rollback": (self._runner.rollback, SnapshotReference),
            "recover": (self._runner.recover, ExperimentReference),
            "archive.create": (self._runner.create_archive, ArchiveSelection),
            "archive.inspect": (self._runner.inspect_archive, ArchiveArguments),
            "archive.install": (self._runner.install_archive, ArchiveInstall),
        }
        if name not in handlers:
            raise NotImplementedError(f"Unsupported command: {name}")
        handler, model = handlers[name]
        # Replacement is currently unsupported and has no effect to validate.
        if model is not None:
            arguments = model.model_validate(args)
            args = {
                name: getattr(arguments, name)
                for name in type(arguments).model_fields
                if name in arguments.model_fields_set
            }
        self._runner._command_context = {
            "command_id": command.command_id,
            "command_chain_id": command.chain_id,
        }
        try:
            data = handler(**args)
            if isinstance(data, Coroutine):
                data = await data
        finally:
            self._runner._command_context = {}
        return data

    async def _read_request(self, request: ControllerCommand) -> JsonObject:
        """Dispatch validated experiment, module, resource, state, or journal reads.

        Args:
            request: Validated read command envelope.

        Returns:
            Requested module, template, experiment, resource, state, or journal
            data.
        """
        args = request.args
        name = request.command
        if name in ("stats.modules", "stats.module", "stats.template"):
            if self._module_manager is None:
                raise RuntimeError("Module manager is not configured for reads.")
            arguments = _validate_module_read_args(name, args)
            if isinstance(arguments, NoArguments):
                return self._module_manager.list_modules()
            if isinstance(arguments, ModuleCoordinates):
                return await self._module_manager.inspect_module(
                    name=arguments.name, version=arguments.version
                )
            assembler = ExperimentAssembler(self._project_root, self._module_manager)
            return await assembler.validate_template(arguments.template_path)
        if name in (
            "stats.experiments",
            "stats.experiment",
            "stats.snapshots",
            "stats.snapshot",
            "stats.artifacts",
            "stats.artifact",
        ):
            return await asyncio.to_thread(
                self._experiment_reader.read,
                name,
                args,
                self._runner.get_state()["experiment_id"],
            )
        if name == "stats.resources":
            NoArguments.model_validate(args)
            return self.resources.get_status()
        if name == "stats.resources.history":
            arguments = ResourceHistoryArguments.model_validate(args)
            return self.resources.read_history(
                **{
                    name: getattr(arguments, name)
                    for name in type(arguments).model_fields
                    if name in arguments.model_fields_set
                }
            )
        if name == "stats.state":
            arguments = StateQueryArguments.model_validate(args)
            state = self._runner.get_state()
            if arguments.experiment_id not in (None, state["experiment_id"]):
                raise FileNotFoundError("The requested experiment is not selected.")
            return copy_json_object(
                {
                    **state,
                    "server_mode": "run",
                    "current_command": None
                    if self._current_command is None
                    else self._current_command.model_dump(exclude_unset=True),
                    "recovery_required": sorted(self._recovery_required),
                },
                "controller state",
            )
        if name == "logs.read":
            arguments = EventReadArguments.model_validate(args)
            return await self._runner.read_events(
                arguments.experiment_id, arguments.cursor, limit=arguments.limit
            )
        raise NotImplementedError(
            "The initial read API provides stats.state and logs.read."
        )

    async def _answer_read(self, command: ControllerCommand) -> None:
        """Execute a read independently and enqueue its outcome for publication."""
        await self._publish_response(await self._execute_command(command))

    async def _publish_response(
        self, response: ControllerOutcome | ControllerRejection
    ) -> None:
        """Append an outcome or intake rejection to the local response outbox."""
        self._outbox.put_nowait(response)

    async def _write_responses(self) -> None:
        """Drain the outbox into bounded multiprocessing IPC until closed.

        Serializes local outbox items and retries bounded IPC insertion while the
        queue is full. Closing stops delivery; this loop does not execute or replay
        commands.
        """
        while not self._closing:
            response = await self._outbox.get()
            document = response.model_dump(exclude_unset=True)
            while not self._closing:
                try:
                    await asyncio.to_thread(self._responses.put, document, True, 0.1)
                    break
                except Full:
                    continue

    def _failure(
        self,
        command: ControllerCommand | JsonObject,
        code: str,
        message: str,
        error: Exception | None = None,
    ) -> ControllerOutcome | ControllerRejection:
        """Build a failed/cancelled outcome, allowing missing IDs for rejected intake.

        Args:
            command: Admitted command envelope or rejected raw intake dictionary,
                possibly lacking a valid UUID.
            code: Machine-readable failure category used in the returned outcome.
            message: Human-readable diagnostic to expose when the operation is
                rejected.
            error: Primary failure retained while cleanup or error translation
                proceeds.

        Returns:
            Typed failed/cancelled outcome for admitted work, or a permissive intake
            rejection when the original command ID was invalid.
        """
        document = {
            "command_id": command.command_id
            if isinstance(command, ControllerCommand)
            else command.get("command_id"),
            "chain_id": command.chain_id
            if isinstance(command, ControllerCommand)
            else command.get("chain_id"),
            "state": "cancelled" if code == "command_cancelled" else "failed",
            "result": "fail",
            "experiment_id": self._runner.get_state()["experiment_id"],
            "data": None,
            "error": {
                "code": code,
                "message": message,
                "details": {"notes": list(getattr(error, "__notes__", []))}
                if error is not None and getattr(error, "__notes__", None)
                else {},
            },
        }
        # Rejected intake can lack even a valid command ID; keep its error reply.
        return (
            ControllerOutcome.model_validate(document)
            if isinstance(command, ControllerCommand)
            else ControllerRejection.model_validate(document)
        )

    async def close(self) -> None:
        """Stop controller tasks and resource monitoring and detach the runner observer.

        Cancels owned loops, reads, and current command, detaches the resource
        observer, closes the collector, and joins the intake thread. The outer
        lifecycle owner remains responsible for stopping/closing the runner.
        """
        if self._closing:
            return
        self._closing = True
        self._intake_stop.set()
        tasks = [*self._loops, *self._reads]
        if self._active_task is not None:
            tasks.append(self._active_task)
        for task in tasks:
            if task is not asyncio.current_task():
                task.cancel()
        await asyncio.gather(
            *(task for task in tasks if task is not asyncio.current_task()),
            return_exceptions=True,
        )
        self._active_tail.clear()
        self._runner.set_resource_observer(None)
        await self.resources.close()
        if self._intake_thread is not None:
            await asyncio.to_thread(self._intake_thread.join, 1)
