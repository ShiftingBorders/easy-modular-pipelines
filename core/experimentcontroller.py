"""Responsive command intake, serial control chains, and independent read requests."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from multiprocessing.queues import Queue
from pathlib import Path
from queue import Empty, Full
from uuid import UUID

from core.logger_utils.events import LoggingError, copy_json_object, require_text
from core.resourcecollector import ResourceCollector
from core.runner_utils.experimentrunner import ExperimentRunner
from core.runner_utils.state import JsonObject
from core.storage_errors import StorageCapacityError, StorageConflict, StorageError


class ExperimentController:
    def __init__(
        self,
        project_root: Path,
        runner: ExperimentRunner,
        requests: Queue,
        responses: Queue,
        *,
        resource_config_path: Path | None = None,
    ) -> None:
        self._project_root = Path(project_root)
        if not self._project_root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._runner = runner
        self._requests = requests
        self._responses = responses
        self._control_queue: asyncio.Queue[list[JsonObject]] = asyncio.Queue()
        self._outbox: asyncio.Queue[JsonObject] = asyncio.Queue()
        self._current_command = None
        self._active_tail: list[JsonObject] = []
        self._active_task = None
        self._loops: list[asyncio.Task] = []
        self._reads: set[asyncio.Task] = set()
        self._closing = False
        self.resources = ResourceCollector(
            Path(__file__).resolve().parents[1]
            / "default_settings"
            / "resource_collector.json"
            if resource_config_path is None
            else resource_config_path
        )

    async def serve(self) -> None:
        if self._loops or self._closing:
            raise RuntimeError("Controller is already running or closed.")
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

    async def _receive_requests(self) -> None:
        while not self._closing:
            try:
                request = await asyncio.to_thread(self._requests.get, True, 0.1)
            except Empty:
                continue
            try:
                request = copy_json_object(request, "request")
                if (
                    type(request.get("api_version")) is not int
                    or request["api_version"] != 1
                ):
                    raise ValueError("api_version must be 1.")
                if "commands" in request:
                    UUID(require_text(request.get("chain_id"), "chain_id"))
                    commands = request["commands"]
                    if not isinstance(commands, list) or not commands:
                        raise ValueError("A command chain must be nonempty.")
                    batch = []
                    for command in commands:
                        command = copy_json_object(command, "chain command")
                        UUID(require_text(command.get("command_id"), "command_id"))
                        if "commands" in command:
                            raise ValueError("Nested chains are not supported.")
                        batch.append(
                            {
                                **command,
                                "api_version": 1,
                                "chain_id": request["chain_id"],
                            }
                        )
                else:
                    UUID(require_text(request.get("command_id"), "command_id"))
                    name = require_text(request.get("command"), "command")
                    if name.startswith(("stats.", "logs.")):
                        task = asyncio.create_task(self._answer_read(request))
                        self._reads.add(task)
                        task.add_done_callback(self._reads.discard)
                        continue
                    batch = [request]
                    if name == "stop":
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
                await self._control_queue.put(batch)
            except (TypeError, ValueError, KeyError) as error:
                command = request if isinstance(request, dict) else {}
                await self._publish_response(
                    self._failure(command, "invalid_request", str(error))
                )

    async def _execute_commands(self) -> None:
        while not self._closing:
            self._active_tail = await self._control_queue.get()
            while self._active_tail and not self._closing:
                command = self._active_tail.pop(0)
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
                await self._publish_response(response)
                if response["result"] == "fail":
                    cancelled, self._active_tail = self._active_tail, []
                    for remaining in cancelled:
                        await self._publish_response(
                            self._failure(
                                remaining,
                                "command_cancelled",
                                "Previous command failed.",
                            )
                        )

    async def _execute_command(self, command: JsonObject) -> JsonObject:
        name = ""
        try:
            if command.keys() - {
                "api_version",
                "command_id",
                "chain_id",
                "command",
                "args",
                "target",
            }:
                raise ValueError("Unknown command fields.")
            name = require_text(command.get("command"), "command")
            args = copy_json_object(command.get("args", {}), "args")
            if name.startswith(("stats.", "logs.")):
                data = await self._read_request(command)
            else:
                target = copy_json_object(command.get("target", {}), "target")
                if target:
                    if target.keys() != {"kind", "position"} or target["kind"] not in (
                        "stage",
                        "service",
                    ):
                        raise ValueError("target requires kind and position.")
                    if type(target["position"]) is not int or target["position"] < 1:
                        raise ValueError("target.position must be a positive integer.")
                    if name == "retry":
                        if target["kind"] != "service":
                            raise ValueError("retry targets a service.")
                        args["position"] = target["position"]
                    elif name in ("replace", "reset_retries"):
                        args.update(target)
                    else:
                        raise ValueError("This command does not accept target.")
                if name == "run" and "continue" in args:
                    args["continue_run"] = args.pop("continue")
                for field in ("template_path", "archive_path", "destination"):
                    if field in args and args[field] is not None:
                        args[field] = Path(require_text(args[field], field))
                handlers = {
                    "run": self._runner.run,
                    "pause": self._runner.pause,
                    "resume": self._runner.resume,
                    "stop": self._runner.stop,
                    "step": self._runner.step,
                    "rerun": self._runner.rerun,
                    "retry": self._runner.retry,
                    "move": self._runner.move,
                    "reset_retries": self._runner.reset_retries,
                    "replace": self._runner.replace,
                    "reload_template": self._runner.reload_template,
                    "snapshot": self._runner.snapshot,
                    "rollback": self._runner.rollback,
                    "recover": self._runner.recover,
                    "archive.create": self._runner.create_archive,
                    "archive.inspect": self._runner.inspect_archive,
                    "archive.install": self._runner.install_archive,
                }
                if name not in handlers:
                    raise NotImplementedError(f"Unsupported command: {name}")
                data = handlers[name](**args)
                if isinstance(data, Coroutine):
                    data = await data
                if data is None:
                    data = {}
            return {
                "command_id": command["command_id"],
                "chain_id": command.get("chain_id"),
                "state": "succeeded",
                "result": "success",
                "experiment_id": self._runner.get_state()["experiment_id"],
                "data": data,
                "error": None,
            }
        except LoggingError as error:
            if not name.startswith("archive.") and (
                not name.startswith(("stats.", "logs."))
                or getattr(error, "journal_failed", False)
            ):
                await self._runner._fail(
                    error, {"command_id": command.get("command_id")}
                )
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

    async def _read_request(self, request: JsonObject) -> JsonObject:
        args = copy_json_object(request.get("args", {}), "args")
        if request["command"] == "stats.resources":
            if args:
                raise ValueError("stats.resources does not accept arguments.")
            return self.resources.get_status()
        if request["command"] == "stats.resources.history":
            if args.keys() - {"after", "limit"}:
                raise ValueError("Unknown resource history arguments.")
            return self.resources.read_history(**args)
        if request["command"] == "stats.state":
            if args.keys() - {"experiment_id"}:
                raise ValueError("Unknown stats.state arguments.")
            state = self._runner.get_state()
            if args.get("experiment_id") not in (None, state["experiment_id"]):
                raise FileNotFoundError("The requested experiment is not selected.")
            return {**state, "current_command": self._current_command}
        if request["command"] == "logs.read":
            if args.keys() - {"experiment_id", "cursor", "limit"}:
                raise ValueError("Unknown logs.read arguments.")
            experiment_id = require_text(args.get("experiment_id"), "experiment_id")
            return await self._runner.read_events(
                experiment_id, args.get("cursor"), limit=args.get("limit", 100)
            )
        raise NotImplementedError(
            "The initial read API provides stats.state and logs.read."
        )

    async def _answer_read(self, command: JsonObject) -> None:
        await self._publish_response(await self._execute_command(command))

    async def _publish_response(self, response: JsonObject) -> None:
        self._outbox.put_nowait(copy_json_object(response, "response"))

    async def _write_responses(self) -> None:
        while not self._closing:
            response = await self._outbox.get()
            while not self._closing:
                try:
                    await asyncio.to_thread(self._responses.put, response, True, 0.1)
                    break
                except Full:
                    continue

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

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
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
