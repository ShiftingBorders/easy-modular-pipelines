"""Shared participant server; module handlers own their actual external work."""

from __future__ import annotations

import asyncio
import hmac
import os
import secrets
import time
from collections.abc import Awaitable, Callable
from functools import partial
from pathlib import Path

from core.logger import OperationLogger
from core.logger_utils.events import (
    JsonObject,
    LoggingError,
    copy_json_object,
    require_number,
)
from core.runner_utils.protocol import (
    PROTOCOL_VERSION,
    encode_frame,
    participant_identity,
    read_frame,
    validate_request,
    validate_response,
)
from core.runner_utils.runtimeio import (
    process_identity,
    process_running,
    read_json,
    write_json,
)


class ParticipantServer:
    def __init__(
        self,
        endpoint_path: Path,
        context: JsonObject,
        logger: OperationLogger,
        handler: Callable[[JsonObject], Awaitable[JsonObject]],
        *,
        module_handler: Callable[[JsonObject], Awaitable[JsonObject]] | None = None,
        describe: Callable[[], JsonObject] | None = None,
        control_timeout_seconds: float = 30,
    ) -> None:
        self.endpoint_path = Path(endpoint_path)
        if not self.endpoint_path.is_absolute():
            raise ValueError("endpoint_path must be absolute.")
        self.identity = participant_identity(context)
        self._context = copy_json_object(context, "participant context")
        self._logger = logger
        self._handler = handler
        self._module_handler = module_handler
        self._describe = describe
        self._timeout = require_number(control_timeout_seconds, "control timeout")
        if self._timeout <= 0:
            raise ValueError("control timeout must be positive.")
        self._server = None
        self._clients: dict[asyncio.StreamWriter, tuple[str, asyncio.Lock]] = {}
        self._client_tasks: set[asyncio.Task] = set()
        self._reply_commands: dict[asyncio.Task, str] = {}
        self._work: dict[str, asyncio.Task] = {}
        self._requests: dict[str, JsonObject] = {}
        self._seen: set[str] = set()
        self._current: str | None = None
        self._work_lock = asyncio.Lock()
        self._completed: dict[str, asyncio.Event] = {}
        self._failure: BaseException | None = None
        self._stopping = False
        self._token_path = self.endpoint_path.with_name(
            f"{self.endpoint_path.stem}.{self.identity['participant_instance_id']}.token"
        )

    async def start(self) -> None:
        if self._server is not None or self._stopping:
            raise RuntimeError(
                "Start requires a new, open participant server instance."
            )
        if self.endpoint_path.exists():
            previous = read_json(self.endpoint_path)
            identity = previous.get("process")
            if isinstance(identity, dict):
                try:
                    # Windows retains creation identity while another process
                    # holds a handle to an already terminated child.
                    if (
                        process_running(identity["pid"])
                        and process_identity(identity["pid"]) == identity
                    ):
                        raise RuntimeError(
                            "The endpoint is still owned by a live participant."
                        )
                except (FileNotFoundError, ProcessLookupError):
                    pass
                except OSError as error:
                    if getattr(error, "winerror", None) not in (87, 1168):
                        raise
        self.endpoint_path.parent.mkdir(parents=True, exist_ok=True)
        self._token = secrets.token_urlsafe(48)
        self._token_path.write_text(self._token, encoding="utf-8")
        self._token_path.chmod(0o600)
        self._server = await asyncio.start_server(self._handle_client, "127.0.0.1", 0)
        try:
            write_json(
                self.endpoint_path,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    **self.identity,
                    "process": process_identity(os.getpid()),
                    "endpoint": {
                        "host": "127.0.0.1",
                        "port": self._server.sockets[0].getsockname()[1],
                        "token_file": str(self._token_path),
                    },
                },
            )
        except BaseException:
            await self.close()
            raise

    async def _send(self, writer: asyncio.StreamWriter, message: JsonObject) -> None:
        entry = self._clients.get(writer)
        if entry is None:
            raise ConnectionError("Participant client disconnected.")
        async with entry[1]:
            writer.write(encode_frame(message))
            await writer.drain()

    async def notify_modules(self, command: str, data: JsonObject) -> None:
        for writer, (role, _) in list(self._clients.items()):
            if role == "module":
                try:
                    async with asyncio.timeout(self._timeout):
                        await self._send(
                            writer,
                            {
                                "protocol_version": PROTOCOL_VERSION,
                                "message_type": "notification",
                                "command": command,
                                "data": data,
                            },
                        )
                except (OSError, TimeoutError):
                    writer.transport.abort()

    def has_module_client(self) -> bool:
        return any(role == "module" for role, _ in self._clients.values())

    async def _handle_client(self, reader, writer) -> None:
        task = asyncio.current_task()
        self._client_tasks.add(task)
        replies: set[asyncio.Task] = set()
        try:
            async with asyncio.timeout(self._timeout):
                hello = await read_frame(reader)
            role = hello.get("role")
            token = hello.get("token")
            if (
                type(hello.get("protocol_version")) is not int
                or hello["protocol_version"] != PROTOCOL_VERSION
                or hello.get("message_type") != "hello"
                or hello.get("identity") != self.identity
                or role not in ("runner", "module")
                or role == "module"
                and self._module_handler is None
                or not isinstance(token, str)
                or not hmac.compare_digest(token, self._token)
            ):
                return
            self._clients[writer] = (role, asyncio.Lock())
            await self._send(
                writer,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "message_type": "hello",
                    "result": "success",
                    "data": self.identity,
                },
            )
            while True:
                request = await read_frame(reader)
                validate_request(request, self.identity)
                request_id = request["request_id"]
                if request_id in self._seen:
                    raise ValueError("A request ID cannot be reused.")
                if (
                    role == "runner"
                    and request["command"]
                    not in ("heartbeat", "command_state", "interrupt", "shutdown")
                    and self._logger.read_command_result(request_id) is not None
                ):
                    raise ValueError(
                        "The request already has a durable journal result."
                    )
                self._seen.add(request_id)
                reply = asyncio.create_task(self._reply(writer, role, request))
                replies.add(reply)
                self._reply_commands[reply] = request["command"]
                reply.add_done_callback(replies.discard)
                reply.add_done_callback(self._reply_commands.pop)
        except LoggingError as error:
            self._failure = error
        except (OSError, EOFError, ValueError, TypeError, KeyError, TimeoutError):
            pass
        finally:
            for reply in replies:
                reply.cancel()
            await asyncio.gather(*replies, return_exceptions=True)
            self._clients.pop(writer, None)
            writer.transport.abort()
            self._client_tasks.discard(task)

    async def _reply(self, writer, role: str, request: JsonObject) -> None:
        try:
            command = request["command"]
            if role == "module":
                response = await self._module_handler(request)
            elif command == "command_state":
                response = {
                    "result": "success",
                    "data": {
                        **({} if self._describe is None else self._describe()),
                        "current": None
                        if self._current is None
                        else self._requests[self._current],
                        "pending": [
                            entry
                            for key, entry in self._requests.items()
                            if key != self._current
                        ],
                    },
                }
            elif command == "heartbeat" and (
                self._failure is not None or self._stopping
            ):
                response = {
                    "result": "fail",
                    "data": {
                        "error": str(self._failure)
                        if self._failure is not None
                        else "stopping"
                    },
                }
            elif command in ("heartbeat", "interrupt", "shutdown"):
                if command == "shutdown":
                    self._stopping = True
                response = await self._handler(request)
                if (
                    command in ("interrupt", "shutdown")
                    and response.get("result") == "success"
                ):
                    target = request["args"].get("request_id")
                    tasks = [
                        job
                        for key, job in self._work.items()
                        if target is None or key == target
                    ]
                    for job in tasks:
                        job.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                if command in ("interrupt", "shutdown"):
                    self._logger.record_command_result(
                        request["request_id"],
                        validate_response(response),
                        author="participant",
                        outcome="succeeded"
                        if response["result"] == "success"
                        else "failed",
                        context={**self._context, "request_id": request["request_id"]},
                    )
            else:
                request_id = request["request_id"]
                self._requests[request_id] = {
                    "request_id": request_id,
                    "command": command,
                }
                job = asyncio.create_task(self._run_work(request))
                self._work[request_id] = job
                job.add_done_callback(partial(self._observe_work, request))
                response = await asyncio.shield(job)
            await self._send(
                writer,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "message_type": "response",
                    "request_id": request["request_id"],
                    **validate_response(response),
                },
            )
        except (OSError, TimeoutError):
            writer.transport.abort()
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - A handler failure must not escape the connection task.
            self._failure = error
            try:
                await self._send(
                    writer,
                    {
                        "protocol_version": PROTOCOL_VERSION,
                        "message_type": "response",
                        "request_id": request["request_id"],
                        "result": "fail",
                        "data": {"error": str(error), "code": "participant_failure"},
                    },
                )
            except OSError:
                pass

    def _observe_work(self, request: JsonObject, task: asyncio.Task) -> None:
        request_id = request["request_id"]
        if task.cancelled():
            # A task cancelled before its first instruction never reaches finally.
            try:
                self._logger.record_command_result(
                    request_id,
                    {"result": "fail", "data": {"reason": "cancelled_before_start"}},
                    author="participant",
                    outcome="cancelled",
                    context=self._call_context(request),
                )
            except Exception as error:  # noqa: BLE001 - Surface a failed durable cancellation.
                self._failure = error
            self._work.pop(request_id, None)
            self._requests.pop(request_id, None)
        elif task.exception() is not None:
            self._failure = task.exception()
        self._completed.setdefault(request_id, asyncio.Event()).set()

    def has_call(self, request_id: str) -> bool:
        completed = self._completed.get(request_id)
        return request_id in self._work or (
            completed is not None and completed.is_set()
        )

    def _call_context(self, request: JsonObject) -> JsonObject:
        call_context = (
            copy_json_object(request["args"].get("context", {}), "call context")
            if request["command"] == "execute"
            else {}
        )
        return {
            **self._context,
            **call_context,
            **self.identity,
            "request_id": request["request_id"],
        }

    async def _run_work(self, request: JsonObject) -> JsonObject:
        request_id = request["request_id"]
        acquired = False
        try:
            context = self._call_context(request)
            deadline = request.get("deadline_monotonic")
            if deadline is not None:
                require_number(deadline, "request deadline")
            remaining = (
                None if deadline is None else max(0, deadline - time.monotonic())
            )
            try:
                if self._stopping:
                    raise RuntimeError(
                        "Participant is stopping; no new work is accepted."
                    )
                async with asyncio.timeout(remaining):
                    await self._work_lock.acquire()
                acquired = True
                self._current = request_id
                if self._stopping:
                    raise RuntimeError(
                        "Participant stopped accepting work while the request was queued."
                    )
                self._logger.record_event(
                    "call.started",
                    {"request_id": request_id, "command": request["command"]},
                    context=context,
                )
                # External work belongs to its handler; an expired waiter alone
                # must not release this lock while that work is still running.
                response = validate_response(await self._handler(request))
            except TimeoutError:
                response = {"result": "fail", "data": {"reason": "queue_timeout"}}
            except asyncio.CancelledError:
                response = {"result": "fail", "data": {"reason": "interrupted"}}
            except LoggingError:
                raise
            except Exception as error:  # noqa: BLE001 - Preserve the failure of an author-provided handler.
                response = {
                    "result": "fail",
                    "data": {"code": "command_failed", "message": str(error)},
                }
            self._logger.record_command_result(
                request_id,
                response,
                author="participant",
                outcome="succeeded" if response["result"] == "success" else "failed",
                context=context,
            )
            return response
        except BaseException as error:
            self._failure = error
            raise
        finally:
            if acquired:
                self._current = None
                self._work_lock.release()
            self._requests.pop(request_id, None)
            self._work.pop(request_id, None)
            self._completed.setdefault(request_id, asyncio.Event()).set()

    async def wait_completed(self, request_id: str) -> None:
        await self._completed.setdefault(request_id, asyncio.Event()).wait()
        if self._failure is not None:
            raise self._failure

    async def close(self) -> None:
        if asyncio.current_task() in self._reply_commands:
            raise RuntimeError(
                "Close the server from its owner, after the request handler returns."
            )
        self._stopping = True
        server, self._server = self._server, None
        if server is not None:
            server.close()
        replies = [
            task
            for task, command in self._reply_commands.items()
            if command in ("interrupt", "shutdown")
        ]
        if replies:
            await asyncio.wait(replies, timeout=self._timeout)
        tasks = [*self._work.values(), *self._client_tasks]
        tasks = [task for task in tasks if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for writer in list(self._clients):
            writer.transport.abort()
        self._clients.clear()
        # Python 3.12 waits for accepted client transports as well as the listener.
        # Close those transports before waiting for the server to finish.
        if server is not None:
            await server.wait_closed()
        # A previous instance must not remove its replacement's endpoint.
        if (
            self.endpoint_path.is_file()
            and participant_identity(read_json(self.endpoint_path)) == self.identity
        ):
            self.endpoint_path.unlink()
        self._token_path.unlink(missing_ok=True)
