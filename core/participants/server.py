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

from core.journal.events import LoggingError
from core.journal.logger import OperationLogger
from core.models.participant_observations import CommandWork
from core.models.participant_protocol import (
    ParticipantHello,
    ParticipantRequest,
    ParticipantResult,
)
from core.participants.protocol import (
    PROTOCOL_VERSION,
    _participant_identity,
    _validated_request,
    encode_frame,
    read_frame,
)
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import JsonObject, copy_json_object, require_number
from core.primitives.processes import process_identity, process_running


class ParticipantServer:
    """Serve authenticated runner/module requests and journal participant outcomes.

    The caller owns the logger and application lifecycle. Start and close are
    explicit; close must be invoked by the owner after a request handler returns.
    """
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
        """Initialize protocol state and bind caller-owned handlers and logger.

        Args:
            endpoint_path: Absolute destination for the published endpoint document.
            context: Participant identity and journal context.
            logger: Open logger owned by the caller.
            handler: Async application/control request handler.
            module_handler: Optional async handler for module-client reports.
            describe: Optional synchronous callback extending command-state data.
            control_timeout_seconds: Positive handshake and control-reply timeout.

        Raises:
            ValueError: Endpoint path, identity, or timeout is invalid.
        """
        self.endpoint_path = Path(endpoint_path)
        if not self.endpoint_path.is_absolute():
            raise ValueError("endpoint_path must be absolute.")
        self.identity = _participant_identity(context)
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
        self._work: dict[str, asyncio.Task[ParticipantResult]] = {}
        self._requests: dict[str, CommandWork] = {}
        self._seen: set[str] = set()
        self._current: str | None = None
        self._work_lock = asyncio.Lock()
        self._completed: dict[str, asyncio.Event] = {}
        self._failure: BaseException | None = None
        self._stopping = False
        self._token_path = self.endpoint_path.with_name(
            f"{self.endpoint_path.stem}.{self.identity.participant_instance_id}.token"
        )

    async def start(self) -> None:
        """Listen on loopback and publish this instance's endpoint and secret token.

        Raises:
            RuntimeError: This instance was started/closed or the endpoint has a live owner.
            OSError: Token/endpoint publication or listener creation fails.
        """
        if self._server is not None or self._stopping:
            raise RuntimeError(
                "Start requires a new, open participant server instance."
            )
        if self.endpoint_path.exists():
            previous = read_json(self.endpoint_path)
            self._check_previous_endpoint(previous)
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
                    **self.identity.model_dump(),
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

    def _check_previous_endpoint(self, previous: JsonObject) -> None:
        """Reject replacement of an endpoint still owned by the recorded live process.

        Args:
            previous: Previously published endpoint JSON whose process ownership
                must be checked.
        """
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

    async def _send(self, writer: asyncio.StreamWriter, message: JsonObject) -> None:
        """Write one frame to a connected client under that client's write lock.

        Args:
            writer: Connected asyncio stream writer owned by the participant server.
            message: Complete JSON frame to send under the selected client's write
                lock.
        """
        entry = self._clients.get(writer)
        if entry is None:
            raise ConnectionError("Participant client disconnected.")
        async with entry[1]:
            writer.write(encode_frame(message))
            await writer.drain()

    async def notify_modules(self, command: str, data: JsonObject) -> None:
        """Send a notification to module clients, aborting connections that cannot receive it.

        Args:
            command: Notification name understood by connected module clients, such
                as cancel.
            data: JSON payload recorded or sent by this operation.
        """
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
        """Return whether an authenticated module client is currently connected."""
        return any(role == "module" for role, _ in self._clients.values())

    async def _handle_client(self, reader, writer) -> None:
        """Authenticate a client, admit fresh requests, and clean up its reply tasks.

        Args:
            reader: Accepted asyncio stream reader used to receive length-prefixed
                JSON frames.
            writer: Connected asyncio stream writer owned by the participant server.
        """
        task = asyncio.current_task()
        self._client_tasks.add(task)
        replies: set[asyncio.Task] = set()
        try:
            async with asyncio.timeout(self._timeout):
                hello = ParticipantHello.model_validate(await read_frame(reader))
            role = hello.role
            if (
                hello.identity != self.identity.model_dump()
                or role == "module"
                and self._module_handler is None
                or not hmac.compare_digest(hello.token, self._token)
            ):
                return
            self._clients[writer] = (role, asyncio.Lock())
            await self._send(
                writer,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "message_type": "hello",
                    "result": "success",
                    "data": self.identity.model_dump(),
                },
            )
            while True:
                request = _validated_request(await read_frame(reader), self.identity)
                request_id = request.request_id
                if request_id in self._seen:
                    raise ValueError("A request ID cannot be reused.")
                if (
                    role == "runner"
                    and request.command
                    not in ("heartbeat", "command_state", "interrupt", "shutdown")
                    and self._logger.read_command_result(request_id) is not None
                ):
                    raise ValueError(
                        "The request already has a durable journal result."
                    )
                self._seen.add(request_id)
                reply = asyncio.create_task(self._reply(writer, role, request))
                replies.add(reply)
                self._reply_commands[reply] = request.command
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

    async def _reply(self, writer, role: str, request: ParticipantRequest) -> None:
        """Route a validated request and send its response, retaining handler failures.

        Args:
            writer: Connected asyncio stream writer owned by the participant server.
            role: Authenticated client role, runner or module.
            request: Validated request envelope assigned to this handler.
        """
        try:
            command = request.command
            if role == "module":
                response = ParticipantResult.model_validate(
                    await self._module_handler(request.model_dump(exclude_unset=True))
                )
            elif command == "command_state":
                response = ParticipantResult.model_validate(self._command_state())
            elif command == "heartbeat" and (
                self._failure is not None or self._stopping
            ):
                response = ParticipantResult(
                    result="fail",
                    data={
                        "error": str(self._failure)
                        if self._failure is not None
                        else "stopping"
                    },
                )
            elif command in ("heartbeat", "interrupt", "shutdown"):
                response = await self._handle_control(
                    request.model_dump(exclude_unset=True), command
                )
            else:
                response = await asyncio.shield(self._start_work(request))
            await self._send(
                writer,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "message_type": "response",
                    "request_id": request.request_id,
                    **response.model_dump(exclude_unset=True),
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
                        "request_id": request.request_id,
                        "result": "fail",
                        "data": {"error": str(error), "code": "participant_failure"},
                    },
                )
            except OSError:
                pass

    def _start_work(
        self, request: ParticipantRequest
    ) -> asyncio.Task[ParticipantResult]:
        """Bind queued metadata and its public callback document before execution.

        Args:
            request: Validated request envelope assigned to this handler.

        Returns:
            Tracked task executing/journaling the request independently of whether
            its client remains connected.
        """
        request_id = request.request_id
        self._requests[request_id] = CommandWork(
            request_id=request_id, command=request.command
        )
        # The handler and its completion observer share the same public
        # document, including any edits made by the application handler.
        message = request.model_dump(exclude_unset=True)
        job = asyncio.create_task(self._run_work(request, message))
        self._work[request_id] = job
        job.add_done_callback(partial(self._observe_work, message))
        return job

    def _command_state(self) -> JsonObject:
        """Return described participant state with current and pending command identities.

        Returns:
            Described participant state with current and pending command identities.
        """
        response = {
            "result": "success",
            "data": {
                **({} if self._describe is None else self._describe()),
                "current": None
                if self._current is None
                else self._requests[self._current].model_dump(exclude_unset=True),
                "pending": [
                    entry.model_dump(exclude_unset=True)
                    for key, entry in self._requests.items()
                    if key != self._current
                ],
            },
        }
        return response

    async def _handle_control(
        self, request: JsonObject, command: str
    ) -> ParticipantResult:
        """Run a control handler and journal interrupt/shutdown outcomes.

        Args:
            request: Detached validated control request.
            command: Heartbeat, interrupt, or shutdown command name.

        Returns:
            Validated handler result after cancelling matching work on successful
            interruption/shutdown. Shutdown also closes admission of new work.
        """
        if command == "shutdown":
            self._stopping = True
        response = await self._handler(request)
        if command in ("interrupt", "shutdown") and response.get("result") == "success":
            target = request["args"].get("request_id")
            tasks = [
                job
                for key, job in self._work.items()
                if target is None or key == target
            ]
            for job in tasks:
                job.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        result = ParticipantResult.model_validate(response)
        if command in ("interrupt", "shutdown"):
            self._logger.record_command_result(
                request["request_id"],
                result.model_dump(exclude_unset=True),
                author="participant",
                outcome="succeeded" if result.result == "success" else "failed",
                context={**self._context, "request_id": request["request_id"]},
            )
        return result

    def _observe_work(
        self, request: JsonObject, task: asyncio.Task[ParticipantResult]
    ) -> None:
        """Record pre-start cancellation, retain task failure, and signal call completion.

        Args:
            request: Validated request envelope assigned to this handler.
            task: Working request task whose completion/cancellation is observed.
        """
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
        """Return whether a call is tracked as running, queued, or completed.

        Args:
            request_id: UUID correlating the admitted request and its eventual
                outcome.

        Returns:
            Whether a call is tracked as running, queued, or completed.
        """
        completed = self._completed.get(request_id)
        return request_id in self._work or (
            completed is not None and completed.is_set()
        )

    def _call_context(self, request: JsonObject) -> JsonObject:
        """Merge base/call context while preserving server identity and the request ID.

        Args:
            request: Detached handler request whose execute context may extend base
                context.

        Returns:
            Merged journal context with server-owned identity and request ID
            overriding caller-supplied values.
        """
        call_context = (
            request["args"].get("context", {})
            if request["command"] == "execute"
            else {}
        )
        return {
            **self._context,
            **call_context,
            **self.identity.model_dump(),
            "request_id": request["request_id"],
        }

    async def _run_work(
        self, request: ParticipantRequest, handler_request: JsonObject
    ) -> ParticipantResult:
        """Serialize application work and journal its outcome before reporting completion.

        Args:
            request: Validated request with identity and optional queue deadline.
            handler_request: Corresponding JSON request passed to the application.

        Returns:
            Validated result, including failures for queue timeout or handler errors.

        The queue deadline bounds lock acquisition; the handler owns stopping its
        external work. Journal failures are retained and propagated.
        """
        request_id = request.request_id
        acquired = False
        try:
            context = self._call_context(handler_request)
            deadline = request.deadline_monotonic
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
                    {"request_id": request_id, "command": request.command},
                    context=context,
                )
                # External work belongs to its handler; an expired waiter alone
                # must not release this lock while that work is still running.
                response = ParticipantResult.model_validate(
                    await self._handler(handler_request)
                )
            except TimeoutError:
                response = ParticipantResult(
                    result="fail", data={"reason": "queue_timeout"}
                )
            except asyncio.CancelledError:
                response = ParticipantResult(
                    result="fail", data={"reason": "interrupted"}
                )
            except LoggingError:
                raise
            except Exception as error:  # noqa: BLE001 - Preserve the failure of an author-provided handler.
                response = ParticipantResult(
                    result="fail",
                    data={"code": "command_failed", "message": str(error)},
                )
            self._logger.record_command_result(
                request_id,
                response.model_dump(exclude_unset=True),
                author="participant",
                outcome="succeeded" if response.result == "success" else "failed",
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
        """Wait for a request's completion signal and propagate any server failure.

        The completion signal is separate from network reply delivery. After it is
        set, any retained server/journal failure is re-raised to the lifecycle
        owner.

        Args:
            request_id: UUID correlating the admitted request and its eventual
                outcome.
        """
        await self._completed.setdefault(request_id, asyncio.Event()).wait()
        if self._failure is not None:
            raise self._failure

    async def close(self) -> None:
        """Stop admission, drain control replies, and release owned tasks and endpoint files.

        The caller remains responsible for the logger and application resources.
        A replacement instance's endpoint is preserved.

        Raises:
            RuntimeError: Called from a reply handler instead of the owning lifecycle.
        """
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
            and _participant_identity(read_json(self.endpoint_path)) == self.identity
        ):
            self.endpoint_path.unlink()
        self._token_path.unlink(missing_ok=True)
