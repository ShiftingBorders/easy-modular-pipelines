"""Authenticated participant client with one independent response reader."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import UUID

from core.models.participant_identity import ParticipantIdentity
from core.models.participant_protocol import (
    ParticipantEndpoint,
    ParticipantHelloReply,
    ParticipantNotification,
    ParticipantReply,
    ParticipantResponse,
)
from core.participants.protocol import (
    PROTOCOL_VERSION,
    _participant_identity,
    encode_frame,
    read_frame,
)
from core.primitives.json_files import read_json
from core.primitives.json_values import JsonObject, copy_json_object, require_text
from core.primitives.processes import process_identity


class ParticipantConnection:
    def __init__(
        self,
        endpoint_path: Path,
        expected_identity: ParticipantIdentity | JsonObject,
        *,
        role: str = "runner",
    ) -> None:
        self._endpoint_path = Path(endpoint_path)
        if not self._endpoint_path.is_absolute():
            raise ValueError("endpoint_path must be absolute.")
        self._identity = _participant_identity(expected_identity)
        if role not in ("runner", "module"):
            raise ValueError("Client role must be runner or module.")
        self._role = role
        self._reader = None
        self._writer = None
        self._receive_task: asyncio.Task | None = None
        self._write_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future[ParticipantResponse]] = {}
        self._used_ids: set[str] = set()
        self._notifications: asyncio.Queue[
            ParticipantNotification | ConnectionError
        ] = asyncio.Queue()

    async def connect(self, *, timeout_seconds: float) -> None:
        await self.close()
        self._notifications = asyncio.Queue()
        async with asyncio.timeout(timeout_seconds):
            endpoint = ParticipantEndpoint.model_validate(
                await asyncio.to_thread(read_json, self._endpoint_path)
            )
            if any(
                getattr(endpoint, name) != getattr(self._identity, name)
                for name in (
                    "experiment_id",
                    "participant_id",
                    "participant_instance_id",
                )
            ):
                raise ValueError("Participant endpoint identity mismatch.")
            actual = await asyncio.to_thread(process_identity, endpoint.process.pid)
            if actual != endpoint.process.model_dump():
                raise ValueError("Participant OS identity differs from its endpoint.")
            address = endpoint.endpoint
            token_path = Path(address.token_file)
            if not token_path.is_absolute():
                token_path = self._endpoint_path.parent / token_path
            token = await asyncio.to_thread(token_path.read_text, encoding="utf-8")
            try:
                self._reader, self._writer = await asyncio.open_connection(
                    address.host, address.port
                )
                await self.send_message(
                    {
                        "protocol_version": PROTOCOL_VERSION,
                        "message_type": "hello",
                        "identity": self._identity.model_dump(),
                        "role": self._role,
                        "token": token,
                    }
                )
                reply = ParticipantHelloReply.model_validate(
                    await read_frame(self._reader)
                )
                if reply.data != self._identity.model_dump():
                    raise ValueError("Participant handshake failed.")
                self._receive_task = asyncio.create_task(self._receive_loop())
            except BaseException:
                await self.close()
                raise

    async def _receive_loop(self) -> None:
        failure = ConnectionError("Participant connection closed.")
        try:
            while True:
                message = ParticipantReply.validate_python(
                    await read_frame(self._reader)
                )
                if isinstance(message, ParticipantNotification):
                    if self._role != "module":
                        raise ValueError(
                            "Unexpected notification on the runner channel."
                        )
                    await self._notifications.put(message)
                    continue
                request_id = str(UUID(message.request_id))
                future = self._pending.get(request_id)
                if future is not None and not future.done():
                    future.set_result(message)
                # A late reply never becomes the result of another request.
        except asyncio.CancelledError:
            pass
        except (OSError, EOFError, ValueError, TypeError, KeyError) as error:
            failure = ConnectionError(f"Participant receive failed: {error}")
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(failure)
            await self._notifications.put(failure)
            if self._writer is not None:
                self._writer.transport.abort()

    async def receive_message(self) -> JsonObject:
        """Receive a notification; replies belong to their request waiter."""
        return (await self._receive_notification()).model_dump(exclude_unset=True)

    async def _receive_notification(self) -> ParticipantNotification:
        message = await self._notifications.get()
        if isinstance(message, Exception):
            raise message
        return message

    async def send_message(self, message: JsonObject) -> None:
        if self._writer is None or self._writer.is_closing():
            raise ConnectionError("Participant connection is not open.")
        async with self._write_lock:
            self._writer.write(encode_frame(message))
            await self._writer.drain()

    async def request(
        self,
        request_id: str,
        command: str,
        args: JsonObject,
        *,
        timeout_seconds: float | None = None,
        deadline_monotonic: float | None = None,
    ) -> JsonObject:
        response = await self._request(
            request_id,
            command,
            args,
            timeout_seconds=timeout_seconds,
            deadline_monotonic=deadline_monotonic,
        )
        return response.model_dump(exclude_unset=True)

    async def _request(
        self,
        request_id: str,
        command: str,
        args: JsonObject,
        *,
        timeout_seconds: float | None = None,
        deadline_monotonic: float | None = None,
    ) -> ParticipantResponse:
        request_id = str(UUID(require_text(request_id, "request_id")))
        if request_id in self._used_ids:
            raise ValueError("A request_id cannot be sent twice.")
        self._used_ids.add(request_id)
        future: asyncio.Future[ParticipantResponse] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[request_id] = future
        try:
            async with asyncio.timeout(timeout_seconds):
                await self.send_message(
                    {
                        "protocol_version": PROTOCOL_VERSION,
                        "message_type": "request",
                        **self._identity.model_dump(),
                        "request_id": request_id,
                        "command": command,
                        "args": copy_json_object(args, "request arguments"),
                        "deadline_monotonic": deadline_monotonic,
                    }
                )
                return await future
        finally:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def query_command_state(
        self, request_id: str, *, timeout_seconds: float
    ) -> JsonObject:
        return await self.request(
            request_id, "command_state", {}, timeout_seconds=timeout_seconds
        )

    async def close(self) -> None:
        task, self._receive_task = self._receive_task, None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        writer, self._writer = self._writer, None
        self._reader = None
        if writer is not None:
            writer.transport.abort()
            try:
                await writer.wait_closed()
            except OSError:
                pass
