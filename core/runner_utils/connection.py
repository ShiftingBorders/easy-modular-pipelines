"""Authenticated participant client with one independent response reader."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import UUID

from core.logger_utils.events import JsonObject, copy_json_object, require_text
from core.runner_utils.protocol import (
    PROTOCOL_VERSION,
    encode_frame,
    participant_identity,
    read_frame,
    validate_response,
)
from core.runner_utils.runtimeio import process_identity, read_json


class ParticipantConnection:
    def __init__(
        self,
        endpoint_path: Path,
        expected_identity: JsonObject,
        *,
        role: str = "runner",
    ) -> None:
        self._endpoint_path = Path(endpoint_path)
        if not self._endpoint_path.is_absolute():
            raise ValueError("endpoint_path must be absolute.")
        self._identity = participant_identity(expected_identity)
        if role not in ("runner", "module"):
            raise ValueError("Client role must be runner or module.")
        self._role = role
        self._reader = None
        self._writer = None
        self._receive_task: asyncio.Task | None = None
        self._write_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future] = {}
        self._used_ids: set[str] = set()
        self._notifications: asyncio.Queue = asyncio.Queue()

    async def connect(self, *, timeout_seconds: float) -> None:
        await self.close()
        self._notifications = asyncio.Queue()
        async with asyncio.timeout(timeout_seconds):
            endpoint = await asyncio.to_thread(read_json, self._endpoint_path)
            if (
                type(endpoint.get("protocol_version")) is not int
                or endpoint["protocol_version"] != PROTOCOL_VERSION
            ):
                raise ValueError(
                    "Unsupported endpoint protocol; old experiments are not supported."
                )
            if participant_identity(endpoint) != self._identity:
                raise ValueError("Participant endpoint identity mismatch.")
            actual = await asyncio.to_thread(
                process_identity, endpoint["process"]["pid"]
            )
            if actual != endpoint["process"]:
                raise ValueError("Participant OS identity differs from its endpoint.")
            address = endpoint["endpoint"]
            if address["host"] != "127.0.0.1" or type(address["port"]) is not int:
                raise ValueError("Participant must listen on loopback.")
            token_path = Path(address["token_file"])
            if not token_path.is_absolute():
                token_path = self._endpoint_path.parent / token_path
            token = await asyncio.to_thread(token_path.read_text, encoding="utf-8")
            try:
                self._reader, self._writer = await asyncio.open_connection(
                    address["host"], address["port"]
                )
                await self.send_message(
                    {
                        "protocol_version": PROTOCOL_VERSION,
                        "message_type": "hello",
                        "identity": self._identity,
                        "role": self._role,
                        "token": token,
                    }
                )
                reply = await read_frame(self._reader)
                if (
                    type(reply.get("protocol_version")) is not int
                    or reply["protocol_version"] != PROTOCOL_VERSION
                    or reply.get("message_type") != "hello"
                    or reply.get("result") != "success"
                    or reply.get("data") != self._identity
                ):
                    raise ValueError("Participant handshake failed.")
                self._receive_task = asyncio.create_task(self._receive_loop())
            except BaseException:
                await self.close()
                raise

    async def _receive_loop(self) -> None:
        failure = ConnectionError("Participant connection closed.")
        try:
            while True:
                message = await read_frame(self._reader)
                if (
                    type(message.get("protocol_version")) is not int
                    or message["protocol_version"] != PROTOCOL_VERSION
                ):
                    raise ValueError("Unsupported response protocol.")
                if message.get("message_type") == "notification":
                    if self._role != "module":
                        raise ValueError(
                            "Unexpected notification on the runner channel."
                        )
                    await self._notifications.put(message)
                    continue
                if message.get("message_type") != "response":
                    raise ValueError("Expected a participant response.")
                request_id = str(
                    UUID(require_text(message.get("request_id"), "request_id"))
                )
                validate_response(message, envelope=True)
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
        request_id = str(UUID(require_text(request_id, "request_id")))
        if request_id in self._used_ids:
            raise ValueError("A request_id cannot be sent twice.")
        self._used_ids.add(request_id)
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            async with asyncio.timeout(timeout_seconds):
                await self.send_message(
                    {
                        "protocol_version": PROTOCOL_VERSION,
                        "message_type": "request",
                        **self._identity,
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
