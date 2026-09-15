"""Authenticated, framed TCP communication with a stage executor."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID

from core.logger_utils.events import copy_json_object
from core.runner_utils.runtimeio import process_identity, read_json
from core.runner_utils.state import JsonObject


class ParticipantConnection:
    def __init__(self, endpoint_path: Path, expected_identity: JsonObject) -> None:
        self._endpoint_path = Path(endpoint_path)
        if not self._endpoint_path.is_absolute():
            raise ValueError("endpoint_path must be absolute.")
        self._expected_identity = copy_json_object(
            expected_identity, "expected_identity"
        )
        self._reader = None
        self._writer = None
        self._write_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()
        self._used_ids: set[str] = set()

    async def connect(self, *, timeout_seconds: float) -> None:
        endpoint = await asyncio.to_thread(read_json, self._endpoint_path)
        for name, value in self._expected_identity.items():
            if endpoint.get(name) != value:
                raise ValueError(f"Participant identity mismatch: {name}")
        identity = await asyncio.to_thread(
            process_identity, endpoint["executor"]["pid"]
        )
        if identity != endpoint["executor"]:
            raise ValueError("Executor OS identity differs from its endpoint file.")
        address = endpoint["endpoint"]
        if address["host"] != "127.0.0.1" or type(address["port"]) is not int:
            raise ValueError("Participant must listen on loopback.")
        token_path = Path(address["token_file"])
        if not token_path.is_absolute():
            token_path = self._endpoint_path.parent / token_path
        token = await asyncio.to_thread(token_path.read_text, encoding="utf-8")
        try:
            async with asyncio.timeout(timeout_seconds):
                self._reader, self._writer = await asyncio.open_connection(
                    address["host"], address["port"]
                )
                await self.send_message(
                    {
                        "protocol_version": 1,
                        "message_type": "hello",
                        "identity": self._expected_identity,
                        "token": token,
                    }
                )
                reply = await self.receive_message()
                if (
                    reply.get("result") != "success"
                    or reply.get("data") != self._expected_identity
                ):
                    raise ValueError("Participant handshake failed.")
        except BaseException:
            await self.close()
            raise

    async def receive_message(self) -> JsonObject:
        if self._reader is None:
            raise RuntimeError("Connection is not open.")
        try:
            size = int.from_bytes(await self._reader.readexactly(8), "big")
            if not size:
                raise ValueError("Empty protocol frame.")
            payload = await self._reader.readexactly(size)
            return copy_json_object(
                json.loads(payload.decode("utf-8")), "protocol message"
            )
        except BaseException:
            await self.close()
            raise

    async def send_message(self, message: JsonObject) -> None:
        if self._writer is None:
            raise RuntimeError("Connection is not open.")
        payload = json.dumps(
            copy_json_object(message, "message"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        async with self._write_lock:
            self._writer.write(len(payload).to_bytes(8, "big") + payload)
            await self._writer.drain()

    async def request(
        self, request_id: str, command: str, args: JsonObject, *, timeout_seconds: float
    ) -> JsonObject:
        UUID(request_id)
        async with self._request_lock:
            if request_id in self._used_ids:
                raise ValueError("A request_id cannot be sent twice.")
            self._used_ids.add(request_id)
            try:
                async with asyncio.timeout(timeout_seconds):
                    await self.send_message(
                        {
                            "protocol_version": 1,
                            "message_type": "request",
                            "request_id": request_id,
                            "command": command,
                            "args": args,
                        }
                    )
                    response = await self.receive_message()
                    if response.get("request_id") != request_id:
                        raise ValueError("Response request_id does not match.")
                    return response
            except BaseException:
                await self.close()
                raise

    async def query_command_state(
        self, request_id: str, *, timeout_seconds: float
    ) -> JsonObject:
        return await self.request(
            request_id, "command_state", {}, timeout_seconds=timeout_seconds
        )

    async def close(self) -> None:
        writer, self._writer = self._writer, None
        self._reader = None
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
