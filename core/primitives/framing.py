"""The existing eight-byte-length UTF-8 JSON framing, async and sync readers."""

from __future__ import annotations

import asyncio
import json
import socket

from core.primitives.json_values import JsonObject, copy_json_object


def encode_frame(message: JsonObject, *, validate: bool = True) -> bytes:
    document = copy_json_object(message, "protocol message") if validate else message
    payload = json.dumps(document, ensure_ascii=not validate, allow_nan=False).encode(
        "utf-8"
    )
    return len(payload).to_bytes(8, "big") + payload


async def read_frame(
    reader: asyncio.StreamReader, *, validate: bool = True
) -> JsonObject:
    size = int.from_bytes(await reader.readexactly(8), "big")
    if size == 0:
        raise ValueError("Empty protocol frame.")
    document = json.loads((await reader.readexactly(size)).decode("utf-8"))
    if type(document) is not dict:
        raise ValueError("A protocol frame must contain an object.")
    return copy_json_object(document, "protocol message") if validate else document


def _receive_exactly(connection: socket.socket, size: int) -> bytes:
    parts = []
    while size:
        part = connection.recv(min(size, 65536))
        if not part:
            raise ConnectionError("Journal connection closed before its response.")
        parts.append(part)
        size -= len(part)
    return b"".join(parts)


def read_frame_sync(connection: socket.socket) -> JsonObject:
    size = int.from_bytes(_receive_exactly(connection, 8), "big")
    if size == 0:
        raise ValueError("Empty protocol frame.")
    document = json.loads(_receive_exactly(connection, size).decode("utf-8"))
    if type(document) is not dict:
        raise ValueError("A protocol frame must contain an object.")
    return document
