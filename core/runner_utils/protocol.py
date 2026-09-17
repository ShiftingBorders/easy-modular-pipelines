"""Shared wire format and identities for experiment participants."""

from __future__ import annotations

import asyncio
import json
from uuid import UUID

from core.logger_utils.events import JsonObject, copy_json_object, require_text

PROTOCOL_VERSION = 2
IDENTITY_FIELDS = ("experiment_id", "participant_id", "participant_instance_id")


def error_details(code: str, message: object | None) -> JsonObject | None:
    if message is None:
        return None
    return {"code": require_text(code, "error code"), "message": str(message)}


def participant_identity(context: JsonObject) -> JsonObject:
    identity = {name: context[name] for name in IDENTITY_FIELDS}
    for name, value in identity.items():
        require_text(value, name)
        if name != "experiment_id":
            UUID(value)
    return identity


def validate_response(response: JsonObject, *, envelope: bool = False) -> JsonObject:
    response = copy_json_object(response, "participant result")
    allowed = {"result", "data", "error", "execution"}
    if envelope:
        allowed.update({"protocol_version", "message_type", "request_id"})
    if response.keys() - allowed:
        raise ValueError("Unknown result fields; domain output belongs in data.")
    if response.get("result") not in ("success", "fail") or "data" not in response:
        raise ValueError("A result requires result=success/fail and data.")
    return response


def encode_frame(message: JsonObject) -> bytes:
    payload = json.dumps(
        copy_json_object(message, "protocol message"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return len(payload).to_bytes(8, "big") + payload


async def read_frame(reader: asyncio.StreamReader) -> JsonObject:
    size = int.from_bytes(await reader.readexactly(8), "big")
    if size == 0:
        raise ValueError("Empty protocol frame.")
    return copy_json_object(
        json.loads((await reader.readexactly(size)).decode("utf-8")),
        "protocol message",
    )


def validate_request(message: JsonObject, identity: JsonObject) -> None:
    if (
        type(message.get("protocol_version")) is not int
        or message["protocol_version"] != PROTOCOL_VERSION
        or message.get("message_type") != "request"
    ):
        raise ValueError("Unsupported participant request protocol.")
    UUID(require_text(message.get("request_id"), "request_id"))
    message["request_id"] = str(UUID(message["request_id"]))
    if participant_identity(message) != identity:
        raise ValueError("Request belongs to a different participant.")
    require_text(message.get("command"), "command")
    copy_json_object(message.get("args"), "request arguments")
