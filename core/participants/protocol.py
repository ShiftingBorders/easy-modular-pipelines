"""Shared wire format and identities for experiment participants."""

from __future__ import annotations

import asyncio
import json

from core.models.participant_identity import ParticipantIdentity
from core.models.participant_protocol import (
    ParticipantRequest,
    ParticipantResult,
    ResultEnvelope,
)
from core.primitives.json_values import JsonObject, copy_json_object, require_text

PROTOCOL_VERSION = 2
IDENTITY_FIELDS = ("experiment_id", "participant_id", "participant_instance_id")


def error_details(code: str, message: object | None) -> JsonObject | None:
    if message is None:
        return None
    return {"code": require_text(code, "error code"), "message": str(message)}


def participant_identity(context: JsonObject) -> JsonObject:
    return _participant_identity(context).model_dump()


def _participant_identity(
    context: ParticipantIdentity | JsonObject,
) -> ParticipantIdentity:
    if isinstance(context, ParticipantIdentity):
        return ParticipantIdentity(
            experiment_id=context.experiment_id,
            participant_id=context.participant_id,
            participant_instance_id=context.participant_instance_id,
        )
    return ParticipantIdentity.model_validate(
        {name: context[name] for name in IDENTITY_FIELDS}
    )


def validate_response(response: JsonObject, *, envelope: bool = False) -> JsonObject:
    model = ResultEnvelope if envelope else ParticipantResult
    return model.model_validate(response).model_dump(exclude_unset=True)


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
    request = _validated_request(message, identity)
    message["request_id"] = request.request_id


def _validated_request(
    message: JsonObject, identity: ParticipantIdentity | JsonObject
) -> ParticipantRequest:
    request = ParticipantRequest.model_validate(message)
    if isinstance(identity, ParticipantIdentity):
        matches = all(
            getattr(request, name) == getattr(identity, name)
            for name in IDENTITY_FIELDS
        )
    else:
        actual_identity = {name: getattr(request, name) for name in IDENTITY_FIELDS}
        matches = actual_identity == identity
    if not matches:
        raise ValueError("Request belongs to a different participant.")
    return request
