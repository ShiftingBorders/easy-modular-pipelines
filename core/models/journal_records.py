"""Journal data contracts; constructors can keep shared standard-library rules."""

import json
from datetime import datetime, timedelta
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationInfo,
    model_validator,
)

from core.journal.events import (
    EVENT_FIELDS,
    validate_context,
    validate_journal_identity,
)
from core.models.values import Boolean, Number, PositiveInteger, Text
from core.primitives.json_values import JsonObject, copy_json_object

SQLitePosition = Annotated[int, Field(strict=True, ge=0, le=9223372036854775807)]
SQLiteSequence = Annotated[PositiveInteger, Field(le=9223372036854775807)]


def _utc_text(value: str) -> str:
    if datetime.fromisoformat(value).utcoffset() != timedelta(0):
        raise ValueError("occurred_at must include the UTC timezone.")
    return value

UTCText = Annotated[Text, AfterValidator(_utc_text)]


class _Document(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "journal document")


class JournalContext(RootModel[dict[str, str | int | None]]):
    model_config = ConfigDict(strict=True, frozen=True, hide_input_in_errors=True)

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        # These same pure rules also protect the no-I/O SQLite constructor.
        return validate_context(document)


class JournalIdentity(_Document):
    journal_id: str
    generation: str

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return validate_journal_identity(document)


class JournalCheckpoint(JournalIdentity):
    position: SQLitePosition

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object, info: ValidationInfo) -> JsonObject:
        key = "position" if info.context is None else info.context["key"]
        document = copy_json_object(document, "checkpoint")
        if document.keys() != {"journal_id", "generation", key}:
            raise ValueError(f"Checkpoint requires journal_id, generation and {key}.")
        identity = validate_journal_identity(
            {name: document[name] for name in ("journal_id", "generation")}
        )
        return {**identity, "position": document[key]}


class JournalEvent(_Document):
    schema_version: Annotated[int, Field(strict=True, ge=2, le=2)]
    event_id: Text
    producer_instance_id: Text
    sequence_number: SQLiteSequence
    occurred_at: UTCText
    event_type: Text
    context: JournalContext
    operation_id: Text | None
    data: JsonObject

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        document = copy_json_object(document, "event")
        if document.keys() != EVENT_FIELDS:
            raise ValueError("Event fields do not match the journal envelope.")
        return document


class JournalEntry(BaseModel):
    """Validated SQL entry with original event JSON kept for output and checksums."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    cursor: int
    event: JournalEvent
    encoded_event: str = Field(exclude=True)
    effective_author: Literal["runner", "participant"] | None = None
    provisional: Boolean = False

    def _with_result(
        self, author: Literal["runner", "participant"] | None, provisional: bool
    ) -> "JournalEntry":
        return JournalEntry(
            cursor=self.cursor,
            event=self.event,
            encoded_event=self.encoded_event,
            effective_author=author,
            provisional=provisional,
        )

    def document(self) -> JsonObject:
        document: JsonObject = {
            "cursor": self.cursor,
            "event": json.loads(self.encoded_event),
        }
        if "effective_author" in self.model_fields_set:
            document["effective_author"] = self.effective_author
        if "provisional" in self.model_fields_set:
            document["provisional"] = self.provisional
        return document


class SupersededObservation(_Document):
    event_id: Text
    ignored: Text


class CommandObservation(_Document):
    request_id: Text
    author: Literal["runner", "participant"]
    outcome: Literal["succeeded", "failed", "cancelled", "timed_out", "invalidated"]
    response: JsonObject
    ignored: Text | None
    supersedes: list[SupersededObservation]


class JournalMeasurement(_Document):
    value: Number | None = None
    unit: Text
    kind: Literal["delta", "total", "gauge", "peak"] = "delta"
    scope: Literal["operation", "process", "service", "host"] = "operation"
    estimated: Boolean = False
    attributes: JsonObject | None = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        document = copy_json_object(document, "resource measurement")
        if document.keys() - cls.model_fields.keys():
            raise ValueError("Unknown resource measurement fields.")
        if "attributes" in document:
            copy_json_object(document["attributes"], "resource attributes")
        return document
