"""Diagnostic and snapshot documents, separate from files and SQL transactions."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AfterValidator, Field, model_validator

from core.models.journal_records import (
    JournalContext,
    SQLitePosition,
    UTCText,
    _Document,
)
from core.models.values import NonnegativeInteger, Text, UUIDText


def _sha256(value: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError("Invalid content checksum.")
    return value


SHA256Text = Annotated[Text, AfterValidator(_sha256)]
UUIDHex = Annotated[UUIDText, AfterValidator(lambda value: UUID(value).hex)]
VersionTwo = Annotated[int, Field(strict=True, ge=2, le=2)]


class AuthorObservation(_Document):
    producer_instance_id: Text
    occurred_at: UTCText
    context: JournalContext
    operation_id: Text | None


class DiagnosticIdentity(_Document):
    experiment_id: Text
    participant_id: Text
    participant_instance_id: Text | None


class DiagnosticObservation(_Document):
    event_id: Text
    observation: AuthorObservation


class DiagnosticCommand(_Document):
    kind: Literal["command"] = "command"
    request_id: Text
    identity: DiagnosticIdentity
    runner: DiagnosticObservation | None
    participant: DiagnosticObservation | None

    @model_validator(mode="after")
    def require_observation(self) -> Self:
        if self.runner is None and self.participant is None:
            raise ValueError("Diagnostic result requires an observation.")
        return self


class DiagnosticManifest(_Document):
    schema_version: VersionTwo
    kind: Literal["journal.diagnostics"]
    diagnostics_id: UUIDText
    journal_id: UUIDText
    generation: UUIDText
    operation_ids: Annotated[list[Text], Field(min_length=1)]
    cursor: NonnegativeInteger
    change_cursor: NonnegativeInteger
    created_at: UTCText
    records: Literal["records.jsonl"]
    records_sha256: SHA256Text
    event_count: NonnegativeInteger
    command_count: NonnegativeInteger


class JournalSnapshotManifest(_Document):
    schema_version: VersionTwo
    snapshot_id: UUIDText
    journal_id: UUIDText
    generation: UUIDText
    storage_schema_version: VersionTwo
    cursor: SQLitePosition
    event_count: SQLitePosition
    change_cursor: SQLitePosition
    content_sha256: SHA256Text
    created_at: UTCText
    database: Literal["journal.sqlite"]


class JournalRestorationInput(_Document):
    snapshot: JournalSnapshotManifest
    restoration_id: UUIDHex
    new_generation: UUIDHex

    @model_validator(mode="after")
    def require_fresh_generation(self) -> Self:
        if self.new_generation == UUID(self.snapshot.generation).hex:
            raise ValueError("Restoration requires a fresh generation.")
        return self
