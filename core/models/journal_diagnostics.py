"""Diagnostic and snapshot documents, separate from files and SQL transactions."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from core.models.journal_records import (
    CommandObservation,
    JournalContext,
    JournalEntry,
    SQLitePosition,
    UTCText,
    _Document,
)
from core.models.values import NonnegativeInteger, Text, UUIDText
from core.primitives.json_values import JsonObject, copy_json_object


def _sha256(value: str) -> str:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError("Invalid content checksum.")
    return value


SHA256Text = Annotated[Text, AfterValidator(_sha256)]
UUIDHex = Annotated[UUIDText, AfterValidator(lambda value: UUID(value).hex)]
VersionTwo = Annotated[int, Field(strict=True, ge=2, le=2)]


class AuthorObservation(_Document):
    """Producer, UTC timestamp, and context of a command-result observation.

    Args:
        producer_instance_id: Identity of the logger session that produced the
            observation.
        occurred_at: ISO 8601 event timestamp with an explicit UTC offset.
        context: Validated journal context identifying the experiment and
            participant scope.
        operation_id: Associated journal operation ID; None denotes an unscoped
            observation.
    """
    producer_instance_id: Text
    occurred_at: UTCText
    context: JournalContext
    operation_id: Text | None


class CommandResultObserver(BaseModel):
    """Indexed observer metadata, with original event representation retained.

    Args:
        author: Result observer, runner or participant; runner acceptance takes
            precedence.
        event_id: Identifier of the original journal event.
        entry: Validated journal entry retaining its original event JSON.
        observation: Producer/context metadata for the associated command-result
            observation.
        ignored: Reason this observation is superseded or ignored; None when no
            reason is recorded.
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    author: Literal["runner", "participant"]
    event_id: str
    entry: JournalEntry
    observation: AuthorObservation
    ignored: str | None

    def document(self) -> JsonObject:
        """Return observer metadata with the original recorded event representation."""
        return {
            "author": self.author,
            "event_id": self.event_id,
            "event": self.entry.document()["event"],
            "observation": self.observation.model_dump(),
            "ignored": self.ignored,
        }


class JournalCommandResult(BaseModel):
    """Effective command outcome and the observations used to determine it.

    Args:
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        event_id: Identifier of the original journal event.
        author: Result observer, runner or participant; runner acceptance takes
            precedence.
        result: Validated considered command observation selected by author
            precedence.
        entry: Validated journal entry retaining its original event JSON.
        observations: All indexed runner/participant observations used to
            determine the command's outcome.
        provisional: Whether the effective result is participant-only and awaits
            runner confirmation.
    """
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    request_id: str
    event_id: str
    author: Literal["runner", "participant"]
    result: CommandObservation
    entry: JournalEntry
    observations: list[CommandResultObserver]
    provisional: bool

    def document(self) -> JsonObject:
        """Return detached response data and original events for the command result.

        Returns:
            Detached response data and original events for the command result.
        """
        return {
            "request_id": self.request_id,
            "event_id": self.event_id,
            "author": self.author,
            "outcome": self.result.outcome,
            "response": copy_json_object(self.result.response, "command response"),
            "event": self.entry.document()["event"],
            "observations": [
                observation.document() for observation in self.observations
            ],
            "provisional": self.provisional,
        }


class DiagnosticIdentity(_Document):
    """Experiment and participant identity attached to exported diagnostics.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        participant_id: UUID of the stage or service represented by the
            participant.
        participant_instance_id: UUID distinguishing this particular participant
            process/attempt from replacements.
    """
    experiment_id: Text
    participant_id: Text
    participant_instance_id: Text | None


class DiagnosticObservation(_Document):
    """Exported event reference and its author's observation metadata.

    Args:
        event_id: Identifier of the original journal event.
        observation: Producer/context metadata for the associated command-result
            observation.
    """
    event_id: Text
    observation: AuthorObservation


class DiagnosticCommand(_Document):
    """Exported command identity with runner and/or participant observations.

    Args:
        kind: Record discriminator, fixed to command. Defaults to command.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        identity: Experiment and participant ownership associated with the
            exported request.
        runner: Runner acceptance observation, or None when not recorded.
        participant: Participant completion observation, or None when not
            recorded; at least one author is required.
    """
    kind: Literal["command"] = "command"
    request_id: Text
    identity: DiagnosticIdentity
    runner: DiagnosticObservation | None
    participant: DiagnosticObservation | None

    @model_validator(mode="after")
    def require_observation(self) -> Self:
        """Return the command after requiring at least one author's observation."""
        if self.runner is None and self.participant is None:
            raise ValueError("Diagnostic result requires an observation.")
        return self


class DiagnosticManifest(_Document):
    """Diagnostic export inventory, journal boundary, and records checksum.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        kind: Export discriminator, fixed to journal.diagnostics.
        diagnostics_id: UUID identifying one exported diagnostic bundle.
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
        operation_ids: Nonempty list of root operation IDs selected for
            diagnostic export.
        cursor: Nonnegative event cursor identifying a position in the source
            journal.
        change_cursor: Nonnegative SQLite change-feed position, independent of
            the event cursor.
        created_at: ISO 8601 creation timestamp in UTC.
        records: Diagnostic records member name, fixed to records.jsonl.
        records_sha256: SHA-256 digest of the exact diagnostic records file
            bytes.
        event_count: Number of source events represented by the recorded
            boundary or export.
        command_count: Number of command-index records included in the
            diagnostic export.
    """
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
    """Journal snapshot identity, SQL boundary, and content checksum.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        snapshot_id: UUID of the snapshot selected or referenced by this
            document.
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
        storage_schema_version: SQLite journal schema version required by the
            snapshot.
        cursor: Nonnegative event cursor identifying a position in the source
            journal.
        event_count: Number of source events represented by the recorded
            boundary or export.
        change_cursor: Nonnegative SQLite change-feed position, independent of
            the event cursor.
        content_sha256: Digest of the journal's canonical snapshot contents.
        created_at: ISO 8601 creation timestamp in UTC.
        database: Snapshot database member name, fixed to journal.sqlite.
    """
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
    """Snapshot and new restoration/generation UUIDs for a journal import.

    Args:
        snapshot: Validated journal snapshot identity, boundary, and content
            checksum.
        restoration_id: UUID identifying one resumable restoration transaction.
        new_generation: Fresh generation UUID chosen for restoration; must
            differ from the snapshot generation.
    """
    snapshot: JournalSnapshotManifest
    restoration_id: UUIDHex
    new_generation: UUIDHex

    @model_validator(mode="after")
    def require_fresh_generation(self) -> Self:
        """Return restoration input after requiring a generation unlike the snapshot's."""
        if self.new_generation == UUID(self.snapshot.generation).hex:
            raise ValueError("Restoration requires a fresh generation.")
        return self
