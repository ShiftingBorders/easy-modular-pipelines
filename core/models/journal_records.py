"""Journal data contracts; constructors can keep shared standard-library rules."""

import json
from datetime import datetime, timedelta
from typing import Annotated, Literal, Self

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
from core.primitives.json_values import (
    JsonObject,
    JsonValue,
    copy_json_object,
    require_text,
)

SQLitePosition = Annotated[int, Field(strict=True, ge=0, le=9223372036854775807)]
SQLiteSequence = Annotated[PositiveInteger, Field(le=9223372036854775807)]


def _utc_text(value: str) -> str:
    if datetime.fromisoformat(value).utcoffset() != timedelta(0):
        raise ValueError("occurred_at must include the UTC timezone.")
    return value

UTCText = Annotated[Text, AfterValidator(_utc_text)]


class _Document(BaseModel):
    """Strict journal document detached from caller-owned JSON data."""
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of a journal document.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "journal document")


class JournalContext(RootModel[dict[str, str | int | None]]):
    """Validated experiment, participant, and operation context fields."""
    model_config = ConfigDict(strict=True, frozen=True, hide_input_in_errors=True)

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        # These same pure rules also protect the no-I/O SQLite constructor.
        """Return a detached context using the shared journal validation rules.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return validate_context(document)


class JournalIdentity(_Document):
    """Normalized journal and generation identifiers.

    Args:
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
    """
    journal_id: str
    generation: str

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Validate and normalize the journal identity into a detached dictionary.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return validate_journal_identity(document)


class JournalCheckpoint(JournalIdentity):
    """Journal identity and a nonnegative SQLite cursor position.

    Args:
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
        position: Nonnegative SQLite position; input key may be cursor or
            change_cursor via validation context.
    """
    position: SQLitePosition

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object, info: ValidationInfo) -> JsonObject:
        """Normalize a checkpoint's context-selected cursor field to position.

        Args:
            document: Journal identity and exactly one cursor field.
            info: Validation context whose key selects the input cursor field;
                position is used when no context is supplied.

        Returns:
            Normalized identity and position fields.

        Raises:
            ValueError: Checkpoint fields or identity are invalid.
        """
        key = "position" if info.context is None else info.context["key"]
        document = copy_json_object(document, "checkpoint")
        if document.keys() != {"journal_id", "generation", key}:
            raise ValueError(f"Checkpoint requires journal_id, generation and {key}.")
        identity = validate_journal_identity(
            {name: document[name] for name in ("journal_id", "generation")}
        )
        return {**identity, "position": document[key]}


class JournalReadBoundary(BaseModel):
    """Observed SQL boundary; identity is validated before reading its counters.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        journal_id: Identity of the source journal, distinct from its
            restoration generation.
        generation: Journal generation used to reject checkpoints from
            superseded history.
        cursor: Nonnegative event cursor identifying a position in the source
            journal.
        event_count: Number of source events represented by the recorded
            boundary or export.
        change_cursor: Nonnegative SQLite change-feed position, independent of
            the event cursor.
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    schema_version: Literal[2]
    journal_id: str
    generation: str
    cursor: int
    event_count: int
    change_cursor: int

    def checkpoint(
        self, key: Literal["cursor", "change_cursor"], position: int
    ) -> JsonObject:
        """Return this boundary's identity with a named cursor and supplied position.

        Args:
            key: Output cursor field name, cursor or change_cursor.
            position: Nonnegative SQLite position to associate with this boundary's
                journal identity.

        Returns:
            This boundary's identity with a named cursor and supplied position.
        """
        return {
            "journal_id": self.journal_id,
            "generation": self.generation,
            key: position,
        }


class JournalEvent(_Document):
    """Versioned journal event envelope with UTC time and typed context.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        event_id: Identifier of the original journal event.
        producer_instance_id: Identity of the logger session that produced the
            observation.
        sequence_number: Positive per-producer sequence number, bounded by
            SQLite's signed integer range.
        occurred_at: ISO 8601 event timestamp with an explicit UTC offset.
        event_type: Nonempty journal event category selecting the meaning of its
            payload.
        context: Validated journal context identifying the experiment and
            participant scope.
        operation_id: Associated journal operation ID; None denotes an unscoped
            observation.
        data: JSON payload for event_type; nested application fields belong
            inside this object.
    """
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
        """Copy event JSON after requiring the exact journal envelope fields.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        document = copy_json_object(document, "event")
        if document.keys() != EVENT_FIELDS:
            raise ValueError("Event fields do not match the journal envelope.")
        return document


class JournalEntry(BaseModel):
    """Validated SQL entry with original event JSON kept for output and checksums.

    Args:
        cursor: Nonnegative event cursor identifying a position in the source
            journal.
        event: Validated journal event envelope associated with this SQLite
            cursor.
        encoded_event: Original event JSON retained for output and checksums;
            excluded from ordinary model dumps.
        effective_author: Author of the effective result, runner or participant;
            None for an ordinary event. Defaults to None.
        provisional: Whether the effective result is participant-only and awaits
            runner confirmation. Defaults to False.
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    cursor: int
    event: JournalEvent
    encoded_event: str = Field(exclude=True)
    effective_author: Literal["runner", "participant"] | None = None
    provisional: Boolean = False

    def _with_result(
        self, author: Literal["runner", "participant"] | None, provisional: bool
    ) -> "JournalEntry":
        """Return a new entry with result metadata and unchanged original event text."""
        return JournalEntry(
            cursor=self.cursor,
            event=self.event,
            encoded_event=self.encoded_event,
            effective_author=author,
            provisional=provisional,
        )

    def document(self) -> JsonObject:
        """Return original event JSON with explicitly supplied result metadata."""
        document: JsonObject = {
            "cursor": self.cursor,
            "event": json.loads(self.encoded_event),
        }
        if "effective_author" in self.model_fields_set:
            document["effective_author"] = self.effective_author
        if "provisional" in self.model_fields_set:
            document["provisional"] = self.provisional
        return document


class JournalEventPage(BaseModel):
    """Typed page operations serialize original event representations at output.

    Args:
        events: Ordered validated journal entries returned in this page.
        boundary: Observed source journal boundary associated with this
            page/publication.
        after: Last traversed raw cursor; None produces a batch response without
            continuation metadata. Defaults to None.
    """

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    events: list[JournalEntry]
    boundary: JournalReadBoundary
    after: int | None = None

    def document(self) -> JsonObject:
        """Serialize the event page and include pagination when an after cursor exists.

        Returns:
            Original event JSON and boundary, with checkpoint/has_more only when
            after was supplied.
        """
        document: JsonObject = {
            "events": [entry.document() for entry in self.events],
            "boundary": self.boundary.model_dump(),
        }
        if self.after is not None:
            document = {
                "events": document["events"],
                "checkpoint": self.boundary.checkpoint("cursor", self.after),
                "boundary": document["boundary"],
                "has_more": self.after < self.boundary.cursor,
            }
        return document


class ChangeObserver(_Document):
    """Author and producer context associated with a journal change.

    Args:
        author: Result observer, runner or participant; runner acceptance takes
            precedence.
        producer_instance_id: Identity of the logger session that produced the
            observation.
        occurred_at: ISO 8601 event timestamp with an explicit UTC offset.
        context: Validated journal context identifying the experiment and
            participant scope.
        operation_id: Associated journal operation ID; None denotes an unscoped
            observation.
    """
    author: Literal["runner", "participant"]
    producer_instance_id: Text
    occurred_at: UTCText
    context: JournalContext
    operation_id: Text | None


class JournalChangeData(_Document):
    """Persisted change structure; actual SQL ownership stays with storage.

    Args:
        kind: Ordinary event or command.result change; determines reference and
            confirmation constraints.
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        effective_event_id: ID of the event currently representing the effective
            result.
        effective_author: Author of the effective result, runner or participant;
            None for an ordinary event.
        provisional: Whether the effective result is participant-only and awaits
            runner confirmation.
        result_changed: Whether this change replaces the considered result
            rather than only confirming it.
        related_event_ids: Unique event IDs participating in this change's
            result history.
        recorded_at: UTC timestamp when this change was appended to the change
            feed.
        observation: Producer/context metadata for the associated command-result
            observation.
    """

    kind: Literal["event", "command.result"]
    request_id: JsonValue
    effective_event_id: JsonValue
    effective_author: Literal["runner", "participant"] | None
    provisional: Boolean
    result_changed: Boolean
    related_event_ids: list[JsonValue]
    recorded_at: UTCText
    observation: ChangeObserver | None

    @model_validator(mode="after")
    def consistent_change(self) -> Self:
        """Return change data after validating references and author consistency.

        Raises:
            ValueError: References are empty or repeated, ordinary-event fields
                disagree, or command confirmation metadata conflicts with its author.
        """
        if not self.related_event_ids or len(set(self.related_event_ids)) != len(
            self.related_event_ids
        ):
            raise ValueError("Invalid change event references.")
        if self.kind == "event":
            if (
                self.request_id is not None
                or self.effective_author is not None
                or self.provisional
                or not self.result_changed
                or self.observation is not None
            ):
                raise ValueError("Invalid ordinary event change.")
        else:
            require_text(self.request_id, "request_id")
            if self.effective_author is None:
                raise ValueError("Invalid effective change author.")
            if self.provisional != (self.effective_author == "participant"):
                raise ValueError("Change confirmation state disagrees with author.")
        return self


class JournalChangeEntry(BaseModel):
    """Recorded change with its effective entry and observed source event.

    Args:
        change_cursor: Nonnegative SQLite change-feed position, independent of
            the event cursor.
        event_id: Identifier of the original journal event.
        change: Validated persisted change metadata, separate from original
            encoded JSON.
        entry: Validated journal entry retaining its original event JSON.
        observed: Original observed event associated with the change, possibly
            different from its effective entry.
        encoded_change: Original change JSON retained for output fidelity;
            excluded from ordinary model dumps.
    """
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    change_cursor: int
    event_id: str
    change: JournalChangeData
    entry: JournalEntry
    observed: JournalEntry
    encoded_change: str = Field(exclude=True)

    def document(self) -> JsonObject:
        """Return original change JSON together with its effective and observed events."""
        return {
            "change_cursor": self.change_cursor,
            "event_id": self.event_id,
            **json.loads(self.encoded_change),
            "entry": self.entry.document(),
            "observed_event": self.observed.document()["event"],
        }


class JournalChangePage(BaseModel):
    """Journal change batch with its read boundary and continuation position.

    Args:
        changes: Ordered validated change-feed entries in this page.
        boundary: Observed source journal boundary associated with this
            page/publication.
        after: Last traversed change cursor used to build the next checkpoint.
    """
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    changes: list[JournalChangeEntry]
    boundary: JournalReadBoundary
    after: int

    def document(self) -> JsonObject:
        """Return serialized changes, their checkpoint, boundary, and has-more flag."""
        return {
            "changes": [change.document() for change in self.changes],
            "checkpoint": self.boundary.checkpoint("change_cursor", self.after),
            "boundary": self.boundary.model_dump(),
            "has_more": self.after < self.boundary.change_cursor,
        }


class SupersededObservation(_Document):
    """Prior result event and the reason it no longer determines the outcome.

    Args:
        event_id: Identifier of the original journal event.
        ignored: Reason this observation is superseded or ignored; None when no
            reason is recorded.
    """
    event_id: Text
    ignored: Text


class CommandObservation(_Document):
    """Runner or participant outcome, response, and superseded result references.

    Args:
        request_id: Identifier correlating one admitted request with its
            observations and outcome.
        author: Result observer, runner or participant; runner acceptance takes
            precedence.
        outcome: Recorded command outcome, such as succeeded, failed, cancelled,
            or timed_out.
        response: Application result envelope accepted or observed for the
            correlated command.
        ignored: Reason this observation is superseded or ignored; None when no
            reason is recorded.
        supersedes: Prior result event IDs and reasons superseded by this
            observation.
    """
    request_id: Text
    author: Literal["runner", "participant"]
    outcome: Literal["succeeded", "failed", "cancelled", "timed_out", "invalidated"]
    response: JsonObject
    ignored: Text | None
    supersedes: list[SupersededObservation]


class JournalMeasurement(_Document):
    """Resource value with unit, aggregation kind, scope, and optional attributes.

    Args:
        value: Finite nonnegative measured value, or None when unavailable.
            Defaults to None.
        unit: Nonempty measurement unit, such as byte, percent, or second.
        kind: Aggregation semantics: delta, total, gauge, or peak. Defaults to
            delta.
        scope: Measurement owner: operation, process, service, or host. Defaults
            to operation.
        estimated: Whether the measurement is an estimate rather than a direct
            observation. Defaults to False.
        attributes: JSON metadata describing the measurement or resource, such
            as provenance and availability. Defaults to None.
    """
    value: Number | None = None
    unit: Text
    kind: Literal["delta", "total", "gauge", "peak"] = "delta"
    scope: Literal["operation", "process", "service", "host"] = "operation"
    estimated: Boolean = False
    attributes: JsonObject | None = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Copy measurement JSON, rejecting unknown fields and invalid attributes.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        document = copy_json_object(document, "resource measurement")
        if document.keys() - cls.model_fields.keys():
            raise ValueError("Unknown resource measurement fields.")
        if "attributes" in document:
            copy_json_object(document["attributes"], "resource attributes")
        return document
