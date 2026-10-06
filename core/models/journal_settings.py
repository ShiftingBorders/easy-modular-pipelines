"""Journal opening constraints shared by file and direct-constructor boundaries."""

from pathlib import Path

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.journal_options import (
    JournalOptions,
    validate_journal_limits,
    validate_journal_options,
)
from core.models.values import PositiveNumber
from core.primitives.json_values import JsonObject


class JournalLimits(BaseModel):
    """SQLite busy timeout in seconds and event/free-space limits in bytes.

    Args:
        busy_timeout_seconds: SQLite lock-wait timeout in seconds, greater than
            zero and at most 60.
        max_event_bytes: Maximum encoded event bytes; None disables the
            additional event-size cap.
        min_free_bytes: Nonnegative free-space reserve in bytes required before
            storage writes.
    """
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        hide_input_in_errors=True,
    )

    busy_timeout_seconds: int | float
    max_event_bytes: int | None
    min_free_bytes: int

    @model_validator(mode="before")
    @classmethod
    def normalize_opening_options(cls, document: object) -> object:
        """Check shared journal limits when all required limit fields are supplied.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Original input after shared limits are checked when all required fields
            are present.
        """
        names = ("busy_timeout_seconds", "max_event_bytes", "min_free_bytes")
        if isinstance(document, dict) and all(name in document for name in names):
            validate_journal_limits(*(document[name] for name in names))
        return document


class JournalConfiguration(JournalLimits):
    """Resolved journal path, opening mode, expected identity, and limits.

    Args:
        busy_timeout_seconds: SQLite lock-wait timeout in seconds, greater than
            zero and at most 60.
        max_event_bytes: Maximum encoded event bytes; None disables the
            additional event-size cap.
        min_free_bytes: Nonnegative free-space reserve in bytes required before
            storage writes.
        db_path: Absolute SQLite path after the configuration loader resolves
            relative paths.
        open_mode: Create reserves a new journal; existing requires the expected
            journal identity.
        expected_journal: Required identity in existing mode; must be None when
            creating a journal.
    """
    db_path: Path
    open_mode: str
    expected_journal: JsonObject | None

    @model_validator(mode="before")
    @classmethod
    def normalize_opening_options(cls, document: object) -> object:
        """Validate complete opening options and replace them with normalized values.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Input with validated normalized opening options when complete, otherwise
            unchanged input for ordinary missing-field validation.
        """
        names = JournalOptions.__dataclass_fields__
        if isinstance(document, dict) and names.keys() <= document.keys():
            options = validate_journal_options(
                **{name: document[name] for name in names}
            )
            return {**document, **vars(options)}
        return document


class LoggingConfiguration(JournalConfiguration):
    """Journal opening settings plus filtered-view refresh interval in seconds.

    Args:
        busy_timeout_seconds: SQLite lock-wait timeout in seconds, greater than
            zero and at most 60.
        max_event_bytes: Maximum encoded event bytes; None disables the
            additional event-size cap.
        min_free_bytes: Nonnegative free-space reserve in bytes required before
            storage writes.
        db_path: Absolute SQLite path after the configuration loader resolves
            relative paths.
        open_mode: Create reserves a new journal; existing requires the expected
            journal identity.
        expected_journal: Required identity in existing mode; must be None when
            creating a journal.
        filtered_refresh_interval_seconds: Positive interval in seconds between
            filtered-view refreshes.
    """
    filtered_refresh_interval_seconds: PositiveNumber
