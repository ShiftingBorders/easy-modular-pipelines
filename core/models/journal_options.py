"""Scalar journal opening rules usable without importing Pydantic.

The public SQLiteEventStore constructor must not perform any file I/O, including
Pydantic's first-use discovery of installed plugin metadata. File configurations
reuse these rules from their Pydantic model after the explicit file read.
"""

from dataclasses import dataclass
from pathlib import Path

from core.journal.events import validate_journal_identity
from core.primitives.json_values import JsonObject, require_number


@dataclass(frozen=True)
class JournalOptions:
    db_path: Path
    busy_timeout_seconds: float
    max_event_bytes: int | None
    open_mode: str
    min_free_bytes: int
    expected_journal: JsonObject | None


def validate_journal_options(
    db_path: str | Path,
    *,
    busy_timeout_seconds: float,
    max_event_bytes: int | None,
    open_mode: str,
    min_free_bytes: int,
    expected_journal: JsonObject | None,
) -> JournalOptions:
    """Validate and detach direct constructor values without reading any files."""
    if not isinstance(db_path, (str, Path)):
        raise TypeError("db_path must be a string or Path.")
    path = Path(db_path)
    if not path.is_absolute() or "\x00" in str(path):
        raise ValueError("db_path must be an absolute filesystem path.")
    timeout = validate_journal_limits(
        busy_timeout_seconds, max_event_bytes, min_free_bytes
    )
    if open_mode not in ("create", "existing"):
        raise ValueError("open_mode must be create or existing.")
    if open_mode == "existing":
        expected_journal = validate_journal_identity(expected_journal)
    elif expected_journal is not None:
        raise ValueError("create requires expected_journal=None.")
    return JournalOptions(
        path,
        float(timeout),
        max_event_bytes,
        open_mode,
        min_free_bytes,
        expected_journal,
    )


def validate_journal_limits(
    busy_timeout_seconds: float,
    max_event_bytes: int | None,
    min_free_bytes: int,
) -> int | float:
    """Check shared template/store limits, preserving the input number representation."""
    timeout = require_number(busy_timeout_seconds, "busy_timeout_seconds")
    if not 0 < timeout <= 60:
        raise ValueError("busy_timeout_seconds must be greater than 0 and at most 60.")
    if max_event_bytes is not None and (
        type(max_event_bytes) is not int or max_event_bytes < 1
    ):
        raise ValueError("max_event_bytes must be a positive integer or None.")
    if type(min_free_bytes) is not int or min_free_bytes < 0:
        raise ValueError("min_free_bytes must be a nonnegative integer.")
    return timeout
