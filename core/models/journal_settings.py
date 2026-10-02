"""Journal opening constraints shared by file and direct-constructor boundaries."""

from pathlib import Path

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.journal_options import JournalOptions, validate_journal_options
from core.models.values import PositiveNumber
from core.primitives.json_values import JsonObject


class JournalConfiguration(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        hide_input_in_errors=True,
    )

    db_path: Path
    busy_timeout_seconds: float
    max_event_bytes: int | None
    open_mode: str
    min_free_bytes: int
    expected_journal: JsonObject | None

    @model_validator(mode="before")
    @classmethod
    def normalize_opening_options(cls, document: object) -> object:
        names = JournalOptions.__dataclass_fields__
        if isinstance(document, dict) and names.keys() <= document.keys():
            options = validate_journal_options(
                **{name: document[name] for name in names}
            )
            return {**document, **vars(options)}
        return document


class LoggingConfiguration(JournalConfiguration):
    filtered_refresh_interval_seconds: PositiveNumber
