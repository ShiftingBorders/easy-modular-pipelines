"""A resource-monitoring target validates a complete process identity at entry."""

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.models.journal_records import JournalContext
from core.models.process_identity import ProcessIdentity
from core.models.values import NonnegativeInteger, UUIDText
from core.primitives.json_values import JsonObject, copy_json_object

ResourceContext = JournalContext


class ResourceProcessIdentity(ProcessIdentity):
    created_at_os: NonnegativeInteger = Field(gt=0)


class ResourceTargetDocument(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    series_id: UUIDText
    identity: ResourceProcessIdentity
    context: ResourceContext

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "resource target")
