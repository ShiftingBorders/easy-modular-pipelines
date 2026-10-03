"""Complete OS identity shape; existence and ownership remain runtime checks."""

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.values import NonnegativeInteger, PositiveInteger, Text
from core.primitives.json_values import JsonObject, copy_json_object


class ProcessIdentity(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    pid: PositiveInteger
    created_at_os: NonnegativeInteger
    host_id: Text
    boot_id: Text

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "process identity")
