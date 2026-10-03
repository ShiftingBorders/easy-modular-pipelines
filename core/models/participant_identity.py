"""Participant identity shared by wire messages and persisted runner records."""

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.values import Text, UUIDText
from core.primitives.json_values import JsonObject, copy_json_object


class ParticipantIdentity(BaseModel):
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    experiment_id: Text
    participant_id: UUIDText
    participant_instance_id: UUIDText

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "participant identity")
