"""Participant identity shared by wire messages and persisted runner records."""

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.values import Text, UUIDText
from core.primitives.json_values import JsonObject, copy_json_object


class ParticipantIdentity(BaseModel):
    """Experiment, participant, and participant-instance identity with extra context.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        participant_id: UUID of the stage or service represented by the
            participant.
        participant_instance_id: UUID distinguishing this particular participant
            process/attempt from replacements.
    """
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    experiment_id: Text
    participant_id: UUIDText
    participant_instance_id: UUIDText

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of participant identity and context.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "participant identity")


class AttemptResultIdentity(ParticipantIdentity):
    """Journal association for one attempt, including its participant identity.

    Args:
        experiment_id: Experiment identifier associating this document with its
            execution history.
        participant_id: UUID of the stage or service represented by the
            participant.
        participant_instance_id: UUID distinguishing this particular participant
            process/attempt from replacements.
        stage_id: Stable UUID of a node in the applied DAG.
        attempt_id: UUID identifying one stage attempt.
    """

    stage_id: UUIDText
    attempt_id: UUIDText
