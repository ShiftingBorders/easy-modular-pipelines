"""A resource-monitoring target validates a complete process identity at entry."""

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.models.journal_records import JournalContext
from core.models.process_identity import ProcessIdentity
from core.models.values import NonnegativeInteger, UUIDText
from core.primitives.json_values import JsonObject, copy_json_object

ResourceContext = JournalContext


class ResourceProcessIdentity(ProcessIdentity):
    """Resource target process identity requiring a positive OS creation time.

    Args:
        pid: Operating-system process identifier; additional identity fields are
            needed to prove ownership.
        created_at_os: Native process creation identity used with PID to detect
            PID reuse; not a wall-clock timestamp string.
        host_id: Identity of the host on which the process was created.
        boot_id: Host boot identity used to reject process records from earlier
            boots.
    """
    created_at_os: NonnegativeInteger = Field(gt=0)


class ResourceTargetDocument(BaseModel):
    """Monitored process identity, series UUID, and journal context.

    Args:
        series_id: Identity of the monitored resource time series.
        identity: Complete expected OS identity with a positive creation
            identity, checked again during sampling.
        context: Validated journal context identifying the experiment and
            participant scope.
    """
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, hide_input_in_errors=True
    )

    series_id: UUIDText
    identity: ResourceProcessIdentity
    context: ResourceContext

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Return a validated JSON copy of a resource target.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "resource target")
