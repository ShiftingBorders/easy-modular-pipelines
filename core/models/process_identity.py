"""Complete OS identity shape; existence and ownership remain runtime checks."""

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.values import NonnegativeInteger, PositiveInteger, Text
from core.primitives.json_values import JsonObject, copy_json_object


class ProcessIdentity(BaseModel):
    """PID, OS creation time, host ID, and boot ID used to establish ownership.

    Args:
        pid: Operating-system process identifier; additional identity fields are
            needed to prove ownership.
        created_at_os: Native process creation identity used with PID to detect
            PID reuse; not a wall-clock timestamp string.
        host_id: Identity of the host on which the process was created.
        boot_id: Host boot identity used to reject process records from earlier
            boots.
    """
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
        """Return a validated JSON copy of a complete process identity.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        return copy_json_object(document, "process identity")
