"""Dashboard command input and local history, distinct from upstream receipts."""

from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.server_commands import ServerCommand
from core.models.server_receipts import CommandReceipt
from core.models.values import UUIDText
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


class DashboardCommand(ServerCommand):
    """Dashboard-supported runtime command with an optional experiment guard.

    Args:
        api_version: System API envelope version, currently 1. Defaults to 1.
        command_id: Normalized command UUID for receipt lookup/replay detection;
            generated when omitted.
        command: Command name selecting the operation to execute.
        args: JSON argument object supplied to the command handler. Defaults to
            a new empty dict.
        target: Optional stage/service target; server lifecycle commands reject
            targets. Defaults to None.
        expected_experiment_id: Local guard against acting on a different
            selected experiment; omitted from upstream wire data. Defaults to
            None.
    """
    command: Literal[
        "run",
        "pause",
        "resume",
        "step",
        "stop",
        "rerun",
        "retry",
        "move",
        "reset_retries",
        "replace",
        "reload_template",
        "snapshot",
        "rollback",
        "recover",
    ]
    expected_experiment_id: JsonValue = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        """Copy the command, assign a missing ID, and reject a supplied API version.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        document = copy_json_object(document, "dashboard command")
        if "api_version" in document:
            raise ValueError("Unsupported dashboard command.")
        document["command_id"] = document.get("command_id") or str(uuid4())
        return super().detach(document)

    def wire_document(self) -> JsonObject:
        """Return explicitly set upstream fields without the local experiment guard."""
        return self.model_dump(exclude_unset=True, exclude={"expected_experiment_id"})


class LocalCommandRecord(BaseModel):
    """Locally retained command status, polling state, and upstream outcome.

    Args:
        command_id: Command UUID used for receipt lookup and duplicate-
            submission detection.
        status: Locally retained command lifecycle status.
        polling: Whether the dashboard is still polling the upstream command
            receipt.
        command: Submitted command name retained for display/history. Defaults
            to None.
        experiment_id: Experiment identifier associating this document with its
            execution history. Defaults to None.
        server_instance_id: UUID of the HTTP runtime instance that owns these
            receipts or observations. Defaults to None.
        args: JSON argument object supplied to the command handler. Defaults to
            None.
        error: Failure details, or None when no failure is reported. Defaults to
            None.
        storage_error: Error persisting local command history, independent of
            the upstream outcome. Defaults to None.
        result: Upstream receipt or retained legacy JSON outcome, if available.
            Defaults to None.
    """
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    command_id: UUIDText
    status: str
    polling: bool
    command: JsonValue = None
    experiment_id: JsonValue = None
    server_instance_id: JsonValue = None
    args: JsonValue = None
    error: JsonValue = None
    storage_error: JsonValue = None
    result: CommandReceipt | JsonValue = None

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> object:
        """Copy command history JSON while retaining a validated receipt instance.

        Args:
            document: Caller-supplied model input before structural validation and
                detachment.

        Returns:
            Detached input for subsequent model validation; recognized typed values
            are retained where the input contract allows them.
        """
        if isinstance(document, dict) and isinstance(
            document.get("result"), CommandReceipt
        ):
            receipt = document["result"]
            values = copy_json_object(
                {**document, "result": receipt.model_dump(exclude_unset=True)},
                "local command record",
            )
            return {**values, "result": receipt}
        return copy_json_object(document, "local command record")


class SavedCommandHistory(BaseModel):
    """Persisted list of dashboard command records."""
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    items: list[LocalCommandRecord]
