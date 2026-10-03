"""Dashboard command input and local history, distinct from upstream receipts."""

from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, model_validator

from core.models.server_commands import ServerCommand
from core.models.values import UUIDText
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


class DashboardCommand(ServerCommand):
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
        document = copy_json_object(document, "dashboard command")
        if "api_version" in document:
            raise ValueError("Unsupported dashboard command.")
        document["command_id"] = document.get("command_id") or str(uuid4())
        return super().detach(document)

    def wire_document(self) -> JsonObject:
        return self.model_dump(exclude_unset=True, exclude={"expected_experiment_id"})


class LocalCommandRecord(BaseModel):
    model_config = ConfigDict(
        extra="allow", strict=True, frozen=True, hide_input_in_errors=True
    )

    command_id: UUIDText
    status: str
    polling: bool

    @model_validator(mode="before")
    @classmethod
    def detach(cls, document: object) -> JsonObject:
        return copy_json_object(document, "local command record")


class SavedCommandHistory(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True, frozen=True)

    items: list[LocalCommandRecord]
