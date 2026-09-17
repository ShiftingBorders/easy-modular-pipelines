"""Prepare immutable module inputs and the participant chosen for a call."""

from __future__ import annotations

from pathlib import Path

from core.experimentassembler import ExperimentAssembler
from core.logger_utils.events import copy_json_object
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.protocol import PROTOCOL_VERSION
from core.runner_utils.runtimeio import write_json
from core.runner_utils.state import JsonObject, JsonValue, RunnerState


class ModuleLauncher:
    def __init__(self, assembler: ExperimentAssembler, journal: RunnerJournal) -> None:
        self._assembler = assembler
        self._journal = journal

    def prepare(
        self,
        state: RunnerState,
        definition: JsonObject,
        context: JsonObject,
        artifacts_directory: Path,
        input_data: JsonValue,
        *,
        command: str = "start",
    ) -> JsonObject:
        if command != "start":
            raise ValueError("Participant shutdown is a protocol command.")
        self._assembler.check_module(state, definition)
        reference = self._assembler.module_reference(state.template, definition)
        code_directory = (
            state.experiment_directory
            / "modules"
            / reference["name"]
            / reference["version"]
        )
        module = self._assembler.read_module(code_directory)
        service_call = "stage_id" in definition and "service_id" in definition
        if service_call and module["role"] != "service":
            raise ValueError("A service node must reference a service module.")
        if not service_call and f"{module['role']}_id" not in definition:
            raise ValueError("Module role does not match its definition.")
        if module["role"] == "service" and not service_call:
            self._assembler.validate_service_definition(definition)
        settings = (
            copy_json_object(definition["settings"], "call settings")
            if service_call
            else self._merge_settings(module["defaults"], definition["settings"])
        )
        artifacts_directory.mkdir(parents=True, exist_ok=False)
        owner_id = (
            definition["service_id"]
            if module["role"] == "service"
            else definition["stage_id"]
        )
        module_data = state.experiment_directory / "module_data" / owner_id
        module_data.mkdir(parents=True, exist_ok=True)
        logging_config = (
            None
            if service_call
            else self._journal.write_client_config(
                state, {**context, "source": "module"}
            )
        )
        executor_config = (
            self._journal.write_client_config(state, {**context, "source": "executor"})
            if module["role"] == "stage"
            else None
        )
        endpoint_path = (
            state.experiment_directory / "runner/endpoints" / f"{owner_id}.json"
            if module["role"] == "service"
            else artifacts_directory / "executor.lock.json"
        )
        runtime_context = {
            "protocol_version": PROTOCOL_VERSION,
            "experiment_directory": str(state.experiment_directory),
            "resources_directory": str(
                state.experiment_directory / "shared_data/resources"
            ),
            "settings_directory": str(state.experiment_directory / "shared_settings"),
            "module_data_directory": str(module_data),
            "artifacts_directory": str(artifacts_directory),
            "logging_config_path": None
            if logging_config is None
            else str(logging_config),
            "endpoint_path": str(endpoint_path),
            "control_timeout_seconds": state.template["unknown_state"][
                "timeout_seconds"
            ],
            "context": context,
            "settings": settings,
            "input_data": input_data,
        }
        context_path = artifacts_directory / "context.json"
        write_json(context_path, runtime_context)
        return {
            "argv": [*module["commands"]["start"], "--emp-context", str(context_path)],
            "code_directory": str(code_directory),
            "experiment_directory": str(state.experiment_directory),
            "executor_logging_config": None
            if executor_config is None
            else str(executor_config),
            "module": module,
            "context": context,
            "runtime_context": runtime_context,
            "call": {
                key: runtime_context[key]
                for key in (
                    "context",
                    "input_data",
                    "settings",
                    "experiment_directory",
                    "resources_directory",
                    "settings_directory",
                    "module_data_directory",
                    "artifacts_directory",
                )
            },
            "effective_settings": settings,
            "endpoint_path": str(endpoint_path),
            "service_id": definition["service_id"] if service_call else None,
            "timeout_seconds": definition.get("timeout_seconds"),
            "control_timeout_seconds": state.template["unknown_state"][
                "timeout_seconds"
            ],
            "stop_timeout_seconds": state.template["start_timeout"],
            "runner_timeout_margin_seconds": state.template[
                "runner_timeout_margin_seconds"
            ],
        }

    def _merge_settings(
        self, defaults: JsonObject, overrides: JsonObject
    ) -> JsonObject:
        result = copy_json_object(defaults, "module defaults")
        overrides = copy_json_object(overrides, "settings overrides")
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = self._merge_settings(result[key], value)
            else:
                result[key] = value
        return result
