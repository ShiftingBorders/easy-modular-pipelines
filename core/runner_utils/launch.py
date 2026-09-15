"""Prepare fixed stage parameters and process-local logger configurations."""

from __future__ import annotations

from pathlib import Path

from core.experimentassembler import ExperimentAssembler
from core.logger_utils.events import copy_json_object
from core.runner_utils.journal import RunnerJournal
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
    ) -> JsonObject:
        self._assembler.check_module(state, definition)
        reference = definition["module"]
        code_directory = (
            state.experiment_directory
            / "modules"
            / reference["name"]
            / reference["version"]
        )
        module = self._assembler.read_module(code_directory)
        settings = self._merge_settings(module["defaults"], definition["settings"])
        artifacts_directory.mkdir(parents=True, exist_ok=False)
        module_data = (
            state.experiment_directory / "module_data" / definition["stage_id"]
        )
        module_data.mkdir(parents=True, exist_ok=True)
        module_config = self._journal.write_client_config(
            state, {**context, "source": "module"}
        )
        executor_config = self._journal.write_client_config(
            state, {**context, "source": "executor"}
        )
        runtime_context = {
            "experiment_directory": str(state.experiment_directory),
            "resources_directory": str(
                state.experiment_directory / "shared_data" / "resources"
            ),
            "settings_directory": str(state.experiment_directory / "shared_settings"),
            "module_data_directory": str(module_data),
            "artifacts_directory": str(artifacts_directory),
            "logging_config_path": str(module_config),
            "context": context,
            "settings": settings,
            "input_data": input_data,
        }
        # One file avoids command-line size limits while preserving all JSON inputs.
        context_path = artifacts_directory / "context.json"
        write_json(context_path, runtime_context)
        return {
            "argv": [*module["commands"]["start"], "--emp-context", str(context_path)],
            "code_directory": str(code_directory),
            "experiment_directory": str(state.experiment_directory),
            "executor_logging_config": str(executor_config),
            "context": context,
            "effective_settings": settings,
            "timeout_seconds": definition["timeout_seconds"],
            "control_timeout_seconds": state.template["unknown_state"][
                "timeout_seconds"
            ],
            "stop_timeout_seconds": state.template["start_timeout"],
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
