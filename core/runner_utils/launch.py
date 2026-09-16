"""Prepare fixed stage parameters and process-local logger configurations."""

from __future__ import annotations

from pathlib import Path

from core.experimentassembler import ExperimentAssembler
from core.logger_utils.events import copy_json_object, require_number
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
        *,
        command: str = "start",
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
        definition_key = "stage_id" if module["role"] == "stage" else "service_id"
        if definition_key not in definition:
            raise ValueError("Module role does not match its definition.")
        if module["role"] == "stage" and command != "start":
            raise ValueError("Stage launch uses only its start command.")
        if command not in module["commands"]:
            raise ValueError(f"Module has no {command!r} command.")
        if module["role"] == "service":
            required = {"service_id", "module", "settings"}
            if module["service_interface"] == "socket":
                required.update(
                    {
                        "heartbeat",
                        "command_timeout_seconds",
                        "on_command_timeout",
                        "state_required",
                        "errors",
                    }
                )
                heartbeat = copy_json_object(definition["heartbeat"], "heartbeat")
                if heartbeat.keys() != {"interval_seconds", "grace_seconds"}:
                    raise ValueError(
                        "heartbeat requires interval_seconds and grace_seconds."
                    )
                for name, value in heartbeat.items():
                    if require_number(value, name) <= 0:
                        raise ValueError(f"{name} must be positive.")
                if (
                    require_number(
                        definition["command_timeout_seconds"], "command_timeout_seconds"
                    )
                    <= 0
                ):
                    raise ValueError("command_timeout_seconds must be positive.")
                if definition["on_command_timeout"] not in ("pause", "restart", "stop"):
                    raise ValueError(
                        "on_command_timeout must be pause, restart, or stop."
                    )
                if type(definition["state_required"]) is not bool:
                    raise TypeError("state_required must be a boolean.")
                errors = copy_json_object(definition["errors"], "service errors")
                if errors.keys() != {"retries", "retry_delay_seconds", "on_exhausted"}:
                    raise ValueError("Invalid service error policy.")
                if type(errors["retries"]) is not int or errors["retries"] < 0:
                    raise ValueError("Service retries must be a nonnegative integer.")
                require_number(errors["retry_delay_seconds"], "retry_delay_seconds")
                if errors["on_exhausted"] not in ("pause", "stop", "skip"):
                    raise ValueError("Invalid service on_exhausted action.")
            if definition.keys() != required:
                raise ValueError("Service fields do not match its interface.")
            if require_number(state.template["start_timeout"], "start_timeout") <= 0:
                raise ValueError("start_timeout must be positive.")
        settings = self._merge_settings(module["defaults"], definition["settings"])
        artifacts_directory.mkdir(parents=True, exist_ok=False)
        module_data = (
            state.experiment_directory / "module_data" / definition[definition_key]
        )
        module_data.mkdir(parents=True, exist_ok=True)
        module_config = self._journal.write_client_config(
            state, {**context, "source": "module"}
        )
        executor_config = (
            self._journal.write_client_config(state, {**context, "source": "executor"})
            if module["role"] == "stage"
            else None
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
        if module["role"] == "service":
            runtime_context["endpoint_path"] = str(
                state.experiment_directory
                / "runner"
                / "endpoints"
                / f"{definition['service_id']}.json"
            )
            runtime_context["service_interface"] = module["service_interface"]
        # One file avoids command-line size limits while preserving all JSON inputs.
        context_path = artifacts_directory / "context.json"
        write_json(context_path, runtime_context)
        return {
            "argv": [*module["commands"][command], "--emp-context", str(context_path)],
            "code_directory": str(code_directory),
            "experiment_directory": str(state.experiment_directory),
            "executor_logging_config": None
            if executor_config is None
            else str(executor_config),
            "module": module,
            "context": context,
            "effective_settings": settings,
            "timeout_seconds": definition["timeout_seconds"]
            if module["role"] == "stage"
            else None,
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
