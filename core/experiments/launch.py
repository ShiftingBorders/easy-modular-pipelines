"""Prepare immutable module inputs and the participant chosen for a call."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from core.experiments.assembler import ExperimentAssembler
from core.experiments.journal import RunnerJournal
from core.experiments.state import RunnerState
from core.models.experiment_template import (
    LaunchInput,
    ServiceCallDefinition,
    ServiceDefinition,
    StageDefinition,
)
from core.models.module_manifest import ModuleManifest
from core.models.participant_identity import ParticipantIdentity
from core.models.participant_launch import (
    ExecutionCall,
    ModulePreparation,
    PreparedLaunch,
    PreparedModuleContext,
)
from core.participants.protocol import PROTOCOL_VERSION
from core.primitives.json_files import write_json
from core.primitives.json_values import JsonObject, JsonValue, copy_json_object


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
        validated = LaunchInput.validate_python(
            copy_json_object(definition, "module definition")
        )
        inputs = ModulePreparation.model_validate(
            {
                "context": context,
                "artifacts_directory": artifacts_directory,
                "input_data": input_data,
            }
        )
        return self._prepare(state, validated, inputs).model_dump(
            mode="json", exclude_unset=True
        )

    def _prepare(
        self,
        state: RunnerState,
        definition: StageDefinition | ServiceCallDefinition | ServiceDefinition,
        inputs: ModulePreparation,
    ) -> PreparedLaunch:
        reference = self._assembler._module_reference(state.template, definition)
        module = self._assembler._check_module(
            state,
            reference,
            "returns_data" in definition.model_fields_set,
        )
        service_call = isinstance(definition, ServiceCallDefinition)
        if service_call and module.role != "service":
            raise ValueError("A service node must reference a service module.")
        if not service_call and (module.role == "service") != isinstance(
            definition, ServiceDefinition
        ):
            raise ValueError("Module role does not match its definition.")
        settings = (
            deepcopy(definition.settings)
            if service_call
            else self._merge_settings(module.defaults, definition.settings)
        )
        owner_id = (
            definition.service_id
            if isinstance(definition, (ServiceCallDefinition, ServiceDefinition))
            else definition.stage_id
        )
        runtime_context, executor_config = self._prepare_context(
            state,
            inputs,
            module,
            service_call,
            owner_id,
            settings,
        )
        context_path = inputs.artifacts_directory / "context.json"
        write_json(
            context_path, runtime_context.model_dump(mode="json", exclude_unset=True)
        )
        return _launch_document(
            state, module, definition, runtime_context, executor_config, context_path
        )

    def _prepare_context(
        self,
        state: RunnerState,
        inputs: ModulePreparation,
        module: ModuleManifest,
        service_call: bool,
        owner_id: str,
        settings: JsonObject,
    ) -> tuple[PreparedModuleContext, Path | None]:
        inputs.artifacts_directory.mkdir(parents=True, exist_ok=False)
        module_data = state.experiment_directory / "module_data" / owner_id
        module_data.mkdir(parents=True, exist_ok=True)
        logging_config, executor_config = self._prepare_logging_configs(
            state, inputs.context, module, service_call
        )
        endpoint_path = (
            state.experiment_directory / "runner/endpoints" / f"{owner_id}.json"
            if module.role == "service"
            else inputs.artifacts_directory / "executor.lock.json"
        )
        return PreparedModuleContext(
            protocol_version=PROTOCOL_VERSION,
            experiment_directory=state.experiment_directory,
            resources_directory=state.experiment_directory / "shared_data/resources",
            settings_directory=state.experiment_directory / "shared_settings",
            module_data_directory=module_data,
            artifacts_directory=inputs.artifacts_directory,
            logging_config_path=logging_config,
            endpoint_path=endpoint_path,
            control_timeout_seconds=state.template.unknown_state.timeout_seconds,
            context=inputs.context,
            settings=settings,
            input_data=inputs.input_data,
        ), executor_config

    def _prepare_logging_configs(
        self,
        state: RunnerState,
        context: ParticipantIdentity,
        module: ModuleManifest,
        service_call: bool,
    ) -> tuple[Path | None, Path | None]:
        logging_config = (
            None
            if service_call
            else self._journal.write_client_config(
                state, {**context.model_dump(), "source": "module"}
            )
        )
        executor_config = (
            self._journal.write_client_config(
                state, {**context.model_dump(), "source": "executor"}
            )
            if module.role == "stage"
            else None
        )
        return logging_config, executor_config

    def _merge_settings(
        self, defaults: JsonObject, overrides: JsonObject
    ) -> JsonObject:
        result = deepcopy(defaults)
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = self._merge_settings(result[key], value)
            else:
                result[key] = deepcopy(value)
        return result


def _launch_document(
    state: RunnerState,
    module: ModuleManifest,
    definition: StageDefinition | ServiceCallDefinition | ServiceDefinition,
    runtime_context: PreparedModuleContext,
    executor_config: Path | None,
    context_path: Path,
) -> PreparedLaunch:
    """Keep launch components typed until the public preparation result is emitted."""
    code_directory = (
        state.experiment_directory / "modules" / module.name / module.version
    )
    service_call = isinstance(definition, ServiceCallDefinition)
    call = ExecutionCall(
        context=runtime_context.context,
        input_data=runtime_context.input_data,
        settings=runtime_context.settings,
        experiment_directory=runtime_context.experiment_directory,
        resources_directory=runtime_context.resources_directory,
        settings_directory=runtime_context.settings_directory,
        module_data_directory=runtime_context.module_data_directory,
        artifacts_directory=runtime_context.artifacts_directory,
    )
    return PreparedLaunch(
        argv=[*module.commands.start, "--emp-context", str(context_path)],
        code_directory=code_directory,
        experiment_directory=state.experiment_directory,
        executor_logging_config=executor_config,
        module=module,
        context=runtime_context.context,
        runtime_context=runtime_context,
        call=call,
        effective_settings=runtime_context.settings,
        endpoint_path=runtime_context.endpoint_path,
        service_id=definition.service_id if service_call else None,
        timeout_seconds=None
        if isinstance(definition, ServiceDefinition)
        else definition.timeout_seconds,
        control_timeout_seconds=state.template.unknown_state.timeout_seconds,
        stop_timeout_seconds=state.template.start_timeout,
        runner_timeout_margin_seconds=state.template.runner_timeout_margin_seconds,
    )
