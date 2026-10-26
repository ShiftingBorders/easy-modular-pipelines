"""Convert a validated template to the existing JSON contract and anchor its paths."""

from pathlib import Path
from uuid import uuid4

from core.models.experiment_template import ExperimentTemplate
from core.models.updates import _update_model
from core.primitives.json_values import JsonObject


def _template_document(template: ExperimentTemplate, config_path: Path) -> JsonObject:
    return _resolve_template_paths(template, config_path).model_dump(exclude_unset=True)


def _resolve_template_paths(
    template: ExperimentTemplate, config_path: Path
) -> ExperimentTemplate:
    resources = []
    for resource in template.resources:
        path = Path(resource.path)
        resources.append(
            _update_model(
                resource,
                path=str(path if path.is_absolute() else config_path.parent / path),
            )
        )
    return _update_model(template, resources=resources)


def _assign_definition_ids(template: ExperimentTemplate) -> ExperimentTemplate:
    stages = [
        stage
        if stage.stage_id is not None
        else _update_model(stage, stage_id=str(uuid4()))
        for stage in template.stages
    ]
    services = [
        service
        if service.service_id is not None
        else _update_model(service, service_id=str(uuid4()))
        for service in template.services
    ]
    return _update_model(template, stages=stages, services=services)
