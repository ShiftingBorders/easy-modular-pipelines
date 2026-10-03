"""Convert a validated template to the existing JSON contract and anchor its paths."""

from pathlib import Path

from core.models.experiment_template import ExperimentTemplate
from core.primitives.json_values import JsonObject


def _template_document(template: ExperimentTemplate, config_path: Path) -> JsonObject:
    document = template.model_dump(exclude_unset=True)
    for resource in document["resources"]:
        path = Path(resource["path"])
        resource["path"] = str(
            path if path.is_absolute() else config_path.parent / path
        )
    return document
