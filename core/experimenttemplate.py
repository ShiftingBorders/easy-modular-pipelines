"""Create an editable experiment draft without opening a server connection."""

from pathlib import Path

import yaml

from core.logger_utils.events import require_text


def create_template(destination: Path, name: str) -> Path:
    destination = Path(destination)
    if not destination.is_absolute():
        raise ValueError("destination must be absolute.")
    name = require_text(name, "experiment name")
    defaults = (
        Path(__file__).resolve().parents[1]
        / "default_settings/experiment_template.yaml"
    )
    document = yaml.safe_load(defaults.read_text(encoding="utf-8"))
    document["name"] = name
    text = "# Draft: fill stages before running this experiment.\n" + yaml.safe_dump(
        document, allow_unicode=True, sort_keys=False
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("x", encoding="utf-8", newline="\n") as output:
        output.write(text)
    return destination
