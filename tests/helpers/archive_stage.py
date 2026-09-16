"""Portable stage used by the approved archive round-trip tests."""

import argparse
import json
import time
from pathlib import Path

from core.logger import OperationLogger
from core.runner_utils.runtimeio import read_json, write_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--emp-context", type=Path, required=True)
    context = read_json(parser.parse_args().emp_context)
    artifacts = Path(context["artifacts_directory"])
    resource = Path(context["resources_directory"]) / "source.txt"
    with OperationLogger(Path(context["logging_config_path"])) as logger:
        logger.record_event("archive_fixture.started", {})
        write_json(artifacts / "ready.json", {"ready": True})
        gate = context["settings"].get("gate")
        while gate and not Path(gate).exists():
            time.sleep(0.025)
        data = {
            "text": resource.read_text(encoding="utf-8"),
            "cycle": context["context"]["cycle_number"],
            "label": context["settings"].get("label", "stage"),
        }
        write_json(artifacts / "output.json", data)
        data["artifact"] = (
            (artifacts / "output.json")
            .relative_to(context["experiment_directory"])
            .as_posix()
        )
        logger.record_event("archive_fixture.finished", data)
    print(json.dumps({"result": "success", "data": data}), flush=True)


if __name__ == "__main__":
    main()
