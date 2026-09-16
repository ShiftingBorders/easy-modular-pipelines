"""Run the small existing socket service or a commands service with portable paths."""

import argparse
import asyncio
import json
from pathlib import Path

from core.logger import OperationLogger
from core.runner_utils.runtimeio import read_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--emp-context", type=Path, required=True)
    parser.add_argument("--action", choices=("start", "stop"), default="start")
    args = parser.parse_args()
    context = read_json(args.emp_context)
    controls = Path(context["module_data_directory"]) / "controls"
    controls.mkdir(parents=True, exist_ok=True)
    context["settings"]["controls"] = str(controls)
    if context["service_interface"] == "commands":
        with OperationLogger(Path(context["logging_config_path"])) as logger:
            logger.record_event(
                "archive_fixture.service_action", {"action": args.action}
            )
        with (controls / "actions.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"action": args.action}) + "\n")
        return
    from service_process import PythonService

    asyncio.run(PythonService(context).run())


if __name__ == "__main__":
    main()
