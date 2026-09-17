"""Run the small existing socket service or a commands service with portable paths."""

import argparse
import asyncio
from pathlib import Path

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
    from service_process import PythonService

    asyncio.run(PythonService(context).run())


if __name__ == "__main__":
    main()
