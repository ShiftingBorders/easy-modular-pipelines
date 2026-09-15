"""A small stage that exposes progress through the shared journal."""

import argparse
import json
import sys
import time
from pathlib import Path

from core.logger import OperationLogger


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--emp-context", type=Path, required=True)
    arguments = parser.parse_args()
    context = json.loads(arguments.emp_context.read_text(encoding="utf-8"))
    settings = context["settings"]
    ticks = settings["ticks"]
    delay = settings["delay_seconds"]
    if (
        type(ticks) is not int
        or ticks < 1
        or not isinstance(delay, (int, float))
        or delay < 0
    ):
        raise ValueError("ticks must be positive; delay_seconds must be nonnegative.")
    with OperationLogger(Path(context["logging_config_path"])) as logger:
        for tick in range(1, ticks + 1):
            logger.record_event("counter.tick", {"tick": tick, "total": ticks})
            print(f"Counter: {tick}/{ticks}", file=sys.stderr, flush=True)
            time.sleep(delay)
        artifact = Path(context["artifacts_directory"]) / "counter.json"
        result = {"ticks": ticks, "input_data": context["input_data"]}
        artifact.write_text(json.dumps(result), encoding="utf-8")
        relative = artifact.relative_to(context["experiment_directory"]).as_posix()
        print(
            json.dumps(
                {"result": "success", "data": {"ticks": ticks, "artifact": relative}}
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
