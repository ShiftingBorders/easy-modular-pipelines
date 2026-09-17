"""A small stage that exposes progress through the shared journal."""

import argparse
import json
import time
from pathlib import Path

from core.logger import OperationLogger
from core.runner_utils.stage_client import StageClient


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--emp-context", type=Path, required=True)
    arguments = parser.parse_args()
    with StageClient(arguments.emp_context) as client:
        context = client.context
        ticks = client.settings["ticks"]
        delay = client.settings["delay_seconds"]
        if (
            type(ticks) is not int
            or ticks < 1
            or type(delay) not in (int, float)
            or delay < 0
        ):
            raise ValueError(
                "ticks must be positive; delay_seconds must be nonnegative."
            )
        for tick in range(1, ticks + 1):
            if client.cancel_requested():
                client.fail({"reason": "cancelled", "completed": tick - 1})
                return
            client.report_progress(tick / ticks, f"Counter: {tick}/{ticks}")
            client.report_state({"tick": tick, "total": ticks})
            time.sleep(delay)
        artifact = Path(context["artifacts_directory"]) / "counter.json"
        result = {"ticks": ticks, "input_data": client.input_data}
        artifact.write_text(json.dumps(result), encoding="utf-8")
        relative = artifact.relative_to(context["experiment_directory"]).as_posix()
        with OperationLogger(Path(context["logging_config_path"])) as logger:
            logger.record_artifact(
                "counter.json",
                purpose="counter data",
                size_bytes=artifact.stat().st_size,
            )
        client.succeed({"ticks": ticks, "artifact": relative})


if __name__ == "__main__":
    main()
