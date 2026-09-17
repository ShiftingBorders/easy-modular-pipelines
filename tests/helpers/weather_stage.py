"""Actual StageClient modules for waiting, formatting and writing the forecast."""

import argparse
import time
from pathlib import Path

from core.logger import OperationLogger
from core.runner_utils.stage_client import StageClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--emp-context", type=Path, required=True)
    with StageClient(parser.parse_args().emp_context) as client:
        operation = client.settings["operation"]
        if operation == "wait":
            started = time.monotonic()
            client.report_state({"phase": "waiting", "seconds": 40})
            while time.monotonic() - started < 40:
                if client.cancel_requested():
                    client.fail({"reason": "cancelled"})
                    return
                time.sleep(0.25)
            client.report_progress(1, "Ожидание завершено")
            client.succeed({"waited_seconds": time.monotonic() - started})
        elif operation == "format":
            forecast = client.input_data
            text = f"{forecast['city']}: {forecast['temperature_c']:+d} °C, {forecast['condition']}.\n"
            client.succeed({"text": text, "forecast": forecast})
        elif operation == "write":
            context = client.context
            path = Path(context["artifacts_directory"]) / "weather.txt"
            path.write_text(client.input_data["text"], encoding="utf-8")
            with OperationLogger(Path(context["logging_config_path"])) as logger:
                logger.record_artifact(
                    "weather.txt",
                    purpose="weather forecast",
                    size_bytes=path.stat().st_size,
                )
            client.succeed(
                {
                    "path": path.relative_to(
                        context["experiment_directory"]
                    ).as_posix(),
                    **client.input_data,
                }
            )


if __name__ == "__main__":
    main()
