"""Observable SDK use in a separately owned stage process."""

import argparse
import time
from pathlib import Path

from core.runner_utils.stage_client import StageClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--emp-context", type=Path, required=True)
    client = StageClient(parser.parse_args().emp_context)
    mode = "normal"
    with client:
        mode = client.settings.get("mode", "normal")
        if mode == "no_result":
            return
        if mode == "invalid":
            invalid = [-0.1, 1.1, True, float("nan"), float("inf"), "0.5"]
            rejected = 0
            for value in invalid:
                try:
                    client.report_progress(value)
                except (TypeError, ValueError):
                    rejected += 1
            try:
                client.report_state([1])
            except (TypeError, ValueError):
                rejected += 1
            client.succeed({"rejected": rejected})
            return
        client.report_progress(0.5, "Halfway")
        client.report_state({"phase": "working", "input": client.input_data})
        path = Path(client.context["artifacts_directory"])
        (path / "client.ready").touch()
        gate = client.settings.get("gate")
        while gate and not Path(gate).exists():
            if client.cancel_requested():
                (path / "client.cancelled").touch()
                client.fail({"reason": "cancelled"})
                return
            time.sleep(0.01)
        client.succeed({"input": client.input_data, "settings": client.settings})
        if mode == "duplicate":
            try:
                client.fail({"second": True})
            except RuntimeError:
                (path / "duplicate.rejected").touch()
        if mode == "nonzero":
            raise RuntimeError(
                "The executor must reject stdout success followed by failure."
            )


if __name__ == "__main__":
    main()
