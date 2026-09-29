"""Controlled subprocess effects for the approved conditional-stage tests."""

import argparse
import copy
import json
import os
from pathlib import Path

from core.runner_utils.stage_client import StageClient


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emp-context", type=Path, required=True)
    arguments = parser.parse_args()
    with StageClient(arguments.emp_context) as client:
        identity = client.context["context"]
        attempt_number = identity["attempt_number"]
        root = Path(client.context["experiment_directory"])
        with (root / "shared_data/trace.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps(
                    {
                        "event": "start",
                        "label": client.settings.get("label", "condition"),
                        "stage_id": identity["stage_id"],
                        "attempt_id": identity["attempt_id"],
                        "cycle": identity["cycle_number"],
                        "attempt": attempt_number,
                        "pid": os.getpid(),
                        "input": client.input_data,
                    }
                )
                + "\n"
            )
        if attempt_number <= client.settings.get(
            "crash_attempts", 0
        ) or attempt_number in client.settings.get("crash_on_attempts", []):
            raise RuntimeError("Intentional conditional-stage fixture crash.")
        if client.settings.get("artifact_probe"):
            previous = client.input_data
            content = (
                None
                if previous is None
                else (root / previous["path"]).read_text(encoding="utf-8")
            )
            output = Path(client.context["artifacts_directory"]) / "payload.txt"
            output.write_text(f"attempt={attempt_number}\n", encoding="utf-8")
            client.succeed(
                {
                    "path": output.relative_to(root).as_posix(),
                    "read": content,
                    "input": previous,
                }
            )
            return
        if "raw_result" in client.settings:
            # Exercise the executor's envelope validation, bypassing only the
            # SDK's well-formed stdout writer in this test fixture.
            print(json.dumps(client.settings["raw_result"]), flush=True)
        else:
            decisions = client.settings.get("decisions")
            decision = (
                client.settings.get("decision")
                if decisions is None
                else decisions[min(attempt_number - 1, len(decisions) - 1)]
            )
            if client.settings.get("forward_input"):
                decision = {} if decision is None else copy.deepcopy(decision)
                decision["data"] = client.input_data
            client.succeed(decision)


if __name__ == "__main__":
    main()
