"""Controlled stage fixture for approved basic-DAG groups B, C, D, and E."""

import argparse
import json
import os
import sys
import time
from pathlib import Path

from core.logger import OperationLogger


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emp-context", type=Path, required=True)
    args = parser.parse_args()
    context = json.loads(args.emp_context.read_text(encoding="utf-8"))
    settings = context["settings"]
    identity = context["context"]
    directory = Path(context["artifacts_directory"])
    shared = Path(context["experiment_directory"]) / "shared_data"
    label = settings.get("label", identity["module_name"])
    trace = {
        "event": "start",
        "label": label,
        "pid": os.getpid(),
        "stage_id": identity["stage_id"],
        "cycle": identity["cycle_number"],
        "attempt": identity["attempt_number"],
        "input": context["input_data"],
    }
    with (shared / "trace.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(trace) + "\n")
    (directory / "received.json").write_text(json.dumps(context), encoding="utf-8")
    with OperationLogger(Path(context["logging_config_path"])) as logger:
        logger.record_event("fixture.ready", {"pid": os.getpid(), "label": label})
        print("fixture stderr before completion", file=sys.stderr, flush=True)
        if settings.get("split_output"):
            print('{"result":"success","data":', end="", flush=True)
        ready = directory / "ready.pending"
        ready.write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
        ready.replace(directory / "ready.json")
        if settings.get("gate"):
            gate = Path(settings["gate"])
            while not gate.exists():
                time.sleep(0.01)
        mode = settings.get("mode", "success")
        failed = mode == "fail" or identity["attempt_number"] <= settings.get(
            "failures", 0
        )
        prior = context["input_data"]
        data = {
            "trail": (prior.get("trail", []) if isinstance(prior, dict) else [])
            + [label],
            "attempt": identity["attempt_number"],
            "input": prior,
            "echo": settings.get("echo"),
        }
        (directory / "output.json").write_text(json.dumps(data), encoding="utf-8")
        data["artifact"] = (
            (directory / "output.json")
            .relative_to(context["experiment_directory"])
            .as_posix()
        )
        with (shared / "trace.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({**trace, "event": "finish"}) + "\n")
        logger.record_event("fixture.finished", {"label": label})
        if settings.get("split_output"):
            print(json.dumps(data) + "}", flush=True)
        elif mode == "empty":
            return
        elif mode == "invalid":
            print("not JSON", flush=True)
        elif mode == "extra":
            print("{}\n{}", flush=True)
        elif mode == "missing_data":
            print('{"result":"success"}', flush=True)
        else:
            print(
                json.dumps({"result": "fail" if failed else "success", "data": data}),
                flush=True,
            )
        if mode == "nonzero":
            sys.exit(7)


if __name__ == "__main__":
    main()
