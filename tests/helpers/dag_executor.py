"""Executor publication checkpoints for approved B8/B9 tests; no production hooks."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from unittest.mock import patch

from core.runner_utils import executor


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--launch", type=Path, required=True)
    parser.add_argument(
        "--fault",
        choices=("before_result", "after_result", "write_failure"),
        required=True,
    )
    args = parser.parse_args()
    original = executor.write_json
    directory = args.launch.parent

    def publish(path, data):
        if path.name != "execution_result.json":
            return original(path, data)
        if args.fault == "after_result":
            original(path, data)
        marker = directory / "checkpoint.pending"
        marker.write_text(
            json.dumps({"pid": os.getpid(), "point": args.fault}), encoding="utf-8"
        )
        marker.replace(directory / "checkpoint.json")
        if args.fault == "write_failure":
            raise OSError("injected result publication failure")
        while not (directory / "release-executor").exists():
            time.sleep(0.01)
        if args.fault == "before_result":
            original(path, data)
        return None

    with patch.object(executor, "write_json", side_effect=publish):
        asyncio.run(executor.StageExecutor(args.launch).run())


if __name__ == "__main__":
    main()
