"""Executor publication checkpoints for approved B8/B9 tests; no production hooks."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import LoggingStorageError
from core.runner_utils import executor


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--launch", type=Path, required=True)
    parser.add_argument(
        "--fault",
        choices=("before_result", "after_result", "write_failure", "no_execute"),
        required=True,
    )
    args = parser.parse_args()
    original = OperationLogger.record_command_result
    directory = args.launch.parent

    def publish(logger, request_id, response, **kwargs):
        if args.fault == "no_execute" or kwargs.get("author") != "participant":
            return original(logger, request_id, response, **kwargs)
        if args.fault == "after_result":
            original(logger, request_id, response, **kwargs)
        marker = directory / "checkpoint.pending"
        marker.write_text(
            json.dumps({"pid": os.getpid(), "point": args.fault}), encoding="utf-8"
        )
        marker.replace(directory / "checkpoint.json")
        if args.fault == "write_failure":
            raise LoggingStorageError("injected result publication failure")
        while not (directory / "release-executor").exists():
            time.sleep(0.01)
        if args.fault == "before_result":
            return original(logger, request_id, response, **kwargs)
        return None

    with patch.object(OperationLogger, "record_command_result", publish):
        asyncio.run(executor.StageExecutor(args.launch).run())


if __name__ == "__main__":
    main()
