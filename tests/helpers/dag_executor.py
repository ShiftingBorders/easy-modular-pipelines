"""Executor publication checkpoints for approved B8/B9 tests; no production hooks."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from unittest.mock import patch

from core.journal.events import LoggingStorageError
from core.journal.logger import OperationLogger
from core.participants import executor


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--launch", type=Path, required=True)
    parser.add_argument(
        "--fault",
        choices=(
            "before_result",
            "after_result",
            "write_failure",
            "no_execute",
            "timeout",
        ),
        required=True,
    )
    args = parser.parse_args()
    original = OperationLogger.record_command_result
    original_execute = executor.StageExecutor._execute
    directory = args.launch.parent

    async def execute(worker, request):
        if args.fault != "timeout":
            return await original_execute(worker, request)
        original_start = worker._start_process

        async def start_ready_process():
            await original_start()
            async with asyncio.timeout(10):
                while not (directory / "ready.json").is_file():
                    if worker._process.returncode is not None:
                        raise RuntimeError(
                            "Stage exited before the timeout checkpoint."
                        )
                    await asyncio.sleep(0.01)
            # Arm the deadline after readiness so this fixture measures executor
            # enforcement independently of interpreter startup and host load.
            request["deadline_monotonic"] = time.monotonic() + 0.1

        with patch.object(worker, "_start_process", start_ready_process):
            return await original_execute(worker, request)

    def publish(logger, request_id, response, **kwargs):
        if args.fault == "no_execute" or kwargs.get("author") != "participant":
            return original(logger, request_id, response, **kwargs)
        if args.fault in ("after_result", "timeout"):
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

    with (
        patch.object(OperationLogger, "record_command_result", publish),
        patch.object(executor.StageExecutor, "_execute", execute),
    ):
        asyncio.run(executor.StageExecutor(args.launch).run())


if __name__ == "__main__":
    main()
