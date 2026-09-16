"""Bounded faults inside disposable test processes, never production protocol features."""

import asyncio
import os
import time
from contextlib import nullcontext
from multiprocessing.reduction import ForkingPickler
from pathlib import Path
from unittest.mock import patch

from core.runner_utils.runtimeio import process_identity, write_json


def blocked_receive(directory):
    """Simulate a partial receive inside a real Queue.get/unpickle operation."""
    directory = Path(directory)
    directory.with_suffix(".entered").touch()
    while not directory.with_suffix(".release").exists():
        time.sleep(0.025)
    return {"_runtime": "error", "message": "test receive released"}


class BlockedMessage:
    def __init__(self, directory):
        self.directory = str(directory)

    def __reduce__(self):
        return blocked_receive, (self.directory,)


def write_frame(queue, data):
    """Keep Queue's count consistent while intentionally replacing only its pickled frame."""
    if not queue._sem.acquire(timeout=2):
        raise TimeoutError("Fixture IPC queue is full.")
    with queue._wlock if queue._wlock is not None else nullcontext():
        queue._writer.send_bytes(data)


def fault_controller(settings, requests, responses, instance_id):
    from core import serverruntime
    from core.experimentcontroller import ExperimentController
    from core.runner_utils.snapshots import ExperimentSnapshots

    control = Path(settings.fixture_control)
    write_json(
        control / "controller-owner.json", {"process": process_identity(os.getpid())}
    )
    if settings.fixture_mode == "startup_hang":
        (control / "startup.entered").touch()
        while not (control / "startup.release").exists():
            time.sleep(0.025)
    execute = ExperimentController._execute_command
    close = ExperimentController.close
    build = ExperimentSnapshots._build_snapshot

    async def execute_command(controller, command):
        name = command["command"]
        if not name.startswith("fixture."):
            return await execute(controller, command)
        if name == "fixture.wait":
            (control / "command.entered").touch()
            while not (control / "command.release").exists():
                await asyncio.sleep(0.025)
        args = command.get("args", {})
        return {
            "command_id": command["command_id"],
            "chain_id": command.get("chain_id"),
            "state": "succeeded",
            "result": "success",
            "experiment_id": None,
            "data": {
                "args": args,
                "target": command.get("target"),
                "blob": "x" * args.get("bytes", 0),
            },
            "error": None,
        }

    async def close_controller(controller):
        if (control / "hold-shutdown").exists():
            (control / "shutdown.entered").touch()
            while not (control / "shutdown.release").exists():
                await asyncio.sleep(0.025)
        await close(controller)

    def build_snapshot(owner, *args, **kwargs):
        if (control / "hold-snapshot").exists():
            (control / "snapshot.entered").touch()
            while not (control / "snapshot.release").exists():
                time.sleep(0.025)
        return build(owner, *args, **kwargs)

    async def faults():
        while True:
            if (control / "truncate-response").exists():
                write_frame(responses, b"\x80\x05\x95")
                (control / "response-corrupted").touch()
                return
            if (control / "block-response").exists():
                write_frame(
                    responses,
                    ForkingPickler.dumps(BlockedMessage(control / "response-read")),
                )
                while not (control / "response-read.entered").exists():
                    await asyncio.sleep(0.025)
                os._exit(26)
            await asyncio.sleep(0.025)

    original_main = serverruntime.controller_main

    async def controlled_main(*args):
        watcher = asyncio.create_task(faults())
        try:
            await original_main(*args)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)

    with (
        patch.object(ExperimentController, "_execute_command", new=execute_command),
        patch.object(ExperimentController, "close", new=close_controller),
        patch.object(ExperimentSnapshots, "_build_snapshot", new=build_snapshot),
        patch.object(serverruntime, "controller_main", new=controlled_main),
    ):
        serverruntime.controller_process(settings, requests, responses, instance_id)
