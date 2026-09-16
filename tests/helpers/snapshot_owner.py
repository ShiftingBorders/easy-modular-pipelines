"""Disposable runner process with explicit crash points for snapshots.md G/I."""

import argparse
import asyncio
import os
import time
from pathlib import Path
from unittest.mock import patch

from core.hashdb import HashDB
from core.modulemanager import ModuleManager
from core.runner_utils.experimentrunner import ExperimentRunner
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.runtimeio import process_identity, write_json
from core.runner_utils.services import ServiceManager
from core.runner_utils.state import RunnerStateStore
from core.seaweed import SeaweedDB


async def run_owner(options):
    hashes = HashDB(options.root / "hashes.json")
    archives = SeaweedDB("http://127.0.0.1:1")
    manager = ModuleManager(
        options.root / "modules", hashes, archives, options.root / "owner-work"
    )
    runner = ExperimentRunner(options.root, manager)
    original_replace = Path.replace
    original_complete = RunnerJournal.complete_restore
    original_load = ServiceManager.load_states
    original_request = ServiceManager.request
    original_save = RunnerStateStore.save

    def halt(phase):
        if phase != options.phase:
            return
        write_json(
            options.root / "owner-fault.json",
            {"phase": phase, "process": process_identity(os.getpid())},
        )
        if options.action == "crash":
            os._exit(23)
        while not (options.root / "release-owner").exists():
            time.sleep(0.05)

    def publish(path, document):
        write_json(path, document)
        if Path(path).parent.name == "restore_transactions":
            halt(document["phase"])

    def move(source, destination):
        result = original_replace(source, destination)
        if Path(destination).name == "previous":
            halt("old_moved")
        if (
            runner._state is not None
            and Path(destination) == runner._state.experiment_directory
        ):
            halt("new_installed")
        return result

    def complete(journal, *args, **kwargs):
        result = original_complete(journal, *args, **kwargs)
        halt("journal_committed")
        return result

    async def load(services, *args, **kwargs):
        await original_load(services, *args, **kwargs)
        halt("load_applied")

    async def request(services, state, service_id, command, args):
        reply = await original_request(services, state, service_id, command, args)
        if command == "freeze_writes":
            halt("freeze_confirmed")
        return reply

    def save(store, state):
        if options.phase == "stage_without_state" and state.active_attempt is not None:
            raise OSError("Optional state publication refused after stage intent.")
        return original_save(store, state)

    try:
        if options.operation == "run":
            await runner.run(
                options.root / "template.yaml",
                experiment_id=options.experiment,
                delayed_start=True,
            )
            await runner._ready.wait()
        else:
            await runner.recover(options.experiment)
        write_json(
            options.root / "owner-ready.json",
            {
                "experiment_id": options.experiment,
                "process": process_identity(os.getpid()),
            },
        )
        with (
            patch("core.runner_utils.snapshots.write_json", publish),
            patch.object(Path, "replace", move),
            patch.object(RunnerJournal, "complete_restore", complete),
            patch.object(ServiceManager, "load_states", load),
            patch.object(ServiceManager, "request", request),
            patch.object(RunnerStateStore, "save", save),
        ):
            if options.operation == "rollback":
                await runner.rollback(options.snapshot)
            elif options.operation == "snapshot":
                await runner.snapshot("owner snapshot")
            elif options.operation == "stage":
                step = asyncio.create_task(runner.step())
                while (
                    runner._state.active_attempt is None
                    or runner._state.active_attempt.process_identity is None
                ):
                    await asyncio.sleep(0.05)
                halt("stage_running")
                halt("stage_without_state")
                await step
            else:
                halt("ready")
        write_json(options.root / "owner-result.json", runner.get_state())
    finally:
        await runner.close()
        archives.close()
        hashes.close_connection()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument(
        "--operation", choices=("run", "rollback", "snapshot", "stage"), required=True
    )
    parser.add_argument("--snapshot")
    parser.add_argument("--phase", required=True)
    parser.add_argument("--action", choices=("crash", "hang"), default="crash")
    asyncio.run(run_owner(parser.parse_args()))


if __name__ == "__main__":
    main()
