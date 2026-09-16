"""A real ServiceManager owner that can hang or crash at snapshot boundaries."""

import argparse
import asyncio
import os
import time
import traceback
from pathlib import Path

from core.experimentassembler import ExperimentAssembler
from core.hashdb import HashDB
from core.modulemanager import ModuleManager
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.launch import ModuleLauncher
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from core.runner_utils.services import ServiceManager
from core.runner_utils.state import RunnerStateStore
from core.seaweed import SeaweedDB
from tests.helpers.services import wait_for


async def run(options):
    controls = options.controls
    write_json(controls / "owner.json", process_identity(os.getpid()))
    hashes = HashDB(options.workspace / "hashes.json")
    archives = SeaweedDB("http://127.0.0.1:1")
    modules = ModuleManager(
        options.workspace / "modules", hashes, archives, options.workspace / "work"
    )
    store = RunnerStateStore()
    state = store.load(options.workspace / "experiment")
    journal = RunnerJournal()
    journal.open(state, create=False)
    manager = ServiceManager(
        ModuleLauncher(ExperimentAssembler(options.workspace, modules), journal),
        journal,
        store,
    )
    try:
        if options.mode == "start-crash":
            startup = asyncio.create_task(manager.start_all(state))
            await wait_for(
                lambda: (
                    state.services
                    and all(
                        item.process_identity and item.endpoint_path.exists()
                        for item in state.services.values()
                    )
                )
            )
            if startup.done():
                raise RuntimeError(
                    "Expected an unfinished startup at the crash boundary."
                )
            store.save(state)
            write_json(controls / "checkpoint.json", {"phase": "starting"})
            os._exit(23)
        if options.mode == "recover":
            action = await manager.recover(state)
            write_json(
                controls / "recovered.json", {"action": action, "mode": state.mode}
            )
            if action == "stop":
                raise RuntimeError("Service recovery required stop.")
            await wait_for(
                lambda: all(
                    item.active_request is None for item in state.services.values()
                ),
                timeout=35,
            )
            await manager.unfreeze(state, options.snapshot)
            counter = (
                state.experiment_directory
                / "module_data"
                / next(iter(state.services))
                / "counter.json"
            )
            await wait_for(lambda: not read_json(counter)["frozen"])
            result = await manager.stop_all(state)
            write_json(controls / "done.json", {"stops": result, "mode": state.mode})
            return
        action = await manager.start_all(state)
        if action != "ready":
            raise RuntimeError(f"Service startup required {action}.")
        write_json(
            controls / "ready.json",
            {key: item.process_identity for key, item in state.services.items()},
        )
        operation = asyncio.create_task(manager.save_states(state, options.snapshot))
        if options.phase == "before_freeze":
            service_controls = Path(
                state.template["services"][0]["settings"]["controls"]
            )

            def reached():
                path = service_controls / "trace.jsonl"
                if not path.exists():
                    return False
                return any(
                    '"event": "freeze_entered"' in line
                    for line in path.read_text().splitlines()
                )

            await wait_for(reached)
        else:
            await operation
            store.save(state)
        write_json(
            controls / "checkpoint.json",
            {"phase": options.phase, "snapshot_id": options.snapshot},
        )
        if options.fault == "crash":
            os._exit(23)
        if options.fault == "hang":
            while not (controls / "release-owner").exists():
                time.sleep(0.05)  # noqa: ASYNC251 - Deliberately hang the owner's event loop.
        if not operation.done():
            await operation
        while not (controls / "unfreeze").exists():
            await asyncio.sleep(0.05)
        await manager.unfreeze(state, options.snapshot)
        write_json(
            controls / "done.json",
            {"stops": await manager.stop_all(state), "mode": state.mode},
        )
    except Exception as error:  # noqa: BLE001 - Preserve subprocess diagnostics for the parent assertion.
        write_json(
            controls / "error.json",
            {
                "type": type(error).__name__,
                "message": str(error),
                "traceback": traceback.format_exc(),
            },
        )
    finally:
        await manager.close()
        journal.close()
        archives.close()
        hashes.close_connection()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--controls", type=Path, required=True)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument(
        "--mode", choices=("freeze", "recover", "start-crash"), default="freeze"
    )
    parser.add_argument(
        "--phase", choices=("before_freeze", "frozen"), default="frozen"
    )
    parser.add_argument("--fault", choices=("none", "hang", "crash"), default="none")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
