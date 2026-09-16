"""Run the basic DAG demo or control a locally configured stage experiment."""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing
import shutil
import sys
from pathlib import Path
from queue import Empty
from uuid import uuid4

import yaml

from core.experimentcontroller import ExperimentController
from core.hashdb import HashDB
from core.modulemanager import ModuleManager
from core.runner_utils.experimentrunner import ExperimentRunner
from core.runner_utils.runtimeio import read_json, write_json
from core.seaweed import SeaweedDB
from core.seaweed_process import SeaweedProcess


async def send_command(
    requests,
    pending: dict,
    command: str,
    args: dict | None = None,
    target: dict | None = None,
) -> dict:
    command_id = str(uuid4())
    future = asyncio.get_running_loop().create_future()
    pending[command_id] = future
    try:
        message = {
            "api_version": 1,
            "command_id": command_id,
            "command": command,
            "args": {} if args is None else args,
        }
        if target is not None:
            message["target"] = target
        await asyncio.to_thread(requests.put, message)
        return await future
    finally:
        pending.pop(command_id, None)


async def receive_responses(responses, pending: dict) -> None:
    while True:
        try:
            response = await asyncio.to_thread(responses.get, True, 0.1)
        except Empty:
            continue
        future = pending.get(response["command_id"])
        if future is not None and not future.done():
            future.set_result(response)


async def print_reply(
    requests,
    pending: dict,
    command: str,
    args: dict | None = None,
    target: dict | None = None,
) -> None:
    result = await send_command(requests, pending, command, args, target)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if result["result"] != "success":
        raise RuntimeError(result["error"])


async def automatic_demo(requests, pending: dict, experiment_id: str) -> None:
    while True:
        response = await send_command(requests, pending, "stats.state")
        state = response["data"]
        if state["phase"] == "failed":
            raise RuntimeError(state["error"])
        if state["phase"] == "waiting":
            break
        await asyncio.sleep(0.1)
    await print_reply(requests, pending, "resume")
    while True:
        state = (await send_command(requests, pending, "stats.state"))["data"]
        if state["phase"] == "failed":
            raise RuntimeError(state["error"])
        if state["executor"] is not None and state["executor"].get("process"):
            print("Live executor:", json.dumps(state["executor"]), flush=True)
            break
        await asyncio.sleep(0.1)
    pause = asyncio.create_task(send_command(requests, pending, "pause"))
    while not pause.done():
        state = (await send_command(requests, pending, "stats.state"))["data"]
        print(
            f"During pause request: {state['phase']}, cycle {state['cycle_number']}",
            flush=True,
        )
        await asyncio.sleep(0.3)
    pause_response = await pause
    print("Pause:", json.dumps(pause_response), flush=True)
    if pause_response["result"] != "success":
        raise RuntimeError(pause_response["error"])
    journal = await send_command(
        requests, pending, "logs.read", {"experiment_id": experiment_id, "limit": 100}
    )
    if journal["result"] != "success":
        raise RuntimeError(journal["error"])
    print("Journal events:", len(journal["data"]["events"]), flush=True)
    for entry in journal["data"]["events"]:
        event = entry["event"]
        if event["event_type"] == "command.output":
            print(event["data"]["text"].rstrip(), flush=True)
    await print_reply(requests, pending, "step")
    final = await send_command(requests, pending, "stats.state")
    print("Final state:", json.dumps(final, indent=2), flush=True)
    if final["result"] != "success" or final["data"]["phase"] != "completed":
        raise RuntimeError("The demonstration did not complete its two cycles.")


async def control(
    project_root: Path,
    template: Path | None,
    manager: ModuleManager,
    automatic: bool,
    resource_config_path: Path | None = None,
    archive_config_path: Path | None = None,
) -> None:
    context = multiprocessing.get_context("spawn")
    requests, responses = context.Queue(), context.Queue()
    runner = ExperimentRunner(
        project_root, manager, archive_config_path=archive_config_path
    )
    controller = ExperimentController(
        project_root,
        runner,
        requests,
        responses,
        resource_config_path=resource_config_path,
    )
    pending = {}
    controller_task = asyncio.create_task(controller.serve())
    response_task = asyncio.create_task(receive_responses(responses, pending))
    display_tasks = []
    try:
        experiment_id = None
        if template is not None:
            response = await send_command(
                requests,
                pending,
                "run",
                {"template_path": str(template), "delayed_start": True},
            )
            if response["result"] != "success":
                raise RuntimeError(response["error"])
            experiment_id = response["experiment_id"]
            print("Experiment:", experiment_id, flush=True)
        if automatic:
            async with asyncio.timeout(60):
                await automatic_demo(requests, pending, experiment_id)
        else:
            print(
                "Commands: state, logs, step, pause, resume, stop, quit; or one command JSON. "
                "Archive commands: archive.create, archive.inspect, archive.install (JSON arguments).",
                flush=True,
            )
            while True:
                try:
                    line = (await asyncio.to_thread(input, "> ")).strip()
                except EOFError:
                    break
                if line == "quit":
                    break
                if not line:
                    continue
                if line.startswith("{"):
                    request = json.loads(line)
                    command, args = request["command"], request.get("args", {})
                    target = request.get("target")
                else:
                    target = None
                    command = {"state": "stats.state", "logs": "logs.read"}.get(
                        line, line
                    )
                    args = {}
                    if line == "logs":
                        selected = await send_command(
                            requests, pending, "stats.state", {}
                        )
                        args = {"experiment_id": selected["data"]["experiment_id"]}
                display_tasks.append(
                    asyncio.create_task(
                        print_reply(requests, pending, command, args, target)
                    )
                )
    finally:
        await runner.stop()
        for task in display_tasks:
            task.cancel()
        await asyncio.gather(*display_tasks, return_exceptions=True)
        await controller.close()
        response_task.cancel()
        await asyncio.gather(controller_task, response_task, return_exceptions=True)
        await runner.close()
        requests.cancel_join_thread()
        responses.cancel_join_thread()
        requests.close()
        responses.close()


def main() -> None:
    # Pipe encoding on Windows follows the legacy code page unless set explicitly.
    # JSON replies and input paths must remain portable, including non-ASCII names.
    for stream in (sys.stdin, sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo", action="store_true")
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Run the two-cycle demo conversation automatically.",
    )
    parser.add_argument("--project-root", type=Path)
    parser.add_argument("--template", type=Path)
    parser.add_argument(
        "--archive-config", type=Path, help="Experiment archiver settings JSON."
    )
    parser.add_argument("--hash-config", type=Path)
    parser.add_argument("--filer-url", default="http://127.0.0.1:8888")
    parser.add_argument(
        "--resource-config",
        type=Path,
        help="Collector settings JSON; defaults to default_settings/resource_collector.json.",
    )
    options = parser.parse_args()
    library_root = Path(__file__).resolve().parent
    storage_process = None
    hashes = None
    archives = None
    try:
        if options.demo:
            project_root = library_root / ".artifacts" / "dag-demo" / str(uuid4())
            project_root.mkdir(parents=True)
            shutil.copytree(
                library_root / "examples" / "basic_dag" / "modules",
                project_root / "modules",
            )
            storage_directory = project_root / "storage"
            storage_directory.mkdir()
            hash_config = project_root / "hashes.json"
            write_json(
                hash_config,
                {
                    "schema_path": str(
                        library_root / "default_settings" / "hash_db_schema.json"
                    ),
                    "db_path": str(project_root / "hashes.db"),
                },
            )
            print("Preparing demo stores in", project_root, flush=True)
            storage_process = SeaweedProcess(storage_directory, 0)
            storage_process.start()
            filer_url = storage_process.filer_url
        else:
            if options.project_root is None or options.hash_config is None:
                parser.error(
                    "Supply --demo or --project-root and --hash-config; --template is optional."
                )
            if options.auto:
                parser.error("--auto is intended for --demo.")
            project_root = options.project_root.resolve()
            hash_config = options.hash_config.resolve()
            filer_url = options.filer_url
        hashes = HashDB(hash_config)
        archives = SeaweedDB(filer_url)
        manager = ModuleManager(
            project_root / "modules", hashes, archives, project_root / "temporary"
        )
        if options.demo:
            module_directory = project_root / "modules" / "counter" / "1.0"
            manager.register_module("counter", "1.0", module_directory)
            template = project_root / "experiment.yaml"
            template.write_text(
                yaml.safe_dump(
                    {
                        "schema_version": 1,
                        "name": "counter-demo",
                        "cycles": 2,
                        "keep_attempts": 1,
                        "start_timeout": 5,
                        "runner_timeout_margin_seconds": 2,
                        "stages": [
                            {
                                "module": {
                                    "name": "counter",
                                    "version": "1.0",
                                    "hash": hashes.get_module_hash("counter", "1.0"),
                                },
                                "settings": {},
                                "timeout_seconds": 20,
                                "errors": {
                                    "retries": 0,
                                    "retry_delay_seconds": 0.1,
                                    "on_exhausted": "pause",
                                },
                            }
                        ],
                        "services": [],
                        "resources": [],
                        "unknown_state": {
                            "timeout_seconds": 10,
                            "on_timeout": "stop",
                            "recovery_limit": 3,
                            "on_recovery_limit": "stop",
                        },
                        "snapshots": {"mode": "off", "keep": 3},
                        "storage": {"min_snapshot_free_bytes": 1073741824},
                        "logging": read_json(
                            library_root / "default_settings" / "logging.json"
                        ),
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
        else:
            template = None if options.template is None else options.template.resolve()
        asyncio.run(
            control(
                project_root,
                template,
                manager,
                options.auto,
                None
                if options.resource_config is None
                else options.resource_config.resolve(),
                None
                if options.archive_config is None
                else options.archive_config.resolve(),
            )
        )
        print("Experiment files:", project_root / "experiments", flush=True)
    finally:
        if archives is not None:
            archives.close()
        if hashes is not None:
            hashes.close_connection()
        if storage_process is not None:
            storage_process.stop()


if __name__ == "__main__":
    main()
