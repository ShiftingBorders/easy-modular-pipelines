"""Disposable runner process with explicit crash points for snapshots.md G/I."""

import argparse
import asyncio
import os
import time
from pathlib import Path
from unittest.mock import patch

from core.experimentassembler import ExperimentAssembler
from core.hashdb import HashDB
from core.logger import OperationLogger
from core.modulemanager import ModuleManager
from core.runner_utils.experimentrunner import ExperimentRunner
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.runtimeio import process_identity, read_json, write_json
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
    original_record = OperationLogger.record_event
    original_template = OperationLogger.record_template_applied
    original_rebuild = ExperimentAssembler.rebuild
    original_prepare = ServiceManager.prepare_rebuild
    stale_rebuild_saved = False

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
        if options.operation == "recover" and Path(path).name == "process.json":
            halt("child_reconciled")

    def read_endpoint(path, **kwargs):
        document = read_json(path, **kwargs)
        if (
            options.operation == "reload"
            and runner._state is not None
            and runner._state.pending_rebuild is not None
            and "endpoint" in document
            and any(
                instance.endpoint_path == path
                and instance.service_instance_id
                == document.get("participant_instance_id")
                and instance.process_identity is not None
                and instance.process_identity != document.get("process")
                for instance in runner._state.services.values()
            )
        ):
            halt("child_endpoint")
        return document

    def move(source, destination):
        result = original_replace(source, destination)
        if Path(destination).name == "previous":
            halt("old_moved")
        if (
            runner._state is not None
            and Path(destination) == runner._state.experiment_directory
        ):
            halt("new_installed")
        if options.operation == "reload" and runner._state is not None:
            if Path(destination) == runner._state.template_path:
                halt("template_published")
            if Path(destination).parent.name == "previous_data":
                halt("module_data_moved")
            if Path(source).is_relative_to(
                runner._state.experiment_directory / "runner/rebuilds"
            ) and Path(destination).is_relative_to(
                runner._state.experiment_directory / "modules"
            ):
                halt("module_published")
        return result

    def record(logger, kind, data=None, **kwargs):
        result = original_record(logger, kind, data, **kwargs)
        if options.operation == "reload" and runner._state is not None:
            if kind == "runner.checkpoint":
                if data["pending_rebuild"] is not None:
                    halt("rebuild_intent")
                elif data["template_revision_id"] != initial_revision:
                    halt("rebuild_committed")
                    halt("committed_stale_file")
            if kind == "service.started" and runner._state.pending_rebuild:
                halt("service_started")
            if kind == "rebuild.checkpoint" and any(
                not s["stopped"] and s["process_identity"] is None
                for s in data["services"].values()
            ):
                halt("service_spawn_intent")
        return result

    def applied(logger, *args, **kwargs):
        result = original_template(logger, *args, **kwargs)
        if options.operation == "reload":
            halt("template_applied")
        return result

    async def rebuild(assembler, *args, **kwargs):
        await original_rebuild(assembler, *args, **kwargs)
        if options.operation == "reload":
            if kwargs.get("prepare_only"):
                halt("rebuild_prepared")
            elif options.phase in (
                "staging",
                "old_moved",
                "new_installed",
                "journal_committed",
                "services_starting",
            ):
                raise OSError("Trigger automatic reload rollback")

    async def prepare(services, *args, **kwargs):
        await original_prepare(services, *args, **kwargs)
        if options.operation == "reload":
            halt("service_stopped")

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
        nonlocal stale_rebuild_saved
        if options.phase == "committed_stale_file":
            if stale_rebuild_saved:
                raise OSError("Optional state publication refused after run change.")
            if (
                state.pending_rebuild is not None
                and state.template_revision_id != initial_revision
            ):
                result = original_save(store, state)
                stale_rebuild_saved = True
                return result
        if options.phase == "stage_without_state" and state.active_attempt is not None:
            raise OSError("Optional state publication refused after stage intent.")
        if state.pending_rebuild is not None and any(
            instance.stopped for instance in state.services.values()
        ):
            halt("service_stopped_uncheckpointed")
        result = original_save(store, state)
        if (
            options.operation == "recover"
            and state.pending_rebuild is not None
            and any(
                instance.stopped
                and instance.definition["settings"].get("launcher_cleanup")
                for instance in state.services.values()
            )
        ):
            halt("child_stopped")
        return result

    try:
        if options.operation == "recover":
            with (
                patch("core.runner_utils.experimentrunner.write_json", publish),
                patch.object(RunnerStateStore, "save", save),
            ):
                await runner.recover(options.experiment)
            return
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
        initial_revision = runner._state.template_revision_id
        with (
            patch("core.runner_utils.snapshots.write_json", publish),
            patch.object(Path, "replace", move),
            patch.object(RunnerJournal, "complete_restore", complete),
            patch.object(ServiceManager, "load_states", load),
            patch.object(ServiceManager, "request", request),
            patch.object(RunnerStateStore, "save", save),
            patch.object(OperationLogger, "record_event", record),
            patch.object(OperationLogger, "record_template_applied", applied),
            patch.object(ExperimentAssembler, "rebuild", rebuild),
            patch.object(ServiceManager, "prepare_rebuild", prepare),
            patch("core.runner_utils.services.read_json", read_endpoint),
        ):
            if options.operation == "rollback":
                await runner.rollback(options.snapshot)
            elif options.operation == "snapshot":
                await runner.snapshot("owner snapshot")
            elif options.operation == "reload":
                await runner.reload_template(options.root / "reload.yaml")
            elif options.operation == "stage":
                step = asyncio.create_task(runner.step())
                while (
                    runner._state.active_attempt is None
                    or runner._state.active_attempt.process_identity is None
                ):
                    await asyncio.sleep(0.05)
                halt("stage_running")
                halt("stage_without_state")
                if options.phase == "service_work":
                    request_id = runner._state.active_attempt.request_id
                    while not any(
                        row["event"]["event_type"] == "call.started"
                        and row["event"]["context"].get("request_id") == request_id
                        for row in runner._journal.client.read_events(limit=1000)[
                            "events"
                        ]
                    ):
                        await asyncio.sleep(0.05)
                    halt("service_work")
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
        "--operation",
        choices=("run", "rollback", "snapshot", "stage", "reload", "recover"),
        required=True,
    )
    parser.add_argument("--snapshot")
    parser.add_argument("--phase", required=True)
    parser.add_argument("--action", choices=("crash", "hang"), default="crash")
    asyncio.run(run_owner(parser.parse_args()))


if __name__ == "__main__":
    main()
