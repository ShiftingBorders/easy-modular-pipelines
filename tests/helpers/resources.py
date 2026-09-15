"""Owned processes, files, and logger clients for the approved collector tests."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
import time
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.logger import OperationLogger
from core.logger_utils.events import LoggingStorageError
from core.resource_utils.sampling import ResourceSampler, collect_resources
from core.resource_utils.state import CollectorSettings
from core.runner_utils.runtimeio import process_identity
from tests.helpers.dag import REPOSITORY

DEFAULT_CONFIG = REPOSITORY / "default_settings/resource_collector.json"
TEMP_ROOT = REPOSITORY / ".artifacts/tmp/resource-tests"


def write_settings(root: Path, **overrides) -> Path:
    settings = json.loads(DEFAULT_CONFIG.read_text(encoding="utf-8"))
    settings.update(
        sample_interval_seconds=0.1,
        status_interval_seconds=0.1,
        startup_timeout_seconds=3,
        heartbeat_timeout_seconds=1,
        shutdown_timeout_seconds=0.5,
        restart_delays_seconds=[0.1, 0.2, 0.4, 0.8],
        stable_reset_seconds=1,
        logging_retry_seconds=0.2,
    )
    settings.update(overrides)
    path = root / f"collector-{uuid4()}.json"
    path.write_text(json.dumps(settings), encoding="utf-8")
    return path


def journal(root: Path) -> tuple[Path, OperationLogger, dict]:
    context = {"experiment_id": str(uuid4()), "run_id": str(uuid4()), "cycle_number": 1}
    settings = {
        "db_path": "events.sqlite",
        "busy_timeout_seconds": 0.05,
        "max_event_bytes": None,
        "open_mode": "create",
        "min_free_bytes": 0,
        "expected_journal": None,
        "filtered_refresh_interval_seconds": 1,
    }
    path = root / "journal.json"
    path.write_text(
        json.dumps({"logging": settings, "operation_context": context}),
        encoding="utf-8",
    )
    client = OperationLogger(path)
    client.open()
    info = client.get_journal_info()
    settings["open_mode"] = "existing"
    settings["expected_journal"] = {
        key: info[key] for key in ("journal_id", "generation")
    }
    path.write_text(
        json.dumps({"logging": settings, "operation_context": context}),
        encoding="utf-8",
    )
    return path, client, context


def events(client: OperationLogger, kind: str) -> list[dict]:
    return [
        item["event"]
        for item in client.read_events(limit=1000)["events"]
        if item["event"]["event_type"] == kind
    ]


def target(identity: dict, context: dict | None = None) -> dict:
    attempt_id = str(uuid4())
    return {
        "series_id": attempt_id,
        "identity": identity,
        "context": {**(context or {}), "attempt_id": attempt_id},
    }


def controlled_collector(connection, settings, *, controls: str) -> None:
    """Inject one local boundary inside the real spawned collector, without production hooks."""
    root = Path(controls)
    sample = ResourceSampler.sample
    record = OperationLogger.record_resources
    opened = OperationLogger.open

    def sample_with_gate(self, *args):
        if (root / "hang").exists():
            (root / "hanging").touch()
            while not (root / "release").exists():
                time.sleep(0.01)
        return sample(self, *args)

    def fail_record(self, *args, **kwargs):
        if (root / "fail-write").exists():
            (root / "write-failed").touch()
            raise LoggingStorageError("collector-only write failure")
        return record(self, *args, **kwargs)

    def fail_open(self):
        if (root / "fail-open").exists():
            (root / "open-failed").touch()
            raise LoggingStorageError("collector-only open failure")
        return opened(self)

    with (
        patch.object(ResourceSampler, "sample", sample_with_gate),
        patch.object(OperationLogger, "record_resources", fail_record),
        patch.object(OperationLogger, "open", fail_open),
    ):
        collect_resources(connection, settings)


def memory_and_cpu_process(connection) -> None:
    connection.send(process_identity(os.getpid()))
    memory = None
    busy = False
    try:
        while True:
            if connection.poll(0 if busy else 0.05):
                command = connection.recv()
                if command == "stop":
                    return
                if command == "allocate":
                    memory = bytearray(64 * 1024 * 1024)
                    for offset in range(0, len(memory), 4096):
                        memory[offset] = 1
                    busy = True
                    connection.send("allocated")
            if busy:
                sum(value * value for value in range(10000))
    finally:
        connection.close()


def run_owner(connection, config_path: str) -> None:
    from core.resourcecollector import ResourceCollector

    async def run():
        collector = ResourceCollector(Path(config_path))
        task = asyncio.create_task(collector.serve())
        try:
            while not collector.get_status()["collector_id"]:
                await asyncio.sleep(0.01)
            connection.send(process_identity(collector.get_status()["pid"]))
            await task
        finally:
            await collector.close()

    asyncio.run(run())


def stop_process(process, connection=None) -> None:
    """Fallback cleanup uses only the exact multiprocessing handle owned by the test."""
    if process.pid is not None:
        if process.is_alive():
            process.terminate()
        process.join(5)
        if process.is_alive():
            process.kill()
            process.join(5)
        if process.is_alive():
            raise RuntimeError("An owned test process did not stop.")
    process.close()
    if connection is not None:
        connection.close()


def spawned(target_function, *args):
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=target_function, args=(child, *args))
    process.start()
    child.close()
    return process, parent


def settings(root: Path, **overrides) -> CollectorSettings:
    return CollectorSettings.load(write_settings(root, **overrides))
