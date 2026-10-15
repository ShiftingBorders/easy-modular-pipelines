"""Fixtures and the small internal integration contract for the single writer.

The new supervisor is not implemented yet. Missing components are reported as
an explicit assertion, not a collection/import error or a skipped test.

Expected internal contract (the public OperationLogger API stays unchanged):
JournalWriterSettings(queue_capacity, heartbeat_interval_seconds,
stall_timeout_seconds, restart_delays_seconds); JournalSupervisor(config_path,
runtime_directory=..., settings=...) with start/close/get_status,
client_config_path and endpoint_path. RunnerJournal owns _supervisor.
Status contains state, process (existing OS identity), writer_instance_id,
queue_depth and queue_capacity. Endpoint uses the existing endpoint host/port
document shape. Test-specific timings are watchdog budgets, not a product SLA.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from core.journal.logger import OperationLogger
from core.journal.settings import _load_logging_settings
from core.primitives.json_files import read_json, write_json
from tests.helpers.dag import process_running, wait_until
from tests.helpers.journal_writer_faults import WriterFaults
from tests.helpers.logging_fixtures import write_context_settings
from tests.helpers.logging_process import SCRATCH_ROOT, cleanup_directory
from tests.helpers.snapshots import SnapshotWorkspace


def require_writer(test: unittest.TestCase) -> tuple[type, type]:
    for name in ("core.journal.supervisor", "core.models.journal_writer"):
        test.assertIsNotNone(
            importlib.util.find_spec(name),
            "Single-writer implementation is absent: "
            + name
            + "; the approved recovery scenario cannot run against direct SQLite clients.",
        )
    supervisor = importlib.import_module("core.journal.supervisor").JournalSupervisor
    settings = importlib.import_module(
        "core.models.journal_writer"
    ).JournalWriterSettings
    return supervisor, settings


def database_snapshot(path: Path) -> dict[str, list[tuple]]:
    """Read original tables in one independent read transaction, then close."""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("BEGIN")
        result = {
            "events": db.execute("SELECT * FROM events ORDER BY cursor").fetchall(),
            "changes": db.execute(
                "SELECT * FROM journal_changes ORDER BY change_cursor"
            ).fetchall(),
            "commands": db.execute(
                "SELECT * FROM command_results ORDER BY request_id"
            ).fetchall(),
        }
        db.rollback()
        return result


def stored_events(path: Path) -> list[dict]:
    return [json.loads(row[-1]) for row in database_snapshot(path)["events"]]


class WriterFixture:
    """Own real writers, loggers and optional existing experiment fixtures."""

    def __init__(self, supervisor_type: type, settings_type: type) -> None:
        self.settings_type = settings_type
        self.settings = self.policy()
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.root = Path(self.temporary.name).resolve()
        self.faults = WriterFaults(self.root / "faults")
        self.faults.open()
        self.loggers: list[OperationLogger] = []
        self.tasks: list[asyncio.Task] = []
        self.workspaces: list[SnapshotWorkspace] = []
        self.supervisors = []
        original_init = supervisor_type.__init__

        def initialize(owner, *args, **kwargs):
            kwargs["settings"] = self.settings
            original_init(owner, *args, **kwargs)
            self.supervisors.append(owner)

        self._settings_patch = patch.object(supervisor_type, "__init__", initialize)
        self._settings_patch.start()
        self.supervisor_type = supervisor_type
        self.supervisor = None
        self.database = self.root / "journal" / "events.db"

    def policy(self, *, stall_timeout: float = 5):
        return self.settings_type(
            queue_capacity=2,
            heartbeat_interval_seconds=0.1,
            stall_timeout_seconds=stall_timeout,
            restart_delays_seconds=[0.05, 0.1],
        )

    async def open(self) -> None:
        config = write_context_settings(self.database.parent)
        self.supervisor = self.supervisor_type(
            config, runtime_directory=self.root / "writer", settings=self.settings
        )
        await asyncio.wait_for(asyncio.to_thread(self.supervisor.start), 30)
        self.logger = await self.client(self.supervisor.client_config_path)

    async def client(self, config: Path, *, read_only: bool = False) -> OperationLogger:
        # Keep old generation configs outside a replaced experiment directory.
        copied = self.root / f"client-{len(self.loggers)}.json"
        settings, context = _load_logging_settings(config)
        document = {
            "logging": settings.model_dump(mode="json"),
            "operation_context": context.model_dump(),
        }
        write_json(copied, document)
        logger = OperationLogger(copied, read_only=read_only)
        self.loggers.append(logger)
        await asyncio.wait_for(asyncio.to_thread(logger.open), 30)
        return logger

    async def experiment(self, *, services: bool = False) -> SnapshotWorkspace:
        workspace = SnapshotWorkspace(services=services)
        self.workspaces.append(workspace)
        await workspace.launch()
        return workspace

    def record(self, call, *args, **kwargs) -> asyncio.Task:
        task = asyncio.create_task(asyncio.to_thread(call, *args, **kwargs))
        self.tasks.append(task)
        return task

    def track(self, coroutine) -> asyncio.Task:
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    async def close(self) -> None:
        # Release gates before awaiting pending work; cancelling to_thread does
        # not release its SQL/client thread. Only owned objects are cleaned up.
        self.faults.release_all()
        standalone = []
        for logger in self.loggers:
            store = logger._store
            if store is not None and not getattr(store, "_managed", True):
                standalone.append(read_json(store.endpoint_path)["process"])
            logger._interrupt_waiting()
        for workspace in self.workspaces:
            journal = workspace.runner._journal
            if getattr(journal, "_opened", False):
                journal.client._interrupt_waiting()
        failures = []
        try:
            if self.tasks:
                await asyncio.wait_for(
                    asyncio.gather(*self.tasks, return_exceptions=True), 30
                )
        except TimeoutError as error:
            failures.append(error)
        try:
            cleanups = [
                *(logger.close for logger in reversed(self.loggers)),
                *(workspace.close for workspace in reversed(self.workspaces)),
                *(supervisor.close for supervisor in reversed(self.supervisors)),
            ]
            for cleanup in cleanups:
                try:
                    if asyncio.iscoroutinefunction(cleanup):
                        await asyncio.wait_for(cleanup(), 90)
                    else:
                        await asyncio.wait_for(asyncio.to_thread(cleanup), 10)
                except Exception as error:  # noqa: BLE001 - Re-raise after every owned cleanup.
                    failures.append(error)
        finally:
            self._settings_patch.stop()
            try:
                await self.faults.close()
            except Exception as error:  # noqa: BLE001 - Retain cleanup failures together.
                failures.append(error)
        if failures:
            raise ExceptionGroup(
                f"Writer fixture cleanup failed; artifacts: {self.root}", failures
            )
        await wait_until(
            lambda: all(
                not process_running(identity["pid"]) for identity in standalone
            ),
            timeout=10,
        )
        cleanup_directory(self.temporary)
