"""Approved N6/N7: real writer read delays and independent bounded diagnostics."""

import asyncio
import importlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.journal.logger import OperationLogger
from tests.helpers.dag import wait_until
from tests.helpers.journal_writer import WriterFixture, require_writer
from tests.helpers.logging_process import SCRATCH_ROOT


class WriterStartupDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        supervisor, settings = require_writer(self)
        self.workspace = WriterFixture(supervisor, settings)
        self.addAsyncCleanup(self.workspace.close)
        await self.workspace.open()

    async def test_read_backlog_and_probe_timing_are_separate_from_write_health_n6(
        self,
    ):
        w = self.workspace
        gate = w.faults.hold_quick_check()
        opening = w.track(w.client(w.supervisor.client_config_path))
        try:
            await gate.entered()
            reading = w.record(w.logger.read_command_result, str(uuid4()))
            await wait_until(
                lambda: w.logger._store.get_status().get("read_queue_depth", 0) >= 1
            )
            await asyncio.sleep(0.2)
            status = w.logger._store.get_status()
            self.assertEqual(status["state"], "ready")
            self.assertFalse(status["busy"])
            self.assertEqual(status["queue_depth"], 0)
            self.assertIn("active_read", status, "Read health is currently invisible.")
            self.assertEqual(status["active_read"]["command"], "open")
            self.assertGreaterEqual(status["active_read"]["age_seconds"], 0.2)
            self.assertGreaterEqual(status["read_queue_depth"], 1)
            self.assertFalse(reading.done())
            self.assertFalse(opening.done())
            self.assertEqual(w.logger._recovery_seconds(), 0)
        finally:
            gate.release()
        await asyncio.wait_for(opening, 15)
        self.assertIsNone(await asyncio.wait_for(reading, 15))
        await wait_until(lambda: w.logger._store.get_status()["active_read"] is None)
        status = w.logger._store.get_status()
        self.assertEqual(status["read_queue_depth"], 0)
        path = w.supervisor.directory / "startup.jsonl"
        self.assertTrue(
            path.is_file(), "Writer phase timing must survive independently of SQLite."
        )
        records = [
            json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
        ]
        checks = [row for row in records if row["phase"] == "sqlite.quick_check.end"]
        self.assertTrue(
            any(row["details"]["duration_seconds"] >= 0.2 for row in checks)
        )
        reads = [
            row
            for row in records
            if row["phase"] == "writer.read.finished"
            and row["details"].get("command") == "read_command_result"
        ]
        self.assertTrue(
            any(row["details"]["queue_wait_seconds"] >= 0.2 for row in reads)
        )
        self.assertTrue(
            all(
                row["details"].get("writer_instance_id") == status["writer_instance_id"]
                for row in reads
            )
        )


class IndependentStartupDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

    def _diagnostics(self, path, context=None, **kwargs):
        name = "core.journal.startup_diagnostics"
        self.assertIsNotNone(
            importlib.util.find_spec(name),
            "Independent startup diagnostics are not implemented.",
        )
        module = importlib.import_module(name)
        return module.StartupDiagnostics(path, context or {}, **kwargs)

    def test_bounded_records_exclude_secret_context_without_journal_access_n7(self):
        destination = self.root / "startup.jsonl"
        attempt_id = str(uuid4())
        sink = self._diagnostics(
            destination,
            {
                "attempt_id": attempt_id,
                "token": "secret-ipc-token",
                "settings": {"password": "secret-settings"},
                "environment": {"KEY": "secret-environment"},
            },
            max_bytes=2048,
        )
        with patch.object(
            OperationLogger,
            "record_event",
            side_effect=AssertionError("Recursive logging"),
        ):
            for _ in range(100):
                sink.record("logger.open.begin", duration_seconds=0.1)
        self.assertLessEqual(destination.stat().st_size, 2048)
        content = destination.read_text(encoding="utf-8")
        records = [json.loads(line) for line in content.splitlines()]
        self.assertTrue(records)
        self.assertEqual(records[0]["context"]["attempt_id"], attempt_id)
        self.assertIn("diagnostics.truncated", [row["phase"] for row in records])
        for secret in ("secret-ipc-token", "secret-settings", "secret-environment"):
            self.assertNotIn(secret, content)
        self.assertFalse(list(self.root.glob("*.sqlite")))

    def test_unwritable_diagnostics_cannot_replace_primary_failure_n7(self):
        occupied = self.root / "occupied"
        occupied.write_text("keep", encoding="utf-8")
        sink = self._diagnostics(occupied / "startup.jsonl")
        primary = RuntimeError("primary startup failure")
        with self.assertRaises(RuntimeError) as caught:
            try:
                raise primary
            finally:
                sink.record("startup.failed")
        self.assertIs(caught.exception, primary)
        self.assertEqual(occupied.read_text(encoding="utf-8"), "keep")
