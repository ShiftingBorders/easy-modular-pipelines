"""Approved archive plan E/F/J: publication, real storage failures and audit logs."""

import asyncio
import ctypes
import os
import shutil
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import LoggingStorageError
from core.storage_errors import StorageConflict, StorageError, StorageUnavailable
from tests.helpers.archives import ArchiveTestCase, inventory, journal_events
from tests.helpers.dag import wait_until


class ArchiveFailureTests(ArchiveTestCase):
    async def test_copy_compression_and_publication_failures_leave_no_ready_archive(
        self,
    ):
        """F: safe injected filesystem failures preserve the source and never publish success."""
        await self.w.prepare()
        before = inventory(self.w.state.experiment_directory)
        for target, method in (
            (self.w.archiver, "_copy"),
            (self.w.archiver, "_pack"),
            (os, "link"),
        ):
            error = OSError(f"injected {method} failure")
            with (
                self.subTest(method=method),
                patch.object(target, method, side_effect=error),
                self.assertRaises(OSError) as raised,
            ):
                await self.w.create()
            self.assertIs(raised.exception, error)
            self.assertFalse(self.w.archive.exists())
            self.assertEqual(inventory(self.w.state.experiment_directory), before)
            self.assert_work_clean()

    async def test_unpack_failure_never_publishes_destination_or_registers_code(self):
        """F: a decompression I/O failure cleans owned staging and preserves the input."""
        await self.source_archive()
        before = self.w.archive.read_bytes()
        with (
            patch.object(
                self.w.importer, "_decompress", side_effect=OSError("read failed")
            ),
            self.assertRaises(OSError),
        ):
            await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertFalse(self.w.destination.exists())
        self.assertEqual(self.w.filer.requests, [])
        self.assertEqual(self.w.archive.read_bytes(), before)
        self.assert_work_clean()

    async def test_http_upload_failure_compensates_hash_and_allows_clean_repeat(self):
        """E/H: a real HTTP failure reaches ModuleManager's normal hash compensation."""
        await self.source_archive()
        self.w.filer.fail_post = 503
        with self.assertRaises(StorageError):
            await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "1"), ""
        )
        self.assertFalse((self.w.filer.root / "modules/portable-stage/1").exists())
        self.assertFalse(self.w.destination.exists())
        self.w.filer.fail_post = 0
        result = await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertTrue(result["modules"][0]["registered"])
        self.assert_work_clean()

    async def test_lost_http_reply_reports_uncertain_registration_without_overwrite(
        self,
    ):
        """E: actual socket loss after disk write leaves a visible incomplete storage pair."""
        await self.source_archive()
        self.w.filer.disconnect_after_write = True
        with self.assertRaises(StorageUnavailable) as raised:
            await self.w.importer.install(self.w.archive, self.w.destination)
        stored = self.w.filer.root / "modules/portable-stage/1"
        self.assertTrue(stored.is_file())
        contents = stored.read_bytes()
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "1"), ""
        )
        self.assertIn("may have committed", " ".join(raised.exception.__notes__))
        self.w.filer.disconnect_after_write = False
        with self.assertRaises(StorageConflict):
            await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertEqual(stored.read_bytes(), contents)
        self.assertFalse(self.w.destination.exists())

    async def test_primary_error_survives_failed_staging_cleanup(self):
        """F: the original copy error remains primary and the leftover path is reported."""
        await self.w.prepare()
        original = shutil.rmtree

        def fail_cleanup(path, *args, **kwargs):
            if Path(path).name.startswith("experiment-archive-"):
                raise OSError("cleanup failed")
            return original(path, *args, **kwargs)

        primary = OSError("primary copy failure")
        with (
            patch.object(self.w.archiver, "_copy", side_effect=primary),
            patch("shutil.rmtree", side_effect=fail_cleanup),
            self.assertRaises(OSError) as raised,
        ):
            await self.w.create()
        self.assertIs(raised.exception, primary)
        self.assertIn("cleanup failed", " ".join(primary.__notes__))
        self.assertFalse(self.w.archive.exists())
        leftovers = list(self.w.root.rglob("experiment-archive-*"))
        self.assertTrue(leftovers)
        for path in leftovers:
            self.assertTrue(path.resolve().is_relative_to(self.w.root))
            shutil.rmtree(path)

    async def test_cleanup_failure_after_publication_reports_completed_archive(self):
        """F: cleanup failure does not claim the already published archive is absent."""
        await self.w.prepare()
        original = shutil.rmtree

        def fail_cleanup(path, *args, **kwargs):
            if Path(path).name.startswith("experiment-archive-"):
                raise OSError("cleanup failed")
            return original(path, *args, **kwargs)

        with (
            patch("shutil.rmtree", side_effect=fail_cleanup),
            self.assertRaises(OSError) as raised,
        ):
            await self.w.create()
        self.assertTrue(self.w.archive.exists())
        self.assertIn("completed before cleanup", " ".join(raised.exception.__notes__))
        await self.w.importer.inspect(self.w.archive)

    @unittest.skipUnless(os.name == "nt", "Windows exclusive file handles")
    async def test_real_exclusive_windows_handle_prevents_copy_without_changing_source(
        self,
    ):
        """F: an owned read handle denies sharing; no ACL or machine settings are changed."""
        from ctypes import wintypes

        await self.w.prepare()
        source = self.w.state.experiment_directory / "shared_data/resources/source.txt"
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.CreateFileW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            ctypes.c_void_p,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        kernel.CreateFileW.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel.CreateFileW(str(source), 0x80000000, 0, None, 3, 0x80, None)
        self.assertNotEqual(handle, ctypes.c_void_p(-1).value)
        try:
            with self.assertRaises(PermissionError):
                await self.w.create()
            self.assertFalse(self.w.archive.exists())
        finally:
            kernel.CloseHandle(handle)
        self.assertEqual(source.read_bytes(), b"portable input\n")
        self.assert_work_clean()

    async def test_cancellation_waits_for_file_worker_and_rejects_concurrent_operation(
        self,
    ):
        """F/H: cancellation cannot leave a copying/compressing thread using cleaned paths."""
        await self.w.prepare()
        entered, release = threading.Event(), threading.Event()
        original = self.w.archiver._pack

        def held_pack(*args):
            entered.set()
            if not release.wait(10):
                raise TimeoutError("test gate expired")
            return original(*args)

        with patch.object(self.w.archiver, "_pack", side_effect=held_pack):
            operation = asyncio.create_task(self.w.create())
            try:
                await wait_until(entered.is_set)
                with self.assertRaises(RuntimeError):
                    await self.w.archiver.inspect(self.w.archive)
                operation.cancel()
                await asyncio.sleep(0.05)
                self.assertFalse(operation.done())
                self.assertFalse(self.w.archive.exists())
            finally:
                release.set()
                result = await asyncio.wait_for(operation, 15)
        self.assertEqual(result["archive_path"], str(self.w.archive))
        await self.w.importer.inspect(self.w.archive)
        self.assert_work_clean()


class ArchiveJournalTests(ArchiveTestCase):
    async def test_logger_close_failure_reports_completed_publication(self):
        """J: failure while releasing the logger is distinct from an archive creation failure."""
        await self.w.prepare()
        close = OperationLogger.close

        def fail_close(logger):
            close(logger)
            raise LoggingStorageError("logger release failed")

        with (
            patch.object(OperationLogger, "close", new=fail_close),
            self.assertRaises(LoggingStorageError) as raised,
        ):
            await self.w.create()
        self.assertIn(
            "completed before logger close", " ".join(raised.exception.__notes__)
        )
        config = next((self.w.source / "controller/archives").glob("*/reader.json"))
        self.assertIn(str(config), " ".join(raised.exception.__notes__))
        self.assertTrue(self.w.archive.is_file())
        await self.w.importer.inspect(self.w.archive)

    async def test_each_operation_has_real_sqlite_audit_without_changing_source_journal(
        self,
    ):
        """J: create/inspect/install use separate persisted journals and expose readable paths."""
        await self.w.prepare()
        source_journal = inventory(self.w.state.experiment_directory / "journals")
        results = [
            await self.w.create(),
            await self.w.importer.inspect(self.w.archive),
            await self.w.importer.install(self.w.archive, self.w.destination),
        ]
        self.assertEqual(len({r["operation_id"] for r in results}), 3)
        for action, result in zip(
            ("create", "inspect", "install"), results, strict=True
        ):
            events = journal_events(result["logging_config_path"])
            with OperationLogger(Path(result["logging_config_path"])) as reader:
                self.assertEqual(len(reader.read_events(limit=100)["events"]), 2)
            self.assertEqual(
                [e["event_type"] for e in events],
                ["archive.started", "archive.completed"],
            )
            self.assertEqual(events[0]["data"]["action"], action)
            self.assertEqual(events[1]["data"]["operation_id"], result["operation_id"])
        self.assertEqual(
            inventory(self.w.state.experiment_directory / "journals"), source_journal
        )

    async def test_failed_operation_records_traceback_and_logging_path(self):
        """J: a failed operation can be inspected after all writer clients have closed."""
        with self.assertRaises(FileNotFoundError) as raised:
            await self.w.importer.inspect(self.w.root / "missing")
        config = next((self.w.target / "controller/archives").glob("*/reader.json"))
        self.assertIn(str(config), " ".join(raised.exception.__notes__))
        events = journal_events(config)
        self.assertEqual(events[-1]["event_type"], "error.recorded")
        self.assertIn("FileNotFoundError", events[-1]["data"]["traceback"])
        self.assertNotIn("archive.completed", [e["event_type"] for e in events])

    async def test_start_logging_failure_prevents_archive_side_effects(self):
        """J: failure to record intent prevents file publication and store modifications."""
        await self.w.prepare()
        original = OperationLogger.record_event

        def fail_start(logger, event_type, data, **kwargs):
            if event_type == "archive.started":
                raise LoggingStorageError("start record failed")
            return original(logger, event_type, data, **kwargs)

        with (
            patch.object(OperationLogger, "record_event", new=fail_start),
            self.assertRaises(LoggingStorageError),
        ):
            await self.w.create()
        self.assertFalse(self.w.archive.exists())
        self.assertEqual(self.w.filer.requests, [])
        self.assert_work_clean()

    async def test_completion_logging_failure_reports_already_published_data(self):
        """J: an audit failure after publication does not hide the committed archive."""
        await self.w.prepare()
        original = OperationLogger.record_event

        def fail_end(logger, event_type, data, **kwargs):
            if event_type == "archive.completed":
                raise LoggingStorageError("completion record failed")
            return original(logger, event_type, data, **kwargs)

        with (
            patch.object(OperationLogger, "record_event", new=fail_end),
            self.assertRaises(LoggingStorageError) as raised,
        ):
            await self.w.create()
        self.assertIn(
            "completed before logging failed", " ".join(raised.exception.__notes__)
        )
        self.assertTrue(self.w.archive.exists())
        await self.w.importer.inspect(self.w.archive)

    async def test_secondary_logging_and_close_failures_preserve_primary_exception(
        self,
    ):
        """J: error-reporting and close failures never replace the original operation error."""
        await self.w.prepare()
        original_close = OperationLogger.close

        def fail_close(logger):
            original_close(logger)
            raise LoggingStorageError("close failed")

        primary = OSError("copy failed first")
        with (
            patch.object(self.w.archiver, "_copy", side_effect=primary),
            patch.object(
                OperationLogger,
                "record_error",
                side_effect=LoggingStorageError("error record failed"),
            ),
            patch.object(OperationLogger, "close", new=fail_close),
            self.assertRaises(OSError) as raised,
        ):
            await self.w.create()
        self.assertIs(raised.exception, primary)
        notes = " ".join(primary.__notes__)
        self.assertIn("error record failed", notes)
        self.assertIn("close failed", notes)
        self.assertFalse(self.w.archive.exists())
