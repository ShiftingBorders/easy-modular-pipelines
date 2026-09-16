"""Approved archive plan H: asynchronous registration with real SQLite and HTTP."""

import asyncio
import sqlite3
import threading
import time
from itertools import pairwise
from unittest.mock import patch

from core.storage_errors import StorageConflict, StorageUnavailable
from tests.helpers.archives import ArchiveTestCase
from tests.helpers.dag import wait_until


class AsyncRegistrationTests(ArchiveTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.module = self.w.module()
        self.folder = self.w.source / "modules/portable-stage/1"
        self.manager = self.w.target_manager

    async def register(self):
        return await self.manager.register_module_async(
            "portable-stage", "1", self.folder
        )

    async def test_registration_repeat_and_conflict_match_synchronous_api(self):
        """H/E: identical registration is False, changed content conflicts without overwrite."""
        self.assertIs(await self.register(), True)
        stored = self.w.filer.root / "modules/portable-stage/1"
        before = stored.read_bytes()
        self.assertIs(await self.register(), False)
        self.assertIs(
            self.manager.register_module("portable-stage", "1", self.folder), False
        )
        (self.folder / "new.txt").write_bytes(b"changed")
        with self.assertRaises(StorageConflict):
            await self.register()
        self.assertEqual(stored.read_bytes(), before)
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "1"),
            self.module["hash"],
        )
        self.assert_work_clean()

    async def test_hash_connection_stays_on_owner_thread_and_network_runs_in_workers(
        self,
    ):
        """H: real thread-bound SQLite is never passed into a file/network worker."""
        owner = threading.get_ident()
        hash_threads, upload_threads = [], []
        get_hash = self.w.target_hashes.get_module_hash
        add_hash = self.w.target_hashes.add_module_hash
        upload = self.w.target_storage.save_module

        def get(*args):
            hash_threads.append(threading.get_ident())
            return get_hash(*args)

        def add(*args):
            hash_threads.append(threading.get_ident())
            return add_hash(*args)

        def save(*args):
            upload_threads.append(threading.get_ident())
            return upload(*args)

        with (
            patch.object(self.w.target_hashes, "get_module_hash", side_effect=get),
            patch.object(self.w.target_hashes, "add_module_hash", side_effect=add),
            patch.object(self.w.target_storage, "save_module", side_effect=save),
        ):
            self.assertTrue(await self.register())
        self.assertTrue(hash_threads)
        self.assertEqual(set(hash_threads), {owner})
        self.assertTrue(upload_threads)
        self.assertNotIn(owner, upload_threads)

    async def test_cancel_during_real_http_upload_waits_for_actual_result(self):
        """H: loop polling continues while the server holds an upload and cancellation is deferred."""
        self.w.filer.release.clear()
        operation = asyncio.create_task(self.register())
        try:
            await wait_until(self.w.filer.entered.is_set)
            operation.cancel()
            ticks = 0
            async with asyncio.timeout(1):
                for _ in range(5):
                    await asyncio.sleep(0.01)
                    ticks += 1
            self.assertEqual(ticks, 5)
            self.assertFalse(operation.done())
        finally:
            self.w.filer.release.set()
        self.assertIs(await asyncio.wait_for(operation, 15), True)
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "1"),
            self.module["hash"],
        )
        self.assert_work_clean()

    async def test_package_failure_does_not_insert_hash_or_upload(self):
        """H/F: a file worker failure precedes persistent registration changes."""
        primary = OSError("package write failed")
        with (
            patch.object(self.manager, "_registration_package", side_effect=primary),
            self.assertRaises(OSError) as raised,
        ):
            await self.register()
        self.assertIs(raised.exception, primary)
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "1"), ""
        )
        self.assertFalse(any(method == "POST" for method, *_ in self.w.filer.requests))
        self.assert_work_clean()

    async def test_failed_hash_compensation_keeps_original_upload_error_and_note(self):
        """H: failure to undo an inserted hash is reported without hiding the HTTP failure."""
        self.w.filer.fail_post = 503
        with (
            patch.object(
                self.w.target_hashes,
                "remove_module_hash",
                side_effect=StorageUnavailable("hash compensation failed"),
            ),
            self.assertRaises(StorageUnavailable) as raised,
        ):
            await self.register()
        self.assertIn("hash compensation failed", " ".join(raised.exception.__notes__))
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "1"),
            self.module["hash"],
        )
        self.assertFalse((self.w.filer.root / "modules/portable-stage/1").exists())

    async def test_invalid_arguments_fail_before_mutating_real_stores(self):
        """H/A: async registration uses the synchronous identity/path validation contract."""
        for name, version, path in (
            ("../bad", "1", self.folder),
            ("ok", "../bad", self.folder),
            ("ok", "1", "relative"),
            (None, "1", self.folder),
            ("ok", True, self.folder),
        ):
            with (
                self.subTest(name=name, version=version, path=path),
                self.assertRaises((TypeError, ValueError)),
            ):
                await self.manager.register_module_async(name, version, path)
        self.assertEqual(self.w.filer.requests, [])
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "1"), ""
        )

    async def test_real_sqlite_lock_is_bounded_and_preserves_registration_state(self):
        """H: measure the documented SQLite wait on an isolated DB, using a short local busy timeout."""
        locked, release = threading.Event(), threading.Event()

        def hold_database():
            connection = sqlite3.connect(self.w.target / "hashes.sqlite")
            try:
                connection.execute("BEGIN EXCLUSIVE")
                locked.set()
                if not release.wait(10):
                    raise TimeoutError("database test gate expired")
                connection.rollback()
            finally:
                connection.close()

        worker = threading.Thread(target=hold_database)
        connection = self.w.target_hashes.hash_db
        previous = connection.execute("PRAGMA busy_timeout").fetchone()[0]
        connection.execute("PRAGMA busy_timeout = 150")
        worker.start()
        ticks = []

        async def ticker():
            while True:
                ticks.append(time.monotonic())
                await asyncio.sleep(0.01)

        polling = asyncio.create_task(ticker())
        try:
            await wait_until(locked.is_set)
            started = time.monotonic()
            with self.assertRaises(StorageUnavailable):
                await self.register()
            elapsed = time.monotonic() - started
            await asyncio.sleep(0.02)
            self.assertLess(elapsed, 3)
            gap = max((b - a for a, b in pairwise(ticks)), default=0)
            print(
                f"Isolated SQLite lock: operation={elapsed:.3f}s, largest poll gap={gap:.3f}s"
            )
        finally:
            release.set()
            await asyncio.to_thread(worker.join, 10)
            polling.cancel()
            await asyncio.gather(polling, return_exceptions=True)
            connection.execute(f"PRAGMA busy_timeout = {previous}")
        self.assertFalse(worker.is_alive())
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "1"), ""
        )
        self.assertFalse(any(method == "POST" for method, *_ in self.w.filer.requests))
