"""Approved local-file corruption, atomic publication and shutdown boundaries."""

import asyncio
import errno
import json
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from dashboard.icmp import ICMPMonitor
from tests.dashboard_tests.helpers import (
    ProbeProcess,
    icmp_settings,
    temporary_directory,
)


@unittest.skipUnless(
    sys.platform == "win32",
    "State/OS-lock checks are approved for Windows in this phase.",
)
class ICMPStateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        temporary = temporary_directory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.monitor = ICMPMonitor(self.directory)
        self.addAsyncCleanup(self.monitor.close)
        self.initial = {
            "settings": dict(self.monitor.settings),
            "history": [],
            "incidents": [],
        }
        self.monitor._write(self.initial)
        self.original = (self.directory / "icmp.json").read_bytes()

    async def test_configuration_is_saved_and_survives_reopen(self) -> None:
        expected = icmp_settings(enabled=False, timeout_seconds=7)
        await self.monitor.configure(expected)
        replacement = ICMPMonitor(self.directory)
        await replacement.open()
        try:
            self.assertEqual(replacement.settings, expected)
            self.assertFalse(replacement.snapshot()["fresh"])
        finally:
            await replacement.close()

    async def test_previous_observations_and_active_incident_survive_restart_but_are_stale(
        self,
    ) -> None:
        self.monitor.settings = icmp_settings()
        with patch(
            "dashboard.icmp.asyncio.create_subprocess_exec",
            new=AsyncMock(
                return_value=ProbeProcess(
                    {"status": "no_reply", "reason": "timeout", "rtt_ms": None}
                )
            ),
        ):
            await self.monitor.probe()
        block = ProbeProcess(blocked=True)
        replacement = ICMPMonitor(self.directory)
        with patch(
            "dashboard.icmp.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=block),
        ):
            await replacement.open()
            try:
                self.assertEqual(replacement.incidents[0]["status"], "active")
                self.assertFalse(replacement.snapshot()["fresh"])
                self.assertEqual(replacement.snapshot()["status"], "waiting")
            finally:
                await replacement.close()

    async def test_second_monitor_cannot_own_same_directory(self) -> None:
        await self.monitor.open()
        second = ICMPMonitor(self.directory)
        with self.assertRaises(OSError):
            await second.open()
        self.assertIsNone(second._file)
        await self.monitor.close()
        await second.open()
        await second.close()

    async def test_corrupt_state_is_rejected_without_rewriting_or_leaking_lock(
        self,
    ) -> None:
        observation = {
            "status": "reply",
            "host": "www.google.com",
            "probe_host": "machine",
            "observed_at": "2026-09-15T10:00:00+00:00",
            "session_id": "old",
            "rtt_ms": 1,
        }
        incident = {
            "id": "incident",
            "status": "resolved",
            "host": "www.google.com",
            "probe_host": "machine",
            "started_at": "2026-09-15T10:00:00+00:00",
            "ended_at": "2026-09-15T10:00:05+00:00",
        }
        bad_documents = [
            None,
            [],
            {},
            {**self.initial, "history": None},
            {**self.initial, "history": [None]},
            {**self.initial, "history": [{**observation, "status": "unknown"}]},
            {**self.initial, "history": [{**observation, "observed_at": "bad-date"}]},
            {
                **self.initial,
                "history": [{**observation, "observed_at": "2026-09-15T10:00:00"}],
            },
            {**self.initial, "history": [{**observation, "rtt_ms": -1}]},
            {**self.initial, "history": [{**observation, "rtt_ms": float("nan")}]},
            {
                **self.initial,
                "incidents": [{**incident, "started_at": "2026-09-15T10:00:00"}],
            },
            {**self.initial, "incidents": [{**incident, "ended_at": "bad-date"}]},
            {**self.initial, "incidents": [{**incident, "status": "active"}] * 2},
        ]
        contents = [b"", b'{"settings":', b"\xff"] + [
            json.dumps(value).encode() for value in bad_documents
        ]
        path = self.directory / "icmp.json"
        for content in contents:
            with self.subTest(content=content[:80]):
                path.write_bytes(content)
                broken = ICMPMonitor(self.directory)
                try:
                    with self.assertRaises((ValueError, TypeError, UnicodeError)):
                        await broken.open()
                    self.assertEqual(path.read_bytes(), content)
                finally:
                    await broken.close()
                path.write_bytes(self.original)
                replacement = ICMPMonitor(self.directory)
                await replacement.open()
                await replacement.close()

    async def test_oversized_state_file_is_rejected_without_overwriting(self) -> None:
        path = self.directory / "icmp.json"
        path.write_bytes(b" " * (4 * 1024 * 1024 + 1))
        original_size = path.stat().st_size
        with self.assertRaisesRegex(ValueError, "too large"):
            await self.monitor.open()
        self.assertEqual(path.stat().st_size, original_size)

    async def test_temp_creation_failure_preserves_published_state(self) -> None:
        original_open = Path.open

        def fail_temporary(path, *args, **kwargs):
            if path.name.startswith(".icmp-"):
                raise PermissionError("cannot create temporary file")
            return original_open(path, *args, **kwargs)

        with (
            patch("dashboard.icmp.Path.open", new=fail_temporary),
            self.assertRaises(PermissionError),
        ):
            await self.monitor.configure(icmp_settings(enabled=False))
        self.assertEqual((self.directory / "icmp.json").read_bytes(), self.original)
        self.assertEqual(self.monitor.settings, self.initial["settings"])

    async def test_partial_write_failure_preserves_old_file_and_removes_temporary(
        self,
    ) -> None:
        def fail_write(document, stream, **kwargs):
            stream.write('{"settings":')
            raise OSError(errno.ENOSPC, "disk full")

        with (
            patch("dashboard.icmp.json.dump", new=fail_write),
            self.assertRaises(OSError),
        ):
            await self.monitor.configure(icmp_settings(enabled=False))
        self.assertEqual((self.directory / "icmp.json").read_bytes(), self.original)
        self.assertEqual(list(self.directory.glob(".icmp-*.json")), [])
        self.assertEqual(self.monitor.settings, self.initial["settings"])

    async def test_flush_failure_preserves_old_configuration(self) -> None:
        with (
            patch("dashboard.icmp.os.fsync", side_effect=OSError("flush failed")),
            self.assertRaises(OSError),
        ):
            await self.monitor.configure(icmp_settings(enabled=False))
        self.assertEqual((self.directory / "icmp.json").read_bytes(), self.original)
        self.assertEqual(list(self.directory.glob(".icmp-*.json")), [])

    async def test_replace_failure_preserves_old_configuration(self) -> None:
        with (
            patch(
                "dashboard.icmp.Path.replace",
                side_effect=PermissionError("replacement denied"),
            ),
            self.assertRaises(PermissionError),
        ):
            await self.monitor.configure(icmp_settings(enabled=False))
        self.assertEqual((self.directory / "icmp.json").read_bytes(), self.original)
        self.assertEqual(self.monitor.settings, self.initial["settings"])
        self.assertEqual(list(self.directory.glob(".icmp-*.json")), [])

    async def test_shutdown_waits_for_pending_state_write_before_releasing_lock(
        self,
    ) -> None:
        started, release, finished = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
        original_write = self.monitor._write

        def blocked_write(document):
            started.set()
            try:
                if not release.wait(3):
                    raise TimeoutError("test did not release the pending write")
                original_write(document)
            finally:
                finished.set()

        await self.monitor.open()
        self.monitor.settings = icmp_settings()
        with (
            patch.object(self.monitor, "_write", side_effect=blocked_write),
            patch(
                "dashboard.icmp.asyncio.create_subprocess_exec",
                new=AsyncMock(return_value=ProbeProcess()),
            ),
        ):
            pending = asyncio.create_task(self.monitor.probe())
            closing = None
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 2))
                closing = asyncio.create_task(self.monitor.close())
                done, _ = await asyncio.wait({closing}, timeout=0.1)
                self.assertFalse(
                    done,
                    "Shutdown released ownership while a state write was still running",
                )
                second = ICMPMonitor(self.directory)
                with self.assertRaises(OSError):
                    await second.open()
            finally:
                release.set()
                self.assertTrue(await asyncio.to_thread(finished.wait, 2))
                if closing:
                    await closing
                await asyncio.gather(pending, return_exceptions=True)
