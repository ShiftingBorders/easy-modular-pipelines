"""Native Windows probe semantics and portable real child-process cleanup."""

import asyncio
import contextlib
import ctypes
import io
import json
import os
import socket
import struct
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from dashboard import icmp_worker
from dashboard.icmp import ICMPMonitor
from tests.dashboard_tests.helpers import (
    FIXTURES,
    cleanup_directory,
    icmp_settings,
    probe_result,
    temporary_directory,
)


@unittest.skipUnless(sys.platform == "win32", "Requires native Windows ICMP APIs.")
class WindowsProbeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.library = Mock()
        self.library.IcmpCreateFile.return_value = 123

    def test_only_success_from_target_is_an_echo_reply(self) -> None:
        destination = int.from_bytes(socket.inet_aton("127.0.0.1"), "little")
        for source, status, expected in [
            (destination, 0, "reply"),
            (destination, 11003, "no_reply"),
            (destination + 1, 0, "no_reply"),
        ]:

            def send(
                handle,
                address,
                payload,
                size,
                options,
                buffer,
                buffer_size,
                timeout,
                *,
                source=source,
                status=status,
            ):
                self.assertEqual(handle, 123)
                self.assertEqual(address, destination)
                self.assertEqual(timeout, 1250)
                struct.pack_into("<III", buffer, 0, source, status, 42)
                return 1

            self.library.IcmpSendEcho.side_effect = send
            with (
                self.subTest(status=status, source=source),
                patch("dashboard.icmp_worker.ctypes.WinDLL", return_value=self.library),
            ):
                result = icmp_worker.windows_probe("127.0.0.1", 1.25)
                self.assertEqual(result["status"], expected)
                self.assertEqual(result["rtt_ms"], 42 if expected == "reply" else None)
                self.library.IcmpCloseHandle.assert_called_with(123)

    def test_timeout_and_unreachable_statuses_are_not_success(self) -> None:
        self.library.IcmpSendEcho.return_value = 0
        for status in [11002, 11003, 11004, 11005, 11009, 11010, 11013, 11014]:
            with (
                self.subTest(status=status),
                patch("dashboard.icmp_worker.ctypes.WinDLL", return_value=self.library),
                patch(
                    "dashboard.icmp_worker.ctypes.get_last_error", return_value=status
                ),
            ):
                self.assertEqual(
                    icmp_worker.windows_probe("127.0.0.1", 1)["status"], "no_reply"
                )

    def test_os_error_releases_handle(self) -> None:
        self.library.IcmpSendEcho.side_effect = OSError("OS failure")
        with (
            patch("dashboard.icmp_worker.ctypes.WinDLL", return_value=self.library),
            self.assertRaises(OSError),
        ):
            icmp_worker.windows_probe("127.0.0.1", 1)
        self.library.IcmpCloseHandle.assert_called_once_with(123)

    def test_invalid_handle_raises_before_send(self) -> None:
        self.library.IcmpCreateFile.return_value = ctypes.c_void_p(-1).value
        with (
            patch("dashboard.icmp_worker.ctypes.WinDLL", return_value=self.library),
            patch("dashboard.icmp_worker.ctypes.get_last_error", return_value=5),
            self.assertRaises(OSError),
        ):
            icmp_worker.windows_probe("127.0.0.1", 1)
        self.library.IcmpSendEcho.assert_not_called()

    def test_worker_resolves_ipv4_on_this_host_and_uses_windows_provider(self) -> None:
        output = io.StringIO()
        with (
            patch.object(sys, "argv", ["icmp_worker", "www.google.com", "1"]),
            patch(
                "dashboard.icmp_worker.socket.getaddrinfo",
                return_value=[(None, None, None, None, ("127.0.0.1", 0))],
            ) as dns,
            patch(
                "dashboard.icmp_worker.windows_probe", return_value=probe_result()
            ) as windows,
            patch("dashboard.icmp_worker.linux_probe") as linux,
            contextlib.redirect_stdout(output),
        ):
            icmp_worker.main()
        dns.assert_called_once_with(
            "www.google.com", None, socket.AF_INET, socket.SOCK_DGRAM
        )
        windows.assert_called_once_with("127.0.0.1", 1.0)
        linux.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())["status"], "reply")

    def test_dns_failure_returns_measurement_error(self) -> None:
        output = io.StringIO()
        with (
            patch.object(sys, "argv", ["icmp_worker", "www.google.com", "1"]),
            patch(
                "dashboard.icmp_worker.socket.getaddrinfo",
                side_effect=socket.gaierror("DNS unavailable"),
            ),
            patch("dashboard.icmp_worker.windows_probe") as windows,
            contextlib.redirect_stdout(output),
        ):
            icmp_worker.main()
        result = json.loads(output.getvalue())
        self.assertEqual(result["status"], "error")
        self.assertIsNone(result["rtt_ms"])
        windows.assert_not_called()

    @unittest.skipUnless(
        os.environ.get("EMP_DASHBOARD_LIVE_ICMP") == "1",
        "Opt-in network check: set EMP_DASHBOARD_LIVE_ICMP=1 for www.google.com.",
    )
    def test_real_google_echo_reply(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                str(Path(icmp_worker.__file__)),
                "www.google.com",
                "5",
            ],
            capture_output=True,
            timeout=15,
            check=False,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))
        observation = json.loads(result.stdout)
        self.assertEqual(observation["status"], "reply", observation)
        self.assertIsInstance(observation["rtt_ms"], (int, float))
        self.assertGreaterEqual(observation["rtt_ms"], 0)


class ProbeProcessCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        self.directory = Path(temporary.name)
        self.monitor = ICMPMonitor(self.directory)
        self.monitor.settings = icmp_settings()
        self.addAsyncCleanup(self.monitor.close)
        self.processes = []
        self.mode = "hang"
        self.original_spawn = asyncio.create_subprocess_exec

    async def spawn(self, *args, **kwargs):
        process = await self.original_spawn(
            sys.executable,
            "-B",
            str(FIXTURES / "probe_process.py"),
            self.mode,
            str(self.directory / "child.pid"),
            **kwargs,
        )
        self.processes.append(process)
        return process

    async def wait_for_child(self) -> None:
        async with asyncio.timeout(3):
            while not (self.directory / "child.pid").exists():
                await asyncio.sleep(0.01)

    async def test_deadline_reaps_hung_worker_and_next_probe_succeeds(self) -> None:
        with patch(
            "dashboard.icmp.asyncio.create_subprocess_exec", side_effect=self.spawn
        ):
            result = await asyncio.wait_for(self.monitor.probe(), 7)
            self.assertEqual(result["status"], "error")
            self.assertIsNone(result["latest"]["rtt_ms"])
            self.assertIsNotNone(self.processes[0].returncode)
            self.mode = "reply"
            self.assertEqual((await self.monitor.probe())["status"], "reply")

    async def test_abrupt_worker_exit_is_observed_without_crashing_owner(self) -> None:
        self.mode = "crash"
        with patch(
            "dashboard.icmp.asyncio.create_subprocess_exec", side_effect=self.spawn
        ):
            result = await self.monitor.probe()
        self.assertEqual(result["status"], "error")
        self.assertEqual(self.processes[0].returncode, 7)

    async def test_shutdown_terminates_real_owned_process(self) -> None:
        with patch(
            "dashboard.icmp.asyncio.create_subprocess_exec", side_effect=self.spawn
        ):
            await self.monitor.open()
            await self.wait_for_child()
            await asyncio.wait_for(self.monitor.close(), 3)
        self.assertEqual(len(self.processes), 1)
        self.assertIsNotNone(self.processes[0].returncode)
        self.assertTrue(self.monitor._task.done())
