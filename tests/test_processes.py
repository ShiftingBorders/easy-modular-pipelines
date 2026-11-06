"""Approved E04-E06/B01: process absence, verified waits and boot identity."""

import os
import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

import psutil

from core.primitives import processes
from tests.helpers.processes import WindowsProcessCalls


@unittest.skipUnless(os.name == "nt", "Native Windows process calls")
class WindowsProcessIdentityTests(unittest.TestCase):
    def setUp(self):
        self.calls = WindowsProcessCalls()
        self.calls.install(self)

    def test_boot_identity_survives_clock_change_but_distinguishes_reboot(self):
        first = processes.process_identity(101)
        self.calls.boot_time -= 3490
        second = processes.process_identity(101)
        self.assertEqual(first, second)
        self.assertEqual(first["created_at_os"], self.calls.created[101])
        self.calls.boot_guid = UUID("04172696-92bc-4f32-90ae-6f62c4aa5652")
        self.assertNotEqual(
            first["boot_id"], processes.process_identity(101)["boot_id"]
        )

    def test_access_denied_for_absent_pid_reports_disappearance(self):
        self.calls.denied.add(101)
        self.calls.listed.remove(101)
        with self.assertRaises(ProcessLookupError):
            processes.process_identity(101)
        self.assertFalse(processes.process_running(101))

    def test_access_denied_for_listed_pid_remains_an_error(self):
        self.calls.denied.add(101)
        for observe in (processes.process_identity, processes.process_running):
            with self.subTest(observe=observe.__name__):
                with self.assertRaises(PermissionError) as caught:
                    observe(101)
                self.assertEqual(caught.exception.winerror, 5)

    def test_failed_enumeration_does_not_confirm_absence(self):
        self.calls.denied.add(101)
        with (
            patch("psutil.pids", side_effect=OSError("enumeration unavailable")),
            self.assertRaises(PermissionError) as caught,
        ):
            processes.process_identity(101)
        self.assertEqual(caught.exception.winerror, 5)

    def test_wait_checks_identity_and_exit_through_one_handle(self):
        identity = processes.process_identity(101)
        self.calls.kernel.OpenProcess.reset_mock()
        self.calls.kernel.CloseHandle.reset_mock()
        processes._wait_process_exit(identity, 0.25)
        self.calls.kernel.OpenProcess.assert_called_once()
        self.calls.kernel.WaitForSingleObject.assert_called_once_with(1101, 250)
        self.calls.kernel.CloseHandle.assert_called_once_with(1101)

    def test_wait_timeout_closes_handle_without_confirming_exit(self):
        identity = processes.process_identity(101)
        self.calls.wait_result = 258
        self.calls.kernel.CloseHandle.reset_mock()
        with self.assertRaises(psutil.TimeoutExpired):
            processes._wait_process_exit(identity, 0)
        self.calls.kernel.CloseHandle.assert_called_once_with(1101)

    def test_reused_pid_is_not_waited_on(self):
        identity = processes.process_identity(101)
        self.calls.created[101] += 1
        processes._wait_process_exit(identity, 0.25)
        self.calls.kernel.WaitForSingleObject.assert_not_called()

    def test_previous_boot_does_not_open_a_now_inaccessible_pid(self):
        identity = processes.process_identity(101)
        self.calls.boot_guid = UUID("04172696-92bc-4f32-90ae-6f62c4aa5652")
        self.calls.denied.add(101)
        self.calls.kernel.OpenProcess.reset_mock()
        processes._wait_process_exit(identity, 0.25)
        self.calls.kernel.OpenProcess.assert_not_called()

    def test_wait_failure_and_access_denial_preserve_uncertainty(self):
        identity = processes.process_identity(101)
        self.calls.wait_result = 0xFFFFFFFF
        with self.assertRaises(PermissionError):
            processes._wait_process_exit(identity, 0.25)
        self.calls.denied.add(101)
        with self.assertRaises(PermissionError):
            processes._wait_process_exit(identity, 0.25)

    def test_unknown_boot_information_never_falls_back_to_wall_time(self):
        self.calls.native.NtQuerySystemInformation.return_value = -1
        self.calls.native.NtQuerySystemInformation.side_effect = None
        with self.assertRaises(OSError):
            processes.process_identity(101)

    def test_live_legacy_identity_is_not_mistaken_for_a_dead_process(self):
        actual = processes.process_identity(101)
        legacy = {**actual, "boot_id": str(self.calls.boot_time)}
        with self.assertRaisesRegex(OSError, "legacy Windows"):
            processes._same_process_identity(actual, legacy)
        with self.assertRaisesRegex(OSError, "legacy Windows"):
            processes._wait_process_exit(legacy, 0.25)
        self.calls.kernel.WaitForSingleObject.assert_not_called()
        replaced = {**actual, "created_at_os": actual["created_at_os"] + 1}
        self.assertFalse(processes._same_process_identity(replaced, legacy))

    def test_archiving_does_not_treat_live_legacy_owner_as_an_old_boot(self):
        from core.experiments.archiver import ExperimentArchiver

        actual = processes.process_identity(101)
        legacy = {**actual, "boot_id": str(self.calls.boot_time)}
        with (
            patch("core.experiments.archiver.process_identity", return_value=actual),
            patch("psutil.Process"),
            self.assertRaisesRegex(OSError, "legacy Windows"),
        ):
            ExperimentArchiver._assert_exited(None, legacy)

    def test_recovery_discovery_does_not_discard_live_legacy_owner(self):
        from core.models.process_identity import ProcessIdentity
        from core.server.runtime import _needs_recovery

        actual = processes.process_identity(101)
        legacy = {**actual, "boot_id": str(self.calls.boot_time)}
        state = SimpleNamespace(
            pending_rebuild=None,
            phase="failed",
            active_attempt=None,
            services={},
            owner_identity=ProcessIdentity.model_validate(legacy),
        )
        with (
            patch("core.primitives.processes.process_identity", return_value=actual),
            patch("psutil.Process"),
            self.assertRaisesRegex(OSError, "legacy Windows"),
        ):
            _needs_recovery(state, Path.cwd(), actual)


class ProcessWaitTests(unittest.TestCase):
    def test_real_process_blocks_until_exit_and_old_identity_stays_finished(self):
        arguments, environment = processes.module_process_arguments(
            ["python", "-B", "-c", "import sys; sys.stdin.buffer.read(1)"]
        )
        child = subprocess.Popen(
            arguments,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        try:
            identity = processes.process_identity(child.pid)
            with self.assertRaises(psutil.TimeoutExpired):
                processes._wait_process_exit(identity, 0)
            child.communicate(b"x", timeout=10)
            processes._wait_process_exit(identity, 1)
            processes._wait_process_exit(identity, 1)
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=10)

    def test_identity_comparison_preserves_exact_creation_host_and_boot(self):
        actual = processes.process_identity(os.getpid())
        self.assertTrue(processes._same_process_identity(actual, actual.copy()))
        for name, replacement in (
            ("pid", actual["pid"] + 1),
            ("created_at_os", actual["created_at_os"] + 1),
            ("host_id", "different-host"),
            ("boot_id", "windows:00000000-0000-0000-0000-000000000001"),
        ):
            with self.subTest(field=name):
                self.assertFalse(
                    processes._same_process_identity(
                        actual, {**actual, name: replacement}
                    )
                )
