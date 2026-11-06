"""Controlled Windows calls for process identity and executor lifecycle tests."""

import ctypes
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import UUID


class WindowsProcessCalls:
    """Model process handles independently of EMP's identity implementation."""

    def __init__(self):
        self.created = {101: 134378544080429711, 202: 134378544080429712}
        self.denied = set()
        self.listed = [101, 202]
        self.boot_time = 134376343994346455
        self.boot_guid = UUID("d9a3b031-a398-49ca-a2f0-75d1b7df5231")
        self.wait_result = 0
        self.error = 5
        self.kernel = Mock()
        self.kernel.OpenProcess = Mock(side_effect=self._open)
        self.kernel.GetProcessTimes = Mock(side_effect=self._times)
        self.kernel.WaitForSingleObject = Mock(
            side_effect=lambda *args: self.wait_result
        )
        self.kernel.CloseHandle = Mock(return_value=1)
        self.native = Mock()
        self.native.NtQuerySystemInformation = Mock(side_effect=self._query)

    def install(self, case):
        native_calls = SimpleNamespace(**vars(ctypes))
        native_calls.WinDLL = Mock(side_effect=self._library)
        native_calls.get_last_error = lambda: self.error
        for replacement in (
            patch("core.primitives.processes.ctypes", native_calls),
            patch("psutil.pids", side_effect=lambda: list(self.listed)),
        ):
            replacement.start()
            case.addCleanup(replacement.stop)

    def _library(self, name, **kwargs):
        return self.kernel if name == "kernel32" else self.native

    def _open(self, access, inherit, pid):
        return None if pid in self.denied else pid + 1000

    def _times(self, handle, created, exited, kernel, user):
        value = self.created[handle - 1000]
        created._obj.dwLowDateTime = value & 0xFFFFFFFF
        created._obj.dwHighDateTime = value >> 32
        return 1

    def _query(self, kind, buffer, size, length):
        if kind == 3:
            data = self.boot_time.to_bytes(8, "little") + bytes(40)
        elif kind == 90:
            data = self.boot_guid.bytes_le + bytes(16)
        else:
            raise AssertionError(f"Unexpected native information class: {kind}")
        ctypes.memmove(buffer, data, len(data))
        length._obj.value = len(data)
        return 0
