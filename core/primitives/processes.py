"""Exact process identity and termination checks on the current OS."""

from __future__ import annotations

import ctypes
import os
import socket
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from core.primitives.paths import repository_root

if TYPE_CHECKING:
    from core.primitives.json_values import JsonObject


def module_process_arguments(argv: list[str]) -> tuple[list[str], dict[str, str]]:
    """Prepare SDK imports and the current uv interpreter without a PID wrapper.

    Explicit executables keep their selection. The bare `python` command uses
    this environment. On Windows, replicate CPython's venv redirector environment
    while starting its base executable directly, so Popen.pid is the stage PID.
    """
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(repository_root()), environment.get("PYTHONPATH")))
    )
    arguments = list(argv)
    if arguments[0] == "python":
        arguments[0] = sys.executable
        if os.name == "nt" and sys.prefix != sys.base_prefix:
            arguments[0] = sys._base_executable
            environment["__PYVENV_LAUNCHER__"] = sys.executable
    return arguments, environment


def process_running(pid: int) -> bool:
    """Check actual termination, including Windows processes with retained handles."""
    if os.name != "nt":
        try:
            state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
            return state not in ("Z", "X")
        except FileNotFoundError:
            return False
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel.OpenProcess(0x100000, False, pid)
    if not handle:
        if ctypes.get_last_error() in (87, 1168):
            return False
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        status = kernel.WaitForSingleObject(handle, 0)
        if status == 0xFFFFFFFF:
            raise ctypes.WinError(ctypes.get_last_error())
        return status == 258
    finally:
        kernel.CloseHandle(handle)


def process_identity(pid: int) -> JsonObject:
    """Use the OS creation value without rounding; PID alone is insufficient."""
    if os.name == "nt":
        created, host_id, boot_id = _windows_process_identity(pid)
    else:
        created, host_id, boot_id = _linux_process_identity(pid)
    return {
        "pid": pid,
        "created_at_os": created,
        "host_id": host_id,
        "boot_id": boot_id,
    }


def _windows_process_identity(pid: int) -> tuple[int, str, str]:
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [
        ctypes.POINTER(wintypes.FILETIME)
    ] * 4
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        times = [wintypes.FILETIME() for _ in range(4)]
        if not kernel.GetProcessTimes(handle, *(ctypes.byref(item) for item in times)):
            raise ctypes.WinError(ctypes.get_last_error())
        created = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
    finally:
        kernel.CloseHandle(handle)
    query = ctypes.WinDLL("ntdll").NtQuerySystemInformation
    query.argtypes = [
        wintypes.ULONG,
        ctypes.c_void_p,
        wintypes.ULONG,
        ctypes.POINTER(wintypes.ULONG),
    ]
    query.restype = ctypes.c_long
    buffer = ctypes.create_string_buffer(48)
    length = wintypes.ULONG()
    if query(3, buffer, len(buffer), ctypes.byref(length)) != 0:
        raise OSError("Cannot determine OS boot identity.")
    boot_id = str(int.from_bytes(buffer.raw[:8], "little"))
    host_id = socket.gethostname()
    return created, host_id, boot_id


def _linux_process_identity(pid: int) -> tuple[int, str, str]:
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    created = int(fields[19])
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    host_id = Path("/etc/machine-id").read_text().strip()
    return created, host_id, boot_id
