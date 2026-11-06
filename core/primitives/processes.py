"""Exact process identity and termination checks on the current OS."""

from __future__ import annotations

import ctypes
import errno
import math
import os
import select
import socket
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

import psutil

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
        except (FileNotFoundError, ProcessLookupError):
            return False
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    try:
        handle = _open_windows_process(pid, 0x100000)
    except ProcessLookupError:
        return False
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
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = _open_windows_process(pid, 0x1000)
    try:
        created = _windows_process_created_at(handle)
    finally:
        kernel.CloseHandle(handle)
    return created, socket.gethostname(), _windows_boot_id()


def _open_windows_process(pid: int, access: int) -> int:
    """Open a handle, distinguishing denied access from independently proven absence."""
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    handle = kernel.OpenProcess(access, False, pid)
    if handle:
        return handle
    error = ctypes.WinError(ctypes.get_last_error())
    absent = error.winerror in (87, 1168)
    if error.winerror == 5:
        try:
            # EnumProcesses does not need a query handle to each listed process.
            # ERROR_ACCESS_DENIED alone cannot establish that a PID has exited.
            absent = pid not in psutil.pids()
        except (OSError, psutil.Error) as observation_error:
            error.add_note(f"Process enumeration failed: {observation_error}")
    if absent:
        raise ProcessLookupError(
            errno.ESRCH, f"Process {pid} no longer exists"
        ) from error
    raise error


def _windows_process_created_at(handle: int) -> int:
    """Read exact FILETIME from an owned handle without reopening its PID."""
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [
        ctypes.POINTER(wintypes.FILETIME)
    ] * 4
    kernel.GetProcessTimes.restype = wintypes.BOOL
    times = [wintypes.FILETIME() for _ in range(4)]
    if not kernel.GetProcessTimes(handle, *(ctypes.byref(item) for item in times)):
        raise ctypes.WinError(ctypes.get_last_error())
    return (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime


def _windows_boot_id() -> str:
    """Use the boot-session GUID, not BootTime, which changes with clock corrections."""
    from ctypes import wintypes

    query = ctypes.WinDLL("ntdll").NtQuerySystemInformation
    query.argtypes = [
        wintypes.ULONG,
        ctypes.c_void_p,
        wintypes.ULONG,
        ctypes.POINTER(wintypes.ULONG),
    ]
    query.restype = ctypes.c_long
    # SYSTEM_BOOT_ENVIRONMENT_INFORMATION starts with a GUID, then firmware
    # type and boot flags. SystemBootEnvironmentInformation is class 90.
    buffer = ctypes.create_string_buffer(32)
    length = wintypes.ULONG()
    status = query(90, buffer, len(buffer), ctypes.byref(length))
    if status != 0 or length.value < 16:
        raise OSError(
            f"Cannot determine stable Windows boot identity: NTSTATUS {status:#x}."
        )
    return f"windows:{UUID(bytes_le=buffer.raw[:16])}"


def _same_process_identity(actual: JsonObject | None, expected: JsonObject) -> bool:
    """Compare exact identities; an ambiguous legacy boot timestamp is not death.

    Old decimal Windows boot IDs remain readable. A missing/reused PID can still
    be reconciled, but a live matching PID/creation pair needs explicit migration
    rather than treating an incompatible boot representation as termination.
    """
    if actual is None:
        return False
    if any(actual[key] != expected[key] for key in ("pid", "created_at_os", "host_id")):
        return False
    if actual["boot_id"] == expected["boot_id"]:
        return True
    if actual["boot_id"].startswith("windows:") and expected["boot_id"].isdecimal():
        raise OSError(
            f"Cannot verify legacy Windows process identity for PID {expected['pid']}; "
            "stop its original owner and participants before upgrading EMP."
        )
    return False


def _wait_process_exit(identity: JsonObject, timeout: float) -> None:
    """Wait for the exact executor, never a reused PID or merely its launcher.

    Timeout is in seconds. Absence or another exact identity confirms the old
    process is gone. Access errors remain errors. The native handle/pidfd is held
    from identity verification through the exit wait and released on every path.
    """
    if identity["host_id"] != socket.gethostname():
        raise OSError("Cannot confirm exit of an executor on another host.")
    pid = identity["pid"]
    try:
        if os.name == "nt":
            _wait_windows_process_exit(identity, timeout)
            return
        descriptor = os.pidfd_open(pid)
        try:
            if not _same_process_identity(process_identity(pid), identity):
                return
            ready, _, _ = select.select([descriptor], [], [], timeout)
            if not ready:
                raise psutil.TimeoutExpired(timeout, pid=pid)
        finally:
            os.close(descriptor)
    except (ProcessLookupError, FileNotFoundError):
        return


def _wait_windows_process_exit(identity: JsonObject, timeout: float) -> None:
    from ctypes import wintypes

    boot_id = _windows_boot_id()
    if identity["boot_id"].startswith("windows:") and identity["boot_id"] != boot_id:
        return
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = _open_windows_process(identity["pid"], 0x100000 | 0x1000)
    try:
        actual = {
            "pid": identity["pid"],
            "created_at_os": _windows_process_created_at(handle),
            "host_id": socket.gethostname(),
            "boot_id": boot_id,
        }
        if not _same_process_identity(actual, identity):
            return
        milliseconds = min(math.ceil(timeout * 1000), 0xFFFFFFFE)
        result = kernel.WaitForSingleObject(handle, milliseconds)
        if result == 258:
            raise psutil.TimeoutExpired(timeout, pid=identity["pid"])
        if result == 0xFFFFFFFF:
            raise ctypes.WinError(ctypes.get_last_error())
        if result != 0:
            raise OSError(f"Unexpected executor wait result: {result}.")
    finally:
        kernel.CloseHandle(handle)


def _linux_process_identity(pid: int) -> tuple[int, str, str]:
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    created = int(fields[19])
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    host_id = Path("/etc/machine-id").read_text().strip()
    return created, host_id, boot_id
