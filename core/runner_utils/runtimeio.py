"""Small filesystem and OS operations shared by the initial runtime."""

from __future__ import annotations

import ctypes
import json
import os
import socket
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from core.logger_utils.events import copy_json_object

if TYPE_CHECKING:
    from core.runner_utils.state import JsonObject


def read_json(path: Path) -> JsonObject:
    with path.open(encoding="utf-8") as stream:
        return copy_json_object(json.load(stream), str(path))


def write_json(path: Path, data: JsonObject) -> None:
    """Publish complete validated JSON; do not expose a partially written file."""
    encoded = json.dumps(
        copy_json_object(data, str(path)), ensure_ascii=False, allow_nan=False
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    failure = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=".publish-",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException as error:
        failure = error
        raise
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as cleanup_error:
                if failure is None:
                    raise
                failure.add_note(f"Temporary JSON cleanup failed: {cleanup_error}")


def process_identity(pid: int) -> JsonObject:
    """Use the OS creation value without rounding; PID alone is insufficient."""
    if os.name == "nt":
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
            if not kernel.GetProcessTimes(
                handle, *(ctypes.byref(item) for item in times)
            ):
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
    else:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        created = int(fields[19])
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        host_id = Path("/etc/machine-id").read_text().strip()
    return {
        "pid": pid,
        "created_at_os": created,
        "host_id": host_id,
        "boot_id": boot_id,
    }
