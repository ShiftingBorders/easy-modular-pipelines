"""Small filesystem and OS operations shared by the initial runtime."""

from __future__ import annotations

import asyncio
import codecs
import ctypes
import json
import os
import socket
import struct
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

from core.logger_utils.events import copy_json_object

if TYPE_CHECKING:
    from core.logger import OperationLogger
    from core.runner_utils.state import JsonObject


async def capture_stream(
    stream: asyncio.StreamReader,
    name: str,
    logger: OperationLogger,
    context: JsonObject,
    output: bytearray | None = None,
) -> None:
    """Drain a child stream in its owning process and record complete text chunks."""
    if name not in ("stdout", "stderr"):
        raise ValueError("Captured stream must be stdout or stderr.")
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    while chunk := await stream.read(65536):
        if output is not None:
            output.extend(chunk)
        text = decoder.decode(chunk)
        if text:
            await asyncio.to_thread(
                logger.record_event,
                "command.output",
                {"stream": name, "text": text},
                context=context,
            )
    remaining = decoder.decode(b"", final=True)
    if remaining:
        await asyncio.to_thread(
            logger.record_event,
            "command.output",
            {"stream": name, "text": remaining},
            context=context,
        )


def read_json(path: Path, *, max_bytes: int | None = None) -> JsonObject:
    """Read one published file, closing the handle before decoding its JSON."""
    if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 0):
        raise ValueError("max_bytes must be a nonnegative integer or None.")
    deadline = time.monotonic() + 1
    retry_delay = 0.001
    while True:
        try:
            if os.name == "nt":
                import msvcrt
                from ctypes import wintypes

                kernel = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel.CreateFileW.argtypes = [
                    wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
                    wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
                ]
                kernel.CreateFileW.restype = wintypes.HANDLE
                kernel.CloseHandle.argtypes = [wintypes.HANDLE]
                kernel.CloseHandle.restype = wintypes.BOOL
                # Share read/write/delete so the native rename fallback can
                # replace the path while this handle keeps the old contents.
                handle = kernel.CreateFileW(
                    str(path), 0x80000000, 7, None, 3, 0x80, None
                )
                if handle == wintypes.HANDLE(-1).value:
                    error = ctypes.WinError(ctypes.get_last_error())
                    error.filename = str(path)
                    raise error
                try:
                    descriptor = msvcrt.open_osfhandle(
                        handle, os.O_RDONLY | os.O_BINARY | os.O_NOINHERIT
                    )
                except BaseException:
                    kernel.CloseHandle(handle)
                    raise
                try:
                    stream = os.fdopen(descriptor, "rb")
                except BaseException:
                    os.close(descriptor)
                    raise
            else:
                stream = path.open("rb")
            with stream:
                encoded = (
                    stream.read() if max_bytes is None else stream.read(max_bytes + 1)
                )
            break
        except PermissionError:
            # A concurrent Windows replacement can briefly deny the open too.
            remaining = deadline - time.monotonic()
            if os.name != "nt" or remaining <= 0:
                raise
            time.sleep(min(retry_delay, remaining))
            retry_delay = min(retry_delay * 2, 0.05)
    if max_bytes is not None and len(encoded) > max_bytes:
        raise ValueError(f"Metadata exceeds its size limit: {path.name}")
    return copy_json_object(json.loads(encoded.decode("utf-8")), str(path))


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
        deadline = time.monotonic() + 1
        retry_delay = 0.001
        while True:
            try:
                os.replace(temporary, path)
                break
            except OSError as error:
                if (
                    os.name != "nt"
                    or getattr(error, "winerror", None) not in (5, 32, 33)
                ):
                    raise
                from ctypes import wintypes

                # MoveFileEx (os.replace) rejects even delete-sharing readers.
                # FileRenameInfoEx with POSIX semantics performs one atomic
                # rename while those readers finish reading the old file.
                kernel = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel.CreateFileW.argtypes = [
                    wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
                    wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
                ]
                kernel.CreateFileW.restype = wintypes.HANDLE
                kernel.CloseHandle.argtypes = [wintypes.HANDLE]
                kernel.CloseHandle.restype = wintypes.BOOL
                kernel.SetFileInformationByHandle.argtypes = [
                    wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
                ]
                kernel.SetFileInformationByHandle.restype = wintypes.BOOL
                handle = kernel.CreateFileW(
                    str(temporary), 0x10000, 7, None, 3, 0x80, None
                )
                native_error = ctypes.get_last_error()
                if handle != wintypes.HANDLE(-1).value:
                    try:
                        name = str(path.absolute()).encode("utf-16-le")
                        # Native DWORD flags, HANDLE root, DWORD byte length,
                        # then a terminated WCHAR filename (length excludes
                        # the terminator). Flags: REPLACE_IF_EXISTS | POSIX.
                        record = struct.pack("@IPI", 3, 0, len(name)) + name
                        buffer = ctypes.create_string_buffer(record + b"\0\0")
                        if kernel.SetFileInformationByHandle(
                            handle, 22, buffer, len(record) + 2
                        ):
                            break
                        native_error = ctypes.get_last_error()
                    finally:
                        kernel.CloseHandle(handle)
                # Older filesystems may not support FileRenameInfoEx. Retain
                # the bounded MoveFileEx retry there and for external locks.
                if native_error not in (1, 5, 32, 33, 50, 87):
                    raise ctypes.WinError(native_error) from error
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(retry_delay, remaining))
                retry_delay = min(retry_delay * 2, 0.05)
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
