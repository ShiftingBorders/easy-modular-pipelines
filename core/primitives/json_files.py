"""UTF-8 JSON loading and atomic publication with native file sharing."""

from __future__ import annotations

import ctypes
import json
import os
import struct
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO

from core.primitives.json_values import copy_json_object


def load_json(path: Path | str) -> Any:
    """Load and return a value from a UTF-8 JSON file."""
    if isinstance(path, str):
        path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"JSON file does not exist: {path}")
    with path.open("r", encoding="utf-8") as file:
        try:
            return json.load(file)
        except json.JSONDecodeError as error:
            raise ValueError(f"JSON file cannot be parsed: {path}") from error


if TYPE_CHECKING:
    from core.primitives.json_values import JsonObject


def read_json(path: Path, *, max_bytes: int | None = None) -> JsonObject:
    """Read one published file, closing the handle before decoding its JSON."""
    if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 0):
        raise ValueError("max_bytes must be a nonnegative integer or None.")
    deadline = time.monotonic() + 1
    retry_delay = 0.001
    while True:
        try:
            if os.name == "nt":
                stream = _open_shared_reader(path)
            else:
                stream = path.open("rb")
            with stream:
                if max_bytes is None:
                    encoded = stream.read()
                else:
                    # Large read buffers are expensive even for tiny Windows
                    # metadata files. Keep the limit without allocating it up front.
                    encoded = bytearray()
                    remaining = max_bytes + 1
                    while remaining:
                        chunk = stream.read(min(remaining, 65536))
                        if not chunk:
                            break
                        encoded.extend(chunk)
                        remaining -= len(chunk)
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


def _open_shared_reader(path: Path) -> BinaryIO:
    import msvcrt
    from ctypes import wintypes

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
    kernel.CloseHandle.restype = wintypes.BOOL
    # Share read/write/delete so the native rename fallback can
    # replace the path while this handle keeps the old contents.
    handle = kernel.CreateFileW(str(path), 0x80000000, 7, None, 3, 0x80, None)
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
    return stream


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
                if os.name != "nt" or getattr(error, "winerror", None) not in (
                    5,
                    32,
                    33,
                ):
                    raise
                native_error = _replace_shared_file(temporary, path)
                if native_error is None:
                    break
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


def _replace_shared_file(temporary: Path, path: Path) -> int | None:
    """Return None on success, otherwise the native code for the caller's retry."""
    from ctypes import wintypes

    # MoveFileEx (os.replace) rejects even delete-sharing readers.
    # FileRenameInfoEx with POSIX semantics performs one atomic
    # rename while those readers finish reading the old file.
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
    kernel.CloseHandle.restype = wintypes.BOOL
    kernel.SetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    kernel.SetFileInformationByHandle.restype = wintypes.BOOL
    handle = kernel.CreateFileW(str(temporary), 0x10000, 7, None, 3, 0x80, None)
    native_error = ctypes.get_last_error()
    if handle != wintypes.HANDLE(-1).value:
        try:
            name = str(path.absolute()).encode("utf-16-le")
            # Native DWORD flags, HANDLE root, DWORD byte length,
            # then a terminated WCHAR filename (length excludes
            # the terminator). Flags: REPLACE_IF_EXISTS | POSIX.
            record = struct.pack("@IPI", 3, 0, len(name)) + name
            buffer = ctypes.create_string_buffer(record + b"\0\0")
            if kernel.SetFileInformationByHandle(handle, 22, buffer, len(record) + 2):
                return None
            native_error = ctypes.get_last_error()
        finally:
            kernel.CloseHandle(handle)
    return native_error
