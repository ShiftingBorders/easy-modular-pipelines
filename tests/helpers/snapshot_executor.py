"""Control publication and cleanup boundaries of the real executor process."""

import os
import runpy
import time
from pathlib import Path
from unittest.mock import patch

from core.runner_utils.runtimeio import write_json


def main():
    original = Path.unlink
    release = Path(os.environ["EMP_TEST_EXECUTOR_RELEASE"])
    entered = Path(os.environ["EMP_TEST_EXECUTOR_ENTERED"])
    mode = os.environ.get("EMP_TEST_EXECUTOR_MODE", "cleanup")

    def unlink(path, *args, **kwargs):
        if (
            path.name.startswith("executor.lock.")
            and path.suffix == ".token"
            and mode == "cleanup"
        ):
            entered.touch()
            deadline = time.monotonic() + 30
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("Test did not release executor cleanup.")
                time.sleep(0.01)
        return original(path, *args, **kwargs)

    def publish(path, document):
        write_json(path, document)
        if mode != "lock_publication" or path.name != "executor.lock.json":
            return
        import ctypes
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
        handle = kernel.CreateFileW(str(path), 0x80000000, 0, None, 3, 0x80, None)
        if handle == ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            entered.touch()
            deadline = time.monotonic() + 30
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("Test did not release endpoint publication.")
                time.sleep(0.01)
        finally:
            kernel.CloseHandle(handle)

    with (
        patch.object(Path, "unlink", unlink),
        patch("core.runner_utils.runtimeio.write_json", publish),
    ):
        runpy.run_module("core.runner_utils.executor", run_name="__main__")


if __name__ == "__main__":
    main()
