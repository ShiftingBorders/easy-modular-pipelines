"""Native nonblocking locks for caller-owned open files."""

from __future__ import annotations

import os
from pathlib import Path
from typing import BinaryIO


def _lock_open_stream(stream: BinaryIO) -> None:
    """Lock an initialized stream; the caller positions it and owns its closure."""
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _open_file_lock(path: Path) -> BinaryIO:
    """Acquire a native lease; the caller must close the returned stream.

    Never replace or unlink the lock file: processes must lock the same object.
    """
    if not path.is_absolute():
        raise ValueError("Lock paths must be absolute.")
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    try:
        if stream.seek(0, os.SEEK_END) == 0:
            stream.write(b"\0")
            stream.flush()
        stream.seek(0)
        _lock_open_stream(stream)
    except BaseException:
        stream.close()
        raise
    return stream
