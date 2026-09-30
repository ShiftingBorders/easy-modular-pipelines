"""Native nonblocking locks for caller-owned open files."""

from __future__ import annotations

import os
from typing import BinaryIO


def _lock_open_stream(stream: BinaryIO) -> None:
    """Lock an initialized stream; the caller positions it and owns its closure."""
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
