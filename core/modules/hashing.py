"""Bounded file hashing with the existing length-prefixed module digest."""

import hashlib
import os
from collections import deque
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from pathlib import Path
from threading import Event

from core.models.module_settings import ModuleHashingSettings


def _check_hash_cancelled(cancelled: Event) -> None:
    if cancelled.is_set():
        raise CancelledError("Module hashing was cancelled.")


def _hash_file(path: Path, settings: ModuleHashingSettings, cancelled: Event) -> bytes:
    """Read a stable regular file with bounded memory and return its raw digest."""
    _check_hash_cancelled(cancelled)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        if before.st_size <= settings.hash_small_file_threshold_bytes:
            # Cap the read even if a supposedly small file grows after fstat.
            content = stream.read(before.st_size + 1)
            if len(content) != before.st_size:
                raise ValueError(f"File changed while hashing: {path}")
            digest.update(content)
        else:
            remaining = before.st_size
            while remaining:
                _check_hash_cancelled(cancelled)
                block = stream.read(min(settings.hash_chunk_size_bytes, remaining))
                if not block:
                    raise ValueError(f"File changed while hashing: {path}")
                digest.update(block)
                remaining -= len(block)
            if stream.read(1):
                raise ValueError(f"File changed while hashing: {path}")
        after = os.fstat(stream.fileno())
    current = path.stat()
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ctime_ns != after.st_ctime_ns
        or after.st_size != current.st_size
        or (after.st_dev, after.st_ino) != (current.st_dev, current.st_ino)
    ):
        raise ValueError(f"File changed while hashing: {path}")
    _check_hash_cancelled(cancelled)
    return digest.digest()


def _hash_files(
    folder: Path,
    files: list[Path],
    settings: ModuleHashingSettings,
    cancelled: Event | None = None,
) -> str:
    """Aggregate sorted POSIX names and file digests; join readers before returning.

    A caller owns source immutability. Only twice the worker count is queued,
    including completed out-of-order results. No storage connections are used.
    """
    cancellation = cancelled if cancelled is not None else Event()
    aggregate = hashlib.sha256()
    if settings.hash_max_workers == 1 or len(files) < 2:
        for relative in files:
            digest = _hash_file(folder / relative, settings, cancellation)
            _add_file_digest(aggregate, relative, digest)
        _check_hash_cancelled(cancellation)
        return aggregate.hexdigest()

    pending: deque[tuple[Path, Future[bytes]]] = deque()
    iterator = iter(files)
    with ThreadPoolExecutor(max_workers=settings.hash_max_workers) as executor:
        try:
            for _ in range(settings.hash_max_workers * 2):
                relative = next(iterator, None)
                if relative is None:
                    break
                _check_hash_cancelled(cancellation)
                pending.append(
                    (
                        relative,
                        executor.submit(
                            _hash_file,
                            folder / relative,
                            settings,
                            cancellation,
                        ),
                    )
                )
            while pending:
                _check_hash_cancelled(cancellation)
                relative, result = pending.popleft()
                _add_file_digest(aggregate, relative, result.result())
                following = next(iterator, None)
                if following is not None:
                    _check_hash_cancelled(cancellation)
                    pending.append(
                        (
                            following,
                            executor.submit(
                                _hash_file,
                                folder / following,
                                settings,
                                cancellation,
                            ),
                        )
                    )
            _check_hash_cancelled(cancellation)
        except BaseException:
            cancellation.set()
            for _, result in pending:
                result.cancel()
            raise
    return aggregate.hexdigest()


def _add_file_digest(aggregate, relative: Path, digest: bytes) -> None:
    encoded_path = relative.as_posix().encode("UTF-8")
    aggregate.update(len(encoded_path).to_bytes(8, "big"))
    aggregate.update(encoded_path)
    aggregate.update(digest)
