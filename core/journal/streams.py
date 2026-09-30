"""Captured child-process streams and best-effort emergency stderr output."""

from __future__ import annotations

import asyncio
import codecs
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.journal.logger import OperationLogger
    from core.primitives.json_values import JsonObject


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


def _write_stderr_best_effort(message: str) -> None:
    try:
        if sys.stderr is not None:
            sys.stderr.write(message + "\n")
            sys.stderr.flush()
    except BaseException:  # noqa: BLE001, S110 - Last-resort sink.
        pass
