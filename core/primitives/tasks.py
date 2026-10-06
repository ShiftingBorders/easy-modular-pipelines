"""Await actual task outcomes without abandoning committed work."""

import asyncio
from threading import Event


async def _await_outcome[T](operation: asyncio.Task[T]) -> T:
    """Wait for the actual outcome, even if the caller is cancelled."""
    while True:
        try:
            return await asyncio.shield(operation)
        except asyncio.CancelledError:
            if operation.cancelled():
                raise


async def _await_read_task[T](reading: asyncio.Task[T], cancelled: Event | None = None) -> T:
    """Preserve caller cancellation after joining readers of replaceable files.

    An optional event lets cooperative workers stop scheduling or reading early.
    Readers without that support are still joined before the caller can clean up.
    """
    try:
        return await asyncio.shield(reading)
    except asyncio.CancelledError as cancellation:
        if cancelled is not None:
            cancelled.set()
        try:
            await _await_outcome(reading)
        except BaseException as error:  # noqa: BLE001 - Reaping must preserve cancellation.
            cancellation.add_note(f"Read ended during cancellation: {error}")
        raise
