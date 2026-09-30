"""Await actual task outcomes without abandoning committed work."""

import asyncio


async def _await_outcome[T](operation: asyncio.Task[T]) -> T:
    """Wait for the actual outcome, even if the caller is cancelled."""
    while True:
        try:
            return await asyncio.shield(operation)
        except asyncio.CancelledError:
            if operation.cancelled():
                raise
