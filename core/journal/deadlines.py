"""Await owned work while excluding the journal owner's recovery intervals."""

from __future__ import annotations

import asyncio
import time


async def wait_journal_deadline(
    awaitable,
    deadline,
    logger,
    *,
    cancel_on_timeout=True,
    recovery_baseline: float | None = None,
):
    if deadline is None:
        return await awaitable
    baseline = (
        logger._recovery_seconds() if recovery_baseline is None else recovery_baseline
    )
    work = asyncio.ensure_future(awaitable)
    try:
        while not work.done():
            remaining = (
                deadline
                + max(0, logger._recovery_seconds() - baseline)
                - time.monotonic()
            )
            if remaining <= 0:
                raise TimeoutError("Execution deadline expired.")
            await asyncio.wait((work,), timeout=min(0.1, remaining))
        return work.result()
    except BaseException:
        if cancel_on_timeout and not work.done():
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)
        raise
