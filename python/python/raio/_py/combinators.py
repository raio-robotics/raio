"""Awaitables built from the ones of raio-rs."""

from __future__ import annotations

from .._rs import core
from .._rs.core import Time


async def timeout(duration: Time, awaitable):
    """Await something, or raise TimeoutError once the logical duration has passed."""
    finished = await core.select(result=awaitable, expired=core.wait(duration))
    if "result" in finished:
        return finished["result"]
    raise TimeoutError(f"no result within {duration}")
