"""
utils/rate_limiter.py
=====================
Token-bucket rate limiter for async coroutines.

Usage:
    limiter = RateLimiter(rate=2, per=1.0)   # 2 requests per second
    async with limiter:
        await session.get(url)
"""

from __future__ import annotations
import asyncio
import time


class RateLimiter:
    """
    Async token-bucket rate limiter.

    Parameters
    ----------
    rate : float
        Number of tokens (requests) allowed per `per` seconds.
    per  : float
        Window size in seconds.
    """

    def __init__(self, rate: float, per: float = 1.0) -> None:
        self._rate      = rate
        self._per       = per
        self._allowance = rate
        self._last_check = time.monotonic()
        self._lock      = asyncio.Lock()

    async def __aenter__(self) -> "RateLimiter":
        async with self._lock:
            now     = time.monotonic()
            elapsed = now - self._last_check
            self._last_check = now
            self._allowance += elapsed * (self._rate / self._per)
            if self._allowance > self._rate:
                self._allowance = self._rate

            if self._allowance < 1.0:
                sleep_time = (1.0 - self._allowance) * (self._per / self._rate)
                await asyncio.sleep(sleep_time)
                self._allowance = 0.0
            else:
                self._allowance -= 1.0
        return self

    async def __aexit__(self, *_: object) -> None:
        pass
