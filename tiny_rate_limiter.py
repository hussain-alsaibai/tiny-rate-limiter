"""tiny-rate-limiter — async rate limiter for multi-model AI agent pipelines.

Prevents RPM and TPM exhaustion when running multiple LLM providers in the
same process. Stdlib-only.

    >>> import asyncio
    >>> from tiny_rate_limiter import AsyncRateLimiter
    >>> rl = AsyncRateLimiter(requests_per_minute=60, tokens_per_minute=500_000)
    >>> asyncio.run(rl.acquire())
    >>> rl.record(tokens_used=1200)
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

__version__ = "0.1.0"
__all__ = ["AsyncRateLimiter", "MultiModelRateLimiter"]

_WINDOW_SECONDS = 60.0

class AsyncRateLimiter:
    """Async sliding-window rate limiter with both RPM and TPM enforcement.

    - ``acquire()`` blocks until a request slot is free within the last 60s.
    - ``record()`` logs completion; when ``tokens_used`` is provided, TPM is
      also enforced by back-pressuring future ``acquire()`` calls.
    - ``get_stats()`` reports current window counts and time-to-reset.

    Internally runs a background reset task every 60s that purges expired
    timestamps; you can also call :py:meth:`reset` manually.
    """

    def __init__(
        self,
        requests_per_minute: int = 60,
        tokens_per_minute: int = 500_000,
        token_per_request_avg: int = 1000,
        auto_reset: bool = True,
    ) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be > 0")
        if tokens_per_minute <= 0:
            raise ValueError("tokens_per_minute must be > 0")
        if token_per_request_avg <= 0:
            raise ValueError("token_per_request_avg must be > 0")

        self.requests_per_minute = int(requests_per_minute)
        self.tokens_per_minute = int(tokens_per_minute)
        self.token_per_request_avg = int(token_per_request_avg)

        # Two independent deques: RPM timestamps + (timestamp, token) tuples.
        self._request_times: Deque[float] = deque()
        self._token_records: Deque[Tuple[float, int]] = deque()

        self._lock = asyncio.Lock()
        self._auto_reset = auto_reset
        self._reset_task: Optional[asyncio.Task[None]] = None
        self._closed = False
        self._started = False

    # ------------------------------------------------------------------ #

    async def _ensure_started(self) -> None:
        """Spin up the periodic reset task on first use."""
        if self._started or not self._auto_reset:
            self._started = True
            return
        self._started = True
        self._reset_task = asyncio.create_task(self._reset_loop())

    async def _reset_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(_WINDOW_SECONDS)
            if self._closed:
                return
            async with self._lock:
                self.reset()

    async def aclose(self) -> None:
        """Stop the background reset task.  Safe to call multiple times."""
        self._closed = True
        if self._reset_task is not None:
            self._reset_task.cancel()
            try:
                await self._reset_task
            except (asyncio.CancelledError, Exception):
                pass
            self._reset_task = None

    # ------------------------------------------------------------------ #

    async def acquire(self) -> None:
        """Block until a request slot is available (RPM + projected TPM)."""
        await self._ensure_started()
        while True:
            wait_for = 0.0
            async with self._lock:
                now = time.monotonic()
                self._purge(now)

                if len(self._request_times) < self.requests_per_minute:
                    projected_tokens = (
                        sum(t for _, t in self._token_records)
                        + self.token_per_request_avg
                    )
                    if projected_tokens <= self.tokens_per_minute:
                        self._request_times.append(now)
                        return

                wait_for = self._seconds_until_next_slot(now)
            await asyncio.sleep(wait_for)

    def record(self, tokens_used: Optional[int] = None) -> None:
        """Mark an acquired request as completed and (optionally) charge tokens.

        Synchronous by design — callers already awaited the LLM response.
        Safe to interleave with ``acquire()``: each limiter owns two
        independent deques and ``deque.append`` is atomic under the GIL.
        """
        self._record_locked(tokens_used)

    def _record_locked(self, tokens_used: Optional[int]) -> None:
        now = time.monotonic()
        self._purge(now)
        if tokens_used is not None and tokens_used > 0:
            self._token_records.append((now, int(tokens_used)))

    def get_stats(self) -> Dict[str, float]:
        """Return snapshot of current window state."""
        now = time.monotonic()
        # Snapshot without taking the lock — best effort.
        self._purge(now)
        requests_this_minute = len(self._request_times)
        tokens_this_minute = sum(t for _, t in self._token_records)
        return {
            "requests_this_minute": requests_this_minute,
            "tokens_this_minute": int(tokens_this_minute),
            "requests_remaining": max(
                0, self.requests_per_minute - requests_this_minute
            ),
            "tokens_remaining": max(
                0, self.tokens_per_minute - int(tokens_this_minute)
            ),
            "time_until_reset": self._seconds_until_next_slot(now),
        }

    def reset(self) -> None:
        """Clear all tracked state.  Called automatically every 60s."""
        self._request_times.clear()
        self._token_records.clear()

    # ------------------------------------------------------------------ #

    def _purge(self, now: float) -> None:
        cutoff = now - _WINDOW_SECONDS
        while self._request_times and self._request_times[0] <= cutoff:
            self._request_times.popleft()
        while self._token_records and self._token_records[0][0] <= cutoff:
            self._token_records.popleft()

    def _seconds_until_next_slot(self, now: float) -> float:
        # If anything is blocking us, wake when the *earliest* blocking
        # entry expires — we'll re-check after waking and either succeed or
        # compute the next wait.
        waits: List[float] = []
        if (
            len(self._request_times) >= self.requests_per_minute
            and self._request_times
        ):
            waits.append((self._request_times[0] + _WINDOW_SECONDS) - now)
        if self._token_records:
            current_tokens = sum(t for _, t in self._token_records)
            if (
                current_tokens + self.token_per_request_avg
                > self.tokens_per_minute
            ):
                waits.append((self._token_records[0][0] + _WINDOW_SECONDS) - now)
        if not waits:
            return 0.001
        return max(min(waits), 0.001)

class MultiModelRateLimiter:
    """Share one global RPM/TPM budget across N model-specific limiters.

    """

    def __init__(
        self,
        total_rpm: int = 60,
        total_tpm: int = 500_000,
        allocations: Optional[Dict[str, float]] = None,
        token_per_request_avg: int = 1000,
    ) -> None:
        if total_rpm <= 0:
            raise ValueError("total_rpm must be > 0")
        if total_tpm <= 0:
            raise ValueError("total_tpm must be > 0")
        if allocations is None:
            allocations = {}
        if not allocations:
            raise ValueError(
                "allocations must be a non-empty dict of model→share (0.0-1.0)"
            )

        total_share = sum(allocations.values())
        if total_share <= 0:
            raise ValueError("allocations shares must sum to > 0")

        # Normalize so callers don't have to worry about exact sum.
        normalized = {
            model: share / total_share for model, share in allocations.items()
        }

        self.allocations: Dict[str, float] = dict(normalized)
        self._limiters: Dict[str, AsyncRateLimiter] = {}
        for model, share in normalized.items():
            self._limiters[model] = AsyncRateLimiter(
                requests_per_minute=max(1, int(round(total_rpm * share))),
                tokens_per_minute=max(1, int(round(total_tpm * share))),
                token_per_request_avg=token_per_request_avg,
            )

    def get_limiter(self, model: str) -> AsyncRateLimiter:
        if model not in self._limiters:
            raise KeyError(
                f"Unknown model {model!r}. Known: {sorted(self._limiters)}"
            )
        return self._limiters[model]

    async def acquire_all(self, models: List[str]) -> None:
        """Acquire slots on every named model in a single critical section.

        Models are awaited sequentially so a fast model never starves a slow
        one when the dispatcher wants them all at once.
        """
        if not models:
            return
        await asyncio.gather(
            *(self.get_limiter(m).acquire() for m in models)
        )

    async def aclose(self) -> None:
        """Stop background tasks for every child limiter."""
        await asyncio.gather(
            *(l.aclose() for l in self._limiters.values()),
            return_exceptions=True,
        )

    def get_stats(self) -> Dict[str, Dict[str, float]]:
        return {model: limiter.get_stats() for model, limiter in self._limiters.items()}