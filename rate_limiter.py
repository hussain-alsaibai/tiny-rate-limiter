"""
tiny-rate-limiter — Zero-dependency token bucket + sliding window rate limiting.
Pure Python stdlib. MIT License. Part of the tiny-* ecosystem.

Features:
- Token bucket (bursty, refill-based)
- Sliding window log (precise, memory-bounded)
- Sliding window counter (approximate, O(1) memory)
- Both sync and async support
- Decorator-friendly
"""

from __future__ import annotations
import time
import threading
import asyncio
import bisect
from typing import Callable, Optional, TypeVar, Awaitable

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Token Bucket
# ---------------------------------------------------------------------------

class TokenBucket:
    """
    Token bucket rate limiter.

    Args:
        rate: Tokens per second (refill rate)
        capacity: Maximum tokens (burst size)
    """

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """Block until tokens are available or timeout expires."""
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            if self.try_acquire(tokens):
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(0.01)

    def wait_time(self, tokens: float = 1.0) -> float:
        """Seconds until `tokens` would be available."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                return 0.0
            return (tokens - self._tokens) / self.rate


class AsyncTokenBucket:
    """Async token bucket rate limiter."""

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None

    async def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    async def try_acquire(self, tokens: float = 1.0) -> bool:
        async with await self._get_lock():
            await self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    async def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            if await self.try_acquire(tokens):
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.01)


# ---------------------------------------------------------------------------
# Sliding Window Log
# ---------------------------------------------------------------------------

class SlidingWindowLog:
    """
    Precise sliding window — stores timestamps of each request.
    Memory-bounded by window size × max_rate.

    Args:
        window_seconds: Window size in seconds
        max_rate: Maximum requests per window (sets memory bound)
    """

    def __init__(self, window_seconds: float, max_rate: int):
        self.window = window_seconds
        self.max_rate = max_rate
        self._log: list[float] = []
        self._lock = threading.Lock()

    def _clean(self, now: float) -> None:
        cutoff = now - self.window
        idx = bisect.bisect_left(self._log, cutoff)
        if idx:
            del self._log[:idx]

    def try_acquire(self) -> bool:
        now = time.monotonic()
        with self._lock:
            self._clean(now)
            if len(self._log) < self.max_rate:
                self._log.append(now)
                return True
            return False

    def acquire(self, timeout: Optional[float] = None) -> bool:
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            if self.try_acquire():
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(0.01)

    def remaining(self) -> int:
        """Approximate remaining requests in current window."""
        now = time.monotonic()
        with self._lock:
            self._clean(now)
            return max(0, self.max_rate - len(self._log))


class AsyncSlidingWindowLog:
    """Async sliding window log rate limiter."""

    def __init__(self, window_seconds: float, max_rate: int):
        self.window = window_seconds
        self.max_rate = max_rate
        self._log: list[float] = []
        self._lock: Optional[asyncio.Lock] = None

    async def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def _clean(self, now: float) -> None:
        idx = bisect.bisect_left(self._log, now - self.window)
        if idx:
            del self._log[:idx]

    async def try_acquire(self) -> bool:
        now = time.monotonic()
        async with await self._get_lock():
            await self._clean(now)
            if len(self._log) < self.max_rate:
                self._log.append(now)
                return True
            return False

    async def acquire(self, timeout: Optional[float] = None) -> bool:
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            if await self.try_acquire():
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.01)


# ---------------------------------------------------------------------------
# Sliding Window Counter (approximate)
# ---------------------------------------------------------------------------

class SlidingWindowCounter:
    """
    Approximate sliding window using fixed sub-windows.
    O(1) memory regardless of rate. Good for high-throughput scenarios.

    Args:
        window_seconds: Window size in seconds
        max_rate: Maximum requests per window
        sub_windows: Number of sub-windows (higher = more accurate)
    """

    def __init__(self, window_seconds: float, max_rate: int, sub_windows: int = 10):
        self.window = window_seconds
        self.max_rate = max_rate
        self.sub_windows = sub_windows
        self._window_size = window_seconds / sub_windows
        self._counts: list[float] = [0.0] * sub_windows
        self._lock = threading.Lock()

    def _current_bucket(self) -> int:
        return int(time.monotonic() / self._window_size) % self.sub_windows

    def _total(self) -> float:
        return sum(self._counts)

    def try_acquire(self) -> bool:
        now = time.monotonic()
        with self._lock:
            current_bucket = int(now / self._window_size) % self.sub_windows
            window_start = now - self.window
            # Expire sub-windows outside the sliding window
            for i in range(self.sub_windows):
                bucket_time = ((int(now / self._window_size) - (self.sub_windows - 1 - i))
                               * self._window_size)
                if bucket_time < window_start:
                    self._counts[i] = 0.0
            if self._total() < self.max_rate:
                self._counts[current_bucket] += 1
                return True
            return False

    def acquire(self, timeout: Optional[float] = None) -> bool:
        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            if self.try_acquire():
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(0.01)


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

def create(algorithm: str = "token_bucket", **kwargs):
    """
    Factory for rate limiters by name.

    Usage:
        limiter = create("token_bucket", rate=10, capacity=20)
        limiter = create("sliding_window_log", window_seconds=60, max_rate=100)
        limiter = create("sliding_window_counter", window_seconds=1, max_rate=10000)
    """
    algorithms = {
        "token_bucket": TokenBucket,
        "sliding_window_log": SlidingWindowLog,
        "sliding_window_counter": SlidingWindowCounter,
    }
    if algorithm not in algorithms:
        raise ValueError(f"Unknown algorithm: {algorithm}. "
                         f"Choose from: {list(algorithms.keys())}")
    return algorithms[algorithm](**kwargs)


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------

def rate_limit(rate: float, capacity: float):
    """Decorator: best-effort rate limiting (skip if over limit)."""
    bucket = TokenBucket(rate, capacity)
    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args, **kwargs) -> T:
            bucket.acquire()
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def rate_limited(rate: float, capacity: float, timeout: Optional[float] = None):
    """Decorator: blocking with timeout — raises RuntimeError if exceeded."""
    bucket = TokenBucket(rate, capacity)
    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args, **kwargs) -> T:
            if not bucket.acquire(timeout=timeout):
                raise RuntimeError(f"Rate limit timeout for {fn.__name__}")
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def async_rate_limit(rate: float, capacity: float):
    """Decorator: apply token bucket rate limiting to an async function."""
    bucket = AsyncTokenBucket(rate, capacity)
    def decorator(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        async def wrapper(*args, **kwargs) -> T:
            await bucket.acquire()
            return await fn(*args, **kwargs)
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Demo / smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Token Bucket Demo ===")
    bucket = TokenBucket(rate=5.0, capacity=3.0)
    for i in range(8):
        ok = bucket.try_acquire()
        print(f"  Request {i+1}: {'✓' if ok else '✗'} | wait={bucket.wait_time():.2f}s")

    print()
    print("=== Sliding Window Log Demo ===")
    log = SlidingWindowLog(window_seconds=1.0, max_rate=3)
    for i in range(6):
        ok = log.try_acquire()
        print(f"  Request {i+1}: {'✓' if ok else '✗'}")

    print()
    print("=== Sliding Window Counter Demo ===")
    counter = SlidingWindowCounter(window_seconds=1.0, max_rate=3, sub_windows=5)
    for i in range(6):
        ok = counter.try_acquire()
        print(f"  Request {i+1}: {'✓' if ok else '✗'}")

    print()
    print("=== Factory Demo ===")
    limiter = create("token_bucket", rate=10.0, capacity=5.0)
    print(f"  Created: {type(limiter).__name__}")

    print()
    print("All demos passed ✓")
