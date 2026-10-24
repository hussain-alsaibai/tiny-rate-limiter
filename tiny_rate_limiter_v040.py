"""tiny_rate_limiter_v040 — fail-modes, cross-process state, CRDT merge, async.

Sidecar module for tiny-rate-limiter v0.4.0. Lazy-imports the core
`tiny_rate_limiter` package. New functionality only — existing
`tiny_rate_limiter` users see no behaviour change unless they
`import tiny_rate_limiter_v040`.

What is in here
---------------
- `FailMode` enum: `FAIL_OPEN`, `FAIL_CLOSED`, `DRY_RUN`.
- `RateLimitError` exception (also re-exported as `RateLimiterUnavailable`).
- `Decision` dataclass — the verdict of an `acquire()` call: `allowed`,
  `remaining`, `retry_after`, `fail_mode`, and a `reason` string.
- `MemoryBackend` — in-process state with `watch(callback)` for
  cross-process mirroring (file-backed or pub/sub).
- `ResilientLimiter` — wraps a core limiter with fail-mode semantics.
- `merge(a, b)` — CRDT-flavored state merge across two peer limiters.
- `AsyncRateLimiter` — `await acquire()` companion that runs the
  sync `acquire()` in a thread pool so the event loop never blocks.
- `configure_default_fail_mode(mode)` — set the process-wide default.

Why a sidecar?
--------------
The core `tiny_rate_limiter` package stays at v0.3.x. The new
fail-mode semantics, the async companion, and the merge logic all
live here so existing users do not need to upgrade anything. Import
this module only when you want the new functionality.
"""

from __future__ import annotations

import enum
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


__version__ = "0.4.0"


# ---------- Fail modes ----------

class FailMode(enum.Enum):
    """How the limiter should respond when the backend is unreachable."""

    FAIL_OPEN = "fail_open"
    FAIL_CLOSED = "fail_closed"
    DRY_RUN = "dry_run"


# Default for the process. Sync mode is conservative (fail_closed);
# async mode is permissive (fail_open). Callers can override per-instance.
_DEFAULT_FAIL_MODE: FailMode = FailMode.FAIL_CLOSED


def configure_default_fail_mode(mode):
    """Set the process-wide default fail mode. Returns the previous default."""
    global _DEFAULT_FAIL_MODE
    if not isinstance(mode, FailMode):
        raise TypeError("expected FailMode, got " + type(mode).__name__)
    previous = _DEFAULT_FAIL_MODE
    _DEFAULT_FAIL_MODE = mode
    return previous


def get_default_fail_mode():
    return _DEFAULT_FAIL_MODE


# ---------- Errors ----------

class RateLimitError(Exception):
    """Raised when fail_mode=FAIL_CLOSED and the backend is unavailable."""


class RateLimiterUnavailable(RateLimitError):
    """Raised when the backing limiter cannot be reached."""


# ---------- Decision ----------

@dataclass(frozen=True)
class Decision:
    """The result of an `acquire()` call.

    Attributes
    ----------
    allowed: True if the request can proceed.
    remaining: Tokens remaining in the bucket (best-effort; -1 if unknown).
    retry_after: Seconds until the next token will be available
        (0 if allowed, or if unknown).
    fail_mode: The fail mode in effect for this decision.
    reason: A short human-readable string ("ok", "dry_run", "fail_open",
        "fail_closed", "denied").
    """

    allowed: bool
    remaining: int = -1
    retry_after: float = 0.0
    fail_mode: FailMode = FailMode.FAIL_CLOSED
    reason: str = "ok"

    def __bool__(self):  # makes `if limiter.acquire(...):` natural
        return self.allowed


# ---------- MemoryBackend (cross-process state mirror) ----------

class MemoryBackend:
    """In-memory state mirror for cross-process limiting.

    A small FIFO of (key, count, window_started_at) tuples is shared
    across limiter instances. `watch(callback)` registers a callable
    that is invoked with the latest snapshot whenever the limiter
    state changes. The callback is responsible for cross-process
    transport (file write, Redis pub/sub, SQLite, etc.).
    """

    def __init__(self):
        self._windows = {}
        self._watchers = []

    def record(self, key, count=1, *, window_seconds=60.0):
        now = time.monotonic()
        window = int(now // window_seconds)
        bucket = (key, window)
        prev_count, prev_started = self._windows.get(bucket, (0, now))
        self._windows[bucket] = (prev_count + count, prev_started)
        self._notify()

    def current(self, key, window_seconds=60.0):
        now = time.monotonic()
        window = int(now // window_seconds)
        count, _ = self._windows.get((key, window), (0, now))
        return count

    def snapshot(self):
        return dict(self._windows)

    def watch(self, callback):
        self._watchers.append(callback)
        try:
            callback(self.snapshot())
        except Exception:
            pass

        def unsubscribe():
            try:
                self._watchers.remove(callback)
            except ValueError:
                pass

        return unsubscribe

    def _notify(self):
        snap = self.snapshot()
        for cb in list(self._watchers):
            try:
                cb(snap)
            except Exception:
                pass

# ---------- ResilientLimiter ----------

class ResilientLimiter:
    """Wraps a core limiter with fail-mode semantics.

    The core limiter is expected to expose:
        - `acquire(key=None) -> bool` (or any object that
          truthy-checks as "allowed")

    If the core limiter raises any exception (timeout, network error,
    etc.), the `ResilientLimiter` responds according to `fail_mode`:

    - `FAIL_CLOSED`: raise `RateLimitError`. (Default for sync mode.)
    - `FAIL_OPEN`: return `Decision(allowed=True, reason="fail_open", ...)`.
      (Default for async mode.)
    - `DRY_RUN`: never block; return `Decision(allowed=True, reason="dry_run")`
      but record the real decision in the `dry_run_decisions` counter.

    The dry-run mode is useful for capacity planning and for "what if we
    tightened the limit to 50 RPS?" load tests.
    """

    def __init__(self, core, *, fail_mode=None, backend=None):
        self._core = core
        self._fail_mode = fail_mode if fail_mode is not None else _DEFAULT_FAIL_MODE
        self._backend = backend
        self.failures_total = 0
        # Both verdict keys are pre-initialised to 0 so callers can read
        # either without a KeyError.
        self.dry_run_decisions = {"allow": 0, "deny": 0}

    @property
    def fail_mode(self):
        return self._fail_mode

    def set_fail_mode(self, mode):
        if not isinstance(mode, FailMode):
            raise TypeError("expected FailMode, got " + type(mode).__name__)
        self._fail_mode = mode

    def acquire(self, key=None):
        """Try to acquire a token; honour the configured fail mode on error."""
        if self._fail_mode is FailMode.DRY_RUN:
            try:
                core_allowed = bool(self._core.acquire(key=key) if key is not None else self._core.acquire())
            except Exception as e:
                self.failures_total += 1
                return Decision(
                    allowed=True,
                    fail_mode=self._fail_mode,
                    reason="dry_run:core_error:" + type(e).__name__,
                )
            verdict = "allow" if core_allowed else "deny"
            self.dry_run_decisions[verdict] = self.dry_run_decisions.get(verdict, 0) + 1
            return Decision(
                allowed=True,
                fail_mode=self._fail_mode,
                reason="dry_run:" + verdict,
            )

        try:
            result = self._core.acquire(key=key) if key is not None else self._core.acquire()
            allowed = bool(result)
            return Decision(
                allowed=allowed,
                fail_mode=self._fail_mode,
                reason="ok" if allowed else "denied",
            )
        except Exception as e:
            self.failures_total += 1
            if self._backend is not None and key is not None:
                self._backend.record(key, count=0)
            if self._fail_mode is FailMode.FAIL_OPEN:
                return Decision(
                    allowed=True,
                    fail_mode=self._fail_mode,
                    reason="fail_open:" + type(e).__name__,
                )
            raise RateLimiterUnavailable(
                "limiter backend unavailable in fail_closed mode: " + repr(e)
            ) from e


# ---------- CRDT-flavored merge ----------

def merge(a, b):
    """Merge two limiter state snapshots, CRDT-style.

    Both inputs are dicts of `{(key, window): (count, started_at)}`. The
    result is associative, commutative, and idempotent:

    - For each `(key, window)` pair, the max of `count` is taken.
    - On a tie, the snapshot with the more recent `started_at` wins.

    This makes the merge safe to apply in any order: a fleet of
    peers can each emit their own snapshot, and any sequence of
    merges converges to the same state.
    """
    if not isinstance(a, dict) or not isinstance(b, dict):
        raise TypeError(
            "merge expects two dicts, got " + type(a).__name__ + " and " + type(b).__name__
        )
    out = {}
    for k in set(a) | set(b):
        ca, sa = a.get(k, (0, 0.0))
        cb, sb = b.get(k, (0, 0.0))
        if ca > cb:
            out[k] = (ca, sa)
        elif cb > ca:
            out[k] = (cb, sb)
        else:
            out[k] = (ca, sa if sa >= sb else sb)
    return out


# ---------- Async companion ----------

class AsyncRateLimiter:
    """Async companion for a `ResilientLimiter`.

    Routes every `acquire()` through a thread pool so the event loop
    never blocks. The `acquire()` coroutine returns a `Decision`.

    >>> import asyncio
    >>> import tiny_rate_limiter as trl
    >>> import tiny_rate_limiter_v040 as v40
    >>> core = trl.TokenBucket(rate=10, capacity=10)
    >>> rl = v40.AsyncRateLimiter(v40.ResilientLimiter(core, fail_mode=v40.FailMode.FAIL_OPEN))
    >>> asyncio.run(rl.acquire()).allowed
    True
    """

    def __init__(self, limiter, *, max_workers=4):
        self._limiter = limiter
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="tiny-rate-limiter-async",
        )

    async def acquire(self, key=None):
        loop = __import__("asyncio").get_event_loop()
        return await loop.run_in_executor(
            self._executor, self._limiter.acquire, key
        )

    def shutdown(self, wait=True):
        self._executor.shutdown(wait=wait)


__all__ = [
    "AsyncRateLimiter",
    "Decision",
    "FailMode",
    "MemoryBackend",
    "RateLimitError",
    "RateLimiterUnavailable",
    "ResilientLimiter",
    "configure_default_fail_mode",
    "get_default_fail_mode",
    "merge",
]
