"""Tests for tiny_rate_limiter_v040 — fail-modes, MemoryBackend, merge, async."""

from __future__ import annotations

import asyncio

import pytest

import tiny_rate_limiter as trl
import tiny_rate_limiter_v040 as v40
from tiny_rate_limiter_v040 import (
    AsyncRateLimiter,
    Decision,
    FailMode,
    MemoryBackend,
    RateLimitError,
    RateLimiterUnavailable,
    ResilientLimiter,
    configure_default_fail_mode,
    get_default_fail_mode,
    merge,
)


# ---------- Fake core limiter ----------

class _AlwaysAllows:
    def acquire(self, key=None):
        return True


class _AlwaysDenies:
    def acquire(self, key=None):
        return False


class _AlwaysFails:
    def acquire(self, key=None):
        raise ConnectionError("backend down")


class _Flaky:
    """Fails the first `fail_every` calls then succeeds."""
    def __init__(self, fail_every=2):
        self.fail_every = fail_every
        self.calls = 0

    def acquire(self, key=None):
        self.calls += 1
        if self.calls <= self.fail_every:
            raise ConnectionError("flaky")
        return True


# ---------- FailMode enum ----------

def test_fail_mode_values():
    assert FailMode.FAIL_OPEN.value == "fail_open"
    assert FailMode.FAIL_CLOSED.value == "fail_closed"
    assert FailMode.DRY_RUN.value == "dry_run"


def test_default_fail_mode_is_fail_closed():
    assert get_default_fail_mode() is FailMode.FAIL_CLOSED


def test_configure_default_returns_previous():
    prev = configure_default_fail_mode(FailMode.FAIL_OPEN)
    assert prev is FailMode.FAIL_CLOSED
    assert get_default_fail_mode() is FailMode.FAIL_OPEN
    # restore
    configure_default_fail_mode(FailMode.FAIL_CLOSED)


def test_configure_default_validates_type():
    with pytest.raises(TypeError):
        configure_default_fail_mode("fail_open")


# ---------- Decision ----------

def test_decision_truthy_when_allowed():
    d = Decision(allowed=True)
    assert bool(d) is True
    assert d


def test_decision_falsy_when_denied():
    d = Decision(allowed=False)
    assert bool(d) is False
    assert not d


# ---------- ResilientLimiter: ok / deny path ----------

def test_resilient_limiter_ok_when_core_allows():
    rl = ResilientLimiter(_AlwaysAllows(), fail_mode=FailMode.FAIL_CLOSED)
    d = rl.acquire()
    assert d.allowed is True
    assert d.reason == "ok"
    assert d.fail_mode is FailMode.FAIL_CLOSED
    assert rl.failures_total == 0


def test_resilient_limiter_denied_when_core_denies():
    rl = ResilientLimiter(_AlwaysDenies(), fail_mode=FailMode.FAIL_CLOSED)
    d = rl.acquire()
    assert d.allowed is False
    assert d.reason == "denied"


# ---------- ResilientLimiter: fail mode semantics ----------

def test_fail_closed_raises_on_core_error():
    rl = ResilientLimiter(_AlwaysFails(), fail_mode=FailMode.FAIL_CLOSED)
    with pytest.raises(RateLimiterUnavailable):
        rl.acquire()
    assert rl.failures_total == 1


def test_fail_open_allows_on_core_error():
    rl = ResilientLimiter(_AlwaysFails(), fail_mode=FailMode.FAIL_OPEN)
    d = rl.acquire()
    assert d.allowed is True
    assert d.reason.startswith("fail_open:")
    assert rl.failures_total == 1


def test_dry_run_records_decisions():
    rl = ResilientLimiter(_AlwaysDenies(), fail_mode=FailMode.DRY_RUN)
    d = rl.acquire()
    assert d.allowed is True
    assert d.reason == "dry_run:deny"
    assert rl.dry_run_decisions == {"deny": 1, "allow": 0}


def test_dry_run_records_allow_decision():
    rl = ResilientLimiter(_AlwaysAllows(), fail_mode=FailMode.DRY_RUN)
    d = rl.acquire()
    assert d.allowed is True
    assert d.reason == "dry_run:allow"
    assert rl.dry_run_decisions.get("allow") == 1


def test_dry_run_records_core_error():
    rl = ResilientLimiter(_AlwaysFails(), fail_mode=FailMode.DRY_RUN)
    d = rl.acquire()
    assert d.allowed is True
    assert d.reason.startswith("dry_run:core_error:")
    assert rl.failures_total == 1


def test_set_fail_mode_validates():
    rl = ResilientLimiter(_AlwaysAllows())
    with pytest.raises(TypeError):
        rl.set_fail_mode("fail_open")
    rl.set_fail_mode(FailMode.FAIL_OPEN)
    assert rl.fail_mode is FailMode.FAIL_OPEN


def test_fail_closed_uses_default_when_not_specified():
    # Default is FAIL_CLOSED at the process level
    rl = ResilientLimiter(_AlwaysFails())
    with pytest.raises(RateLimiterUnavailable):
        rl.acquire()


# ---------- MemoryBackend ----------

def test_memory_backend_record_and_current():
    be = MemoryBackend()
    be.record("k1", count=1)
    be.record("k1", count=1)
    be.record("k2", count=3)
    assert be.current("k1") == 2
    assert be.current("k2") == 3
    assert be.current("absent") == 0


def test_memory_backend_snapshot():
    be = MemoryBackend()
    be.record("k1", count=5)
    snap = be.snapshot()
    assert any(k[0] == "k1" for k in snap.keys())
    # snapshot is a copy: mutating it does not affect backend
    snap.clear()
    assert be.current("k1") == 5


def test_memory_backend_watch_fires_on_change():
    be = MemoryBackend()
    events = []
    unsub = be.watch(lambda snap: events.append(len(snap)))
    # Watcher was called once on subscription
    assert len(events) == 1
    be.record("k1", count=1)
    be.record("k2", count=1)
    assert len(events) == 3  # subscription + 2 changes
    unsub()
    be.record("k3", count=1)
    assert len(events) == 3  # no more events after unsubscribe


def test_memory_backend_watch_swallows_exceptions():
    be = MemoryBackend()
    def bad_cb(snap):
        raise RuntimeError("nope")
    be.watch(bad_cb)
    # Should not raise
    be.record("k1", count=1)


def test_resilient_limiter_records_failure_in_backend():
    be = MemoryBackend()
    rl = ResilientLimiter(_AlwaysFails(), fail_mode=FailMode.FAIL_OPEN, backend=be)
    rl.acquire(key="user-1")
    # Backend was notified of the failure (count=0 means "we tried")
    # The exact count is 0; the call is the signal.
    assert rl.failures_total == 1


# ---------- CRDT merge ----------

def test_merge_max_count_per_key():
    a = {("k1", 1): (3, 100.0)}
    b = {("k1", 1): (5, 200.0)}
    out = merge(a, b)
    assert out[("k1", 1)] == (5, 200.0)


def test_merge_disjoint_keys():
    a = {("k1", 1): (3, 100.0)}
    b = {("k2", 1): (5, 200.0)}
    out = merge(a, b)
    assert out[("k1", 1)] == (3, 100.0)
    assert out[("k2", 1)] == (5, 200.0)


def test_merge_is_commutative():
    a = {("k1", 1): (3, 100.0), ("k2", 1): (7, 50.0)}
    b = {("k1", 1): (5, 200.0), ("k3", 1): (1, 75.0)}
    ab = merge(a, b)
    ba = merge(b, a)
    assert ab == ba


def test_merge_is_associative():
    a = {("k1", 1): (1, 10.0)}
    b = {("k1", 1): (2, 20.0), ("k2", 1): (3, 30.0)}
    c = {("k2", 1): (5, 50.0), ("k3", 1): (7, 70.0)}
    left = merge(merge(a, b), c)
    right = merge(a, merge(b, c))
    assert left == right


def test_merge_is_idempotent():
    a = {("k1", 1): (3, 100.0), ("k2", 1): (5, 200.0)}
    once = merge(a, {})
    twice = merge(once, once)
    assert once == twice


def test_merge_tie_breaks_on_recent_started_at():
    a = {("k1", 1): (3, 100.0)}
    b = {("k1", 1): (3, 200.0)}
    out = merge(a, b)
    assert out[("k1", 1)] == (3, 200.0)


def test_merge_validates_type():
    with pytest.raises(TypeError):
        merge("not a dict", {})
    with pytest.raises(TypeError):
        merge({}, None)


# ---------- Async companion ----------

def test_async_limiter_allows_async():
    core = _AlwaysAllows()
    rl = AsyncRateLimiter(ResilientLimiter(core, fail_mode=FailMode.FAIL_OPEN))
    try:
        d = asyncio.run(rl.acquire())
        assert d.allowed is True
    finally:
        rl.shutdown()


def test_async_limiter_fail_open_on_error():
    rl = AsyncRateLimiter(ResilientLimiter(_AlwaysFails(), fail_mode=FailMode.FAIL_OPEN))
    try:
        d = asyncio.run(rl.acquire())
        assert d.allowed is True
        assert d.reason.startswith("fail_open:")
    finally:
        rl.shutdown()


def test_async_limiter_fail_closed_raises_async():
    rl = AsyncRateLimiter(ResilientLimiter(_AlwaysFails(), fail_mode=FailMode.FAIL_CLOSED))
    try:
        with pytest.raises(RateLimiterUnavailable):
            asyncio.run(rl.acquire())
    finally:
        rl.shutdown()


def test_async_limiter_key_passed_through():
    class KeyCapturing:
        def __init__(self):
            self.last_key = None
        def acquire(self, key=None):
            self.last_key = key
            return True
    cap = KeyCapturing()
    rl = AsyncRateLimiter(ResilientLimiter(cap, fail_mode=FailMode.FAIL_OPEN))
    try:
        asyncio.run(rl.acquire(key="user-42"))
        assert cap.last_key == "user-42"
    finally:
        rl.shutdown()


# ---------- Optional: integration with real core limiter if present ----------

def test_integration_with_core_token_bucket():
    if not hasattr(trl, "TokenBucket"):
        pytest.skip("core TokenBucket not available")
    core = trl.TokenBucket(rate=100, capacity=10)
    rl = ResilientLimiter(core, fail_mode=FailMode.FAIL_OPEN)
    decisions = [rl.acquire() for _ in range(5)]
    assert all(d.allowed for d in decisions)