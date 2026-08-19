"""Tests for tiny-rate-limiter."""
import asyncio, time, pytest
from rate_limiter import TokenBucketRateLimiter, SlidingWindowRateLimiter, AcquireTimeout

def test_token_bucket_basic():
    tb = TokenBucketRateLimiter(rate=10, capacity=10, name="test")
    assert tb.capacity == 10
    assert tb.available == 10.0
    async def run():
        await tb.acquire()
        assert tb.available < 10.0
    asyncio.run(run())

def test_token_bucket_refill():
    tb = TokenBucketRateLimiter(rate=100, capacity=10, name="test")
    async def run():
        for _ in range(10): await tb.acquire()
        assert tb.available == 0.0
        await asyncio.sleep(0.15)
        assert tb.available >= 10.0
    asyncio.run(run())

def test_token_bucket_try_acquire():
    tb = TokenBucketRateLimiter(rate=1, capacity=2, name="test")
    assert tb.try_acquire()[0] is True
    assert tb.try_acquire()[0] is True
    allowed, retry = tb.try_acquire()
    assert allowed is False
    assert retry > 0

def test_token_bucket_stats():
    tb = TokenBucketRateLimiter(rate=5, capacity=10, name="test")
    asyncio.run(tb.acquire())
    stats = tb.get_stats()
    assert stats["algorithm"] == "token_bucket"
    assert stats["rate"] == 5

def test_sliding_window_basic():
    sw = SlidingWindowRateLimiter(max_requests=5, window=60, name="test")
    async def run():
        for i in range(5):
            allowed, _ = await sw.try_acquire()
            assert allowed is True
        allowed, _ = await sw.try_acquire()
        assert allowed is False
    asyncio.run(run())

def test_sliding_window_expires():
    sw = SlidingWindowRateLimiter(max_requests=2, window=0.1, name="test")
    async def run():
        await sw.try_acquire()
        await sw.try_acquire()
        allowed, _ = await sw.try_acquire()
        assert allowed is False
        await asyncio.sleep(0.15)
        allowed, _ = await sw.try_acquire()
        assert allowed is True
    asyncio.run(run())

def test_sliding_window_stats():
    sw = SlidingWindowRateLimiter(max_requests=10, window=60, name="test")
    async def run():
        for _ in range(3): await sw.try_acquire()
        stats = sw.get_stats()
        assert stats["algorithm"] == "sliding_window"
        assert stats["requests_in_window"] == 3
        assert stats["remaining"] == 7
    asyncio.run(run())

def test_sliding_window_acquire_timeout():
    sw = SlidingWindowRateLimiter(max_requests=1, window=60, name="test")
    async def run():
        await sw.try_acquire()
        with pytest.raises(AcquireTimeout):
            await sw.acquire(timeout=0.1)
    asyncio.run(run())

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
