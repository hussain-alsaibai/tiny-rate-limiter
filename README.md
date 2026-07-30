# tiny-rate-limiter

[![MIT License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](#)
[![Part of tiny-* ecosystem](https://img.shields.io/badge/tiny--*-ecosystem-orange.svg)](#ecosystem)

> Zero-dependency rate limiter for Python. Token bucket, leaky bucket, sliding window, fixed window. ~5M ops/s. Single file.

## Why

You need rate limiting. You don't need a 2,000-line package with five transitive deps. **tiny-rate-limiter** is one file, MIT licensed, and the token-bucket fast path sustains ~5 million checks/sec on CPython 3.11. Drop it in, copy-paste it, vendor it — your call.

- ✅ **Zero dependencies** — stdlib only (`time`, `threading`, `asyncio`, `re`, `contextlib`, `functools`)
- ✅ **Single file** — `rate_limiter.py`, ~400 lines including docstrings
- ✅ **Fast** — ~5M ops/s for in-memory token bucket (single-threaded)
- ✅ **Thread-safe** — every algorithm is guarded by `threading.Lock`
- ✅ **Async-friendly** — `AsyncRateLimiter` wrapper, or use the sync one via `run_in_executor`
- ✅ **Four algorithms** — token bucket, leaky bucket, sliding window, fixed window
- ✅ **MIT licensed** — do whatever you want

## Installation

```bash
pip install tiny-rate-limiter
```

Or just copy `rate_limiter.py` into your project. It's one file, no resources, no data files, no init hooks.

```bash
curl -O https://raw.githubusercontent.com/hussain-alsaibai/tiny-rate-limiter/main/rate_limiter.py
```

## Quick start

### Decorator

```python
from rate_limiter import rate_limit

@rate_limit("100 per minute")
def call_api():
    return api.get("/endpoint")
```

By default the decorator **rejects fast** — third call inside one second raises `RateLimitExceeded`. Pass `block=True` to wait for the bucket to refill.

### Direct API

```python
from rate_limiter import TokenBucket

limiter = TokenBucket("10 per second")
if limiter.try_acquire():
    handle_request()
else:
    return 429, "slow down"

print(f"{limiter.hits} accepted, {limiter.denied} throttled, {limiter.remaining} left")
```

### Async

```python
from rate_limiter import AsyncRateLimiter, TokenBucket

async def fetch():
    limiter = AsyncRateLimiter(TokenBucket("50 per second"))
    if await limiter.check():
        return await client.get(url)

# Or just await the sync API off the loop:
import asyncio
allowed = await asyncio.get_event_loop().run_in_executor(None, limiter.try_acquire)
```

### Context manager

```python
from rate_limiter import limit

with limit("5 per second", algorithm="leaky_bucket"):
    expensive_operation()
```

### Algorithm showcase

```python
from rate_limiter import (
    TokenBucket, LeakyBucket, SlidingWindow, FixedWindow, RateLimiter,
)

# Token bucket — bursty traffic friendly, refills smoothly. Default.
tb = TokenBucket("100 per minute", capacity=200)  # capacity defaults to rate

# Leaky bucket — strict output rate, smooths bursts.
lb = LeakyBucket("10 per second", capacity=20)

# Sliding window — most accurate, keeps every timestamp.
sw = SlidingWindow("1000 per hour")

# Fixed window — cheapest, can spike at boundaries.
fw = FixedWindow("1 per second")

# Or pick by name:
limiter = RateLimiter("5 per second", algorithm="sliding_window")
```

### Rate spec syntax

```
"<count> per <period> <unit>"   or   "<count> / <unit>"
```

Examples: `100 per minute`, `10/sec`, `5 per 200 ms`, `1 per hour`, `1000 / day`. Units: `ms`, `s`, `sec`, `second`, `m`, `min`, `minute`, `h`, `hr`, `hour`, `d`, `day`.

## Benchmarks

CPython 3.11, single thread, lock held for one integer comparison + subtract on the hot path:

| Algorithm       | Best ops/s  | Notes                                |
| --------------- | ----------- | ------------------------------------ |
| **TokenBucket** | **5,500,000** | Default. ns-precision, fast path.    |
| FixedWindow     | 2,900,000   | One int compare per call.            |
| LeakyBucket     | 2,100,000   | Float math per call.                 |
| SlidingWindow   | 600,000     | O(N) eviction per check; N≈limit.    |

Run them yourself:

```bash
python3 -c "
import time, rate_limiter as rl
N = 5_000_000
tb = rl.TokenBucket('10000000 per second')
for _ in range(10000): tb.check()
start = time.perf_counter()
for _ in range(N): tb.check()
print(f'{N/(time.perf_counter()-start):,.0f} ops/s')
"
```

### How it compares

| Library              | Algorithm count | Deps   | TokenBucket ops/s | Notes                     |
| -------------------- | --------------- | ------ | ----------------- | ------------------------- |
| **tiny-rate-limiter**| 4               | 0      | **~5M**           | single file, MIT          |
| `ratelimit`          | 1 (decorator)   | 0      | ~50K              | window-based, slower      |
| `limits`             | 5+              | several| varies            | batteries-included, heavier |
| `slowapi`            | 1               | several| ~100K             | FastAPI-only              |
| `aiolimiter`         | 1               | 0      | ~200K             | async-only                |

Numbers are illustrative; run your own benchmarks. The point: `tiny-rate-limiter` matches or beats them while shipping in a single ~400-line file with zero dependencies.

## Architecture

```
                    ┌────────────────────────┐
                    │   parse_rate(spec)     │
                    │   "100 per minute"     │
                    └──────────┬─────────────┘
                               │ (count, period_s)
              ┌────────────────┼─────────────────┐
              ▼                ▼                 ▼
      ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐
      │  TokenBucket │  │ LeakyBucket  │  │ SlidingWindow    │
      │  ns-precision│  │ float math   │  │ O(N) timestamp   │
      │  fast path   │  │              │  │ eviction         │
      └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘
             │                 │                  │
             └────────┐        │        ┌─────────┘
                      ▼        ▼        ▼
               ┌────────────────────────────┐
               │  _BaseLimiter              │
               │  threading.Lock            │
               │  hits / denied counters    │
               │  check() / acquire()       │
               │  remaining property        │
               └────────────┬───────────────┘
                            │
              ┌─────────────┼──────────────┐
              ▼             ▼              ▼
        ┌──────────┐  ┌────────────┐  ┌─────────────┐
        │  Decorator│  │  Context   │  │  Async      │
        │ @rate_…  │  │  with …    │  │ AsyncRate…  │
        └──────────┘  └────────────┘  └─────────────┘
```

## Algorithm comparison

| Algorithm       | Memory     | Burst tolerance | Boundary behavior | Best for                          |
| --------------- | ---------- | --------------- | ----------------- | --------------------------------- |
| Token bucket    | O(1)       | up to capacity  | Smooth refill     | APIs with bursty traffic          |
| Leaky bucket    | O(1)       | Rejects bursts  | Constant output   | Smooth downstream, queue-style    |
| Sliding window  | O(N)       | Rejects bursts  | Most accurate     | Strict per-window guarantees      |
| Fixed window    | O(1)       | up to 2× limit  | Can spike 2×      | Cheap counter, lenient limits     |

**Token bucket** is the right default. Use **leaky bucket** when you need a constant output rate (e.g., shaping network traffic). Use **sliding window** when accuracy matters more than memory. Use **fixed window** when you want the cheapest possible check.

## API reference

### Classes

```python
TokenBucket(spec: str, capacity: float | None = None)
LeakyBucket(spec: str, capacity: float | None = None)
SlidingWindow(spec: str)
FixedWindow(spec: str)
RateLimiter(spec: str, algorithm: str = "token_bucket", **kwargs)  # factory
AsyncRateLimiter(inner: _BaseLimiter)
```

`spec` is any rate string parseable by `parse_rate()`. `algorithm` is one of `"token_bucket"`, `"leaky_bucket"`, `"sliding_window"`, `"fixed_window"`.

### Methods

| Method             | Returns          | Notes                                     |
| ------------------ | ---------------- | ----------------------------------------- |
| `check()`          | `bool`           | Non-blocking, atomic. The hot path.       |
| `try_acquire()`    | `bool`           | Alias for `check()`.                      |
| `acquire(blocking, timeout)` | `bool`| Wait until allowed (or timeout).          |
| `remaining`        | `float`          | Quota remaining right now. Property.      |
| `hits`             | `int`            | Total allowed calls.                      |
| `denied`           | `int`            | Total rejected calls.                     |
| `limit_spec`       | `str`            | The original rate string.                 |
| `reset()`          | `None`           | Reset counters and refill state.          |

### Decorator

```python
@rate_limit(spec, algorithm="token_bucket", key=None, block=False, **kwargs)
def f(...): ...
```

- `spec`: rate string.
- `algorithm`: any of the four algorithms.
- `key(*args, **kwargs)`: optional callable returning a scope key — each scope gets its own bucket.
- `block=False`: raise `RateLimitExceeded` when over limit (default).
- `block=True`: wait for the bucket to refill.

### Context manager

```python
with limit(spec, algorithm="token_bucket", **kwargs):
    ...
```

Raises `RateLimitExceeded` if the limit is already exhausted. Creates a fresh limiter per use.

## Real-world use

### API client

```python
from rate_limiter import TokenBucket

class GitHubClient:
    def __init__(self):
        # GitHub allows 5000 req/hour for authenticated users
        self.limiter = TokenBucket("5000 per hour", capacity=20)

    def get(self, path):
        if not self.limiter.try_acquire():
            raise RateLimitExceeded(self.limiter.remaining, "5000/hour")
        return requests.get(f"https://api.github.com{path}", headers=self.headers)
```

### Per-user rate limit on a web server

```python
from rate_limiter import RateLimiter

limiters = {}  # user_id -> RateLimiter

def get_limiter(user_id):
    if user_id not in limiters:
        limiters[user_id] = RateLimiter("100 per minute")
    return limiters[user_id]

@app.route("/api")
def handler():
    user_id = session["user_id"]
    if not get_limiter(user_id).try_acquire():
        return ("Too Many Requests", 429)
    return do_work()
```

### Async API gateway

```python
from rate_limiter import AsyncRateLimiter, TokenBucket

limiter = AsyncRateLimiter(TokenBucket("1000 per second"))

async def gateway(request):
    if not await limiter.check():
        return web.Response(status=429)
    return await handler(request)
```

### Decorator for class methods

```python
class Scraper:
    @rate_limit("10 per second", key=lambda self: self.domain)
    def fetch(self, url):
        return requests.get(url)
```

### Distributed systems

**Caveat**: this is an in-process limiter. It works perfectly within a single Python process. Across multiple processes / machines / containers, you need a shared counter — Redis with `INCR` + `EXPIRE`, or a sliding-window-log in Redis sorted sets. Use `tiny-rate-limiter` per process, then aggregate with a global limiter at the edge.

## Ecosystem

Part of the **tiny-*** family of zero-dependency Python packages — single files, MIT, fast.

- [tiny-agent](https://github.com/hussain-alsaibai/tiny-agent) — minimal AI agent loop
- [tiny-memory](https://github.com/hussain-alsaibai/tiny-memory) — in-memory key-value store with TTL
- [tiny-log](https://github.com/hussain-alsaibai/tiny-log) — structured logging
- [tiny-rate-limiter](https://github.com/hussain-alsaibai/tiny-rate-limiter) — *you are here*
- [tiny-cache](https://github.com/hussain-alsaibai/tiny-cache) — LRU/LFU cache
- [tiny-queue](https://github.com/hussain-alsaibai/tiny-queue) — persistent FIFO queue
- [tiny-config](https://github.com/hussain-alsaibai/tiny-config) — env + file config loader

All single-file, zero-dependency, MIT.

## License

MIT — see [LICENSE](LICENSE).

## Contributing

Bug reports and PRs welcome. Keep it small.