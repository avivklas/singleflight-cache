# singleflight-cache

A high-performance, thread-safe local cache for Python featuring **Single-Flight** execution, **Inflight Disowning**, and **O(1) LRU Eviction**.

## The Problem it Solves
If multiple threads simultaneously experience a cache miss for the same key, traditional caches will queue them up behind a lock, or allow all of them to execute the expensive fetch operation concurrently ("thundering herd"). 

`singleflight-cache` solves this gracefully:
- **Single-Flight Execution:** Only the first thread executes the data fetch. Other concurrent threads requesting the same key wait passively for the first thread's result without duplicating work.
- **Inflight Disowning & Invalidation Protection:** Explicit `set()`, `remove()`, `invalidate()`, or `clear()` calls disown ongoing in-flight computations so stale values never overwrite newly cached data.
- **Reentrancy Guard:** Detects recursive calls on the same thread for the same key to prevent deadlocks with clear `RecursionError` diagnostics.
- **O(1) LRU & Bounded Eviction:** Uses `OrderedDict` for true O(1) LRU order updates and bounded eviction scanning.
- **TTL Expiration:** Optional Time-To-Live expiration per key.

## Installation

```bash
pip install singleflight-cache
```

## Usage

```python
from singleflight_cache import SingleFlightCache  # FastCache is also available as an alias

# Create a cache with up to 1000 items and a 60-second TTL
cache = SingleFlightCache(max_size=1000, ttl=60)

def fetch_user_data(user_id):
    # This expensive operation will only run ONCE even if 100 threads 
    # ask for 'user_123' at the exact same moment.
    response = requests.get(f"https://api.example.com/users/{user_id}")
    return response.json()

# Single-Flight execution:
data = cache.get("user_123", fetch_user_data, "user_123")

# Direct cache read (without factory):
value = cache.get("user_123", default=None)

# Direct write (disowns any running in-flight computation):
cache.set("user_123", {"name": "Alice"})

# Explicit invalidation (e.g. after a write). Returns True if an entry was cached:
cache.invalidate("user_123")  # or cache.remove("user_123")
# The next get() fetches fresh data, even if a fetch was in flight during invalidation.
```

## Features

### In-Flight Disowning
If key `k` is removed or overwritten (`cache.set("k", new_val)`) while a slow generator is still computing in the background, `singleflight-cache` disowns the running computation. When the slow thread finishes, it detects it no longer owns the slot and discards the stale result instead of corrupting the cache.

### Reentrancy Protection
Attempting to recursively request the same key from within its own generator function raises a `RecursionError` instead of causing a silent thread deadlock.

## Benchmarks

We simulated a highly concurrent environment experiencing a heavy cache miss ("thundering herd" scenario):
- **5 distinct keys** requested simultaneously.
- **10 concurrent threads per key** (50 threads total).
- Simulated **0.5s** underlying fetch time per miss.

| Library | Total Time | Fetch Executions | Notes |
|---------|------------|------------------|-------|
| `functools.lru_cache` | 0.507s | 50 | Fast, but suffers from a massive **Thundering Herd**. |
| `cachetools` + Global Lock | 2.522s | 5 | Prevents herd, but creates a **Global Bottleneck**. |
| **`singleflight-cache`** | **0.507s** | **5** | **Perfect.** Fully concurrent and strictly single-flight. |
