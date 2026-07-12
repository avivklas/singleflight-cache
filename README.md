# singleflight-cache

A high-performance thread-safe local cache for Python, featuring **Single-Flight** resolution.

## The Problem it Solves
If multiple threads simultaneously experience a cache miss for the same key, traditional caches will queue them up behind a lock, or allow all of them to execute the expensive fetch operation concurrently ("thundering herd"). 

`singleflight-cache` solves this gracefully:
- **Single-Flight:** Only the first thread executes the data fetch. Other concurrent threads requesting the same key wait passively for the first thread's result without duplicating work.
- **Lock-Free Reads:** Cache hits operate without any read locks, making highly concurrent access incredibly fast.
- **Built-in Eviction:** Supports TTL (Time-To-Live) and max-size LRU eviction.

## Installation

```bash
pip install singleflight-cache
```

## Usage

```python
from singleflight_cache import FastCache
import requests

# Create a cache with up to 1000 items and a 60-second TTL
cache = FastCache(max_size=1000, ttl=60)

def fetch_user_data(user_id):
    # This expensive operation will only run ONCE even if 100 threads 
    # ask for 'user_123' at the exact same moment.
    response = requests.get(f"https://api.example.com/users/{user_id}")
    return response.json()

# In your worker threads:
data = cache.get("user_123", fetch_user_data, "user_123")
```

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
