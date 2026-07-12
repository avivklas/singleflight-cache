import time
import threading
from concurrent.futures import Future


class CacheEntry:
    __slots__ = ('value', 'expiry', 'accessed')

    def __init__(self, value, ttl):
        self.value = value
        self.expiry = time.monotonic() + ttl if ttl is not None else None
        self.accessed = time.monotonic()


class FastCache:
    """
    High-performance thread-safe local cache with Single-Flight logic.
    
    Features:
    - Single-Flight: Concurrent cache misses for the same key trigger the underlying
      fetch operation exactly once. Other threads wait for the result.
    - Optimized Locking: Reads (cache hits) are lock-free and extremely fast.
      A lock is only acquired during a cache miss to handle inflight futures and writes.
    - TTL & LRU: Supports time-to-live expiration and maximum size eviction.
    """

    def __init__(self, max_size=None, ttl=None):
        """
        :param max_size: Maximum number of items in the cache (triggers LRU eviction).
        :param ttl: Time-to-live in seconds for cached items.
        """
        self.max_size = max_size
        self.ttl = ttl
        self._cache = {}
        self._inflight = {}
        self._lock = threading.Lock()

    def get(self, key, factory_func, *args, **kwargs):
        """
        Gets the value for the key from the cache. If it misses, it calls the 
        factory_func(*args, **kwargs) to fetch it, caches it, and returns it.
        """
        # 1. Optimistic read (lock-free fast path)
        entry = self._cache.get(key)
        if entry is not None:
            if entry.expiry is None or time.monotonic() <= entry.expiry:
                # Update accessed timestamp without acquiring a lock.
                # In standard CPython this is safe and atomic enough.
                # In free-threaded Python, concurrent float updates are safe enough for LRU.
                entry.accessed = time.monotonic()
                return entry.value

        # 2. Cache miss or expired (needs lock for inflight management)
        with self._lock:
            # Double check within the lock to prevent race conditions
            entry = self._cache.get(key)
            if entry is not None:
                if entry.expiry is None or time.monotonic() <= entry.expiry:
                    entry.accessed = time.monotonic()
                    return entry.value

            future = self._inflight.get(key)
            if future is None:
                # We are the first to encounter the miss, register our future
                future = Future()
                self._inflight[key] = future
                must_compute = True
            else:
                # Someone else is already fetching it
                must_compute = False

        # 3. Compute or Wait (outside the lock)
        if must_compute:
            try:
                # Do the expensive work completely outside the lock
                value = factory_func(*args, **kwargs)
                
                with self._lock:
                    # Apply eviction if needed before adding a new item
                    if self.max_size is not None and len(self._cache) >= self.max_size and key not in self._cache:
                        self._evict()
                    
                    self._cache[key] = CacheEntry(value, self.ttl)
                    del self._inflight[key]

                # Notify all waiting threads
                future.set_result(value)
                return value
            except Exception as e:
                # Clean up inflight state and propagate the exception to waiters
                with self._lock:
                    if key in self._inflight:
                        del self._inflight[key]
                future.set_exception(e)
                raise
        else:
            # We are not the first thread. Wait for the computing thread to finish.
            # This blocks until future.set_result or future.set_exception is called.
            return future.result()

    def _evict(self):
        """
        Evict the least recently used item, prioritizing expired items first.
        Must be called with self._lock acquired.
        """
        if not self._cache:
            return
            
        now = time.monotonic()
        
        # 1. Try to evict expired items first (lazy expiration cleanup)
        expired_keys = [k for k, v in self._cache.items() if v.expiry is not None and now > v.expiry]
        if expired_keys:
            # Delete one expired key to make room
            del self._cache[expired_keys[0]]
            return

        # 2. If no expired items, do exact LRU eviction based on access timestamp
        oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].accessed)
        del self._cache[oldest_key]
        
    def invalidate(self, key):
        """Remove a key from the cache explicitly."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]

    def clear(self):
        """Clear all items from the cache."""
        with self._lock:
            self._cache.clear()
            
    def __len__(self):
        """Return the number of items in the cache."""
        return len(self._cache)
