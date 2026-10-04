import threading
import time
from collections import OrderedDict
from concurrent.futures import Future
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar

T = TypeVar("T")


class CacheEntry:
    __slots__ = ("value", "expiry")

    def __init__(self, value: Any, ttl: Optional[float]):
        self.value = value
        self.expiry = time.monotonic() + ttl if ttl is not None else None


class SingleFlightCache:
    """
    High-performance thread-safe local cache with Single-Flight logic.

    Features:
    - Single-Flight: Concurrent cache misses for the same key trigger the underlying
      fetch operation exactly once. Other threads wait for the result.
    - Inflight Disowning: Invalidating (set/remove/clear) a key during inflight computation
      disowns the running future so stale results never overwrite fresh data.
    - Reentrancy Guard: Detects recursive calls on the same thread for the same key to prevent deadlocks.
    - Optimized O(1) LRU & TTL: Uses OrderedDict for O(1) LRU updates and bounded eviction scanning.
    """

    def __init__(self, max_size: Optional[int] = None, ttl: Optional[float] = None):
        """
        :param max_size: Maximum number of items in the cache (triggers LRU eviction).
        :param ttl: Time-to-live in seconds for cached items.
        """
        self._max_size = max_size
        self._ttl = ttl
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        # Maps key -> (future, owner_thread_id)
        self._inflight: Dict[str, Tuple[Future[Any], int]] = {}
        self._lock = threading.Lock()

    @property
    def ttl(self) -> Optional[float]:
        return self._ttl

    @property
    def max_size(self) -> Optional[int]:
        return self._max_size

    _SENTINEL = object()

    def _try_read(self, key: str) -> Any:
        """Return cached value if present and not expired, else _SENTINEL.
        Must be called with self._lock held."""
        entry = self._cache.get(key)
        if entry is not None:
            if entry.expiry is None or time.monotonic() <= entry.expiry:
                self._cache.move_to_end(key)
                return entry.value
        return self._SENTINEL

    def _store(self, key: str, value: Any) -> None:
        """Write a value into the cache, evicting if needed.
        Must be called with self._lock held."""
        if (
            self._max_size is not None
            and len(self._cache) >= self._max_size
            and key not in self._cache
        ):
            self._evict()
        self._cache[key] = CacheEntry(value, self._ttl)
        self._cache.move_to_end(key)

    def get(self, key: str, factory_or_default: Any = None, *args: Any, **kwargs: Any) -> Any:
        """
        Gets a value from the cache.
        - If `factory_or_default` is callable, fetches or computes the value using Single-Flight logic.
        - If `factory_or_default` is not callable, returns the cached value if present, else `factory_or_default`.
        """
        if callable(factory_or_default):
            return self.get_or_set_if_doesnt_exist(key, factory_or_default, *args, **kwargs)

        with self._lock:
            val = self._try_read(key)
        if val is not self._SENTINEL:
            return val
        return factory_or_default

    def set(self, key: str, value: Any) -> None:
        """Sets a value directly into the cache, disowning any in-flight computation."""
        with self._lock:
            self._store(key, value)
            self._inflight.pop(key, None)

    def get_or_set_if_doesnt_exist(
        self, key: str, generator: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """
        Returns the cached value if present. On a miss, calls `generator(*args, **kwargs)` to
        produce it — concurrent callers for the same key share one computation.
        """
        with self._lock:
            value = self._try_read(key)
            if value is not self._SENTINEL:
                return value

            entry = self._inflight.get(key)
            current_tid = threading.get_ident()

            future: Future[Any]
            if entry is not None:
                if entry[1] == current_tid:
                    raise RecursionError(
                        f"Reentrant get_or_set_if_doesnt_exist() for key '{key}' on the same thread would deadlock"
                    )
                future = entry[0]
                must_compute = False
            else:
                future = Future()
                self._inflight[key] = (future, current_tid)
                must_compute = True

        if must_compute:
            try:
                value = generator(*args, **kwargs)
            except BaseException as e:
                with self._lock:
                    if self._inflight.get(key, (None,))[0] is future:
                        del self._inflight[key]
                    future.set_exception(e)
                raise

            with self._lock:
                owned = self._inflight.get(key, (None,))[0] is future
                if owned:
                    del self._inflight[key]
                    if value is not None:
                        self._store(key, value)
                future.set_result(value)
            return value

        # Waiter path: another thread is computing — wait for its result.
        result = future.result()
        with self._lock:
            cached = self._try_read(key)
        if cached is not self._SENTINEL:
            return cached
        if result is None:
            return None
        # Result didn't land in cache (invalidated mid-flight) — retry.
        return self.get_or_set_if_doesnt_exist(key, generator, *args, **kwargs)

    # Alias for get_or_set_if_doesnt_exist
    get_or_set = get_or_set_if_doesnt_exist

    def remove(self, key: str) -> bool:
        """Remove a key from the cache, disowning any in-flight computation.

        :return: True if a cached entry was removed, False if the key was not cached.
        """
        with self._lock:
            self._inflight.pop(key, None)
            return self._cache.pop(key, None) is not None

    def invalidate(self, key: str) -> bool:
        """Force the next get() for this key to fetch fresh data. Alias for remove()."""
        return self.remove(key)

    def clear(self) -> None:
        """Clear all items, disowning all in-flight computations."""
        with self._lock:
            self._cache.clear()
            self._inflight.clear()

    def __len__(self) -> int:
        return len(self._cache)

    _EVICT_SCAN_LIMIT = 5

    def _evict(self) -> None:
        """Evict one item: prefer an expired LRU entry, else true LRU. O(1).
        Must be called with self._lock held."""
        if not self._cache:
            return

        now = time.monotonic()
        scanned = 0
        for key in self._cache:
            if self._cache[key].expiry is not None and now > self._cache[key].expiry:
                del self._cache[key]
                return
            scanned += 1
            if scanned >= self._EVICT_SCAN_LIMIT:
                break

        self._cache.popitem(last=False)


# FastCache is an alias for SingleFlightCache for backward compatibility
FastCache = SingleFlightCache
