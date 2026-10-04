import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event, Thread
from typing import Any, List
from unittest import TestCase
from unittest.mock import Mock

from singleflight_cache import FastCache, SingleFlightCache


class TestSingleFlightCache(TestCase):
    def setUp(self) -> None:
        self.cache: SingleFlightCache = SingleFlightCache(max_size=3, ttl=None)

    def test_backwards_compatibility_fast_cache_alias(self):
        self.assertIs(FastCache, SingleFlightCache)
        fc = FastCache(max_size=5, ttl=10)
        self.assertEqual(fc.max_size, 5)
        self.assertEqual(fc.ttl, 10)

    def test_ttl_and_max_size_properties(self):
        cache = SingleFlightCache(max_size=7, ttl=42)
        self.assertEqual(cache.max_size, 7)
        self.assertEqual(cache.ttl, 42)

    def test_get_returns_default_when_key_missing(self):
        self.assertIsNone(self.cache.get("missing"))
        self.assertEqual(self.cache.get("missing", "fallback"), "fallback")

    def test_set_then_get_returns_stored_value(self):
        self.cache.set("key", "value")
        self.assertEqual(self.cache.get("key"), "value")

    def test_get_returns_default_when_entry_expired(self):
        cache = SingleFlightCache(ttl=10)
        cache.set("key", "value")
        entry = cache._cache["key"]
        entry.expiry = time.monotonic() - 1

        self.assertIsNone(cache.get("key"))

    def test_len_reflects_number_of_stored_items(self):
        self.assertEqual(len(self.cache), 0)
        self.cache.set("a", 1)
        self.cache.set("b", 2)
        self.assertEqual(len(self.cache), 2)

    def test_remove_and_invalidate(self):
        self.cache.set("key1", "val1")
        self.cache.set("key2", "val2")
        self.cache.remove("key1")
        self.cache.invalidate("key2")
        self.assertIsNone(self.cache.get("key1"))
        self.assertIsNone(self.cache.get("key2"))
        self.assertEqual(len(self.cache), 0)

    def test_invalidate_returns_whether_a_cached_entry_was_removed(self):
        self.cache.set("key", "value")

        self.assertTrue(self.cache.invalidate("key"))
        self.assertFalse(self.cache.invalidate("key"))
        self.assertFalse(self.cache.invalidate("never-cached"))

    def test_invalidate_forces_refetch_on_next_get(self):
        self.cache.get("key", lambda: "old")
        self.cache.invalidate("key")

        self.assertEqual(self.cache.get("key", lambda: "new"), "new")

    def test_invalidate_only_affects_given_key(self):
        self.cache.set("a", 1)
        self.cache.set("b", 2)

        self.cache.invalidate("a")

        self.assertIsNone(self.cache.get("a"))
        self.assertEqual(self.cache.get("b"), 2)

    def test_clear_removes_all_items(self):
        self.cache.set("a", 1)
        self.cache.set("b", 2)
        self.cache.clear()
        self.assertEqual(len(self.cache), 0)

    def test_single_flight_execution(self):
        call_count = 0
        lock = threading.Lock()

        def factory(key):
            nonlocal call_count
            with lock:
                call_count += 1
            time.sleep(0.1)
            return f"value_for_{key}"

        results = []
        threads = []

        def worker():
            res = self.cache.get("test_key", factory, "test_key")
            results.append(res)

        for _ in range(20):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(len(results), 20)
        self.assertTrue(all(r == "value_for_test_key" for r in results))
        self.assertEqual(call_count, 1)

    def test_set_evicts_least_recently_used_item(self):
        cache = SingleFlightCache(max_size=2)
        cache.set("a", "va")
        cache.set("b", "vb")

        # Access 'a' to make it most recently used
        cache.get("a")

        # Add 'c', which should evict 'b'
        cache.set("c", "vc")

        self.assertIsNone(cache.get("b"))
        self.assertEqual(cache.get("a"), "va")
        self.assertEqual(cache.get("c"), "vc")
        self.assertEqual(len(cache), 2)

    def test_eviction_prefers_expired_items(self):
        cache = SingleFlightCache(max_size=2)
        cache.set("a", "va")
        cache.set("b", "vb")

        entry_a = cache._cache["a"]
        entry_a.expiry = time.monotonic() - 10
        cache.get("b")

        cache.set("c", "vc")

        self.assertIsNone(cache.get("a"))
        self.assertEqual(cache.get("b"), "vb")
        self.assertEqual(cache.get("c"), "vc")

    def test_get_or_set_if_doesnt_exist_caches_result(self):
        generator = Mock(return_value="generated-value")
        result = self.cache.get_or_set_if_doesnt_exist("key", generator)
        self.assertEqual(result, "generated-value")
        generator.assert_called_once()
        self.assertEqual(self.cache.get("key"), "generated-value")

    def test_exception_propagation(self):
        class MyError(Exception):
            pass

        def faulty_factory():
            time.sleep(0.05)
            raise MyError("Failed")

        threads = []
        exceptions = []

        def worker():
            try:
                self.cache.get("bad_key", faulty_factory)
            except Exception as e:
                exceptions.append(e)

        for _ in range(10):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(len(exceptions), 10)
        self.assertTrue(all(isinstance(e, MyError) for e in exceptions))
        self.assertEqual(len(self.cache), 0)

        good_val = self.cache.get("bad_key", lambda: "success")
        self.assertEqual(good_val, "success")

    def test_reentrant_call_raises_recursion_error(self):
        def reentrant_generator():
            return self.cache.get("key", lambda: "inner")

        with self.assertRaises(RecursionError):
            self.cache.get("key", reentrant_generator)

    def test_remove_during_inflight_computation_prevents_stale_value(self):
        computation_started = Event()
        release_computation = Event()

        def generator():
            computation_started.set()
            release_computation.wait(timeout=5)
            return "stale-value"

        results = []
        computing_thread = Thread(
            target=lambda: results.append(self.cache.get("key", generator))
        )
        computing_thread.start()
        self.assertTrue(computation_started.wait(timeout=5))

        self.cache.remove("key")
        release_computation.set()
        computing_thread.join(timeout=5)

        self.assertEqual(results, ["stale-value"])
        self.assertIsNone(self.cache.get("key"))
        self.assertEqual(len(self.cache), 0)

    def test_set_during_inflight_prevents_stale_overwrite(self):
        computation_started = Event()
        release_computation = Event()

        def slow_generator():
            computation_started.set()
            release_computation.wait(timeout=5)
            return "stale-generated-value"

        computing_thread = Thread(
            target=lambda: self.cache.get("key", slow_generator)
        )
        computing_thread.start()
        self.assertTrue(computation_started.wait(timeout=5))

        self.cache.set("key", "fresh-explicit-value")
        release_computation.set()
        computing_thread.join(timeout=5)

        self.assertEqual(self.cache.get("key"), "fresh-explicit-value")

    def test_does_not_cache_none_result(self):
        generator = Mock(return_value=None)
        result = self.cache.get("key", generator)

        self.assertIsNone(result)
        self.assertEqual(len(self.cache), 0)
        self.assertIsNone(self.cache.get("key"))
