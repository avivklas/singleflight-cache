import time
import threading
from singleflight_cache import FastCache

def test_single_flight():
    cache = FastCache()
    call_count = 0
    lock = threading.Lock()

    def factory(key):
        nonlocal call_count
        with lock:
            call_count += 1
        time.sleep(0.1) # Simulate expensive work
        return f"value_for_{key}"

    # Spawn 50 threads that all ask for the same key at the same time
    threads = []
    results = []

    def worker():
        res = cache.get("test_key", factory, "test_key")
        results.append(res)

    for _ in range(50):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Assert that all threads got the correct value
    assert len(results) == 50
    assert all(r == "value_for_test_key" for r in results)

    # Assert that the factory was only called EXACTLY ONCE
    assert call_count == 1


def test_lru_eviction():
    cache = FastCache(max_size=3)
    
    # Fill cache to max
    cache.get("k1", lambda: "v1")
    cache.get("k2", lambda: "v2")
    cache.get("k3", lambda: "v3")
    
    assert len(cache) == 3
    
    # Access k1 to make it most recently used
    # Wait a tiny bit so time.monotonic() advances
    time.sleep(0.01)
    cache.get("k1", lambda: "v1")
    time.sleep(0.01)
    
    # Add k4, which should evict k2 because k1 was just accessed, and k2 is oldest
    cache.get("k4", lambda: "v4")
    
    assert len(cache) == 3
    assert cache._cache.get("k1") is not None
    assert cache._cache.get("k2") is None # Evicted
    assert cache._cache.get("k3") is not None
    assert cache._cache.get("k4") is not None


def test_ttl_expiration():
    cache = FastCache(ttl=0.1)
    
    cache.get("k1", lambda: "v1")
    assert cache._cache.get("k1") is not None
    
    # Read before TTL
    val = cache.get("k1", lambda: "v_new")
    assert val == "v1"
    
    # Wait for TTL to expire
    time.sleep(0.15)
    
    # Now it should fetch again
    val = cache.get("k1", lambda: "v_new")
    assert val == "v_new"


def test_exception_propagation():
    cache = FastCache()
    
    class MyError(Exception):
        pass

    def faulty_factory():
        time.sleep(0.1)
        raise MyError("Failed")
        
    threads = []
    results = []
    exceptions = []

    def worker():
        try:
            res = cache.get("bad_key", faulty_factory)
            results.append(res)
        except Exception as e:
            exceptions.append(e)

    for _ in range(10):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Nobody should get a result
    assert len(results) == 0
    # Everyone should get an exception
    assert len(exceptions) == 10
    assert all(isinstance(e, MyError) for e in exceptions)
    
    # Inflight state should be cleared, so next call should retry
    # We provide a good factory this time
    good_val = cache.get("bad_key", lambda: "success")
    assert good_val == "success"
