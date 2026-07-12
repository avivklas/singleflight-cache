import time
import threading
import functools
import sys

sys.path.insert(0, './src')
from singleflight_cache import FastCache
try:
    from cachetools import TTLCache
except ImportError:
    print("Please install cachetools to run this benchmark: pip install cachetools")
    sys.exit(1)

def simulate_work(duration=0.1):
    """Simulate a costly data fetch operation."""
    time.sleep(duration)
    return "result"

def run_benchmark(fetch_func, num_keys=5, threads_per_key=10):
    start = time.time()
    threads = []
    
    for k in range(num_keys):
        key = f"key_{k}"
        for _ in range(threads_per_key):
            t = threading.Thread(target=fetch_func, args=(key,))
            threads.append(t)
            t.start()
            
    for t in threads:
        t.join()
        
    end = time.time()
    return end - start

def main():
    num_keys = 5
    threads_per_key = 10
    fetch_time = 0.5
    
    print("="*80)
    print("BENCHMARK: Concurrent Cache Misses (Thundering Herd)")
    print("="*80)
    print(f"Distinct Keys Requested : {num_keys}")
    print(f"Concurrent Threads / Key: {threads_per_key}")
    print(f"Total Threads           : {num_keys * threads_per_key}")
    print(f"Simulated Fetch Time    : {fetch_time}s")
    print("-" * 80)
    print(f"{'Library':<30} | {'Time (s)':<10} | {'Fetch Executions':<20} | {'Notes'}")
    print("-" * 80)

    # 1. functools.lru_cache (Built-in)
    call_count_lru = [0]
    lock_lru = threading.Lock()
    
    @functools.lru_cache(maxsize=1000)
    def builtin_fetch(key):
        with lock_lru:
            call_count_lru[0] += 1
        return simulate_work(fetch_time)

    t_lru = run_benchmark(builtin_fetch, num_keys, threads_per_key)
    print(f"{'functools.lru_cache':<30} | {t_lru:<10.3f} | {call_count_lru[0]:<20} | Thundering Herd!")

    # 2. Naive Thread-Safe Cache (cachetools + Lock)
    naive_cache = TTLCache(maxsize=1000, ttl=60)
    naive_lock = threading.RLock()
    call_count_naive = [0]
    
    def naive_fetch(key):
        with naive_lock:
            if key in naive_cache:
                return naive_cache[key]
            call_count_naive[0] += 1
            val = simulate_work(fetch_time)
            naive_cache[key] = val
            return val

    t_naive = run_benchmark(naive_fetch, num_keys, threads_per_key)
    print(f"{'cachetools + Global Lock':<30} | {t_naive:<10.3f} | {call_count_naive[0]:<20} | Bottlenecked!")

    # 3. singleflight-cache (Our library)
    fast_cache = FastCache(ttl=60, max_size=1000)
    call_count_fast = [0]
    lock_fast = threading.Lock()

    def fast_factory():
        with lock_fast:
            call_count_fast[0] += 1
        return simulate_work(fetch_time)

    def fast_fetch(key):
        return fast_cache.get(key, fast_factory)

    t_fast = run_benchmark(fast_fetch, num_keys, threads_per_key)
    print(f"{'singleflight-cache':<30} | {t_fast:<10.3f} | {call_count_fast[0]:<20} | Perfect!")
    print("=" * 80)

if __name__ == "__main__":
    main()
