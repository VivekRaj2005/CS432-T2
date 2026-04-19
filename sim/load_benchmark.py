import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import mean
import requests
from workload_generators import make_flat_record, make_update_payload


def worker(base, start_i, count):
    lat = []
    for i in range(start_i, start_i + count):
        p = i % 10
        t0 = time.perf_counter()
        if p < 6:
            r = requests.get(f"{base}/fetch", params={"source": "merged", "limit": 50}, timeout=30)
            r.raise_for_status()
        elif p < 9:
            r = requests.post(f"{base}/create", json=make_flat_record(200000 + i), timeout=30)
            r.raise_for_status()
        else:
            r = requests.post(f"{base}/update", json=make_update_payload(i), timeout=30)
            r.raise_for_status()
        lat.append((time.perf_counter() - t0) * 1000)
    return lat


def run(base, total, users):
    per = total // users
    extra = total % users
    all_lat = []
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=users) as ex:
        futs = []
        cursor = 0
        for u in range(users):
            c = per + (1 if u < extra else 0)
            futs.append(ex.submit(worker, base, cursor, c))
            cursor += c
        for f in as_completed(futs):
            all_lat.extend(f.result())
    elapsed = time.perf_counter() - t0
    return {
        "total_queries": total,
        "users": users,
        "avg_latency_ms": mean(all_lat) if all_lat else 0.0,
        "total_time_sec": elapsed,
        "throughput_ops_per_sec": (total / elapsed) if elapsed > 0 else 0.0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--out", default="sim/results/load_results.json")
    args = ap.parse_args()

    out = {
        "vary_sizes_users5": [run(args.base, n, users=5) for n in [100, 500, 1000, 5000]],
        "vary_users_queries2000": [run(args.base, 2000, users=u) for u in [1, 2, 5, 10, 20]],
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
