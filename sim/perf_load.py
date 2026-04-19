import time
import json
import requests
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE = "http://127.0.0.1:8000"
QUERY_SIZES = [5, 10, 15, 20, 30, 40]
USERS = 1
OUT_DIR = Path("sim/results")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def one_read():
    t0 = time.perf_counter()
    r = requests.get(f"{BASE}/fetch", params={"source": "merged", "limit": 20}, timeout=20)
    r.raise_for_status()
    return (time.perf_counter() - t0) * 1000

def run(users, total):
    t0 = time.perf_counter()
    lats = []
    with ThreadPoolExecutor(max_workers=users) as ex:
        futures = [ex.submit(one_read) for _ in range(total)]
        for f in as_completed(futures):
            lats.append(f.result())
    elapsed = time.perf_counter() - t0
    return {
        "users": users,
        "total": total,
        "avg_ms": sum(lats) / len(lats),
        "min_ms": min(lats),
        "max_ms": max(lats),
        "throughput_qps": total / elapsed
    }

def print_table(results):
    print(f"{'Total':>8} {'Avg(ms)':>12} {'Min(ms)':>12} {'Max(ms)':>12} {'QPS':>10}")
    for r in results:
        print(f"{r['total']:>8} {r['avg_ms']:>12.2f} {r['min_ms']:>12.2f} {r['max_ms']:>12.2f} {r['throughput_qps']:>10.2f}")

def plot_results(results):
    totals = [r["total"] for r in results]
    avg_ms = [r["avg_ms"] for r in results]
    qps = [r["throughput_qps"] for r in results]

    # Latency plot
    plt.figure(figsize=(8, 5))
    plt.plot(totals, avg_ms, marker="o")
    plt.title("Avg Latency vs Total Queries")
    plt.xlabel("Total Queries")
    plt.ylabel("Average Latency (ms)")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "progressive_latency_merged.png", dpi=150)
    plt.close()

    # Throughput plot
    plt.figure(figsize=(8, 5))
    plt.plot(totals, qps, marker="o")
    plt.title("Throughput vs Total Queries")
    plt.xlabel("Total Queries")
    plt.ylabel("Throughput (queries/sec)")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "progressive_throughput_merged.png", dpi=150)
    plt.close()

def main():
    out = []
    for qs in QUERY_SIZES:
        res = run(USERS, qs)
        out.append(res)
        print(f"Done: total={qs}, avg={res['avg_ms']:.2f} ms, qps={res['throughput_qps']:.2f}")

    # Save JSON once
    json_path = OUT_DIR / "progressive_volume_results_merged.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print_table(out)
    print(f"\nSaved JSON: {json_path}")

    plot_results(out)
    print(f"Saved graphs: {OUT_DIR/'progressive_latency.png'}")
    print(f"Saved graphs: {OUT_DIR/'progressive_throughput.png'}")

if __name__ == "__main__":
    main()