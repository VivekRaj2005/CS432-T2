import argparse
import json
import time
from statistics import mean
import requests
import mysql.connector
from pymongo import MongoClient
from workload_generators import make_flat_record, make_nested_record, make_update_payload


def dashboard_mixed(base, n=1000):
    lat = []
    for i in range(n):
        p = i % 10
        t0 = time.perf_counter()
        if p < 6:
            r = requests.get(f"{base}/fetch", params={"source": "merged", "limit": 50}, timeout=30)
            r.raise_for_status()
        elif p < 9:
            r = requests.post(f"{base}/create", json=make_flat_record(300000 + i), timeout=30)
            r.raise_for_status()
        else:
            r = requests.post(f"{base}/update", json=make_update_payload(i), timeout=30)
            r.raise_for_status()
        lat.append((time.perf_counter() - t0) * 1000)
    elapsed = sum(lat) / 1000.0
    return {"avg_latency_ms": mean(lat), "throughput_ops_per_sec": n / elapsed if elapsed > 0 else 0.0}


def direct_sql_mixed(cfg, n=1000):
    conn = mysql.connector.connect(**cfg)
    cur = conn.cursor()
    lat = []
    for i in range(n):
        p = i % 10
        t0 = time.perf_counter()
        if p < 6:
            cur.execute("SELECT * FROM root LIMIT 50")
            cur.fetchall()
        elif p < 9:
            r = make_flat_record(400000 + i)
            cur.execute(
                "INSERT INTO root (table_autogen_id) VALUES (%s)",
                (int(900000 + i),),
            )
            conn.commit()
        else:
            u = make_update_payload(i)
            # best-effort update on generated id column
            cur.execute(
                "UPDATE root SET table_autogen_id = table_autogen_id WHERE table_autogen_id = %s",
                (u['criteria']['user_id'],),
            )
            conn.commit()
        lat.append((time.perf_counter() - t0) * 1000)
    cur.close()
    conn.close()
    elapsed = sum(lat) / 1000.0
    return {"avg_latency_ms": mean(lat), "throughput_ops_per_sec": n / elapsed if elapsed > 0 else 0.0}


def direct_mongo_mixed(uri, db_name, n=1000):
    cli = MongoClient(uri)
    db = cli[db_name]
    col = db["root"]
    lat = []
    for i in range(n):
        p = i % 10
        t0 = time.perf_counter()
        if p < 6:
            list(col.find({}, {"_id": 0}).limit(50))
        elif p < 9:
            doc = make_nested_record(500000 + i)
            doc["table_autogen_id"] = int(950000 + i)
            col.insert_one(doc)
        else:
            u = make_update_payload(i)
            col.update_many({"user_id": u["criteria"]["user_id"]}, {"$set": u["set"]})
        lat.append((time.perf_counter() - t0) * 1000)
    cli.close()
    elapsed = sum(lat) / 1000.0
    return {"avg_latency_ms": mean(lat), "throughput_ops_per_sec": n / elapsed if elapsed > 0 else 0.0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--mysql-host", default="127.0.0.1")
    ap.add_argument("--mysql-port", type=int, default=3306)
    ap.add_argument("--mysql-user", default="root")
    ap.add_argument("--mysql-password", default="")
    ap.add_argument("--mysql-db", default="adapter")
    ap.add_argument("--mongo-uri", default="mongodb://127.0.0.1:27017")
    ap.add_argument("--mongo-db", default="adapter")
    ap.add_argument("--out", default="sim/results/comparative_results.json")
    args = ap.parse_args()

    sql_cfg = {
        "host": args.mysql_host,
        "port": args.mysql_port,
        "user": args.mysql_user,
        "password": args.mysql_password,
        "database": args.mysql_db,
    }

    dashboard = dashboard_mixed(args.base, 1000)
    sql_direct = direct_sql_mixed(sql_cfg, 1000)
    mongo_direct = direct_mongo_mixed(args.mongo_uri, args.mongo_db, 1000)

    out = {
        "dashboard_mixed": dashboard,
        "direct_sql_mixed": sql_direct,
        "direct_mongo_mixed": mongo_direct,
        "dashboard_overhead_vs_sql_ms": dashboard["avg_latency_ms"] - sql_direct["avg_latency_ms"],
        "dashboard_overhead_vs_mongo_ms": dashboard["avg_latency_ms"] - mongo_direct["avg_latency_ms"],
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
