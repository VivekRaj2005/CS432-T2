import csv
import os
import threading
import time
import uuid
from collections import defaultdict
from statistics import mean
from typing import Dict, Any, List


class PerfMetrics:

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._spans: Dict[str, Dict[str, float]] = {}  # req_id --> {stage: ms}
        self._stage_values: Dict[str, List[float]] = defaultdict(list)
        self._ops_completed = 0
        self._ops_started = 0
        self._started_at = time.perf_counter()

    def new_request_id(self) -> str:
        return str(uuid.uuid4())

    def mark_started(self) -> None:
        with self._lock:
            self._ops_started += 1

    def mark_completed(self) -> None:
        with self._lock:
            self._ops_completed += 1

    def add_stage_ms(self, req_id: str, stage: str, value_ms: float) -> None:
        with self._lock:
            if req_id not in self._spans:
                self._spans[req_id] = {}
            self._spans[req_id][stage] = value_ms
            self._stage_values[stage].append(value_ms)

    def add_stage_ns(self, req_id: str, stage: str, start_ns: int, end_ns: int) -> None:
        self.add_stage_ms(req_id, stage, (end_ns - start_ns) / 1_000_000.0)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            now = time.perf_counter()
            elapsed = max(now - self._started_at, 1e-9)
            stage_stats = {}
            for stage, vals in self._stage_values.items():
                if not vals:
                    continue
                stage_stats[stage] = {
                    "count": len(vals),
                    "avg_ms": mean(vals),
                    "min_ms": min(vals),
                    "max_ms": max(vals),
                }

            return {
                "ops_started": self._ops_started,
                "ops_completed": self._ops_completed,
                "elapsed_seconds": elapsed,
                "throughput_ops_per_sec": self._ops_completed / elapsed,
                "stage_stats": stage_stats,
                "requests_count": len(self._spans),
            }

    def export_request_spans_csv(self, out_path: str) -> None:
        with self._lock:
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            all_stages = set()
            for rec in self._spans.values():
                all_stages.update(rec.keys())
            stage_cols = sorted(all_stages)

            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["req_id"] + stage_cols + ["total_ms"])
                for req_id, rec in self._spans.items():
                    row = [req_id]
                    total = 0.0
                    for s in stage_cols:
                        v = rec.get(s, 0.0)
                        row.append(v)
                        total += v
                    row.append(total)
                    writer.writerow(row)

    def reset(self) -> None:
        with self._lock:
            self._spans.clear()
            self._stage_values.clear()
            self._ops_completed = 0
            self._ops_started = 0
            self._started_at = time.perf_counter()


perf_metrics = PerfMetrics()