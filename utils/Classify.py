from __future__ import annotations
import math
import os
import pickle
from collections import defaultdict
from typing import Any, Dict

from utils.log import logger

ALTER_DEMOTION_RATIO   = 0.05
LENGTH_VARIANCE_CUTOFF = 2000.0


def _is_nested(value: Any) -> bool:
    if isinstance(value, dict):
        return True
    if isinstance(value, list):
        return any(isinstance(item, (dict, list)) for item in value)
    return False


class PresenceTracker:
    def __init__(self):
        self.total_records: int = 0
        self.present_count: Dict[str, int] = defaultdict(int)

    def observe_record(self, flat_fields: Dict[str, Any]) -> None:
        self.total_records += 1
        for field, value in flat_fields.items():
            if value is not None:
                self.present_count[field] += 1

    def presence_ratio(self, field: str) -> float:
        if self.total_records == 0:
            return 0.0
        return self.present_count.get(field, 0) / self.total_records


class CardinalityTracker:
    def __init__(self):
        self.total_seen: Dict[str, int] = defaultdict(int)
        self.unique_values: Dict[str, set] = defaultdict(set)

    def observe(self, field: str, value: Any) -> None:
        self.total_seen[field] += 1
        try:
            hash(value)
            self.unique_values[field].add(value)
        except TypeError:
            pass

    def cardinality_ratio(self, field: str) -> float:
        total = self.total_seen.get(field, 0)
        return 0.0 if total == 0 else len(self.unique_values[field]) / total

    def is_unique(self, field: str) -> bool:
        return self.cardinality_ratio(field) == 1.0

    def report(self, field: str) -> dict:
        return {
            "field":             field,
            "total_seen":        self.total_seen[field],
            "unique_count":      len(self.unique_values[field]),
            "cardinality_ratio": self.cardinality_ratio(field),
            "is_unique":         self.is_unique(field),
        }


class StabilityTracker:
    def __init__(self):
        self.alter_count: Dict[str, int] = defaultdict(int)

    def record_alter(self, field: str) -> None:
        self.alter_count[field] += 1

    def alter_ratio(self, field: str, total_seen: int) -> float:
        return 0.0 if total_seen == 0 else self.alter_count[field] / total_seen

    def is_unstable(self, field: str, total_seen: int) -> bool:
        return self.alter_ratio(field, total_seen) > ALTER_DEMOTION_RATIO

    def report(self, field: str, total_seen: int) -> dict:
        return {
            "field":       field,
            "alter_count": self.alter_count[field],
            "alter_ratio": self.alter_ratio(field, total_seen),
            "is_unstable": self.is_unstable(field, total_seen),
        }


class LengthVarianceTracker:
    def __init__(self):
        self._n:    Dict[str, int]   = defaultdict(int)
        self._mean: Dict[str, float] = defaultdict(float)
        self._M2:   Dict[str, float] = defaultdict(float)

    def observe(self, field: str, value: str) -> None:
        length = len(value)
        self._n[field] += 1
        delta = length - self._mean[field]
        self._mean[field] += delta / self._n[field]
        delta2 = length - self._mean[field]
        self._M2[field] += delta * delta2

    def variance(self, field: str) -> float:
        n = self._n[field]
        return 0.0 if n < 2 else self._M2[field] / (n - 1)

    def is_high_variance(self, field: str) -> bool:
        return self.variance(field) > LENGTH_VARIANCE_CUTOFF

    def report(self, field: str) -> dict:
        return {
            "field":            field,
            "samples":          self._n[field],
            "mean_length":      round(self._mean[field], 2),
            "variance":         round(self.variance(field), 2),
            "std_dev":          round(math.sqrt(self.variance(field)), 2),
            "is_high_variance": self.is_high_variance(field),
        }


class FieldClassifier:
    PERSISTENCE_FILE = "metadata_store/field_classifications.pkl"

    def __init__(self, persistence_file: str = None):
        self.persistence_file      = persistence_file or self.PERSISTENCE_FILE
        self.presence_tracker      = PresenceTracker()
        self.cardinality_tracker   = CardinalityTracker()
        self.stability_tracker     = StabilityTracker()
        self.length_tracker        = LengthVarianceTracker()
        self.classifications: Dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.persistence_file):
            logger.info("FieldClassifier: no persisted state; starting fresh")
            return
        try:
            with open(self.persistence_file, "rb") as fh:
                state = pickle.load(fh)
            self.classifications    = state.get("classifications",     {})
            self.presence_tracker   = state.get("presence_tracker",    PresenceTracker())
            self.cardinality_tracker= state.get("cardinality_tracker", CardinalityTracker())
            self.stability_tracker  = state.get("stability_tracker",   StabilityTracker())
            self.length_tracker     = state.get("length_tracker",      LengthVarianceTracker())
            logger.info("FieldClassifier: loaded state from %s", self.persistence_file)
        except Exception as exc:
            logger.warning("FieldClassifier: failed to load state – %s", exc)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.persistence_file), exist_ok=True)
        try:
            with open(self.persistence_file, "wb") as fh:
                pickle.dump({
                    "classifications":     self.classifications,
                    "presence_tracker":    self.presence_tracker,
                    "cardinality_tracker": self.cardinality_tracker,
                    "stability_tracker":   self.stability_tracker,
                    "length_tracker":      self.length_tracker,
                }, fh)
        except Exception as exc:
            logger.error("FieldClassifier: failed to save state – %s", exc)

    def ingest_alter_events(self, update_order) -> None:
        for op in update_order:
            if op.get("type") == "ALTER":
                col = op.get("column_name")
                if col:
                    self.stability_tracker.record_alter(col)
                    logger.debug("StabilityTracker: ALTER for '%s' (total=%d)",
                                 col, self.stability_tracker.alter_count[col])

    def classify_record(self, record: Dict[str, Any]) -> Dict[str, str]:
        flat_fields = {k: v for k, v in record.items() if not _is_nested(v)}
        self.presence_tracker.observe_record(flat_fields)

        result: Dict[str, str] = {}

        for field, value in record.items():

            if _is_nested(value):
                dest = "mongodb"

            else:
                self.cardinality_tracker.observe(field, value)

                if isinstance(value, str):
                    self.length_tracker.observe(field, value)

                total_seen = self.cardinality_tracker.total_seen[field]

                if self.stability_tracker.is_unstable(field, total_seen):
                    dest = "mongodb"
                    if self.classifications.get(field) == "sql":
                        logger.warning(
                            "FieldClassifier: demoting '%s' sql->mongodb "
                            "(alter_ratio=%.3f > %.3f)",
                            field,
                            self.stability_tracker.alter_ratio(field, total_seen),
                            ALTER_DEMOTION_RATIO,
                        )

                elif isinstance(value, str) and self.length_tracker.is_high_variance(field):
                    dest = "mongodb"
                    if self.classifications.get(field) == "sql":
                        logger.warning(
                            "FieldClassifier: demoting '%s' sql->mongodb "
                            "(length variance=%.1f > %.1f)",
                            field,
                            self.length_tracker.variance(field),
                            LENGTH_VARIANCE_CUTOFF,
                        )

                else:
                    ratio = self.presence_tracker.presence_ratio(field)
                    dest  = "sql" if ratio >= 0.80 else "mongodb"

            prev = self.classifications.get(field)
            if prev is not None and prev != dest:
                logger.info("FieldClassifier: '%s' reclassified %s -> %s",
                            field, prev, dest)

            self.classifications[field] = dest
            result[field] = dest

        self.save()
        return result

    def get_classification(self, field: str) -> str:
        return self.classifications.get(field, "mongodb")

    def cardinality_report(self) -> list:
        return [self.cardinality_tracker.report(f)
                for f in self.cardinality_tracker.total_seen]

    def stability_report(self) -> list:
        return [
            self.stability_tracker.report(f, self.cardinality_tracker.total_seen[f])
            for f in self.stability_tracker.alter_count
        ]

    def length_variance_report(self) -> list:
        return [self.length_tracker.report(f) for f in self.length_tracker._n]
