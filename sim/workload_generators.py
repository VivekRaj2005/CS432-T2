import random
import string
from typing import Dict, Any


def rand_str(n=8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def make_flat_record(i: int) -> Dict[str, Any]:
    return {
        "user_id": i,
        "name": f"user_{i}_{rand_str(4)}",
        "age": random.randint(18, 65),
        "salary": random.randint(30000, 200000),
        "dept": random.choice(["eng", "ops", "hr", "sales"]),
        "active": random.choice([True, False]),
    }


def make_nested_record(i: int) -> Dict[str, Any]:
    return {
        "user_id": i,
        "name": f"user_{i}_{rand_str(4)}",
        "profile": {
            "skills": random.sample(["python", "sql", "ml", "cloud", "ts"], k=3),
            "projects": [{"name": rand_str(5), "score": random.randint(1, 10)} for _ in range(2)],
        },
        "prefs": {"theme": random.choice(["dark", "light"]), "lang": "en"},
        "active": random.choice([True, False]),
    }


def make_update_payload(i: int):
    return {
        "criteria": {"user_id": i},
        "set": {"salary": random.randint(40000, 220000), "active": random.choice([True, False])},
    }
