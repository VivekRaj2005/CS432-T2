from __future__ import annotations
import json
import os
import time
import uuid
from typing import Any
import httpx
import pytest

BASE_URL      = os.environ.get("ACID_BASE_URL", "http://127.0.0.1:8000")
POLL_TIMEOUT  = float(os.environ.get("ACID_POLL_TIMEOUT",  "15"))
POLL_INTERVAL = float(os.environ.get("ACID_POLL_INTERVAL", "0.25"))
HTTP_TIMEOUT  = float(os.environ.get("ACID_HTTP_TIMEOUT",  "10"))


def make_client() -> httpx.Client:
    return httpx.Client(base_url=BASE_URL, timeout=HTTP_TIMEOUT)


def uid() -> str:
    return str(uuid.uuid4())[:12]


def api_health(c: httpx.Client) -> bool:
    try:
        return c.get("/health").status_code == 200
    except Exception:
        return False


def api_create(c: httpx.Client, payload: dict) -> httpx.Response:
    return c.post("/create", json=payload)


def api_update(c: httpx.Client, payload: dict) -> httpx.Response:
    return c.post("/update", json=payload)


def api_delete(c: httpx.Client, payload: dict) -> httpx.Response:
    return c.post("/delete", json=payload)


def api_fetch(
    c: httpx.Client,
    conditions: dict | None = None,
    source: str = "merged",
    limit: int = 200,
) -> list[dict]:
    params: dict[str, Any] = {"source": source, "limit": limit}
    if conditions:
        params["conditions"] = json.dumps(conditions)
    r = c.get("/fetch", params=params)
    assert r.status_code == 200, f"Fetch failed ({r.status_code}): {r.text}"
    return r.json()["data"]


def api_dump(c: httpx.Client) -> dict:
    r = c.get("/dump-json")
    assert r.status_code == 200, f"Dump failed: {r.text}"
    return r.json()


def api_load(c: httpx.Client, dump: dict) -> httpx.Response:
    return c.post("/load-dump-json", json={"dump": dump})


def poll_until(
    c: httpx.Client,
    conditions: dict,
    predicate,
    *,
    source: str = "merged",
    timeout: float = POLL_TIMEOUT,
    interval: float = POLL_INTERVAL,
) -> list[dict]:
    deadline = time.monotonic() + timeout
    rows: list[dict] = []
    while time.monotonic() < deadline:
        try:
            rows = api_fetch(c, conditions=conditions, source=source)
        except AssertionError:
            rows = []
        if predicate(rows):
            return rows
        time.sleep(interval)
    raise AssertionError(
        f"poll_until timed out after {timeout}s. "
        f"Final rows: {json.dumps(rows, default=str)[:500]}"
    )


def poll_until_absent(
    c: httpx.Client,
    conditions: dict,
    *,
    source: str = "merged",
    timeout: float = POLL_TIMEOUT,
) -> None:
    poll_until(c, conditions, predicate=lambda rs: len(rs) == 0,
               source=source, timeout=timeout)


@pytest.fixture(scope="session")
def client():
    with make_client() as c:
        assert api_health(c), f"Server not reachable at {BASE_URL}"
        yield c