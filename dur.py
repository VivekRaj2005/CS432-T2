from conftest import (
    make_client, uid, api_create, api_update, api_delete,
    api_fetch, api_dump, api_load, poll_until, poll_until_absent, POLL_TIMEOUT, POLL_INTERVAL,
)
import json
import os
import signal
import subprocess
import time
import pytest
import sys


BASE_URL = os.environ.get("ACID_BASE_URL", "http://127.0.0.1:8000")

def _clean_mr(mr):
    """
    Recursively removes volatile runtime counters and sorts lists 
    to ensure strictly deterministic schema comparison across dumps.
    """
    if isinstance(mr, dict):
        cleaned = {}
        for k, v in mr.items():
            if k in ("request_count", "save_file_name"):
                continue
            cleaned[k] = _clean_mr(v)
        return cleaned
    elif isinstance(mr, list):
        # Sort lists so unordered sets/lists are deterministic when converted to JSON
        try:
            return sorted(_clean_mr(x) for x in mr)
        except TypeError:
            # Fallback for unorderable mixed types (e.g., lists of dicts)
            return sorted((_clean_mr(x) for x in mr), key=lambda item: json.dumps(item, sort_keys=True))
    return mr


class TestDurability:

    def test_committed_record_survives_dump_load(self, client):
        marker = uid()
        api_create(client, {"acid": "durability_committed", "marker": marker, "v": 99})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        snap = api_dump(client)
        r = api_load(client, snap)
        assert r.status_code == 200, f"Load failed: {r.text}"

        rows = poll_until(client, {"marker": {"op": "eq", "value": marker}},
                          predicate=lambda rs: len(rs) >= 1)
        
        assert rows, "Committed record missing after dump/load"
        assert str(rows[0].get("v")) == "99", \
            f"value corrupted after dump/load: {rows[0]}"

    def test_committed_update_survives_dump_load(self, client):
        marker = uid()
        api_create(client, {"acid": "durability_update", "marker": marker, "v": "before"})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        api_update(client, {"criteria": {"marker": marker}, "set": {"v": "after"}})
        poll_until(client,
                   {"marker": {"op": "eq", "value": marker},
                    "v": {"op": "eq", "value": "after"}},
                   predicate=lambda rs: len(rs) >= 1)

        snap = api_dump(client)
        assert api_load(client, snap).status_code == 200

        rows = api_fetch(client, {"marker": {"op": "eq", "value": marker}})
        assert rows, "Record missing after dump/load"
        assert str(rows[0].get("v")) == "after", \
            f"old value restored after dump/load: {rows[0]}"

    def test_committed_delete_survives_dump_load(self, client):
        marker = uid()
        api_create(client, {"acid": "durability_delete", "marker": marker})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        api_delete(client, {"criteria": {"marker": marker}})
        poll_until_absent(client, {"marker": {"op": "eq", "value": marker}})

        snap = api_dump(client)
        assert api_load(client, snap).status_code == 200

        time.sleep(POLL_INTERVAL * 4)

        rows = api_fetch(client, {"marker": {"op": "eq", "value": marker}})
        assert rows == [], f"Deleted record resurrected after dump/load: {rows}"

    def test_schema_survives_dump_load_cycle(self, client):
        snap = api_dump(client)
        assert api_load(client, snap).status_code == 200

        restored = api_dump(client)
        orig = json.dumps(_clean_mr(snap.get("map_register", {})), sort_keys=True)
        rest = json.dumps(_clean_mr(restored.get("map_register", {})), sort_keys=True)
        assert orig == rest, "MapRegister schema changed across dump/load cycle"

    def test_in_flight_queue_events_survive_dump_load(self, client):
        marker = uid()
        api_create(client, {"acid": "durability_inflight", "marker": marker})

        snap = api_dump(client)
        in_snap = any(
            isinstance(e, dict) and e.get("data", {}).get("marker") == marker
            for e in snap.get("ingest_queue", [])
        )

        assert api_load(client, snap).status_code == 200

        restored = api_dump(client)
        in_restored = any(
            isinstance(e, dict) and e.get("data", {}).get("marker") == marker
            for e in restored.get("ingest_queue", [])
        )

        if in_snap:
            assert in_restored, \
                f"In-flight event marker={marker} was in snapshot but lost after reload"

    def test_multiple_dump_load_cycles_stable(self, client):
        signatures = []
        for cycle in range(3):
            d = api_dump(client)
            signatures.append(json.dumps(_clean_mr(d.get("map_register", {})), sort_keys=True))
            r = api_load(client, d)
            assert r.status_code == 200, f"Cycle {cycle}: load failed: {r.text}"

        assert len(set(signatures)) == 1, \
            "MapRegister drifted across three dump/load cycles"

    def test_committed_data_survives_server_restart(self):
        start_cmd = os.environ.get("IITGNDB_MAIN_CMD")
        if not start_cmd:
            pytest.skip("Set IITGNDB_MAIN_CMD to run the restart durability test")

        with make_client() as c:
            marker = uid()
            api_create(c, {"acid": "durability_restart", "marker": marker, "v": 77})
            poll_until(c, {"marker": {"op": "eq", "value": marker}},
                       predicate=lambda rs: len(rs) >= 1)

            dump_path = "/tmp/acid_restart_dump.json"
            # Adjust path for Windows if /tmp/ doesn't exist or isn't writable
            if sys.platform == "win32":
                dump_path = "acid_restart_dump.json" 
                
            r = c.get("/dump", params={"path": dump_path})
            assert r.status_code == 200, f"Pre-restart dump failed: {r.text}"

        # --- CROSS-PLATFORM PROCESS KILL ---
        if sys.platform == "win32":
            # Windows approach using wmic
            result = subprocess.run(
                ["wmic", "process", "where", "CommandLine like '%main.py%'", "get", "ProcessId"],
                capture_output=True, text=True
            )
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.isdigit():
                    try:
                        os.kill(int(line), signal.SIGTERM)
                    except (ProcessLookupError, OSError):
                        pass
        else:
            # Unix/Linux approach using pgrep
            result = subprocess.run(["pgrep", "-f", "main.py"], capture_output=True, text=True)
            for pid in [int(p) for p in result.stdout.split() if p.strip()]:
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        # -----------------------------------

        time.sleep(2.0)

        proc = subprocess.Popen(start_cmd, shell=True,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        deadline = time.monotonic() + 30.0
        with make_client() as c:
            while time.monotonic() < deadline:
                try:
                    if c.get("/health").status_code == 200:
                        break
                except Exception:
                    pass
                time.sleep(0.5)
            else:
                proc.terminate()
                pytest.fail("Server did not become healthy within 30s after restart")

            rows = poll_until(c, {"marker": {"op": "eq", "value": marker}},
                              predicate=lambda rs: len(rs) >= 1,
                              timeout=POLL_TIMEOUT)
            assert rows, "Committed record missing after restart"
            assert str(rows[0].get("v")) == "77", \
                f"value corrupted after restart: {rows[0]}"