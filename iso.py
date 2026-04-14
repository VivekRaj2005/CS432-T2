from conftest import (
    make_client, uid, api_create, api_update, api_delete,
    api_fetch, poll_until, poll_until_absent, POLL_TIMEOUT, POLL_INTERVAL,
)
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


SETTLE = POLL_INTERVAL * 3


class TestIsolation:

    def test_concurrent_writes_produce_distinct_records(self, client):
        n = 3
        markers = [uid() for _ in range(n)]

        def do_create(marker):
            with make_client() as c:
                r = api_create(c, {"acid": "isolation_distinct", "marker": marker})
                assert r.status_code == 200

        with ThreadPoolExecutor(max_workers=n) as pool:
            for f in as_completed([pool.submit(do_create, m) for m in markers]):
                f.result()

        time.sleep(SETTLE)

        missing, collisions = [], []
        for marker in markers:
            rows = poll_until(client, {"marker": {"op": "eq", "value": marker}},
                              predicate=lambda rs: len(rs) >= 1, timeout=POLL_TIMEOUT)
            if len(rows) == 0:
                missing.append(marker)
            elif len(rows) > 1:
                collisions.append((marker, len(rows)))

        assert not missing,    f"{len(missing)}/{n} records never appeared"
        assert not collisions, f"Duplicate records from concurrent creates: {collisions}"

    def test_concurrent_updates_to_different_records_no_cross_contamination(self, client):
        time.sleep(SETTLE)
        marker_a, marker_b = uid(), uid()
        for m in (marker_a, marker_b):
            api_create(client, {"acid": "isolation_cross", "marker": m, "v": "original"})
        for m in (marker_a, marker_b):
            poll_until(client, {"marker": {"op": "eq", "value": m}},
                       predicate=lambda rs: len(rs) >= 1)

        def update_a():
            with make_client() as c:
                api_update(c, {"criteria": {"marker": marker_a}, "set": {"v": "value_a"}})

        def update_b():
            with make_client() as c:
                api_update(c, {"criteria": {"marker": marker_b}, "set": {"v": "value_b"}})

        t_a = threading.Thread(target=update_a)
        t_b = threading.Thread(target=update_b)
        t_a.start(); t_b.start()
        t_a.join();  t_b.join()

        time.sleep(SETTLE)

        rows_a = poll_until(client,
                            {"marker": {"op": "eq", "value": marker_a},
                             "v": {"op": "eq", "value": "value_a"}},
                            predicate=lambda rs: len(rs) >= 1)
        rows_b = poll_until(client,
                            {"marker": {"op": "eq", "value": marker_b},
                             "v": {"op": "eq", "value": "value_b"}},
                            predicate=lambda rs: len(rs) >= 1)

        assert rows_a[0].get("v") == "value_a", \
            f"Record A contaminated by transaction B: {rows_a[0]}"
        assert rows_b[0].get("v") == "value_b", \
            f"Record B contaminated by transaction A: {rows_b[0]}"

    def test_concurrent_reads_see_no_partial_writes(self, client):
        time.sleep(SETTLE)
        marker = uid()
        api_create(client, {"acid": "isolation_partial", "marker": marker, "v": "present"})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        partial_records = []

        def reader():
            with make_client() as c:
                for _ in range(10):
                    try:
                        rows = api_fetch(c, {"marker": {"op": "eq", "value": marker}})
                        for row in rows:
                            if row.get("acid") is None or row.get("marker") is None:
                                partial_records.append(row)
                    except Exception:
                        pass
                    time.sleep(0.1)

        def writer():
            with make_client() as c:
                for i in range(3):
                    try:
                        api_create(c, {
                            "acid": "isolation_partial_writer",
                            "marker": uid(),
                            "v": f"v{i}",
                        })
                    except Exception:
                        pass
                    time.sleep(0.15)

        t_r = threading.Thread(target=reader)
        t_w = threading.Thread(target=writer)
        t_r.start(); t_w.start()
        t_r.join();  t_w.join()

        assert not partial_records, \
            f"Readers observed {len(partial_records)} partial write(s): {partial_records[:3]}"

    def test_concurrent_updates_same_record_no_split_brain(self, client):
        time.sleep(SETTLE)
        marker = uid()
        api_create(client, {"acid": "isolation_lww", "marker": marker, "v": "0"})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        def do_update(val):
            with make_client() as c:
                api_update(c, {"criteria": {"marker": marker}, "set": {"v": val}})

        t1 = threading.Thread(target=do_update, args=("1",))
        t2 = threading.Thread(target=do_update, args=("2",))
        t1.start(); t2.start()
        t1.join();  t2.join()

        time.sleep(SETTLE)

        rows = api_fetch(client, {"marker": {"op": "eq", "value": marker}}, source="merged")
        assert rows, "Record missing after concurrent updates"
        v = rows[0].get("v")
        assert str(v) in ("1", "2"), \
            f"Unexpected value after concurrent updates: {v!r}"

    def test_delete_concurrent_with_reads_no_torn_state(self, client):
        time.sleep(SETTLE)
        marker = uid()
        api_create(client, {"acid": "isolation_del_read", "marker": marker, "v": "present"})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        torn_states = []

        def reader():
            with make_client() as c:
                for _ in range(15):
                    try:
                        rows = api_fetch(c, {"marker": {"op": "eq", "value": marker}})
                        for row in rows:
                            if row.get("acid") is None or row.get("marker") is None:
                                torn_states.append(row)
                    except Exception:
                        pass
                    time.sleep(0.05)

        def deleter():
            time.sleep(0.2)
            with make_client() as c:
                api_delete(c, {"criteria": {"marker": marker}})

        t_r = threading.Thread(target=reader)
        t_d = threading.Thread(target=deleter)
        t_r.start(); t_d.start()
        t_r.join();  t_d.join()

        assert not torn_states, \
            f"Torn records observed during concurrent delete: {torn_states[:3]}"