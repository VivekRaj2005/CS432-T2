from conftest import (
    make_client, uid, api_create, api_update, api_delete,
    api_fetch, poll_until, poll_until_absent, POLL_INTERVAL,
)
import time


class TestAtomicity:

    def test_create_appears_in_merged(self, client):
        marker = uid()
        r = api_create(client, {"acid": "atomicity_create", "marker": marker})
        assert r.status_code == 200, r.text

        rows = poll_until(client, {"marker": {"op": "eq", "value": marker}},
                          predicate=lambda rs: len(rs) >= 1, source="merged")

        assert rows, "Record missing after create"
        assert rows[0].get("marker") == marker
        assert rows[0].get("acid") == "atomicity_create"

    def test_update_applies(self, client):
        marker = uid()
        api_create(client, {"acid": "atomicity_update", "marker": marker, "v": "old"})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        api_update(client, {"criteria": {"marker": marker}, "set": {"v": "new"}})

        rows = poll_until(
            client,
            {"marker": {"op": "eq", "value": marker}, "v": {"op": "eq", "value": "new"}},
            predicate=lambda rs: len(rs) >= 1,
            source="merged",
        )
        assert rows[0].get("v") == "new", f"Update not applied: {rows[0]}"

    def test_delete_removes_record(self, client):
        marker = uid()
        api_create(client, {"acid": "atomicity_delete", "marker": marker})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        api_delete(client, {"criteria": {"marker": marker}})
        poll_until_absent(client, {"marker": {"op": "eq", "value": marker}}, source="merged")

        rows = api_fetch(client, {"marker": {"op": "eq", "value": marker}}, source="merged")
        assert rows == [], f"Record still present after delete: {rows}"

    def test_invalid_create_leaves_state_unchanged(self, client):
        before = len(api_fetch(client, source="merged"))

        r = api_create(client, {})
        assert r.status_code == 400

        time.sleep(POLL_INTERVAL * 4)

        assert len(api_fetch(client, source="merged")) == before, \
            "Row count changed after rejected create"

    def test_invalid_update_leaves_record_unchanged(self, client):
        marker = uid()
        api_create(client, {"acid": "atomicity_invalid_update", "marker": marker, "v": "stable"})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        r = api_update(client, {"criteria": {"marker": marker}, "set": {}})
        assert r.status_code == 400

        time.sleep(POLL_INTERVAL * 4)

        rows = api_fetch(client, {"marker": {"op": "eq", "value": marker}}, source="merged")
        assert rows, "Record disappeared after rejected update"
        assert rows[0].get("v") == "stable", \
            f"Record modified despite rejected update: {rows[0]}"

    def test_multi_field_update_applied_atomically(self, client):
        marker = uid()
        api_create(client, {
            "acid": "atomicity_multi", "marker": marker,
            "f1": "a", "f2": "b", "f3": "c",
        })
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        api_update(client, {
            "criteria": {"marker": marker},
            "set":      {"f1": "x", "f2": "y", "f3": "z"},
        })

        rows = poll_until(
            client,
            {"marker": {"op": "eq", "value": marker}, "f1": {"op": "eq", "value": "x"}},
            predicate=lambda rs: len(rs) >= 1,
            source="merged",
        )
        row = rows[0]
        assert row.get("f1") == "x", f"f1 not updated: {row}"
        assert row.get("f2") == "y", f"f2 not updated: {row}"
        assert row.get("f3") == "z", f"f3 not updated: {row}"