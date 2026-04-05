from conftest import (
    make_client, uid, api_create, api_update, api_delete,
    api_fetch, poll_until, poll_until_absent, POLL_INTERVAL,
)
import time


class TestAtomicity:

    def test_create_appears_in_both_backends(self, client):
        marker = uid()
        payload = {"acid": "atomicity_create", "marker": marker, "value": 42}
        r = api_create(client, payload)
        assert r.status_code == 200, r.text

        sql_rows   = poll_until(client, {"marker": {"op": "eq", "value": marker}},
                                predicate=lambda rs: len(rs) >= 1, source="sql")
        nosql_rows = poll_until(client, {"marker": {"op": "eq", "value": marker}},
                                predicate=lambda rs: len(rs) >= 1, source="nosql")

        assert sql_rows,   "Record missing from SQL after create"
        assert nosql_rows, "Record missing from MongoDB after create"

        for field, expected in payload.items():
            assert str(sql_rows[0].get(field))   == str(expected), \
                f"SQL '{field}': expected {expected!r}, got {sql_rows[0].get(field)!r}"
            assert str(nosql_rows[0].get(field)) == str(expected), \
                f"MongoDB '{field}': expected {expected!r}, got {nosql_rows[0].get(field)!r}"

    def test_update_applies_to_both_backends(self, client):
        marker = uid()
        api_create(client, {"acid": "atomicity_update", "marker": marker, "v": "old"})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        api_update(client, {"criteria": {"marker": marker}, "set": {"v": "new"}})

        for source in ("sql", "nosql"):
            rows = poll_until(
                client,
                {"marker": {"op": "eq", "value": marker}, "v": {"op": "eq", "value": "new"}},
                predicate=lambda rs: len(rs) >= 1,
                source=source,
            )
            assert rows[0].get("v") == "new", f"{source} did not apply update: {rows[0]}"

    def test_delete_removes_from_both_backends(self, client):
        marker = uid()
        api_create(client, {"acid": "atomicity_delete", "marker": marker})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        api_delete(client, {"criteria": {"marker": marker}})
        poll_until_absent(client, {"marker": {"op": "eq", "value": marker}}, source="sql")
        poll_until_absent(client, {"marker": {"op": "eq", "value": marker}}, source="nosql")

        for source in ("sql", "nosql"):
            rows = api_fetch(client, {"marker": {"op": "eq", "value": marker}}, source=source)
            assert rows == [], f"Record still in {source} after delete: {rows}"

    def test_invalid_create_leaves_both_backends_unchanged(self, client):
        before_sql   = len(api_fetch(client, source="sql"))
        before_nosql = len(api_fetch(client, source="nosql"))

        r = api_create(client, {})
        assert r.status_code == 400

        time.sleep(POLL_INTERVAL * 4)

        assert len(api_fetch(client, source="sql"))   == before_sql,   \
            "SQL row count changed after rejected create"
        assert len(api_fetch(client, source="nosql")) == before_nosql, \
            "MongoDB row count changed after rejected create"

    def test_invalid_update_leaves_both_backends_unchanged(self, client):
        marker = uid()
        api_create(client, {"acid": "atomicity_invalid_update", "marker": marker, "v": "stable"})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        r = api_update(client, {"criteria": {"marker": marker}, "set": {}})
        assert r.status_code == 400

        time.sleep(POLL_INTERVAL * 4)

        for source in ("sql", "nosql"):
            rows = api_fetch(client, {"marker": {"op": "eq", "value": marker}}, source=source)
            assert rows, f"Record disappeared from {source} after rejected update"
            assert rows[0].get("v") == "stable", \
                f"{source} modified despite rejected update: {rows[0]}"

    def test_multi_field_update_all_fields_applied_atomically(self, client):
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

        for source in ("sql", "nosql"):
            rows = poll_until(
                client,
                {"marker": {"op": "eq", "value": marker}, "f1": {"op": "eq", "value": "x"}},
                predicate=lambda rs: len(rs) >= 1,
                source=source,
            )
            row = rows[0]
            assert row.get("f1") == "x", f"{source} f1 not updated: {row}"
            assert row.get("f2") == "y", f"{source} f2 not updated: {row}"
            assert row.get("f3") == "z", f"{source} f3 not updated: {row}"