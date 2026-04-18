from conftest import (
    make_client, uid, api_create, api_update, api_delete,
    api_fetch, poll_until, poll_until_absent, POLL_INTERVAL,
)
import time


class TestConsistency:

    def test_merged_view_equals_union_of_both_backends(self, client):
        marker = uid()
        api_create(client, {"acid": "consistency_merge", "marker": marker})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1, source="merged")

        sql_ids    = {r.get("table_autogen_id") for r in api_fetch(client, source="sql")}
        nosql_ids  = {r.get("table_autogen_id") for r in api_fetch(client, source="nosql")}
        merged_ids = {r.get("table_autogen_id") for r in api_fetch(client, source="merged")}

        missing = (sql_ids | nosql_ids) - merged_ids
        assert not missing, \
            f"Merged view missing {len(missing)} record(s) present in SQL or MongoDB: {list(missing)[:5]}"

    def test_update_preserves_untouched_fields(self, client):
        marker = uid()
        api_create(client, {
            "acid": "consistency_preserve", "marker": marker,
            "f1": "locked", "mutable": "old",
        })
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        api_update(client, {"criteria": {"marker": marker}, "set": {"mutable": "updated"}})
        rows = poll_until(
            client,
            {"marker": {"op": "eq", "value": marker}, "mutable": {"op": "eq", "value": "updated"}},
            predicate=lambda rs: len(rs) >= 1,
            source="merged",
        )

        assert rows, "Record missing after update"
        assert rows[0].get("f1") == "locked", \
            f"Untouched field altered: {rows[0]}"
        assert rows[0].get("mutable") == "updated", \
            f"Mutable field not updated: {rows[0]}"

    def test_schema_consistent_after_new_field_introduced(self, client):
        novel_field = f"novel_{uid()}"
        api_create(client, {"acid": "consistency_schema", novel_field: "value"})
        time.sleep(POLL_INTERVAL * 4)

        schema = client.get("/schema").json()
        assert schema["field_count"] == len(schema["fields"]), (
            f"Schema inconsistent: field_count={schema['field_count']}, "
            f"len(fields)={len(schema['fields'])}"
        )

    def test_no_phantom_reads_after_delete(self, client):
        marker = uid()
        api_create(client, {"acid": "consistency_phantom", "marker": marker})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        api_delete(client, {"criteria": {"marker": marker}})
        poll_until_absent(client, {"marker": {"op": "eq", "value": marker}})

        for source in ("sql", "nosql", "merged"):
            rows = api_fetch(client, {"marker": {"op": "eq", "value": marker}}, source=source)
            assert rows == [], f"Phantom read in source='{source}': {rows}"

    def test_sequential_updates_reach_final_consistent_state(self, client):
        marker = uid()
        api_create(client, {"acid": "consistency_sequential", "marker": marker, "v": "0"})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        for val in ("1", "2", "3"):
            api_update(client, {"criteria": {"marker": marker}, "set": {"v": val}})

        rows = poll_until(
            client,
            {"marker": {"op": "eq", "value": marker}, "v": {"op": "eq", "value": "3"}},
            predicate=lambda rs: len(rs) >= 1,
            source="merged",
        )
        assert rows[0].get("v") == "3", \
            f"Stuck on intermediate value: {rows[0]}"

    def test_create_then_delete_leaves_no_residue(self, client):
        marker = uid()
        api_create(client, {"acid": "consistency_residue", "marker": marker})
        poll_until(client, {"marker": {"op": "eq", "value": marker}},
                   predicate=lambda rs: len(rs) >= 1)

        api_delete(client, {"criteria": {"marker": marker}})
        poll_until_absent(client, {"marker": {"op": "eq", "value": marker}})

        for source in ("sql", "nosql"):
            rows = api_fetch(client, {"marker": {"op": "eq", "value": marker}}, source=source)
            assert rows == [], f"Residue in {source} after create+delete: {rows}"