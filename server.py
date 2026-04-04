from __future__ import annotations

import ast
import json
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from utils.resolve import Metadata

app = FastAPI(title="IITGnDB API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _serialize_metadata(meta: Metadata | None) -> dict[str, Any] | None:
    if meta is None:
        return None

    return {
        "type": meta.type,
        "subtype": _serialize_metadata(meta.subtype) if meta.subtype else None,
        "auto": meta.auto,
        "current_value": meta.current_value,
        "stability": meta.stabiltiy,
        "persistence": meta.persistance,
        "length": meta.length,
        "storage": meta.storage,
        "samples_seen": meta.idX,
        "type_locked": meta.type_locked,
    }


def _serialize_map_register(register: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {}

    for key, value in register.map.items():
        if isinstance(value, Metadata):
            fields[key] = _serialize_metadata(value)
        else:
            # Legacy checkpoints may contain non-Metadata values.
            fields[key] = {"raw": repr(value)}

    nested_registers = getattr(register, "nested_registers", {}) or {}
    nested_schema: dict[str, Any] = {}
    for nested_field, nested_register in nested_registers.items():
        nested_schema[nested_field] = _serialize_map_register(nested_register)

    foreign_key_references = getattr(register, "foreign_key_refs", {}) or {}

    return {
        "source": "main.py runtime",
        "table_name": register.table_name,
        "request_count": register.request_count,
        "field_count": len(register.map),
        "field_classifications": register.get_field_classifications(),
        "fields": fields,
        "foreign_key_references": foreign_key_references,
        "nested_schema": nested_schema,
    }


def _fetch_single_row(
    sql_server: Any,
    mongo_server: Any,
    table_name: str,
    row_id: Any,
    source: str,
) -> dict[str, Any] | None:
    criteria = {"table_autogen_id": row_id}
    if source == "sql":
        rows = sql_server.fetch_records(table_name=table_name, criteria=criteria, limit=1)
        return rows[0] if rows else None
    if source == "nosql":
        rows = mongo_server.fetch_records(table_name=table_name, criteria=criteria, limit=1)
        return rows[0] if rows else None

    sql_rows = sql_server.fetch_records(table_name=table_name, criteria=criteria, limit=1)
    nosql_rows = mongo_server.fetch_records(table_name=table_name, criteria=criteria, limit=1)
    merged = _merge_by_id(sql_rows, nosql_rows)
    return merged[0] if merged else None


def _expand_foreign_key_links(
    rows: list[dict[str, Any]],
    register: Any,
    sql_server: Any,
    mongo_server: Any,
    source: str,
) -> list[dict[str, Any]]:
    refs = getattr(register, "foreign_key_refs", {}) or {}
    if not refs:
        return rows

    cache: dict[tuple[str, Any, str], dict[str, Any] | None] = {}
    expanded: list[dict[str, Any]] = []
    for row in rows:
        row_copy = dict(row)
        for nested_field, ref_meta in refs.items():
            if not isinstance(ref_meta, dict):
                continue
            fk_field = ref_meta.get("fk_field")
            child_table = ref_meta.get("child_table")
            if not isinstance(fk_field, str) or not isinstance(child_table, str):
                continue

            fk_value = row_copy.get(fk_field)
            if fk_value is None:
                continue

            cache_key = (child_table, fk_value, source)
            if cache_key not in cache:
                cache[cache_key] = _fetch_single_row(
                    sql_server=sql_server,
                    mongo_server=mongo_server,
                    table_name=child_table,
                    row_id=fk_value,
                    source=source,
                )

            linked = cache[cache_key]
            if linked is not None:
                row_copy[nested_field] = linked

        expanded.append(row_copy)

    return expanded


def _get_ingest_queue() -> Any:
    queue = getattr(app.state, "ingest_queue", None)
    if queue is None:
        raise HTTPException(
            status_code=503,
            detail="Ingest queue is not attached. Start API through main.py.",
        )
    return queue


def _get_register() -> Any:
    register = getattr(app.state, "map_register", None)
    if register is None:
        raise HTTPException(
            status_code=503,
            detail="MapRegister is not attached. Start API through main.py.",
        )
    return register


def _decode_conditions(conditions: str | None) -> dict[str, Any]:
    if not conditions:
        return {}

    try:
        parsed = json.loads(conditions)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid conditions JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="Conditions must be a JSON object")

    return parsed


def _normalize_filters(conditions: dict[str, Any]) -> list[dict[str, Any]]:
    if "$filters" in conditions:
        filters = conditions.get("$filters")
        if not isinstance(filters, list):
            raise HTTPException(status_code=400, detail="$filters must be a list")
        normalized = []
        for item in filters:
            if not isinstance(item, dict):
                raise HTTPException(status_code=400, detail="Each filter must be an object")
            field = item.get("field")
            op = item.get("op", "eq")
            if not isinstance(field, str) or not field.strip():
                raise HTTPException(status_code=400, detail="Filter field is required")
            normalized.append({"field": field.strip(), "op": op, "value": item.get("value")})
        return normalized

    normalized = []
    for field, condition in conditions.items():
        if isinstance(condition, dict):
            if "op" in condition:
                normalized.append(
                    {
                        "field": field,
                        "op": condition.get("op", "eq"),
                        "value": condition.get("value"),
                    }
                )
                continue

            for op_key, op_value in condition.items():
                if op_key == "len":
                    if isinstance(op_value, dict):
                        for len_op, len_value in op_value.items():
                            normalized.append(
                                {"field": field, "op": f"len_{len_op}", "value": len_value}
                            )
                    else:
                        normalized.append({"field": field, "op": "len_eq", "value": op_value})
                else:
                    normalized.append({"field": field, "op": op_key, "value": op_value})
        else:
            normalized.append({"field": field, "op": "eq", "value": condition})

    return normalized


def _field_value(record: dict[str, Any], field: str) -> Any:
    current: Any = record
    for part in field.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _as_number(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean is not numeric")
    return float(value)


def _to_bool_like(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except Exception:
            return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _eval_math_expression(value: Any) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if not isinstance(value, str):
        raise ValueError("Expression must be a number or string")

    tree = ast.parse(value, mode="eval")

    def _eval(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return float(node.value)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            val = _eval(node.operand)
            return val if isinstance(node.op, ast.UAdd) else -val
        if isinstance(node, ast.BinOp):
            left = _eval(node.left)
            right = _eval(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.FloorDiv):
                return left // right
            if isinstance(node.op, ast.Mod):
                return left % right
            if isinstance(node.op, ast.Pow):
                return left**right
        raise ValueError("Unsupported expression")

    return _eval(tree)


def _matches_filter(record: dict[str, Any], flt: dict[str, Any]) -> bool:
    op = str(flt.get("op", "eq"))
    expected = flt.get("value")
    actual = _field_value(record, flt["field"])

    actual_bool = _to_bool_like(actual)
    expected_bool = _to_bool_like(expected)
    if actual_bool is not None and expected_bool is not None and op in {"eq", "ne", "neq", "not_equals"}:
        return actual_bool == expected_bool if op == "eq" else actual_bool != expected_bool

    if op == "eq":
        return actual == expected
    if op in {"ne", "neq", "not_equals"}:
        return actual != expected

    if op in {"gt", "gte", "lt", "lte"}:
        try:
            actual_num = _as_number(actual)
            expected_num = _as_number(expected)
        except Exception:
            return False
        if op == "gt":
            return actual_num > expected_num
        if op == "gte":
            return actual_num >= expected_num
        if op == "lt":
            return actual_num < expected_num
        return actual_num <= expected_num

    if op.startswith("len_"):
        if actual is None:
            return False
        try:
            actual_len = len(actual)
            expected_len = _eval_math_expression(expected)
        except Exception:
            return False

        len_op = op[4:]
        if len_op in {"eq", "equals"}:
            return actual_len == expected_len
        if len_op in {"ne", "neq"}:
            return actual_len != expected_len
        if len_op == "gt":
            return actual_len > expected_len
        if len_op == "gte":
            return actual_len >= expected_len
        if len_op == "lt":
            return actual_len < expected_len
        if len_op == "lte":
            return actual_len <= expected_len
        return False

    if op == "isMember":
        if actual is None:
            return False
        if isinstance(actual, dict):
            return expected in actual
        if isinstance(actual, (list, tuple, set, str)):
            return expected in actual
        return False

    if op == "array_contains":
        return isinstance(actual, (list, tuple, set)) and expected in actual

    if op == "array_contains_all":
        if not isinstance(actual, (list, tuple, set)):
            return False
        if not isinstance(expected, list):
            return False
        return all(item in actual for item in expected)

    if op == "array_contains_any":
        if not isinstance(actual, (list, tuple, set)):
            return False
        if not isinstance(expected, list):
            return False
        return any(item in actual for item in expected)

    if op == "dict_has_key":
        return isinstance(actual, dict) and expected in actual

    if op == "dict_has_value":
        return isinstance(actual, dict) and expected in actual.values()

    raise HTTPException(status_code=400, detail=f"Unsupported operator: {op}")


def _apply_filters(rows: list[dict[str, Any]], filters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not filters:
        return rows
    return [row for row in rows if all(_matches_filter(row, flt) for flt in filters)]


def _merge_by_id(sql_rows: list[dict[str, Any]], nosql_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[Any, dict[str, Any]] = {}

    for row in sql_rows:
        key = row.get("table_autogen_id")
        if key is None:
            continue
        merged[key] = {**row}

    for row in nosql_rows:
        key = row.get("table_autogen_id")
        if key is None:
            continue
        if key in merged:
            merged[key].update(row)
        else:
            merged[key] = {**row}

    return list(merged.values())


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/schema")
def get_schema() -> dict[str, Any]:
    register = _get_register()

    try:
        return _serialize_map_register(register)
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Failed to read map register: {exc}") from exc


@app.get("/map-register")
def get_map_register() -> dict[str, Any]:
    return get_schema()


@app.get("/fetch")
def fetch_records(
    conditions: str | None = Query(default=None, description="JSON object string"),
    limit: int = Query(default=100, ge=1, le=1000),
    source: str = Query(default="merged", pattern="^(sql|nosql|merged)$"),
) -> dict[str, Any]:
    register = _get_register()
    sql_server = getattr(app.state, "sql_server", None)
    mongo_server = getattr(app.state, "mongo_server", None)
    if sql_server is None or mongo_server is None:
        raise HTTPException(status_code=503, detail="Database executors are not attached")

    table_name = register.table_name
    criteria = _decode_conditions(conditions)
    filters = _normalize_filters(criteria)

    read_limit = min(max(limit * 5, 200), 1000)
    sql_rows = sql_server.fetch_records(table_name=table_name, criteria={}, limit=read_limit)
    nosql_rows = mongo_server.fetch_records(table_name=table_name, criteria={}, limit=read_limit)

    if source == "sql":
        data = sql_rows
    elif source == "nosql":
        data = nosql_rows
    else:
        data = _merge_by_id(sql_rows, nosql_rows)

    data = _expand_foreign_key_links(
        rows=data,
        register=register,
        sql_server=sql_server,
        mongo_server=mongo_server,
        source=source,
    )

    data = _apply_filters(data, filters)
    data = data[:limit]

    return {
        "table_name": table_name,
        "criteria": criteria,
        "filters": filters,
        "source": source,
        "count": len(data),
        "data": data,
    }


@app.post("/create")
def create_record(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        raise HTTPException(status_code=400, detail="Payload cannot be empty")

    queue = _get_ingest_queue()
    queue.append({"event": "add", "data": payload})

    return {
        "status": "queued",
        "event": "add",
        "queued_fields": list(payload.keys()),
        "queue_size": len(queue),
    }


@app.post("/update")
def update_record(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or not payload:
        raise HTTPException(status_code=400, detail="Payload cannot be empty")

    # Accept common aliases to keep the endpoint tolerant of client variations.
    criteria = payload.get("criteria")
    if criteria is None:
        criteria = payload.get("criterias")
    if criteria is None:
        criteria = payload.get("where")
    if criteria is None:
        criteria = payload.get("filter")

    conditions = payload.get("conditions")
    if conditions is None:
        conditions = payload.get("condition")
    if conditions is None:
        conditions = payload.get("filters")

    set_fields = payload.get("set")
    if set_fields is None:
        set_fields = payload.get("updates")
    if set_fields is None:
        set_fields = payload.get("changes")
    if set_fields is None:
        set_fields = payload.get("values")

    if not isinstance(set_fields, dict) or not set_fields:
        raise HTTPException(status_code=400, detail="'set' must be a non-empty object")

    queue = _get_ingest_queue()
    # Backward-compatible direct criteria mode.
    if isinstance(criteria, dict) and criteria:
        queue.append(
            {
                "event": "update",
                "data": {
                    "criteria": criteria,
                    "set": set_fields,
                },
            }
        )
        return {
            "status": "queued",
            "event": "update",
            "mode": "criteria",
            "criteria_fields": list(criteria.keys()),
            "set_fields": list(set_fields.keys()),
            "queue_size": len(queue),
        }

    # Fetch-style conditions mode for advanced matching operators.
    if not isinstance(conditions, dict) or not conditions:
        raise HTTPException(
            status_code=400,
            detail="Provide either non-empty 'criteria' or non-empty 'conditions' object",
        )

    filters = _normalize_filters(conditions)
    if not filters:
        raise HTTPException(status_code=400, detail="No usable filters were provided in 'conditions'")

    source = str(payload.get("source", "merged")).lower()
    if source not in {"sql", "nosql", "merged"}:
        raise HTTPException(status_code=400, detail="source must be one of: sql, nosql, merged")

    try:
        limit = int(payload.get("limit", 1000))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="limit must be an integer") from exc
    limit = max(1, min(limit, 1000))

    register = _get_register()
    sql_server = getattr(app.state, "sql_server", None)
    mongo_server = getattr(app.state, "mongo_server", None)
    if sql_server is None or mongo_server is None:
        raise HTTPException(status_code=503, detail="Database executors are not attached")

    table_name = register.table_name
    read_limit = min(max(limit * 5, 200), 1000)
    sql_rows = sql_server.fetch_records(table_name=table_name, criteria={}, limit=read_limit)
    nosql_rows = mongo_server.fetch_records(table_name=table_name, criteria={}, limit=read_limit)

    if source == "sql":
        rows = sql_rows
    elif source == "nosql":
        rows = nosql_rows
    else:
        rows = _merge_by_id(sql_rows, nosql_rows)

    matched_rows = _apply_filters(rows, filters)[:limit]
    matched_ids = []
    seen_ids = set()
    for row in matched_rows:
        row_id = row.get("table_autogen_id")
        if row_id is None or row_id in seen_ids:
            continue
        seen_ids.add(row_id)
        matched_ids.append(row_id)

    for row_id in matched_ids:
        queue.append(
            {
                "event": "update",
                "data": {
                    "criteria": {"table_autogen_id": row_id},
                    "set": set_fields,
                },
            }
        )

    return {
        "status": "queued",
        "event": "update",
        "mode": "conditions",
        "source": source,
        "filters": filters,
        "matched_rows": len(matched_rows),
        "queued_updates": len(matched_ids),
        "set_fields": list(set_fields.keys()),
        "queue_size": len(queue),
    }


@app.post("/delete")
def delete_record(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or not payload:
        raise HTTPException(status_code=400, detail="Payload cannot be empty")

    # Accept common aliases to keep the endpoint tolerant of client variations.
    criteria = payload.get("criteria")
    if criteria is None:
        criteria = payload.get("criterias")
    if criteria is None:
        criteria = payload.get("where")
    if criteria is None:
        criteria = payload.get("filter")

    conditions = payload.get("conditions")
    if conditions is None:
        conditions = payload.get("condition")
    if conditions is None:
        conditions = payload.get("filters")

    full_delete = bool(payload.get("full_delete", False))

    source = str(payload.get("source", "merged")).lower()
    if source not in {"sql", "nosql", "merged"}:
        raise HTTPException(status_code=400, detail="source must be one of: sql, nosql, merged")

    try:
        limit = int(payload.get("limit", 1000))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="limit must be an integer") from exc
    limit = max(1, min(limit, 1000))

    queue = _get_ingest_queue()
    # Backward-compatible direct criteria mode.
    if isinstance(criteria, dict) and criteria:
        queue.append(
            {
                "event": "delete",
                "data": criteria,
            }
        )
        return {
            "status": "queued",
            "event": "delete",
            "mode": "criteria",
            "criteria_fields": list(criteria.keys()),
            "queue_size": len(queue),
        }

    filters: list[dict[str, Any]] = []
    # Fetch-style conditions mode for advanced matching operators.
    if isinstance(conditions, dict) and conditions:
        filters = _normalize_filters(conditions)
        if not filters:
            raise HTTPException(status_code=400, detail="No usable filters were provided in 'conditions'")
    elif not full_delete:
        raise HTTPException(
            status_code=400,
            detail="Provide either non-empty 'criteria' or non-empty 'conditions' object",
        )

    register = _get_register()
    sql_server = getattr(app.state, "sql_server", None)
    mongo_server = getattr(app.state, "mongo_server", None)
    if sql_server is None or mongo_server is None:
        raise HTTPException(status_code=503, detail="Database executors are not attached")

    table_name = register.table_name
    read_limit = min(max(limit * 5, 200), 1000)
    sql_rows = sql_server.fetch_records(table_name=table_name, criteria={}, limit=read_limit)
    nosql_rows = mongo_server.fetch_records(table_name=table_name, criteria={}, limit=read_limit)

    if source == "sql":
        rows = sql_rows
    elif source == "nosql":
        rows = nosql_rows
    else:
        rows = _merge_by_id(sql_rows, nosql_rows)

    matched_rows = rows[:limit] if full_delete and not filters else _apply_filters(rows, filters)[:limit]
    matched_ids = []
    seen_ids = set()
    for row in matched_rows:
        row_id = row.get("table_autogen_id")
        if row_id is None or row_id in seen_ids:
            continue
        seen_ids.add(row_id)
        matched_ids.append(row_id)

    for row_id in matched_ids:
        queue.append(
            {
                "event": "delete",
                "data": {"table_autogen_id": row_id},
            }
        )

    return {
        "status": "queued",
        "event": "delete",
        "mode": "full_delete" if full_delete and not filters else "conditions",
        "source": source,
        "filters": filters,
        "matched_rows": len(matched_rows),
        "queued_deletes": len(matched_ids),
        "queue_size": len(queue),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
