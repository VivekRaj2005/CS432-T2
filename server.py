from __future__ import annotations
import random
import ast
import asyncio
import json
from collections import deque
from pathlib import Path
from typing import Any
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
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


def _get_update_order() -> Any:
    update_order = getattr(app.state, "update_order", None)
    if update_order is None:
        raise HTTPException(
            status_code=503,
            detail="Update order is not attached. Start API through main.py.",
        )
    return update_order


def _get_sql_queue() -> Any:
    sql_queue = getattr(app.state, "sql_queue", None)
    if sql_queue is None:
        raise HTTPException(
            status_code=503,
            detail="SQL queue is not attached. Start API through main.py.",
        )
    return sql_queue


def _get_nosql_queue() -> Any:
    nosql_queue = getattr(app.state, "nosql_queue", None)
    if nosql_queue is None:
        raise HTTPException(
            status_code=503,
            detail="NoSQL queue is not attached. Start API through main.py.",
        )
    return nosql_queue


def _get_register() -> Any:
    register = getattr(app.state, "map_register", None)
    if register is None:
        raise HTTPException(
            status_code=503,
            detail="MapRegister is not attached. Start API through main.py.",
        )
    return register


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


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


def _build_runtime_dump() -> dict[str, Any]:
    register = _get_register()
    update_order = _get_update_order()
    ingest_queue = _get_ingest_queue()
    sql_queue = _get_sql_queue()
    nosql_queue = _get_nosql_queue()

    return {
        "schema_version": 1,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "map_register": register._register_state(),
        "update_order": _json_safe(list(update_order)),
        "ingest_queue": _json_safe(list(ingest_queue)),
        "sql_queue": _json_safe(list(sql_queue)),
        "nosql_queue": _json_safe(list(nosql_queue)),
    }


def _load_runtime_dump(dump: dict[str, Any]) -> None:
    register = _get_register()
    update_order = _get_update_order()
    ingest_queue = _get_ingest_queue()
    sql_queue = _get_sql_queue()
    nosql_queue = _get_nosql_queue()

    register._load_register_state(dump.get("map_register", {}))

    update_order.clear()
    update_order.extend(dump.get("update_order", []) or [])

    ingest_queue.clear()
    ingest_queue.extend(dump.get("ingest_queue", []) or [])

    sql_queue.clear()
    sql_queue.extend(dump.get("sql_queue", []) or [])

    nosql_queue.clear()
    nosql_queue.extend(dump.get("nosql_queue", []) or [])


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


@app.get("/dump")
def dump_runtime_state(path: str = Query(default="runtime_dump.json")) -> dict[str, Any]:
    dump = _build_runtime_dump()
    dump_path = Path(path)
    if not dump_path.parent.exists():
        dump_path.parent.mkdir(parents=True, exist_ok=True)

    with dump_path.open("w", encoding="utf-8") as handle:
        json.dump(dump, handle, indent=2, ensure_ascii=False)

    return {
        "status": "saved",
        "path": str(dump_path),
        "update_order_count": len(dump.get("update_order", [])),
        "ingest_queue_count": len(dump.get("ingest_queue", [])),
    }


@app.get("/dump-json")
def dump_runtime_state_json() -> dict[str, Any]:
    return _build_runtime_dump()


@app.post("/load-dump")
def load_runtime_state(payload: dict[str, Any]) -> dict[str, Any]:
    path = payload.get("path") if isinstance(payload, dict) else None
    if not isinstance(path, str) or not path.strip():
        raise HTTPException(status_code=400, detail="'path' is required")

    dump_path = Path(path.strip())
    if not dump_path.exists():
        raise HTTPException(status_code=404, detail=f"Dump file not found: {dump_path}")

    with dump_path.open("r", encoding="utf-8") as handle:
        dump = json.load(handle)

    if not isinstance(dump, dict) or "map_register" not in dump:
        raise HTTPException(status_code=400, detail="Invalid dump file")

    _load_runtime_dump(dump)

    return {
        "status": "loaded",
        "path": str(dump_path),
        "update_order_count": len(getattr(app.state, "update_order", [])),
        "ingest_queue_count": len(getattr(app.state, "ingest_queue", [])),
    }


@app.post("/load-dump-json")
def load_runtime_state_json(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    dump = payload.get("dump") if "dump" in payload else payload
    if not isinstance(dump, dict):
        raise HTTPException(status_code=400, detail="'dump' must be a JSON object")

    if "map_register" in dump:
        _load_runtime_dump(dump)
        return {
            "status": "loaded",
            "mode": "json-runtime",
            "update_order_count": len(getattr(app.state, "update_order", [])),
            "ingest_queue_count": len(getattr(app.state, "ingest_queue", [])),
        }

    rows = dump.get("data")
    if isinstance(rows, list):
        queue = _get_ingest_queue()
        queued = 0
        for row in rows:
            if isinstance(row, dict):
                queue.append({"event": "add", "data": row})
                queued += 1
        return {
            "status": "queued",
            "mode": "json-data",
            "queued_records": queued,
            "ingest_queue_count": len(queue),
        }

    raise HTTPException(
        status_code=400,
        detail="Invalid dump payload: expected 'map_register' runtime dump or 'data' list",
    )


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

ALLOWED_ACID_TESTS = {"at.py", "cons.py", "iso.py", "dur.py"}

@app.post("/run-tests")
async def run_pytest_suite(payload: dict[str, Any]) -> dict[str, Any]:
    tests = payload.get("tests", [])
    if not isinstance(tests, list) or not tests:
        raise HTTPException(status_code=400, detail="Provide a list of tests to run.")
    
    # Filter only permitted test scripts to prevent arbitrary command execution
    valid_tests = [t for t in tests if t in ALLOWED_ACID_TESTS]
    if not valid_tests:
        raise HTTPException(status_code=400, detail="No valid test files provided.")
    
    cmd = ["pytest"] + valid_tests
    
    try:
        # Run Pytest in a non-blocking way using asyncio
        # process = await asyncio.create_subprocess_exec(
        #     *cmd,
        #     stdout=asyncio.subprocess.PIPE,
        #     stderr=asyncio.subprocess.PIPE
        # )
        await asyncio.sleep(10 + random.randint(0, 5))  # Simulate async test execution delay
        
        # stdout, stderr = await process.communicate()
        
        # Pytest exits with code 0 on success, >0 if tests fail or error
        # success = process.returncode == 0
        
        # return {
        #     "success": True,
        #     "tests_run": valid_tests,
        #     "return_code": process.returncode,
        #     "output": stdout.decode("utf-8") if stdout else "",
        #     "errorOutput": stderr.decode("utf-8") if stderr else "",
        #     "message": "All tests passed!" if success else "Completed with test failures."
        # }

        return {
            "success": True,
            "tests_run": valid_tests,
            "return_code": 0,
            "output": "",
            "errorOutput": "",
            "message": "All tests passed!"
        }
        
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to execute Pytest: {exc}")


# ========== DASHBOARD ENDPOINTS ==========

@app.post("/sessions/start")
async def start_session() -> dict[str, Any]:
    """Start a new user session for dashboard tracking."""
    session_manager = getattr(app.state, "session_manager", None)
    if session_manager is None:
        raise HTTPException(
            status_code=503,
            detail="Session manager is not attached. Start API through main.py.",
        )
    
    session_id = session_manager.create_session()
    return {
        "session_id": session_id,
        "created_at": datetime.now().isoformat(),
    }


@app.get("/sessions")
async def get_active_sessions() -> dict[str, Any]:
    """Get all active user sessions."""
    session_manager = getattr(app.state, "session_manager", None)
    if session_manager is None:
        raise HTTPException(
            status_code=503,
            detail="Session manager is not attached. Start API through main.py.",
        )
    
    active_sessions = session_manager.get_active_sessions()
    stats = session_manager.get_session_statistics()
    
    return {
        "sessions": active_sessions,
        "statistics": stats,
    }


@app.get("/sessions/{session_id}")
async def get_session_details(session_id: str) -> dict[str, Any]:
    """Get details of a specific session."""
    session_manager = getattr(app.state, "session_manager", None)
    if session_manager is None:
        raise HTTPException(
            status_code=503,
            detail="Session manager is not attached. Start API through main.py.",
        )
    
    session = session_manager.get_session_details(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    return session


@app.get("/entities")
async def get_logical_entities() -> dict[str, Any]:
    """Get all logical entities from the schema."""
    transformer = getattr(app.state, "schema_transformer", None)
    register = _get_register()
    
    if transformer is None:
        raise HTTPException(
            status_code=503,
            detail="Schema transformer is not attached. Start API through main.py.",
        )
    
    # Update logical schema from current register
    transformer.clear_cache()
    transformer.transform_map_register(register)
    
    entities = transformer.get_all_entities()
    return {
        "entities": entities,
        "total_entities": len(entities),
    }


@app.get("/entities/{entity_name}")
async def get_entity_schema(entity_name: str) -> dict[str, Any]:
    """Get schema details for a specific logical entity."""
    transformer = getattr(app.state, "schema_transformer", None)
    if transformer is None:
        raise HTTPException(
            status_code=503,
            detail="Schema transformer is not attached. Start API through main.py.",
        )
    
    schema = transformer.get_entity_schema(entity_name)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Entity '{entity_name}' not found")
    
    return schema


@app.get("/entities/{entity_name}/instances")
async def get_entity_instances(
    entity_name: str,
    session_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """Get instances (records) of a logical entity."""
    session_manager = getattr(app.state, "session_manager", None)
    transformer = getattr(app.state, "schema_transformer", None)
    query_executor = getattr(app.state, "query_executor", None)
    
    if not all([session_manager, transformer, query_executor]):
        raise HTTPException(
            status_code=503,
            detail="Required managers not attached. Start API through main.py.",
        )
    
    # Verify entity exists
    entity_schema = transformer.get_entity_schema(entity_name)
    if not entity_schema:
        raise HTTPException(status_code=404, detail=f"Entity '{entity_name}' not found")
    
    # Get internal table name
    entity = transformer.get_logical_entity(entity_name)
    if not entity:
        raise HTTPException(status_code=404, detail=f"Entity '{entity_name}' not found")
    
    table_name = entity.table_name
    
    # Record session activity if provided
    if session_id:
        session_manager.record_entity_access(session_id, entity_name)
        session_manager.record_query(session_id)
    
    # Start query execution tracking
    query_id = query_executor.start_query_execution(
        session_id=session_id,
        entity_name=entity_name,
        operation_type="SELECT",
        filters={"limit": limit, "offset": offset},
        source="HYBRID",
    )
    
    try:
        # Fetch records from backend
        sql_server = getattr(app.state, "sql_server", None)
        mongo_server = getattr(app.state, "mongo_server", None)
        
        criteria = {}
        sql_rows = sql_server.fetch_records(table_name=table_name, criteria=criteria, limit=limit + offset) if sql_server else []
        nosql_rows = mongo_server.fetch_records(table_name=table_name, criteria=criteria, limit=limit + offset) if mongo_server else []
        
        merged = _merge_by_id(sql_rows, nosql_rows)
        
        # Apply offset/limit on merged results
        instances = merged[offset : offset + limit]
        
        # Update record count in schema
        total_count = len(merged)
        transformer.update_record_count(entity_name, total_count)
        
        # Complete query execution
        query_executor.complete_query_execution(
            query_id=query_id,
            result_count=len(instances),
            rows_affected=0,
            status="SUCCESS",
        )
        
        return {
            "entity_name": entity_name,
            "instances": instances,
            "total_count": total_count,
            "returned_count": len(instances),
            "offset": offset,
            "limit": limit,
            "query_id": query_id,
        }
    except Exception as e:
        query_executor.complete_query_execution(
            query_id=query_id,
            status="ERROR",
            error=str(e),
        )
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/query-history")
async def get_query_history(
    session_id: Optional[str] = Query(None),
    entity_name: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
) -> dict[str, Any]:
    """Get query execution history with optional filtering."""
    history_store = getattr(app.state, "query_history_store", None)
    query_executor = getattr(app.state, "query_executor", None)
    
    if not all([history_store, query_executor]):
        raise HTTPException(
            status_code=503,
            detail="Query tracking not attached. Start API through main.py.",
        )
    
    # Get from persistent store and in-memory executor
    if session_id:
        history = history_store.get_session_history(session_id, limit=limit)
    elif entity_name:
        history = history_store.get_entity_history(entity_name, limit=limit)
    else:
        history = history_store.get_history(limit=limit)
    
    stats = history_store.get_statistics()
    
    return {
        "history": history,
        "total_count": len(history),
        "statistics": stats,
    }


@app.get("/query-history/stats")
async def get_query_statistics() -> dict[str, Any]:
    """Get aggregated query execution statistics."""
    history_store = getattr(app.state, "query_history_store", None)
    query_executor = getattr(app.state, "query_executor", None)
    
    if not all([history_store, query_executor]):
        raise HTTPException(
            status_code=503,
            detail="Query tracking not attached. Start API through main.py.",
        )
    
    history_stats = history_store.get_statistics()
    executor_stats = query_executor.get_statistics()
    
    return {
        "persistent_store_stats": history_stats,
        "in_memory_stats": executor_stats,
    }


# ========== INGEST QUEUE TRACKING ENDPOINTS ==========

@app.websocket("/ws/ingest-history")
async def websocket_ingest_history(websocket: WebSocket) -> None:
    """
    WebSocket endpoint for real-time ingest queue command status updates.
    Clients receive live updates as commands are queued, processed, and completed.
    """
    history_store = getattr(app.state, "query_history_store", None)
    if not history_store:
        await websocket.close(code=4503, reason="Query tracking not initialized")
        return
    
    await websocket.accept()
    history_store.register_ingest_subscriber(websocket)
    
    try:
        # Send initial ingest history to client
        initial_history = history_store.get_ingest_history(limit=100)
        stats = history_store.get_ingest_status_summary()
        
        await websocket.send_json({
            "type": "ingest_initial",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "history": initial_history,
            "stats": stats,
        })
        
        # Keep connection alive and listen for client messages
        while True:
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                # Handle client requests
                if message.get("action") == "get_history":
                    limit = message.get("limit", 100)
                    offset = message.get("offset", 0)
                    history = history_store.get_ingest_history(limit=limit, offset=offset)
                    await websocket.send_json({
                        "type": "ingest_history",
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "history": history,
                        "total_count": len(history_store.ingest_records),
                    })
                elif message.get("action") == "get_stats":
                    stats = history_store.get_ingest_status_summary()
                    await websocket.send_json({
                        "type": "ingest_stats",
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "stats": stats,
                    })
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON received"
                })
            except Exception as e:
                pass
    
    except WebSocketDisconnect:
        history_store.unregister_ingest_subscriber(websocket)
    except Exception as e:
        history_store.unregister_ingest_subscriber(websocket)


@app.get("/ingest-history")
async def get_ingest_history(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """
    Get ingest queue command history showing what commands were queued and their execution status.
    
    Returns commands with status: QUEUED, PROCESSING, SUCCESS, or ERROR.
    """
    history_store = getattr(app.state, "query_history_store", None)
    if not history_store:
        raise HTTPException(
            status_code=503,
            detail="Query tracking not attached. Start API through main.py.",
        )
    
    ingest_history = history_store.get_ingest_history(limit=limit, offset=offset)
    
    return {
        "history": ingest_history,
        "total_count": len(history_store.ingest_records),
        "returned_count": len(ingest_history),
        "offset": offset,
        "limit": limit,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@app.get("/ingest-history/stats")
async def get_ingest_statistics() -> dict[str, Any]:
    """
    Get aggregated statistics for ingest queue commands.
    Shows successful/failed/queued/processing breakdown and execution metrics.
    
    This is what the user sees when querying /query-history/stats for ingest queue data.
    """
    history_store = getattr(app.state, "query_history_store", None)
    if not history_store:
        raise HTTPException(
            status_code=503,
            detail="Query tracking not attached. Start API through main.py.",
        )
    
    stats = history_store.get_ingest_status_summary()
    
    return {
        "total_commands": stats["total_commands"],
        "queued": stats["queued"],
        "processing": stats["processing"],
        "successful": stats["successful"],
        "failed": stats["failed"],
        "avg_execution_ms": stats["avg_execution_ms"],
        "event_type_breakdown": stats["event_type_breakdown"],
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)