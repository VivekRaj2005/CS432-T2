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

    return {
        "source": "main.py runtime",
        "table_name": register.table_name,
        "request_count": register.request_count,
        "field_count": len(register.map),
        "field_classifications": register.get_field_classifications(),
        "fields": fields,
    }


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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
