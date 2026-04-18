"use client";

import { useMemo, useState } from "react";
import Navbar from "../Components/Navbar";

type ConditionType = "string" | "number" | "boolean" | "array" | "dict";

type ConditionOperator =
  | "eq"
  | "ne"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "len_eq"
  | "len_gt"
  | "len_gte"
  | "len_lt"
  | "len_lte"
  | "isMember"
  | "array_contains"
  | "array_contains_all"
  | "array_contains_any"
  | "dict_has_key"
  | "dict_has_value";

type ConditionRow = {
  id: string;
  field: string;
  type: ConditionType;
  op: ConditionOperator;
  value: string;
};

type FieldType =
  | "string"
  | "number"
  | "boolean"
  | "dict"
  | "list<string>"
  | "list<number>"
  | "list<boolean>";

type UpdateField = {
  id: string;
  name: string;
  type: FieldType;
  value: string;
  listValues: string[];
};

const FIELD_TYPES: FieldType[] = [
  "string",
  "number",
  "boolean",
  "dict",
  "list<string>",
  "list<number>",
  "list<boolean>",
];

const CONDITION_OPERATORS: ConditionOperator[] = [
  "eq",
  "ne",
  "gt",
  "gte",
  "lt",
  "lte",
  "len_eq",
  "len_gt",
  "len_gte",
  "len_lt",
  "len_lte",
  "isMember",
  "array_contains",
  "array_contains_all",
  "array_contains_any",
  "dict_has_key",
  "dict_has_value",
];

function nextId(prefix: string): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${prefix}-${crypto.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function createField(id: string): UpdateField {
  return {
    id,
    name: "",
    type: "string",
    value: "",
    listValues: [""],
  };
}

function createCondition(id: string): ConditionRow {
  return {
    id,
    field: "",
    type: "string",
    op: "eq",
    value: "",
  };
}

function parseFieldValue(
  field: UpdateField,
):
  | string
  | number
  | boolean
  | Array<string | number | boolean>
  | Record<string, unknown> {
  if (field.type === "string") {
    return field.value;
  }

  if (field.type === "number") {
    return Number(field.value);
  }

  if (field.type === "boolean") {
    return field.value === "true";
  }

  if (field.type === "dict") {
    const parsed = JSON.parse(field.value || "{}");
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      throw new Error(
        `Field '${field.name || "(unnamed)"}' must be a JSON object`,
      );
    }
    return parsed as Record<string, unknown>;
  }

  if (field.type === "list<string>") {
    return field.listValues.filter((item) => item.trim() !== "");
  }

  if (field.type === "list<number>") {
    return field.listValues
      .filter((item) => item.trim() !== "")
      .map((item) => Number(item));
  }

  return field.listValues
    .filter((item) => item.trim() !== "")
    .map((item) => item === "true");
}

function parseConditionValue(row: ConditionRow): string | number | boolean {
  if (row.type === "array") {
    const parsed = JSON.parse(row.value || "[]");
    if (!Array.isArray(parsed)) {
      throw new Error("Array type requires a JSON array value");
    }
    return JSON.stringify(parsed);
  }
  if (row.type === "dict") {
    const parsed = JSON.parse(row.value || "{}");
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      throw new Error("Dict type requires a JSON object value");
    }
    return JSON.stringify(parsed);
  }
  if (row.type === "number") {
    return Number(row.value);
  }
  if (row.type === "boolean") {
    return row.value === "true";
  }
  return row.value;
}

function parseConditionPayload(row: ConditionRow): {
  field: string;
  op: ConditionOperator;
  value: unknown;
} {
  const op = (row.op ?? "eq") as ConditionOperator;
  const rawValue = row.value.trim();

  if (
    op === "array_contains" ||
    op === "isMember" ||
    op === "dict_has_key" ||
    op === "dict_has_value"
  ) {
    if (row.type === "number") {
      return { field: row.field.trim(), op, value: Number(rawValue) };
    }
    if (row.type === "boolean") {
      return { field: row.field.trim(), op, value: rawValue === "true" };
    }
    return { field: row.field.trim(), op, value: rawValue };
  }

  if (op === "array_contains_all" || op === "array_contains_any") {
    const parsed = JSON.parse(rawValue || "[]");
    if (!Array.isArray(parsed)) {
      throw new Error(`${op} expects a JSON array value`);
    }
    return {
      field: row.field.trim(),
      op,
      value: parsed,
    };
  }

  if (op.startsWith("len_")) {
    return {
      field: row.field.trim(),
      op,
      value: rawValue,
    };
  }

  let value: unknown = parseConditionValue(row);
  if (row.type === "array" || row.type === "dict") {
    value = JSON.parse(String(value));
  }

  return {
    field: row.field.trim(),
    op,
    value,
  };
}

export default function UpdatePage() {
  const endpoint = useMemo(
    () =>
      process.env.NEXT_PUBLIC_UPDATE_ENDPOINT ?? "http://127.0.0.1:8000/update",
    [],
  );
  const fetchEndpoint = useMemo(
    () =>
      process.env.NEXT_PUBLIC_FETCH_ENDPOINT ?? "http://127.0.0.1:8000/fetch",
    [],
  );

  const [conditions, setConditions] = useState<ConditionRow[]>([
    createCondition("cond-1"),
  ]);
  const [source, setSource] = useState<"merged" | "sql" | "nosql">("merged");
  const [limit, setLimit] = useState(100);
  const [setFields, setSetFields] = useState<UpdateField[]>([
    createField("set-1"),
  ]);
  const [status, setStatus] = useState<string>("Idle");
  const [error, setError] = useState<string | null>(null);
  const [responseText, setResponseText] = useState<string>("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  function updateCondition(
    id: string,
    updater: (prev: ConditionRow) => ConditionRow,
  ) {
    setConditions((prev) =>
      prev.map((item) => (item.id === id ? updater(item) : item)),
    );
  }

  function addCondition() {
    setConditions((prev) => [...prev, createCondition(nextId("cond"))]);
  }

  function removeCondition(id: string) {
    setConditions((prev) =>
      prev.length > 1 ? prev.filter((item) => item.id !== id) : prev,
    );
  }

  function updateSetField(
    id: string,
    updater: (prev: UpdateField) => UpdateField,
  ) {
    setSetFields((prev) =>
      prev.map((item) => (item.id === id ? updater(item) : item)),
    );
  }

  function addSetField() {
    setSetFields((prev) => [...prev, createField(nextId("set"))]);
  }

  function removeSetField(id: string) {
    setSetFields((prev) =>
      prev.length > 1 ? prev.filter((item) => item.id !== id) : prev,
    );
  }

  function addListInput(id: string) {
    updateSetField(id, (prev) => ({
      ...prev,
      listValues: [...prev.listValues, ""],
    }));
  }

  function removeListInput(id: string, index: number) {
    updateSetField(id, (prev) => {
      if (prev.listValues.length <= 1) {
        return prev;
      }
      return {
        ...prev,
        listValues: prev.listValues.filter((_, idx) => idx !== index),
      };
    });
  }

  function renderSetFields() {
    return (
      <div className="space-y-4 rounded-xl border border-slate-200 bg-slate-50 p-4 sm:p-5">
        <div className="flex items-center justify-between gap-3">
          <div>
            <h2 className="text-sm font-semibold tracking-wide text-slate-800 uppercase">
              Set Fields
            </h2>
            <p className="mt-1 text-xs text-slate-600">
              These values are written on matched records.
            </p>
          </div>
          <button
            type="button"
            onClick={addSetField}
            className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100"
          >
            Add Field
          </button>
        </div>

        {setFields.map((field, fieldIndex) => {
          const isListType = field.type.startsWith("list<");
          const isBooleanType = field.type === "boolean";
          const isDictType = field.type === "dict";

          return (
            <div
              key={field.id}
              className="rounded-xl border border-slate-200 bg-white p-4"
            >
              <div className="mb-4 flex items-center justify-between">
                <p className="text-sm font-semibold text-slate-700">
                  Field {fieldIndex + 1}
                </p>
                <button
                  type="button"
                  onClick={() => removeSetField(field.id)}
                  className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-600 hover:bg-slate-100"
                >
                  Remove
                </button>
              </div>

              <div className="grid gap-4 md:grid-cols-3">
                <label className="flex flex-col gap-2 text-sm text-slate-700">
                  Field Name
                  <input
                    value={field.name}
                    onChange={(e) =>
                      updateSetField(field.id, (prev) => ({
                        ...prev,
                        name: e.target.value,
                      }))
                    }
                    placeholder="e.g. salary"
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2 outline-none ring-slate-200 transition focus:ring"
                  />
                </label>

                <label className="flex flex-col gap-2 text-sm text-slate-700">
                  Type
                  <select
                    value={field.type}
                    onChange={(e) =>
                      updateSetField(field.id, (prev) => ({
                        ...prev,
                        type: e.target.value as FieldType,
                        value: "",
                        listValues: [""],
                      }))
                    }
                    className="rounded-lg border border-slate-300 bg-white px-3 py-2 outline-none ring-slate-200 transition focus:ring"
                  >
                    {FIELD_TYPES.map((fieldType) => (
                      <option key={fieldType} value={fieldType}>
                        {fieldType}
                      </option>
                    ))}
                  </select>
                </label>

                {!isListType ? (
                  <label className="flex flex-col gap-2 text-sm text-slate-700">
                    Input
                    {isBooleanType ? (
                      <select
                        value={field.value || "false"}
                        onChange={(e) =>
                          updateSetField(field.id, (prev) => ({
                            ...prev,
                            value: e.target.value,
                          }))
                        }
                        className="rounded-lg border border-slate-300 bg-white px-3 py-2 outline-none ring-slate-200 transition focus:ring"
                      >
                        <option value="false">false</option>
                        <option value="true">true</option>
                      </select>
                    ) : isDictType ? (
                      <textarea
                        value={field.value}
                        onChange={(e) =>
                          updateSetField(field.id, (prev) => ({
                            ...prev,
                            value: e.target.value,
                          }))
                        }
                        placeholder={`{\n  "nested": { "key": "value" }\n}`}
                        rows={4}
                        className="rounded-lg border border-slate-300 bg-white px-3 py-2 font-mono text-xs outline-none ring-slate-200 transition focus:ring"
                      />
                    ) : (
                      <input
                        type={field.type === "number" ? "number" : "text"}
                        value={field.value}
                        onChange={(e) =>
                          updateSetField(field.id, (prev) => ({
                            ...prev,
                            value: e.target.value,
                          }))
                        }
                        placeholder={
                          field.type === "number" ? "e.g. 42" : "Enter value"
                        }
                        className="rounded-lg border border-slate-300 bg-white px-3 py-2 outline-none ring-slate-200 transition focus:ring"
                      />
                    )}
                  </label>
                ) : (
                  <div className="md:col-span-1" />
                )}
              </div>

              {isListType ? (
                <div className="mt-4 space-y-3 rounded-lg border border-dashed border-slate-300 bg-slate-50 p-4">
                  <div className="flex items-center justify-between">
                    <p className="text-sm font-medium text-slate-700">
                      List Input Items
                    </p>
                    <button
                      type="button"
                      onClick={() => addListInput(field.id)}
                      className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100"
                    >
                      Add List Item
                    </button>
                  </div>

                  {field.listValues.map((item, index) => (
                    <div
                      key={`${field.id}-list-${index}`}
                      className="flex items-center gap-2"
                    >
                      <input
                        type={field.type === "list<number>" ? "number" : "text"}
                        value={item}
                        onChange={(e) =>
                          updateSetField(field.id, (prev) => ({
                            ...prev,
                            listValues: prev.listValues.map(
                              (listItem, listIndex) =>
                                listIndex === index ? e.target.value : listItem,
                            ),
                          }))
                        }
                        placeholder={
                          field.type === "list<number>" ? "e.g. 12" : "Value"
                        }
                        className="flex-1 rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm outline-none ring-slate-200 transition focus:ring"
                      />
                      <button
                        type="button"
                        onClick={() => removeListInput(field.id, index)}
                        className="rounded-md border border-slate-300 bg-white px-3 py-2 text-xs font-medium text-slate-600 hover:bg-slate-100"
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    );
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    setResponseText("");

    const filters: Array<{
      field: string;
      op: ConditionOperator;
      value: unknown;
    }> = [];
    const setPayload: Record<string, unknown> = {};

    try {
      for (const row of conditions) {
        if (!row.field.trim()) {
          continue;
        }
        filters.push(parseConditionPayload(row));
      }

      for (const field of setFields) {
        const cleanName = field.name.trim();
        if (!cleanName) {
          continue;
        }
        setPayload[cleanName] = parseFieldValue(field);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Invalid update payload");
      return;
    }

    if (filters.length === 0) {
      setError("Add at least one condition.");
      return;
    }

    if (Object.keys(setPayload).length === 0) {
      setError("Add at least one field to update in Set Fields.");
      return;
    }

    setIsSubmitting(true);
    setStatus("Finding matches...");

    try {
      const fetchUrl = new URL(fetchEndpoint);
      fetchUrl.searchParams.set("source", source);
      fetchUrl.searchParams.set("limit", String(limit));
      fetchUrl.searchParams.set(
        "conditions",
        JSON.stringify({ $filters: filters }),
      );

      const fetchResponse = await fetch(fetchUrl.toString(), { method: "GET" });
      const fetchData = await fetchResponse.json();
      if (!fetchResponse.ok) {
        throw new Error(
          fetchData?.detail ??
            `Fetch match request failed: ${fetchResponse.status}`,
        );
      }

      const rows: unknown[] = Array.isArray(fetchData?.data)
        ? fetchData.data
        : [];
      const matchedIds = Array.from(
        new Set(
          rows
            .map((row) => {
              if (
                typeof row === "object" &&
                row !== null &&
                "table_autogen_id" in row
              ) {
                return (row as { table_autogen_id?: unknown }).table_autogen_id;
              }
              return undefined;
            })
            .filter(
              (id: unknown): id is number | string =>
                typeof id === "number" || typeof id === "string",
            ),
        ),
      );

      if (matchedIds.length === 0) {
        setStatus("No matching rows");
        setResponseText(
          JSON.stringify(
            {
              matched_rows: 0,
              queued_updates: 0,
              message: "No records matched the current conditions.",
            },
            null,
            2,
          ),
        );
        return;
      }

      setStatus("Queueing updates...");
      const updateResults = await Promise.all(
        matchedIds.map(async (id) => {
          const response = await fetch(endpoint, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              criteria: { table_autogen_id: id },
              set: setPayload,
            }),
          });
          const data = await response.json().catch(() => ({}));
          if (!response.ok) {
            return {
              ok: false,
              id,
              error: data?.detail ?? `Update failed: ${response.status}`,
            };
          }
          return {
            ok: true,
            id,
            response: data,
          };
        }),
      );

      const succeeded = updateResults.filter((item) => item.ok).length;
      const failed = updateResults.length - succeeded;

      setStatus(
        failed === 0 ? "Update queued successfully" : "Partially queued",
      );
      setResponseText(
        JSON.stringify(
          {
            matched_rows: matchedIds.length,
            queued_updates: succeeded,
            failed_updates: failed,
            errors: updateResults
              .filter((item) => !item.ok)
              .map((item) => ({ id: item.id, error: item.error })),
          },
          null,
          2,
        ),
      );
    } catch (err) {
      setStatus("Failed");
      setError(err instanceof Error ? err.message : "Failed to update record");
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <main className="mx-auto w-full max-w-5xl px-6 py-10 sm:px-10">
      <Navbar />

      <section className="mt-8 rounded-2xl border border-slate-200 bg-white p-6 shadow-sm sm:p-8">
        <h1 className="text-3xl font-semibold tracking-tight text-slate-900">
          Update Record
        </h1>
        <p className="mt-2 text-sm text-slate-600">Endpoint: {endpoint}</p>

        <div className="mt-6 grid gap-4 rounded-xl border border-slate-200 bg-slate-50 p-4 md:grid-cols-2">
          <div>
            <p className="text-sm font-semibold tracking-wide text-slate-800 uppercase">
              Condition Builder Guide
            </p>
            <p className="mt-2 text-sm text-slate-600">
              Use the same operators as Fetch. All conditions are combined using
              AND.
            </p>
          </div>
          <div className="space-y-2 text-xs text-slate-700">
            <p>
              <span className="font-semibold">Numeric:</span> salary &gt;= 50000
            </p>
            <p>
              <span className="font-semibold">Array:</span> tags array_contains
              &quot;urgent&quot;
            </p>
            <p>
              <span className="font-semibold">Length:</span> len(tags) &gt; 2+3
            </p>
            <p>
              <span className="font-semibold">Dict:</span> profile dict_has_key
              &quot;skills&quot;
            </p>
          </div>
        </div>

        <form className="mt-8 space-y-6" onSubmit={handleSubmit}>
          <div className="grid gap-4 md:grid-cols-2">
            <label className="flex flex-col gap-2 text-sm text-slate-700">
              Source
              <select
                value={source}
                onChange={(e) =>
                  setSource(e.target.value as "merged" | "sql" | "nosql")
                }
                className="rounded-lg border border-slate-300 bg-white px-3 py-2"
              >
                <option value="merged">merged</option>
                <option value="sql">sql</option>
                <option value="nosql">nosql</option>
              </select>
            </label>

            <label className="flex flex-col gap-2 text-sm text-slate-700">
              Limit
              <input
                type="number"
                min={1}
                max={1000}
                value={limit}
                onChange={(e) => setLimit(Number(e.target.value) || 1)}
                className="rounded-lg border border-slate-300 bg-white px-3 py-2"
              />
            </label>
          </div>

          <div className="space-y-4 rounded-xl border border-slate-200 bg-slate-50 p-4 sm:p-5">
            <div className="flex items-center justify-between gap-3">
              <div>
                <h2 className="text-sm font-semibold tracking-wide text-slate-800 uppercase">
                  Conditions
                </h2>
                <p className="mt-1 text-xs text-slate-600">
                  Choose records to update using Fetch-style operators.
                </p>
              </div>
              <button
                type="button"
                onClick={addCondition}
                className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100"
              >
                Add Condition
              </button>
            </div>

            {conditions.map((row, idx) => (
              <div
                key={row.id}
                className="rounded-xl border border-slate-200 bg-white p-4"
              >
                <div className="mb-3 flex items-center justify-between">
                  <p className="text-sm font-medium text-slate-700">
                    Condition {idx + 1}
                  </p>
                  <button
                    type="button"
                    onClick={() => removeCondition(row.id)}
                    className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-600 hover:bg-slate-100"
                  >
                    Remove
                  </button>
                </div>

                <div className="grid gap-3 md:grid-cols-4">
                  <label className="flex flex-col gap-2 text-sm text-slate-700">
                    Field
                    <input
                      value={row.field}
                      onChange={(e) =>
                        updateCondition(row.id, (prev) => ({
                          ...prev,
                          field: e.target.value,
                        }))
                      }
                      placeholder="e.g. profile.skills"
                      className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                    />
                  </label>

                  <label className="flex flex-col gap-2 text-sm text-slate-700">
                    Type
                    <select
                      value={row.type}
                      onChange={(e) =>
                        updateCondition(row.id, (prev) => ({
                          ...prev,
                          type: e.target.value as ConditionType,
                        }))
                      }
                      className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                    >
                      <option value="string">string</option>
                      <option value="number">number</option>
                      <option value="boolean">boolean</option>
                      <option value="array">array</option>
                      <option value="dict">dict</option>
                    </select>
                  </label>

                  <label className="flex flex-col gap-2 text-sm text-slate-700">
                    Operator
                    <select
                      value={row.op}
                      onChange={(e) =>
                        updateCondition(row.id, (prev) => ({
                          ...prev,
                          op: e.target.value as ConditionOperator,
                        }))
                      }
                      className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                    >
                      {CONDITION_OPERATORS.map((op) => (
                        <option key={op} value={op}>
                          {op}
                        </option>
                      ))}
                    </select>
                  </label>

                  <label className="flex flex-col gap-2 text-sm text-slate-700">
                    Value
                    {row.type === "boolean" ? (
                      <select
                        value={row.value || "false"}
                        onChange={(e) =>
                          updateCondition(row.id, (prev) => ({
                            ...prev,
                            value: e.target.value,
                          }))
                        }
                        className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                      >
                        <option value="false">false</option>
                        <option value="true">true</option>
                      </select>
                    ) : (
                      <input
                        type={row.type === "number" ? "number" : "text"}
                        value={row.value}
                        onChange={(e) =>
                          updateCondition(row.id, (prev) => ({
                            ...prev,
                            value: e.target.value,
                          }))
                        }
                        placeholder={
                          row.type === "array"
                            ? '["a","b"]'
                            : row.type === "dict"
                              ? '{"k":"v"}'
                              : "Enter value"
                        }
                        className="rounded-lg border border-slate-300 bg-white px-3 py-2"
                      />
                    )}
                  </label>
                </div>
              </div>
            ))}
          </div>

          {renderSetFields()}

          <div className="flex flex-wrap items-center gap-3">
            <button
              type="submit"
              disabled={isSubmitting}
              className="rounded-lg bg-slate-900 px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {isSubmitting ? "Submitting..." : "Submit Update"}
            </button>
            <p className="text-sm text-slate-600">Status: {status}</p>
          </div>

          {error ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
              {error}
            </div>
          ) : null}

          {responseText ? (
            <div className="rounded-xl border border-slate-200 bg-slate-950/95 p-4 text-slate-100">
              <p className="mb-2 text-xs font-semibold tracking-wide text-slate-300 uppercase">
                Response
              </p>
              <pre className="max-h-80 overflow-auto whitespace-pre-wrap text-xs leading-5">
                {responseText}
              </pre>
            </div>
          ) : null}
        </form>
      </section>
    </main>
  );
}
