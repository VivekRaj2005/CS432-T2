"use client";

import { useState } from "react";
import { QueryExecution } from "../hooks/useDashboardData";

interface QueryHistoryProps {
  history: QueryExecution[];
  onRefresh: () => Promise<void>;
  loading: boolean;
}

export default function QueryHistory({
  history,
  onRefresh,
  loading,
}: QueryHistoryProps) {
  const [filter, setFilter] = useState<"all" | "success" | "error">("all");

  const filteredHistory = history.filter((q) => {
    if (filter === "success") return q.status === "SUCCESS";
    if (filter === "error") return q.status === "ERROR";
    return true;
  });

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-6">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-slate-900">Query History</h2>
        <button
          onClick={onRefresh}
          disabled={loading}
          className="rounded-md bg-slate-100 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
        >
          Refresh
        </button>
      </div>

      <div className="mb-4 flex gap-2 border-b border-slate-200">
        {(["all", "success", "error"] as const).map((f) => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-3 py-2 text-xs font-medium transition-colors ${
              filter === f
                ? "border-b-2 border-blue-500 text-blue-600"
                : "text-slate-600 hover:text-slate-900"
            }`}
          >
            {f === "all" && "All"}
            {f === "success" && "Success"}
            {f === "error" && "Error"}
          </button>
        ))}
      </div>

      {filteredHistory.length === 0 ? (
        <div className="flex h-32 items-center justify-center text-center text-sm text-slate-500">
          No queries to display
        </div>
      ) : (
        <div className="space-y-2 max-h-96 overflow-y-auto">
          {filteredHistory.map((query) => (
            <div
              key={query.query_id}
              className={`rounded-md border p-3 text-sm ${
                query.status === "SUCCESS"
                  ? "border-emerald-200 bg-emerald-50"
                  : "border-red-200 bg-red-50"
              }`}
            >
              <div className="mb-2 flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span
                    className={`rounded-full px-2 py-1 text-xs font-medium ${
                      query.status === "SUCCESS"
                        ? "bg-emerald-200 text-emerald-700"
                        : "bg-red-200 text-red-700"
                    }`}
                  >
                    {query.operation_type}
                  </span>
                  <span className="font-mono text-xs text-slate-600">
                    {query.query_id.substring(0, 8)}
                  </span>
                </div>
                <span className="text-xs text-slate-600">
                  {query.execution_ms}ms
                </span>
              </div>

              <div className="grid grid-cols-3 gap-2 text-xs text-slate-700">
                <div>
                  <span className="font-medium">Entity:</span> {query.entity_name || "—"}
                </div>
                <div>
                  <span className="font-medium">Results:</span> {query.result_count}
                </div>
                <div>
                  <span className="font-medium">Source:</span> {query.source}
                </div>
              </div>

              <div className="mt-2 text-xs text-slate-600">
                <span className="font-medium">Time:</span>{" "}
                {new Date(query.started_at).toLocaleTimeString()}
              </div>

              {query.error_message && (
                <div className="mt-2 rounded bg-red-100 p-2 text-xs text-red-700">
                  {query.error_message}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
