"use client";

import { useState } from "react";
import { IngestCommand, IngestStats } from "../hooks/useIngestHistory";

interface IngestHistoryProps {
  history: IngestCommand[];
  stats: IngestStats | null;
  onRefresh: () => Promise<void>;
  loading: boolean;
  isConnected: boolean;
}

export default function IngestHistory({
  history,
  stats,
  onRefresh,
  loading,
  isConnected,
}: IngestHistoryProps) {
  const [filter, setFilter] = useState<"all" | "success" | "error" | "queued" | "processing">("all");

  const filteredHistory = history.filter((cmd) => {
    if (filter === "success") return cmd.status === "SUCCESS";
    if (filter === "error") return cmd.status === "ERROR";
    if (filter === "queued") return cmd.status === "QUEUED";
    if (filter === "processing") return cmd.status === "PROCESSING";
    return true;
  });

  const getStatusColor = (status: string) => {
    switch (status) {
      case "SUCCESS":
        return "border-emerald-200 bg-emerald-50";
      case "ERROR":
        return "border-red-200 bg-red-50";
      case "PROCESSING":
        return "border-yellow-200 bg-yellow-50";
      case "QUEUED":
        return "border-slate-200 bg-slate-50";
      default:
        return "border-slate-200 bg-slate-50";
    }
  };

  const getStatusBadgeColor = (status: string) => {
    switch (status) {
      case "SUCCESS":
        return "bg-emerald-200 text-emerald-700";
      case "ERROR":
        return "bg-red-200 text-red-700";
      case "PROCESSING":
        return "bg-yellow-200 text-yellow-700";
      case "QUEUED":
        return "bg-slate-200 text-slate-700";
      default:
        return "bg-slate-200 text-slate-700";
    }
  };

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-6">
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-slate-900">Ingest Queue History</h2>
          <p className="mt-1 text-xs text-slate-600">
            {isConnected ? (
              <span className="inline-flex items-center gap-1">
                <span className="inline-block h-2 w-2 rounded-full bg-emerald-500"></span>
                Live — Connected to ingest stream
              </span>
            ) : (
              <span className="text-red-600">
                Disconnected — Refresh to reconnect
              </span>
            )}
          </p>
        </div>
        <button
          onClick={onRefresh}
          disabled={loading}
          className="rounded-md bg-slate-100 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
        >
          Refresh
        </button>
      </div>

      {/* Statistics Cards */}
      {stats && (
        <div className="mb-4 grid grid-cols-5 gap-2 rounded-md bg-slate-50 p-3">
          <div className="text-center">
            <p className="text-xs font-medium text-slate-600">Total</p>
            <p className="text-lg font-bold text-slate-900">{stats.total_commands}</p>
          </div>
          <div className="text-center">
            <p className="text-xs font-medium text-slate-600">Success</p>
            <p className="text-lg font-bold text-emerald-600">{stats.successful}</p>
          </div>
          <div className="text-center">
            <p className="text-xs font-medium text-slate-600">Failed</p>
            <p className="text-lg font-bold text-red-600">{stats.failed}</p>
          </div>
          <div className="text-center">
            <p className="text-xs font-medium text-slate-600">Processing</p>
            <p className="text-lg font-bold text-yellow-600">{stats.processing}</p>
          </div>
          <div className="text-center">
            <p className="text-xs font-medium text-slate-600">Avg Time</p>
            <p className="text-lg font-bold text-slate-900">{stats.avg_execution_ms.toFixed(0)}ms</p>
          </div>
        </div>
      )}

      {/* Event Type Breakdown */}
      {stats && Object.keys(stats.event_type_breakdown).length > 0 && (
        <div className="mb-4 flex gap-2 border-b border-slate-200 pb-2">
          {Object.entries(stats.event_type_breakdown).map(([eventType, count]) => (
            <span key={eventType} className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2 py-1 text-xs">
              <span className="font-medium text-slate-700">{eventType}</span>
              <span className="rounded-full bg-slate-200 px-1.5 text-slate-600 font-semibold">{count}</span>
            </span>
          ))}
        </div>
      )}

      {/* Filter Tabs */}
      <div className="mb-4 flex gap-2 border-b border-slate-200">
        {(["all", "success", "error", "processing", "queued"] as const).map((f) => (
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
            {f === "success" && "✓ Success"}
            {f === "error" && "✗ Error"}
            {f === "processing" && "⟳ Processing"}
            {f === "queued" && "◌ Queued"}
          </button>
        ))}
      </div>

      {filteredHistory.length === 0 ? (
        <div className="flex h-32 items-center justify-center text-center text-sm text-slate-500">
          No ingest commands to display
        </div>
      ) : (
        <div className="space-y-2 max-h-96 overflow-y-auto">
          {filteredHistory.map((cmd) => (
            <div
              key={cmd.command_id}
              className={`rounded-md border p-3 text-sm transition-colors ${getStatusColor(cmd.status)}`}
            >
              <div className="mb-2 flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className={`rounded-full px-2 py-1 text-xs font-medium ${getStatusBadgeColor(cmd.status)}`}>
                    {cmd.status === "SUCCESS" && "✓"}
                    {cmd.status === "ERROR" && "✗"}
                    {cmd.status === "PROCESSING" && "⟳"}
                    {cmd.status === "QUEUED" && "◌"}
                    {cmd.event_type.toUpperCase()}
                  </span>
                  <span className="font-mono text-xs text-slate-600">
                    {cmd.command_id.substring(0, 8)}
                  </span>
                </div>
                <span className="text-xs font-semibold text-slate-700">
                  {cmd.execution_ms}ms
                </span>
              </div>

              <div className="mb-2 text-xs text-slate-700">
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <span className="font-medium">Event:</span> {cmd.event_type}
                  </div>
                  <div>
                    <span className="font-medium">Status:</span> {cmd.status}
                  </div>
                </div>
              </div>

              <div className="text-xs text-slate-600">
                <span className="font-medium">Queued:</span>{" "}
                {new Date(cmd.queued_at).toLocaleTimeString()}
                {cmd.completed_at && (
                  <>
                    {" "}
                    <span className="font-medium">• Completed:</span>{" "}
                    {new Date(cmd.completed_at).toLocaleTimeString()}
                  </>
                )}
              </div>

              {cmd.error_message && (
                <div className="mt-2 rounded bg-red-100 p-2 text-xs text-red-700 font-mono">
                  {cmd.error_message}
                </div>
              )}

              {/* Preview of data payload */}
              {Object.keys(cmd.data).length > 0 && (
                <div className="mt-2 rounded-md bg-slate-100 p-2 text-xs text-slate-700">
                  <span className="font-medium">Data Keys:</span> {Object.keys(cmd.data).join(", ")}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
