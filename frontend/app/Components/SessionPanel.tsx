"use client";

import { useEffect, useState } from "react";
import { Session } from "../hooks/useDashboardData";

interface SessionPanelProps {
  sessions: Session[];
  onRefresh: () => Promise<void>;
  loading: boolean;
}

export default function SessionPanel({
  sessions,
  onRefresh,
  loading,
}: SessionPanelProps) {
  const [autoRefresh, setAutoRefresh] = useState(true);

  // Auto-refresh every 10 seconds
  useEffect(() => {
    if (!autoRefresh) return;

    const interval = setInterval(() => {
      onRefresh();
    }, 10000);

    return () => clearInterval(interval);
  }, [autoRefresh, onRefresh]);

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-6">
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-slate-900">Active Sessions</h2>
        <div className="flex items-center gap-2">
          <label className="flex items-center gap-2 text-xs text-slate-600">
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={(e) => setAutoRefresh(e.target.checked)}
              className="rounded border-slate-300"
            />
            Auto-refresh
          </label>
          <button
            onClick={onRefresh}
            disabled={loading}
            className="rounded-md bg-slate-100 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-200 disabled:opacity-50"
          >
            Refresh
          </button>
        </div>
      </div>

      {sessions.length === 0 ? (
        <div className="flex h-32 items-center justify-center text-center text-sm text-slate-500">
          No active sessions
        </div>
      ) : (
        <div className="space-y-2">
          {sessions.map((session) => (
            <div
              key={session.session_id}
              className="rounded-md border border-slate-100 bg-slate-50 p-3 text-sm"
            >
              <div className="mb-2 flex items-center justify-between">
                <span className="font-mono font-medium text-slate-800">
                  {session.session_id.substring(0, 8)}...
                </span>
                <span
                  className={`rounded-full px-2 py-1 text-xs font-medium ${
                    session.is_active
                      ? "bg-emerald-100 text-emerald-700"
                      : "bg-slate-100 text-slate-700"
                  }`}
                >
                  {session.is_active ? "Active" : "Inactive"}
                </span>
              </div>
              <div className="grid grid-cols-2 gap-2 text-xs text-slate-600">
                <div>
                  <span className="font-medium">Queries:</span> {session.query_count}
                </div>
                <div>
                  <span className="font-medium">Duration:</span>{" "}
                  {Math.round(session.duration_seconds)}s
                </div>
                <div>
                  <span className="font-medium">Entities:</span>{" "}
                  {session.entities_accessed.length}
                </div>
                <div>
                  <span className="font-medium">Started:</span>{" "}
                  {new Date(session.created_at).toLocaleTimeString()}
                </div>
              </div>
              {session.entities_accessed.length > 0 && (
                <div className="mt-2 flex flex-wrap gap-1">
                  {session.entities_accessed.slice(0, 3).map((entity) => (
                    <span
                      key={entity}
                      className="inline-block rounded-full bg-slate-200 px-2 py-1 text-xs text-slate-700"
                    >
                      {entity}
                    </span>
                  ))}
                  {session.entities_accessed.length > 3 && (
                    <span className="inline-block px-2 py-1 text-xs text-slate-600">
                      +{session.entities_accessed.length - 3} more
                    </span>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
