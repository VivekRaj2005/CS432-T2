"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export interface IngestCommand {
  command_id: string;
  event_type: string;
  data: Record<string, any>;
  queued_at: string;
  completed_at: string | null;
  status: string; // QUEUED, PROCESSING, SUCCESS, ERROR
  execution_ms: number;
  error_message: string | null;
}

export interface IngestStats {
  total_commands: number;
  queued: number;
  processing: number;
  successful: number;
  failed: number;
  avg_execution_ms: number;
  event_type_breakdown: Record<string, number>;
  timestamp: string;
}

const API_BASE = "http://127.0.0.1:8000";
const WS_BASE = "ws://127.0.0.1:8000";

export function useIngestHistory() {
  const [ingestHistory, setIngestHistory] = useState<IngestCommand[]>([]);
  const [ingestStats, setIngestStats] = useState<IngestStats | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isConnected, setIsConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  // Fetch ingest history
  const fetchIngestHistory = useCallback(
    async (limit: number = 100, offset: number = 0) => {
      setLoading(true);
      setError(null);
      try {
        const url = new URL(`${API_BASE}/ingest-history`);
        url.searchParams.set("limit", limit.toString());
        url.searchParams.set("offset", offset.toString());

        const response = await fetch(url.toString());
        if (!response.ok) throw new Error("Failed to fetch ingest history");
        const data = await response.json();
        setIngestHistory(data.history || []);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    },
    []
  );

  // Fetch ingest statistics
  const fetchIngestStats = useCallback(async () => {
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/ingest-history/stats`);
      if (!response.ok) throw new Error("Failed to fetch ingest stats");
      const data = await response.json();
      setIngestStats(data);
    } catch (err: any) {
      setError(err.message);
    }
  }, []);

  // Connect to websocket for real-time updates
  const connectWebSocket = useCallback(() => {
    try {
      const ws = new WebSocket(`${WS_BASE}/ws/ingest-history`);

      ws.onopen = () => {
        setIsConnected(true);
        setError(null);
      };

      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);

          if (message.type === "ingest_initial") {
            // Initial load of history and stats
            setIngestHistory(message.history || []);
            setIngestStats(message.stats);
          } else if (message.type === "ingest_update") {
            // Real-time update of a command's status
            const update = message.data;
            setIngestHistory((prev) =>
              prev.map((cmd) =>
                cmd.command_id === update.command_id
                  ? {
                      ...cmd,
                      status: update.status,
                      execution_ms: update.execution_ms || cmd.execution_ms,
                      error_message: update.error || cmd.error_message,
                      completed_at:
                        update.status === "SUCCESS" || update.status === "ERROR"
                          ? new Date().toISOString()
                          : cmd.completed_at,
                    }
                  : cmd
              )
            );

            // Refresh stats after update
            fetchIngestStats();
          }
        } catch (err) {
          console.error("Failed to parse websocket message:", err);
        }
      };

      ws.onerror = () => {
        setIsConnected(false);
        setError("WebSocket connection failed");
      };

      ws.onclose = () => {
        setIsConnected(false);
      };

      wsRef.current = ws;
    } catch (err: any) {
      setError(err.message);
      setIsConnected(false);
    }
  }, [fetchIngestStats]);

  // Disconnect websocket
  const disconnectWebSocket = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
      setIsConnected(false);
    }
  }, []);

  // Request history via websocket
  const requestHistoryViaWS = useCallback((limit: number = 100) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(
        JSON.stringify({
          action: "get_history",
          limit,
        })
      );
    }
  }, []);

  // Request stats via websocket
  const requestStatsViaWS = useCallback(() => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(
        JSON.stringify({
          action: "get_stats",
        })
      );
    }
  }, []);

  // Initialize websocket on mount
  useEffect(() => {
    connectWebSocket();
    fetchIngestStats();

    return () => {
      disconnectWebSocket();
    };
  }, [connectWebSocket, disconnectWebSocket, fetchIngestStats]);

  return {
    ingestHistory,
    ingestStats,
    loading,
    error,
    isConnected,
    fetchIngestHistory,
    fetchIngestStats,
    connectWebSocket,
    disconnectWebSocket,
    requestHistoryViaWS,
    requestStatsViaWS,
  };
}
