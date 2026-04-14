"use client";

import { useCallback, useEffect, useState } from "react";

export interface Session {
  session_id: string;
  created_at: string;
  last_activity: string;
  entities_accessed: string[];
  query_count: number;
  is_active: boolean;
  duration_seconds: number;
}

export interface LogicalField {
  name: string;
  logical_type: string;
  is_key: boolean;
  is_required: boolean;
  is_auto_generated: boolean;
  sample_value: any;
}

export interface LogicalEntity {
  entity_name: string;
  fields: LogicalField[];
  relationships: Array<{ entity: string; type: string }>;
  record_count: number;
  request_count: number;
}

export interface QueryExecution {
  query_id: string;
  session_id: string | null;
  entity_name: string | null;
  operation_type: string;
  filters: Record<string, any>;
  started_at: string;
  completed_at: string | null;
  execution_ms: number;
  result_count: number;
  rows_affected: number;
  status: string;
  error_message: string | null;
  source: string;
}

const API_BASE = "http://127.0.0.1:8000";

export function useDashboardData() {
  const [sessions, setSessions] = useState<Session[]>([]);
  const [entities, setEntities] = useState<LogicalEntity[]>([]);
  const [queryHistory, setQueryHistory] = useState<QueryExecution[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sessionId, setSessionId] = useState<string | null>(null);

  // Start a new session
  const startSession = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/sessions/start`, {
        method: "POST",
      });
      if (!response.ok) throw new Error("Failed to start session");
      const data = await response.json();
      setSessionId(data.session_id);
      return data.session_id;
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  // Fetch active sessions
  const fetchSessions = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/sessions`);
      if (!response.ok) throw new Error("Failed to fetch sessions");
      const data = await response.json();
      setSessions(data.sessions ? Object.values(data.sessions) : []);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  // Fetch logical entities
  const fetchEntities = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/entities`);
      if (!response.ok) throw new Error("Failed to fetch entities");
      const data = await response.json();
      setEntities(data.entities || []);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  // Fetch entity schema
  const fetchEntitySchema = useCallback(
    async (entityName: string): Promise<LogicalEntity | null> => {
      try {
        const response = await fetch(`${API_BASE}/entities/${entityName}`);
        if (!response.ok) throw new Error("Failed to fetch entity schema");
        return await response.json();
      } catch (err: any) {
        setError(err.message);
        return null;
      }
    },
    []
  );

  // Fetch entity instances
  const fetchEntityInstances = useCallback(
    async (
      entityName: string,
      limit: number = 100,
      offset: number = 0
    ): Promise<any> => {
      setLoading(true);
      setError(null);
      try {
        const url = new URL(`${API_BASE}/entities/${entityName}/instances`);
        url.searchParams.set("limit", limit.toString());
        url.searchParams.set("offset", offset.toString());
        if (sessionId) {
          url.searchParams.set("session_id", sessionId);
        }

        const response = await fetch(url.toString());
        if (!response.ok) throw new Error("Failed to fetch instances");
        return await response.json();
      } catch (err: any) {
        setError(err.message);
        return null;
      } finally {
        setLoading(false);
      }
    },
    [sessionId]
  );

  // Fetch query history
  const fetchQueryHistory = useCallback(
    async (
      filters?: { sessionId?: string; entityName?: string; limit?: number }
    ) => {
      setLoading(true);
      setError(null);
      try {
        const url = new URL(`${API_BASE}/query-history`);
        if (filters?.sessionId) {
          url.searchParams.set("session_id", filters.sessionId);
        }
        if (filters?.entityName) {
          url.searchParams.set("entity_name", filters.entityName);
        }
        if (filters?.limit) {
          url.searchParams.set("limit", filters.limit.toString());
        }

        const response = await fetch(url.toString());
        if (!response.ok) throw new Error("Failed to fetch query history");
        const data = await response.json();
        setQueryHistory(data.history || []);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    },
    []
  );

  // Initialize session on mount
  useEffect(() => {
    if (!sessionId) {
      startSession();
    }
  }, []);

  return {
    sessions,
    entities,
    queryHistory,
    sessionId,
    loading,
    error,
    startSession,
    fetchSessions,
    fetchEntities,
    fetchEntitySchema,
    fetchEntityInstances,
    fetchQueryHistory,
  };
}
