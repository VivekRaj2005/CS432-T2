"use client";

import { useState } from "react";
import EntityExplorer from "../Components/EntityExplorer";
import InstanceViewer from "../Components/InstanceViewer";
import Navbar from "../Components/Navbar";
import IngestHistory from "../Components/IngestHistory";
import SessionPanel from "../Components/SessionPanel";
import { useDashboardData } from "../hooks/useDashboardData";
import { useIngestHistory } from "../hooks/useIngestHistory";

export default function Dashboard() {
  const {
    sessions,
    entities,
    sessionId,
    loading,
    error,
    fetchSessions,
    fetchEntities,
    fetchEntityInstances,
  } = useDashboardData();

  const {
    ingestHistory,
    ingestStats,
    loading: ingestLoading,
    isConnected,
    fetchIngestHistory,
    fetchIngestStats,
  } = useIngestHistory();

  const [selectedEntity, setSelectedEntity] = useState<string | null>(null);
  const selectedEntityObj = entities.find((e) => e.entity_name === selectedEntity);

  const handleRefreshSessions = async () => {
    await fetchSessions();
  };

  const handleRefreshIngestHistory = async () => {
    await fetchIngestHistory();
    await fetchIngestStats();
  };

  const handleSelectEntity = (entityName: string) => {
    setSelectedEntity(entityName);
  };

  // Load initial data
  const handleLoadDashboard = async () => {
    await fetchEntities();
    await fetchSessions();
    await fetchIngestHistory();
    await fetchIngestStats();
  };

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top,_#f8fbff_0%,_#fbfdff_38%,_#ffffff_100%)] text-slate-800">
      <main className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-6 py-8">
        <Navbar />

        <section className="space-y-4">
          <div className="rounded-lg border border-slate-200 bg-white p-6">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h1 className="text-3xl font-bold text-slate-900">
                  Logical Database Dashboard
                </h1>
                <p className="text-sm text-slate-600">
                  View active sessions, explore entities, and monitor query execution
                </p>
              </div>
              <button
                onClick={handleLoadDashboard}
                className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-slate-800"
              >
                Load Dashboard
              </button>
            </div>

            {sessionId && (
              <div className="rounded-md bg-blue-50 p-3 text-sm text-blue-700">
                <span className="font-medium">Session ID:</span>{" "}
                <span className="font-mono">{sessionId.substring(0, 12)}...</span>
              </div>
            )}

            {error && (
              <div className="mt-3 rounded-md bg-red-50 p-3 text-sm text-red-700">
                <span className="font-medium">Error:</span> {error}
              </div>
            )}
          </div>
        </section>

        <section className="grid grid-cols-1 gap-6 lg:grid-cols-4">
          {/* Left Column: Sessions & Entities */}
          <div className="space-y-6 lg:col-span-1">
            <SessionPanel
              sessions={sessions}
              onRefresh={handleRefreshSessions}
              loading={loading}
            />

            <EntityExplorer
              entities={entities}
              selectedEntity={selectedEntity}
              onSelectEntity={handleSelectEntity}
              loading={loading}
            />
          </div>

          {/* Right Column: Instances & Query History */}
          <div className="space-y-6 lg:col-span-3">
            <InstanceViewer
              entity={selectedEntityObj || null}
              entityName={selectedEntity}
              onFetchInstances={fetchEntityInstances}
              loading={loading}
            />
          </div>
        </section>

        <section className="rounded-lg border border-slate-200 bg-white">
          <IngestHistory
            history={ingestHistory}
            stats={ingestStats}
            onRefresh={handleRefreshIngestHistory}
            loading={ingestLoading}
            isConnected={isConnected}
          />
        </section>

        <section className="rounded-lg border border-slate-200 bg-white p-6">
          <h2 className="mb-4 text-lg font-semibold text-slate-900">System Statistics</h2>
          <div className="grid gap-4 md:grid-cols-5">
            <div className="rounded-md bg-slate-50 p-4">
              <p className="text-xs font-semibold text-slate-600 uppercase">
                Total Entities
              </p>
              <p className="mt-1 text-2xl font-bold text-slate-900">
                {entities.length}
              </p>
            </div>
            <div className="rounded-md bg-slate-50 p-4">
              <p className="text-xs font-semibold text-slate-600 uppercase">
                Active Sessions
              </p>
              <p className="mt-1 text-2xl font-bold text-slate-900">
                {sessions.length}
              </p>
            </div>
            <div className="rounded-md bg-emerald-50 p-4">
              <p className="text-xs font-semibold text-emerald-600 uppercase">
                Ingest Success
              </p>
              <p className="mt-1 text-2xl font-bold text-emerald-700">
                {ingestStats?.successful || 0}
              </p>
            </div>
            <div className="rounded-md bg-red-50 p-4">
              <p className="text-xs font-semibold text-red-600 uppercase">
                Ingest Failed
              </p>
              <p className="mt-1 text-2xl font-bold text-red-700">
                {ingestStats?.failed || 0}
              </p>
            </div>
            <div className="rounded-md bg-slate-100 p-4">
              <p className="text-xs font-semibold text-slate-600 uppercase">
                Avg Ingest Time
              </p>
              <p className="mt-1 text-2xl font-bold text-slate-900">
                {ingestStats?.avg_execution_ms.toFixed(0)}ms
              </p>
            </div>
          </div>
        </section>
      </main>
    </div>
  );
}
