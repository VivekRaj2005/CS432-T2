"use client";

import { useEffect, useState } from "react";
import { LogicalEntity } from "../hooks/useDashboardData";

interface InstanceViewerProps {
  entity: LogicalEntity | null;
  entityName: string | null;
  onFetchInstances: (
    entityName: string,
    limit: number,
    offset: number
  ) => Promise<any>;
  loading: boolean;
}

export default function InstanceViewer({
  entity,
  entityName,
  onFetchInstances,
  loading,
}: InstanceViewerProps) {
  const [instances, setInstances] = useState<any[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [offset, setOffset] = useState(0);
  const [limit, setLimit] = useState(10);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!entityName) {
      setInstances([]);
      return;
    }

    setIsLoading(true);
    onFetchInstances(entityName, limit, offset)
      .then((result) => {
        if (result) {
          setInstances(result.instances || []);
          setTotalCount(result.total_count || 0);
        }
      })
      .finally(() => setIsLoading(false));
  }, [entityName, limit, offset, onFetchInstances]);

  if (!entityName) {
    return (
      <div className="rounded-lg border border-slate-200 bg-white p-6">
        <h2 className="mb-2 text-lg font-semibold text-slate-900">Entity Details</h2>
        <p className="mb-4 text-xs text-slate-600">Schema properties and instance data</p>
        <div className="flex h-32 items-center justify-center text-center text-sm text-slate-500">
          Select an entity from the list to view schema properties and instances
        </div>
      </div>
    );
  }

  const totalPages = Math.ceil(totalCount / limit);
  const currentPage = Math.floor(offset / limit) + 1;

  return (
    <div className="space-y-4">
      {/* Entity Schema Section */}
      <div className="rounded-lg border border-slate-200 bg-white p-6">
        <div className="mb-3 flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Entity Schema</h2>
            <p className="text-xs text-slate-600">{entityName}</p>
          </div>
        </div>

        {entity && entity.fields && entity.fields.length > 0 ? (
          <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
            {entity.fields.map((field) => (
              <div key={field.name} className="rounded-md bg-slate-50 p-3">
                <div className="flex items-center gap-2 mb-1">
                  <span className="font-mono text-sm font-semibold text-slate-900">
                    {field.name}
                  </span>
                  <span className="rounded-full bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-700">
                    {field.logical_type}
                  </span>
                </div>
                <div className="flex gap-2 text-xs text-slate-600">
                  {field.is_key && <span className="inline-block bg-blue-50 px-1.5 rounded text-blue-600">🔑 Key</span>}
                  {field.is_required && <span className="inline-block bg-red-50 px-1.5 rounded text-red-600">Required</span>}
                  {field.is_auto_generated && <span className="inline-block bg-green-50 px-1.5 rounded text-green-600">Auto</span>}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-sm text-slate-500">No fields available</div>
        )}
      </div>

      {/* Instance Data Section */}
      <div className="rounded-lg border border-slate-200 bg-white p-6">
        <div className="mb-4 flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Instance Data</h2>
            <p className="text-xs text-slate-600">
              {totalCount} total records
            </p>
          </div>
          <select
            value={limit}
            onChange={(e) => {
              setLimit(parseInt(e.target.value));
              setOffset(0);
            }}
            className="rounded-md border border-slate-300 bg-white px-2 py-1 text-xs"
          >
            <option value="10">10 per page</option>
            <option value="25">25 per page</option>
            <option value="50">50 per page</option>
          </select>
        </div>

      {instances.length === 0 ? (
        <div className="flex h-32 items-center justify-center text-center text-sm text-slate-500">
          {isLoading ? "Loading..." : "No instances found"}
        </div>
      ) : (
        <>
          <div className="mb-4 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200">
                  {entity?.fields.slice(0, 5).map((field) => (
                    <th
                      key={field.name}
                      className="px-3 py-2 text-left font-medium text-slate-700"
                    >
                      {field.name}
                    </th>
                  ))}
                  {entity && entity.fields.length > 5 && (
                    <th className="px-3 py-2 text-left font-medium text-slate-700">
                      ...
                    </th>
                  )}
                </tr>
              </thead>
              <tbody>
                {instances.map((instance, idx) => (
                  <tr
                    key={idx}
                    className="border-b border-slate-100 hover:bg-slate-50"
                  >
                    {entity?.fields.slice(0, 5).map((field) => (
                      <td key={field.name} className="px-3 py-2 text-slate-700">
                        <div className="max-w-xs truncate text-xs">
                          {instance[field.name] !== undefined
                            ? JSON.stringify(instance[field.name]).substring(
                                0,
                                50
                              )
                            : "—"}
                        </div>
                      </td>
                    ))}
                    {entity && entity.fields.length > 5 && (
                      <td className="px-3 py-2 text-slate-500 text-xs">+{entity.fields.length - 5}</td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="flex items-center justify-between border-t border-slate-200 pt-4 text-xs text-slate-600">
            <span>
              Page {currentPage} of {totalPages}
            </span>
            <div className="flex gap-2">
              <button
                onClick={() => setOffset(Math.max(0, offset - limit))}
                disabled={offset === 0 || isLoading}
                className="rounded-md bg-slate-100 px-3 py-1 hover:bg-slate-200 disabled:opacity-50"
              >
                ← Previous
              </button>
              <button
                onClick={() =>
                  setOffset(Math.min(totalCount - limit, offset + limit))
                }
                disabled={offset + limit >= totalCount || isLoading}
                className="rounded-md bg-slate-100 px-3 py-1 hover:bg-slate-200 disabled:opacity-50"
              >
                Next →
              </button>
            </div>
          </div>
        </>
      )}
      </div>
    </div>
  );
}
