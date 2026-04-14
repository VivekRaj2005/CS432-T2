"use client";

import { useState } from "react";
import { LogicalEntity } from "../hooks/useDashboardData";

interface EntityExplorerProps {
  entities: LogicalEntity[];
  selectedEntity: string | null;
  onSelectEntity: (entityName: string) => void;
  loading: boolean;
}

export default function EntityExplorer({
  entities,
  selectedEntity,
  onSelectEntity,
  loading,
}: EntityExplorerProps) {
  const [expandedEntity, setExpandedEntity] = useState<string | null>(null);

  const selected = entities.find((e) => e.entity_name === selectedEntity);

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-6">
      <h2 className="mb-4 text-lg font-semibold text-slate-900">
        Logical Entities
      </h2>

      <div className="space-y-2">
        {entities.length === 0 ? (
          <div className="flex h-32 items-center justify-center text-center text-sm text-slate-500">
            No entities available
          </div>
        ) : (
          entities.map((entity) => (
            <div key={entity.entity_name}>
              <button
                onClick={() => {
                  onSelectEntity(entity.entity_name);
                  setExpandedEntity(
                    expandedEntity === entity.entity_name
                      ? null
                      : entity.entity_name
                  );
                }}
                className={`w-full rounded-md px-3 py-2 text-left text-sm font-medium transition-colors ${
                  selectedEntity === entity.entity_name
                    ? "bg-blue-50 text-blue-900 border border-blue-200"
                    : "bg-slate-50 text-slate-900 hover:bg-slate-100"
                }`}
              >
                <div className="flex items-center justify-between">
                  <span>{entity.entity_name}</span>
                  <span className="rounded-full bg-slate-200 px-2 py-0.5 text-xs text-slate-700">
                    {entity.record_count} records
                  </span>
                </div>
              </button>

              {expandedEntity === entity.entity_name && (
                <div className="mt-2 rounded-md bg-slate-50 p-3">
                  <div className="mb-3">
                    <h3 className="text-xs font-semibold text-slate-700 uppercase">
                      Fields ({entity.fields.length})
                    </h3>
                    <div className="mt-2 space-y-1">
                      {entity.fields.map((field) => (
                        <div
                          key={field.name}
                          className="flex items-center justify-between text-xs text-slate-600"
                        >
                          <div className="flex items-center gap-2">
                            <span className="font-mono font-medium">
                              {field.name}
                            </span>
                            <span className="rounded-full bg-slate-200 px-1.5 py-0.5 text-xs text-slate-700">
                              {field.logical_type}
                            </span>
                            {field.is_key && (
                              <span className="text-blue-600">🔑</span>
                            )}
                            {field.is_auto_generated && (
                              <span className="text-amber-600">⚙</span>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>

                  {entity.relationships.length > 0 && (
                    <div>
                      <h3 className="text-xs font-semibold text-slate-700 uppercase">
                        Relationships
                      </h3>
                      <div className="mt-2 space-y-1">
                        {entity.relationships.map((rel, idx) => (
                          <div
                            key={idx}
                            className="text-xs text-slate-600"
                          >
                            <span className="font-medium">{rel.entity}</span>
                            <span className="ml-2 text-slate-500">({rel.type})</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
}
