"use client";

import { useEffect, useMemo, useState } from "react";
import Navbar from "../Components/Navbar";

type SchemaResponse = Record<string, unknown> | unknown[] | string;

export default function SchemaPage() {
	const endpoint = useMemo(
		() => process.env.NEXT_PUBLIC_SCHEMA_ENDPOINT ?? "http://127.0.0.1:8000/schema",
		[],
	);

	const [schemaData, setSchemaData] = useState<SchemaResponse | null>(null);
	const [loading, setLoading] = useState(true);
	const [error, setError] = useState<string | null>(null);

	const fieldRows = useMemo(() => {
		if (!schemaData || typeof schemaData !== "object" || Array.isArray(schemaData)) {
			return [] as Array<{
				key: string;
				type: string;
				subtype: string;
				storage: string;
				samplesSeen: string;
			}>;
		}

		const fieldsValue = (schemaData as Record<string, unknown>).fields;
		if (!fieldsValue || typeof fieldsValue !== "object" || Array.isArray(fieldsValue)) {
			return [];
		}

		const rows = Object.entries(fieldsValue as Record<string, unknown>).map(([fieldKey, fieldValue]) => {
			const meta =
				typeof fieldValue === "object" && fieldValue !== null && !Array.isArray(fieldValue)
					? (fieldValue as Record<string, unknown>)
					: {};

			const type = typeof meta.type === "string" ? meta.type : "-";
			const storage = typeof meta.storage === "string" ? meta.storage : "-";
			const samplesSeenRaw = meta.samples_seen;
			const samplesSeen =
				typeof samplesSeenRaw === "number" || typeof samplesSeenRaw === "string"
					? String(samplesSeenRaw)
					: "-";

			let subtype = "-";
			const subtypeRaw = meta.subtype;
			if (typeof subtypeRaw === "string") {
				subtype = subtypeRaw;
			} else if (typeof subtypeRaw === "object" && subtypeRaw !== null && !Array.isArray(subtypeRaw)) {
				const subtypeType = (subtypeRaw as Record<string, unknown>).type;
				subtype = typeof subtypeType === "string" ? subtypeType : JSON.stringify(subtypeRaw);
			} else if (subtypeRaw !== null && subtypeRaw !== undefined) {
				subtype = String(subtypeRaw);
			}

			return {
				key: fieldKey,
				type,
				subtype,
				storage,
				samplesSeen,
			};
		});

		return rows.sort((a, b) => a.key.localeCompare(b.key));
	}, [schemaData]);

	const fkRows = useMemo(() => {
		if (!schemaData || typeof schemaData !== "object" || Array.isArray(schemaData)) {
			return [] as Array<{ field: string; fkField: string; childTable: string; storage: string }>;
		}

		const refsValue = (schemaData as Record<string, unknown>).foreign_key_references;
		if (!refsValue || typeof refsValue !== "object" || Array.isArray(refsValue)) {
			return [];
		}

		return Object.entries(refsValue as Record<string, unknown>)
			.map(([field, meta]) => {
				const ref =
					typeof meta === "object" && meta !== null && !Array.isArray(meta)
						? (meta as Record<string, unknown>)
						: {};
				return {
					field,
					fkField: typeof ref.fk_field === "string" ? ref.fk_field : "-",
					childTable: typeof ref.child_table === "string" ? ref.child_table : "-",
					storage: typeof ref.storage === "string" ? ref.storage : "-",
				};
			})
			.sort((a, b) => a.field.localeCompare(b.field));
	}, [schemaData]);

	const nestedRows = useMemo(() => {
		if (!schemaData || typeof schemaData !== "object" || Array.isArray(schemaData)) {
			return [] as Array<{ field: string; table: string; fieldCount: string; requestCount: string }>;
		}

		const nestedValue = (schemaData as Record<string, unknown>).nested_schema;
		if (!nestedValue || typeof nestedValue !== "object" || Array.isArray(nestedValue)) {
			return [];
		}

		return Object.entries(nestedValue as Record<string, unknown>)
			.map(([field, nested]) => {
				const node =
					typeof nested === "object" && nested !== null && !Array.isArray(nested)
						? (nested as Record<string, unknown>)
						: {};
				return {
					field,
					table: typeof node.table_name === "string" ? node.table_name : "-",
					fieldCount:
						typeof node.field_count === "number" || typeof node.field_count === "string"
							? String(node.field_count)
							: "-",
					requestCount:
						typeof node.request_count === "number" || typeof node.request_count === "string"
							? String(node.request_count)
							: "-",
				};
			})
			.sort((a, b) => a.field.localeCompare(b.field));
	}, [schemaData]);

	useEffect(() => {
		const controller = new AbortController();

		async function loadSchema() {
			setLoading(true);
			setError(null);

			try {
				const response = await fetch(endpoint, { signal: controller.signal });

				if (!response.ok) {
					throw new Error(`Request failed: ${response.status} ${response.statusText}`);
				}

				const contentType = response.headers.get("content-type") ?? "";
				const payload = contentType.includes("application/json")
					? ((await response.json()) as SchemaResponse)
					: await response.text();

				setSchemaData(payload);
			} catch (err) {
				if (err instanceof Error && err.name === "AbortError") {
					return;
				}

				setError(err instanceof Error ? err.message : "Failed to fetch schema");
			} finally {
				setLoading(false);
			}
		}

		loadSchema();

		return () => controller.abort();
	}, [endpoint]);

	return (
		<main className="mx-auto w-full max-w-5xl px-6 py-10 sm:px-10">
            <Navbar />
			<h1 className="text-3xl font-semibold tracking-tight text-slate-900">Schema</h1>
			<p className="mt-2 text-sm text-slate-600">Endpoint: {endpoint}</p>

			<div className="mt-6 min-h-64 rounded-xl border border-slate-200 bg-white p-4 text-sm text-slate-800 shadow-sm">
				{loading ? <p>Loading schema...</p> : null}

				{!loading && error ? <p className="text-red-600">Error: {error}</p> : null}

				{!loading && !error ? (
					<div className="space-y-5">
						<div className="rounded-lg border border-blue-200 bg-blue-50 px-3 py-2 text-sm text-blue-900">
							Nested fields stay modeled as nested schema. Foreign keys are stored only as cross-store references and may route to SQL or NoSQL based on classification.
						</div>

						<div>
							<h2 className="text-base font-semibold text-slate-900">Field Metadata</h2>
							<p className="mt-1 text-xs text-slate-600">
								Showing all entries from response.fields.
							</p>
						</div>

						{fieldRows.length > 0 ? (
							<div className="overflow-x-auto rounded-lg border border-slate-200">
								<table className="min-w-full divide-y divide-slate-200 text-left text-sm">
									<thead className="bg-slate-50 text-xs font-semibold tracking-wide text-slate-700 uppercase">
										<tr>
											<th className="px-4 py-3">Key</th>
											<th className="px-4 py-3">Type</th>
											<th className="px-4 py-3">Subtype</th>
											<th className="px-4 py-3">Storage</th>
											<th className="px-4 py-3">samples_seen</th>
										</tr>
									</thead>
									<tbody className="divide-y divide-slate-100">
										{fieldRows.map((row) => (
											<tr key={row.key} className="hover:bg-slate-50/70">
												<td className="px-4 py-3 font-medium text-slate-900">{row.key}</td>
												<td className="px-4 py-3 text-slate-700">{row.type}</td>
												<td className="px-4 py-3 text-slate-700">{row.subtype}</td>
												<td className="px-4 py-3 text-slate-700">{row.storage}</td>
												<td className="px-4 py-3 text-slate-700">{row.samplesSeen}</td>
											</tr>
										))}
									</tbody>
								</table>
							</div>
						) : (
							<p className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
								No response.fields object found in schema payload.
							</p>
						)}

						<div>
							<h2 className="text-base font-semibold text-slate-900">Foreign Key References</h2>
							<p className="mt-1 text-xs text-slate-600">
								Reference-only links from parent nested fields to child tables.
							</p>
						</div>

						{fkRows.length > 0 ? (
							<div className="overflow-x-auto rounded-lg border border-slate-200">
								<table className="min-w-full divide-y divide-slate-200 text-left text-sm">
									<thead className="bg-slate-50 text-xs font-semibold tracking-wide text-slate-700 uppercase">
										<tr>
											<th className="px-4 py-3">Nested Field</th>
											<th className="px-4 py-3">FK Field</th>
											<th className="px-4 py-3">Child Table</th>
											<th className="px-4 py-3">Reference Storage</th>
										</tr>
									</thead>
									<tbody className="divide-y divide-slate-100">
										{fkRows.map((row) => (
											<tr key={row.field} className="hover:bg-slate-50/70">
												<td className="px-4 py-3 font-medium text-slate-900">{row.field}</td>
												<td className="px-4 py-3 text-slate-700">{row.fkField}</td>
												<td className="px-4 py-3 text-slate-700">{row.childTable}</td>
												<td className="px-4 py-3 text-slate-700">{row.storage}</td>
											</tr>
										))}
									</tbody>
								</table>
							</div>
						) : (
							<p className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
								No foreign key reference metadata is available yet.
							</p>
						)}

						<div>
							<h2 className="text-base font-semibold text-slate-900">Nested Schema Tables</h2>
							<p className="mt-1 text-xs text-slate-600">
								Nested field groups tracked by child MapRegisters.
							</p>
						</div>

						{nestedRows.length > 0 ? (
							<div className="overflow-x-auto rounded-lg border border-slate-200">
								<table className="min-w-full divide-y divide-slate-200 text-left text-sm">
									<thead className="bg-slate-50 text-xs font-semibold tracking-wide text-slate-700 uppercase">
										<tr>
											<th className="px-4 py-3">Nested Field</th>
											<th className="px-4 py-3">Child Table</th>
											<th className="px-4 py-3">Field Count</th>
											<th className="px-4 py-3">Request Count</th>
										</tr>
									</thead>
									<tbody className="divide-y divide-slate-100">
										{nestedRows.map((row) => (
											<tr key={row.field} className="hover:bg-slate-50/70">
												<td className="px-4 py-3 font-medium text-slate-900">{row.field}</td>
												<td className="px-4 py-3 text-slate-700">{row.table}</td>
												<td className="px-4 py-3 text-slate-700">{row.fieldCount}</td>
												<td className="px-4 py-3 text-slate-700">{row.requestCount}</td>
											</tr>
										))}
									</tbody>
								</table>
							</div>
						) : (
							<p className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
								No nested schema tables are available yet.
							</p>
						)}

						<details className="rounded-lg border border-slate-200 bg-slate-50 p-3">
							<summary className="cursor-pointer text-xs font-semibold tracking-wide text-slate-700 uppercase">
								Raw Response
							</summary>
							<pre className="mt-3 overflow-x-auto whitespace-pre-wrap break-words text-xs text-slate-700">
								{typeof schemaData === "string"
									? schemaData
									: JSON.stringify(schemaData, null, 2)}
							</pre>
						</details>
					</div>
				) : null}
			</div>
		</main>
	);
}
