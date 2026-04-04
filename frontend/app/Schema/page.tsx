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
					<pre className="overflow-x-auto whitespace-pre-wrap break-words">
						{typeof schemaData === "string"
							? schemaData
							: JSON.stringify(schemaData, null, 2)}
					</pre>
				) : null}
			</div>
		</main>
	);
}
