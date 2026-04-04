"use client";

import { useMemo, useState } from "react";
import Navbar from "../Components/Navbar";

type ConditionType = "string" | "number" | "boolean" | "array" | "dict";

type ConditionOperator =
	| "eq"
	| "ne"
	| "gt"
	| "gte"
	| "lt"
	| "lte"
	| "len_eq"
	| "len_gt"
	| "len_gte"
	| "len_lt"
	| "len_lte"
	| "isMember"
	| "array_contains"
	| "array_contains_all"
	| "array_contains_any"
	| "dict_has_key"
	| "dict_has_value";

type ConditionRow = {
	id: string;
	field: string;
	type: ConditionType;
	op: ConditionOperator;
	value: string;
};

function createCondition(id: string): ConditionRow {
	return {
		id,
		field: "",
		type: "string",
		op: "eq",
		value: "",
	};
}

function parseConditionValue(row: ConditionRow): string | number | boolean {
	if (row.type === "array") {
		const parsed = JSON.parse(row.value || "[]");
		if (!Array.isArray(parsed)) {
			throw new Error("Array type requires a JSON array value")
		}
		return JSON.stringify(parsed);
	}
	if (row.type === "dict") {
		const parsed = JSON.parse(row.value || "{}");
		if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
			throw new Error("Dict type requires a JSON object value")
		}
		return JSON.stringify(parsed);
	}
	if (row.type === "number") {
		return Number(row.value);
	}
	if (row.type === "boolean") {
		return row.value === "true";
	}
	return row.value;
}

function parseConditionPayload(row: ConditionRow): { field: string; op: ConditionOperator; value: unknown } {
	const op = (row.op ?? "eq") as ConditionOperator;

	if (op === "array_contains_all" || op === "array_contains_any") {
		const parsed = JSON.parse(row.value || "[]");
		if (!Array.isArray(parsed)) {
			throw new Error(`${op} expects a JSON array value`);
		}
		return {
			field: row.field.trim(),
			op,
			value: parsed,
		};
	}

	if (op.startsWith("len_")) {
		return {
			field: row.field.trim(),
			op,
			value: row.value.trim(),
		};
	}

	let value: unknown = parseConditionValue(row);
	if (row.type === "array" || row.type === "dict") {
		value = JSON.parse(String(value));
	}

	return {
		field: row.field.trim(),
		op,
		value,
	};
}

export default function FetchPage() {
	const endpoint = useMemo(
		() => {
			const runtimeEnv = (globalThis as unknown as {
				process?: { env?: Record<string, string | undefined> };
			}).process?.env;
			return runtimeEnv?.NEXT_PUBLIC_FETCH_ENDPOINT ?? "http://127.0.0.1:8000/fetch";
		},
		[],
	);

	const [conditions, setConditions] = useState<ConditionRow[]>([createCondition("cond-1")]);
	const [source, setSource] = useState<"merged" | "sql" | "nosql">("merged");
	const [limit, setLimit] = useState(100);
	const [status, setStatus] = useState("Idle");
	const [error, setError] = useState<string | null>(null);
	const [result, setResult] = useState<string>("");
	const [isLoading, setIsLoading] = useState(false);

	function updateCondition(id: string, updater: (prev: ConditionRow) => ConditionRow) {
		setConditions((prev) => prev.map((row) => (row.id === id ? updater(row) : row)));
	}

	function addCondition() {
		setConditions((prev) => [...prev, createCondition(`cond-${Date.now()}`)]);
	}

	function removeCondition(id: string) {
		setConditions((prev) => (prev.length > 1 ? prev.filter((row) => row.id !== id) : prev));
	}

	async function handleFetch(event: React.FormEvent<HTMLFormElement>) {
		event.preventDefault();
		setIsLoading(true);
		setError(null);
		setResult("");
		setStatus("Fetching...");

		const payload: Record<string, unknown> = {};
		const filters: Array<{ field: string; op: ConditionOperator; value: unknown }> = [];
		try {
			for (const row of conditions) {
				const key = row.field.trim();
				if (!key) {
					continue;
				}
				filters.push(parseConditionPayload(row));
				payload[key] = parseConditionValue(row);
			}
		} catch (err) {
			setIsLoading(false);
			setStatus("Failed");
			setError(err instanceof Error ? err.message : "Invalid condition input");
			return;
		}

		const url = new URL(endpoint);
		url.searchParams.set("source", source);
		url.searchParams.set("limit", String(limit));
		if (filters.length > 0) {
			url.searchParams.set("conditions", JSON.stringify({ $filters: filters }));
		}

		try {
			const response = await fetch(url.toString(), { method: "GET" });
			const data = await response.json();
			if (!response.ok) {
				throw new Error(data?.detail ?? `Request failed: ${response.status}`);
			}

			setStatus("Fetched");
			setResult(JSON.stringify(data, null, 2));
		} catch (err) {
			setStatus("Failed");
			setError(err instanceof Error ? err.message : "Failed to fetch data");
		} finally {
			setIsLoading(false);
		}
	}

	return (
		<main className="mx-auto w-full max-w-5xl px-6 py-10 sm:px-10">
			<Navbar />

			<section className="mt-8 rounded-2xl border border-slate-200 bg-white p-6 shadow-sm sm:p-8">
				<h1 className="text-3xl font-semibold tracking-tight text-slate-900">Fetch Records</h1>
				<p className="mt-2 text-sm text-slate-600">Endpoint: {endpoint}</p>

				<div className="mt-6 grid gap-4 rounded-xl border border-slate-200 bg-slate-50 p-4 md:grid-cols-2">
					<div>
						<p className="text-sm font-semibold tracking-wide text-slate-800 uppercase">Condition Builder Guide</p>
						<p className="mt-2 text-sm text-slate-600">
							Use one row per filter. All rows are combined using AND. Field supports nested paths like
							 <span className="font-medium text-slate-800"> profile.skills </span>.
						</p>
					</div>
					<div className="space-y-2 text-xs text-slate-700">
						<p><span className="font-semibold">Numeric:</span> salary &gt;= 50000</p>
						<p><span className="font-semibold">Not Equals:</span> dept_name != &quot;HR&quot;</p>
						<p><span className="font-semibold">Membership:</span> tags isMember &quot;urgent&quot;</p>
						<p><span className="font-semibold">Length Math:</span> len(tags) &gt; 2+3</p>
						<p><span className="font-semibold">Array Contains All:</span> tags array_contains_all [&quot;urgent&quot;, &quot;prod&quot;]</p>
						<p><span className="font-semibold">Dict Key:</span> profile dict_has_key &quot;skills&quot;</p>
					</div>
				</div>

				<form className="mt-8 space-y-6" onSubmit={handleFetch}>
					<div className="grid gap-4 md:grid-cols-2">
						<label className="flex flex-col gap-2 text-sm text-slate-700">
							Source
							<select
								value={source}
								onChange={(e) => setSource(e.target.value as "merged" | "sql" | "nosql")}
								className="rounded-lg border border-slate-300 bg-white px-3 py-2"
							>
								<option value="merged">merged</option>
								<option value="sql">sql</option>
								<option value="nosql">nosql</option>
							</select>
						</label>

						<label className="flex flex-col gap-2 text-sm text-slate-700">
							Limit
							<input
								type="number"
								min={1}
								max={1000}
								value={limit}
								onChange={(e) => setLimit(Number(e.target.value) || 1)}
								className="rounded-lg border border-slate-300 bg-white px-3 py-2"
							/>
						</label>
					</div>

					<div className="space-y-4">
						<div className="flex items-center justify-between">
							<h2 className="text-sm font-semibold tracking-wide text-slate-700 uppercase">Conditions</h2>
							<button
								type="button"
								onClick={addCondition}
								className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
							>
								Add Condition
							</button>
						</div>

						{conditions.map((row, idx) => (
							<div key={row.id} className="rounded-xl border border-slate-200 bg-slate-50 p-4">
								<div className="mb-3 flex items-center justify-between">
									<p className="text-sm font-medium text-slate-700">Condition {idx + 1}</p>
									<button
										type="button"
										onClick={() => removeCondition(row.id)}
										className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100"
									>
										Remove
									</button>
								</div>

								<div className="grid gap-3 md:grid-cols-3">
									<input
										value={row.field}
										onChange={(e) => updateCondition(row.id, (prev) => ({ ...prev, field: e.target.value }))}
										placeholder="field name"
										className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
									/>

									<select
										value={row.type}
										onChange={(e) =>
											updateCondition(row.id, (prev) => ({
												...prev,
												type: e.target.value as ConditionType,
												value: "",
											}))
										}
										className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
									>
										<option value="string">string</option>
										<option value="number">number</option>
										<option value="boolean">boolean</option>
										<option value="array">array</option>
										<option value="dict">dict</option>
									</select>

									<select
										value={row.op ?? "eq"}
										onChange={(e) =>
											updateCondition(row.id, (prev) => ({
												...prev,
												op: e.target.value as ConditionOperator,
												value: "",
											}))
										}
										className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
									>
										<option value="eq">=</option>
										<option value="ne">!=</option>
										<option value="gt">&gt;</option>
										<option value="gte">&gt;=</option>
										<option value="lt">&lt;</option>
										<option value="lte">&lt;=</option>
										<option value="len_eq">len =</option>
										<option value="len_gt">len &gt;</option>
										<option value="len_gte">len &gt;=</option>
										<option value="len_lt">len &lt;</option>
										<option value="len_lte">len &lt;=</option>
										<option value="isMember">isMember</option>
										<option value="array_contains">array contains</option>
										<option value="array_contains_all">array contains all</option>
										<option value="array_contains_any">array contains any</option>
										<option value="dict_has_key">dict has key</option>
										<option value="dict_has_value">dict has value</option>
									</select>

										{(row.op ?? "eq").startsWith("len_") ? (
											<input
												type="text"
												value={row.value}
												onChange={(e) => updateCondition(row.id, (prev) => ({ ...prev, value: e.target.value }))}
												placeholder="e.g. 2+3*2"
												className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
											/>
										) : row.type === "boolean" ? (
										<select
											value={row.value || "false"}
											onChange={(e) => updateCondition(row.id, (prev) => ({ ...prev, value: e.target.value }))}
											className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
										>
											<option value="false">false</option>
											<option value="true">true</option>
										</select>
										) : (
										<input
											type={row.type === "number" ? "number" : "text"}
											value={row.value}
											onChange={(e) => updateCondition(row.id, (prev) => ({ ...prev, value: e.target.value }))}
											placeholder={
												row.type === "array"
													? "JSON array, e.g. [\"urgent\",\"prod\"]"
													: row.type === "dict"
														? "JSON object, e.g. {\"role\":\"admin\"}"
														: "value"
											}
											className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
										/>
									)}
								</div>
							</div>
						))}
					</div>

					<div className="flex items-center gap-3">
						<button
							type="submit"
							disabled={isLoading}
							className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
						>
							{isLoading ? "Fetching..." : "Send GET Request"}
						</button>
						<span className="text-sm text-slate-600">Status: {status}</span>
					</div>
				</form>

				{error ? <p className="mt-4 text-sm text-red-600">Error: {error}</p> : null}

				{result ? (
					<div className="mt-6 rounded-lg border border-slate-200 bg-slate-50 p-4">
						<p className="mb-2 text-sm font-semibold text-slate-700">Response</p>
						<pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs text-slate-700">{result}</pre>
					</div>
				) : null}
			</section>
		</main>
	);
}
