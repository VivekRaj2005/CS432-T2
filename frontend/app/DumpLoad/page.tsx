"use client";

import { useMemo, useState } from "react";
import Navbar from "../Components/Navbar";

type DumpSummary = {
	status?: string;
	mode?: string;
	path?: string;
	update_order_count?: number;
	ingest_queue_count?: number;
	message?: string;
	detail?: string;
};

export default function DumpLoadPage() {
	const dumpJsonEndpoint = useMemo(
		() => process.env.NEXT_PUBLIC_DUMP_JSON_ENDPOINT ?? "http://127.0.0.1:8000/dump-json",
		[],
	);
	const loadJsonEndpoint = useMemo(
		() => process.env.NEXT_PUBLIC_LOAD_DUMP_JSON_ENDPOINT ?? "http://127.0.0.1:8000/load-dump-json",
		[],
	);

	const [status, setStatus] = useState("Idle");
	const [error, setError] = useState<string | null>(null);
	const [responseText, setResponseText] = useState("");
	const [selectedFileName, setSelectedFileName] = useState<string>("");
	const [selectedDump, setSelectedDump] = useState<Record<string, unknown> | null>(null);
	const [isBusy, setIsBusy] = useState(false);

	async function handleDownloadDump() {
		setIsBusy(true);
		setError(null);
		setResponseText("");
		setStatus("Downloading...");

		try {
			const response = await fetch(dumpJsonEndpoint, { method: "GET" });
			const data = (await response.json()) as Record<string, unknown>;
			if (!response.ok) {
				const summary = data as DumpSummary;
				throw new Error(summary?.detail ?? summary?.message ?? `Request failed: ${response.status}`);
			}

			const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
			const href = URL.createObjectURL(blob);
			const anchor = document.createElement("a");
			anchor.href = href;
			anchor.download = "runtime_dump.json";
			document.body.appendChild(anchor);
			anchor.click();
			anchor.remove();
			URL.revokeObjectURL(href);

			setStatus("Dump downloaded");
			setResponseText(JSON.stringify(data, null, 2));
		} catch (err) {
			setStatus("Failed");
			setError(err instanceof Error ? err.message : "Failed to download dump");
		} finally {
			setIsBusy(false);
		}
	}

	async function handleFileSelected(file: File | null) {
		setError(null);
		setResponseText("");
		setSelectedFileName(file?.name ?? "");
		setSelectedDump(null);

		if (!file) {
			return;
		}

		try {
			const text = await file.text();
			const parsed = JSON.parse(text) as Record<string, unknown>;
			setSelectedDump(parsed);
		} catch (err) {
			setError(err instanceof Error ? err.message : "Invalid JSON file");
		}
	}

	async function handleLoadFromFile() {
		if (!selectedDump) {
			setError("Select a JSON dump file first.");
			return;
		}

		setIsBusy(true);
		setError(null);
		setResponseText("");
		setStatus("Restoring from file...");

		try {
			const response = await fetch(loadJsonEndpoint, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ dump: selectedDump }),
			});
			const data = (await response.json()) as DumpSummary;
			if (!response.ok) {
				throw new Error(data?.detail ?? data?.message ?? `Request failed: ${response.status}`);
			}
			setStatus("File dump loaded");
			setResponseText(JSON.stringify(data, null, 2));
		} catch (err) {
			setStatus("Failed");
			setError(err instanceof Error ? err.message : "Failed to load JSON dump");
		} finally {
			setIsBusy(false);
		}
	}

	return (
		<main className="mx-auto w-full max-w-5xl px-6 py-10 sm:px-10">
			<Navbar />

			<section className="mt-8 rounded-2xl border border-slate-200 bg-white p-6 shadow-sm sm:p-8">
				<h1 className="text-3xl font-semibold tracking-tight text-slate-900">Dump / Load Runtime State</h1>
				<p className="mt-2 text-sm text-slate-600">
					Download the live runtime dump as JSON and restore from a selected JSON file.
				</p>

				<div className="mt-6 grid gap-4 lg:grid-cols-2">
					<div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
						<p className="text-sm font-semibold tracking-wide text-slate-800 uppercase">Download Dump File</p>
						<p className="mt-2 text-sm text-slate-600">
							Fetches the current runtime snapshot and downloads it as a JSON file.
						</p>
						<button
							type="button"
							onClick={handleDownloadDump}
							disabled={isBusy}
							className="mt-4 rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
						>
							{isBusy ? "Working..." : "Download Runtime Dump"}
						</button>
						<p className="mt-3 text-xs text-slate-500">Endpoint: {dumpJsonEndpoint}</p>
					</div>

					<div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
						<p className="text-sm font-semibold tracking-wide text-slate-800 uppercase">Load From File</p>
						<p className="mt-2 text-sm text-slate-600">
							Upload a JSON dump file from your machine and restore it exactly.
						</p>
						<input
							type="file"
							accept="application/json,.json"
							onChange={(e) => void handleFileSelected(e.target.files?.[0] ?? null)}
							className="mt-4 block w-full text-sm text-slate-700 file:mr-4 file:rounded-lg file:border-0 file:bg-slate-900 file:px-4 file:py-2 file:text-sm file:font-semibold file:text-white hover:file:bg-slate-800"
						/>
						<button
							type="button"
							onClick={handleLoadFromFile}
							disabled={isBusy || !selectedDump}
							className="mt-4 rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:bg-slate-200"
						>
							{isBusy ? "Working..." : "Restore Uploaded Dump"}
						</button>
						{selectedFileName ? <p className="mt-3 text-xs text-slate-500">Selected: {selectedFileName}</p> : null}
					</div>
				</div>

				<div className="mt-5 rounded-lg border border-slate-200 bg-white px-4 py-3 text-sm text-slate-700">
					Status: {status}
				</div>

				{error ? <p className="mt-4 text-sm text-red-600">Error: {error}</p> : null}
				{responseText ? (
					<div className="mt-6 rounded-lg border border-slate-200 bg-slate-50 p-4">
						<p className="mb-2 text-sm font-semibold text-slate-700">Response</p>
						<pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs text-slate-700">{responseText}</pre>
					</div>
				) : null}
			</section>
		</main>
	);
}
