"use client";

import { useMemo, useState } from "react";
import Navbar from "../Components/Navbar";

type FieldType =
	| "string"
	| "number"
	| "boolean"
	| "dict"
	| "list<string>"
	| "list<number>"
	| "list<boolean>";

type FieldInput = {
	id: string;
	name: string;
	type: FieldType;
	value: string;
	listValues: string[];
	dictFields: FieldInput[];
};

const FIELD_TYPES: FieldType[] = [
	"string",
	"number",
	"boolean",
	"dict",
	"list<string>",
	"list<number>",
	"list<boolean>",
];

function nextId(prefix: string): string {
	if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
		return `${prefix}-${crypto.randomUUID()}`;
	}
	return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function createField(id: string): FieldInput {
	return {
		id,
		name: "",
		type: "string",
		value: "",
		listValues: [""],
		dictFields: [],
	};
}

function updateFieldTree(fields: FieldInput[], id: string, updater: (field: FieldInput) => FieldInput): FieldInput[] {
	return fields.map((field) => {
		if (field.id === id) {
			return updater(field);
		}

		if (field.dictFields.length > 0) {
			return {
				...field,
				dictFields: updateFieldTree(field.dictFields, id, updater),
			};
		}

		return field;
	});
}

function removeFieldTree(fields: FieldInput[], id: string): FieldInput[] {
	return fields
		.filter((field) => field.id !== id)
		.map((field) => ({
			...field,
			dictFields: removeFieldTree(field.dictFields, id),
		}));
}

function parseFieldValue(
	field: FieldInput,
): string | number | boolean | Array<string | number | boolean> | Record<string, unknown> {
	if (field.type === "string") {
		return field.value;
	}

	if (field.type === "number") {
		if (field.value.trim() === "") {
			throw new Error(`Field '${field.name || "(unnamed)"}' must have a number value`);
		}
		const num = Number(field.value);
		if (Number.isNaN(num)) {
			throw new Error(`Field '${field.name || "(unnamed)"}' has invalid number input`);
		}
		return num;
	}

	if (field.type === "boolean") {
		return field.value === "true";
	}

	if (field.type === "dict") {
		const nestedObject: Record<string, unknown> = {};
		for (const nestedField of field.dictFields) {
			const cleanName = nestedField.name.trim();
			if (!cleanName) {
				continue;
			}
			nestedObject[cleanName] = parseFieldValue(nestedField);
		}
		return nestedObject;
	}

	if (field.type === "list<string>") {
		return field.listValues.filter((item) => item.trim() !== "");
	}

	if (field.type === "list<number>") {
		return field.listValues
			.filter((item) => item.trim() !== "")
			.map((item) => {
				const num = Number(item);
				if (Number.isNaN(num)) {
					throw new Error(`Field '${field.name || "(unnamed)"}' has invalid number in list`);
				}
				return num;
			});
	}

	return field.listValues
		.filter((item) => item.trim() !== "")
		.map((item) => item === "true");
}

export default function CreatePage() {
	const endpoint = useMemo(
		() => process.env.NEXT_PUBLIC_CREATE_ENDPOINT ?? "http://127.0.0.1:8000/create",
		[],
	);

	const [fields, setFields] = useState<FieldInput[]>([createField("field-1")]);
	const [status, setStatus] = useState<string>("Idle");
	const [error, setError] = useState<string | null>(null);
	const [responseText, setResponseText] = useState<string>("");
	const [isSubmitting, setIsSubmitting] = useState(false);

	function updateField(id: string, updater: (prev: FieldInput) => FieldInput) {
		setFields((prev) => updateFieldTree(prev, id, updater));
	}

	function addField() {
		setFields((prev) => [...prev, createField(nextId("field"))]);
	}

	function removeField(id: string) {
		setFields((prev) => {
			const existsAtRoot = prev.some((item) => item.id === id);
			if (existsAtRoot && prev.length <= 1) {
				return prev;
			}
			return removeFieldTree(prev, id);
		});
	}

	function addNestedField(parentId: string) {
		updateField(parentId, (prev) => ({
			...prev,
			dictFields: [...prev.dictFields, createField(nextId("field"))],
		}));
	}

	function addListInput(id: string) {
		updateField(id, (prev) => ({ ...prev, listValues: [...prev.listValues, ""] }));
	}

	function removeListInput(id: string, index: number) {
		updateField(id, (prev) => {
			if (prev.listValues.length <= 1) {
				return prev;
			}

			return {
				...prev,
				listValues: prev.listValues.filter((_, idx) => idx !== index),
			};
		});
	}

	async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
		event.preventDefault();
		setError(null);
		setResponseText("");

		const payload: Record<string, unknown> = {};

		try {
			for (const field of fields) {
				const cleanName = field.name.trim();
				if (!cleanName) {
					continue;
				}

				payload[cleanName] = parseFieldValue(field);
			}
		} catch (err) {
			setError(err instanceof Error ? err.message : "Invalid field input");
			return;
		}

		if (Object.keys(payload).length === 0) {
			setError("Add at least one field with a valid name.");
			return;
		}

		setIsSubmitting(true);
		setStatus("Submitting...");

		try {
			const response = await fetch(endpoint, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
				},
				body: JSON.stringify(payload),
			});

			const data = await response.json();
			if (!response.ok) {
				throw new Error(data?.detail ?? `Request failed: ${response.status}`);
			}

			setStatus("Queued successfully");
			setResponseText(JSON.stringify(data, null, 2));
		} catch (err) {
			setStatus("Failed");
			setError(err instanceof Error ? err.message : "Failed to create record");
		} finally {
			setIsSubmitting(false);
		}
	}

	function renderFieldEditor(field: FieldInput, fieldIndex: number, level: number): React.JSX.Element {
		const isListType = field.type.startsWith("list<");
		const isBooleanType = field.type === "boolean";
		const isDictType = field.type === "dict";
		const label = level === 0 ? `Field ${fieldIndex + 1}` : `Nested Field ${fieldIndex + 1}`;

		return (
			<div key={field.id} className="rounded-xl border border-slate-200 bg-slate-50 p-4">
				<div className="mb-4 flex items-center justify-between">
					<p className="text-sm font-semibold text-slate-700">{label}</p>
					<button
						type="button"
						onClick={() => removeField(field.id)}
						className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-600 hover:bg-slate-100"
					>
						Remove
					</button>
				</div>

				<div className="grid gap-4 md:grid-cols-3">
					<label className="flex flex-col gap-2 text-sm text-slate-700">
						Field Name
						<input
							value={field.name}
							onChange={(e) => updateField(field.id, (prev) => ({ ...prev, name: e.target.value }))}
							placeholder="e.g. salary"
							className="rounded-lg border border-slate-300 bg-white px-3 py-2 outline-none ring-slate-200 transition focus:ring"
						/>
					</label>

					<label className="flex flex-col gap-2 text-sm text-slate-700">
						Type
						<select
							value={field.type}
							onChange={(e) =>
								updateField(field.id, (prev) => ({
									...prev,
									type: e.target.value as FieldType,
									value: "",
									listValues: [""],
									dictFields: e.target.value === "dict" ? [createField(nextId("field"))] : [],
								}))
							}
							className="rounded-lg border border-slate-300 bg-white px-3 py-2 outline-none ring-slate-200 transition focus:ring"
						>
							{FIELD_TYPES.map((fieldType) => (
								<option key={fieldType} value={fieldType}>
									{fieldType}
								</option>
							))}
						</select>
					</label>

					{!isListType && !isDictType ? (
						<label className="flex flex-col gap-2 text-sm text-slate-700">
							Input
							{isBooleanType ? (
								<select
									value={field.value || "false"}
									onChange={(e) => updateField(field.id, (prev) => ({ ...prev, value: e.target.value }))}
									className="rounded-lg border border-slate-300 bg-white px-3 py-2 outline-none ring-slate-200 transition focus:ring"
								>
									<option value="false">false</option>
									<option value="true">true</option>
								</select>
							) : (
								<input
									type={field.type === "number" ? "number" : "text"}
									value={field.value}
									onChange={(e) => updateField(field.id, (prev) => ({ ...prev, value: e.target.value }))}
									placeholder={field.type === "number" ? "e.g. 42" : "Enter value"}
									className="rounded-lg border border-slate-300 bg-white px-3 py-2 outline-none ring-slate-200 transition focus:ring"
								/>
							)}
						</label>
					) : (
						<div className="md:col-span-1" />
					)}
				</div>

				{isListType ? (
					<div className="mt-4 space-y-3 rounded-lg border border-dashed border-slate-300 bg-white p-4">
						<div className="flex items-center justify-between">
							<p className="text-sm font-medium text-slate-700">List Input Items</p>
							<button
								type="button"
								onClick={() => addListInput(field.id)}
								className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-600 hover:bg-slate-100"
							>
								Add List Item
							</button>
						</div>

						<div className="space-y-2">
							{field.listValues.map((item, index) => (
								<div key={`${field.id}-item-${index}`} className="flex gap-2">
									{field.type === "list<boolean>" ? (
										<select
											value={item || "false"}
											onChange={(e) =>
												updateField(field.id, (prev) => ({
													...prev,
													listValues: prev.listValues.map((entry, idx) =>
														idx === index ? e.target.value : entry,
													),
												}))
											}
											className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 outline-none ring-slate-200 transition focus:ring"
										>
											<option value="false">false</option>
											<option value="true">true</option>
										</select>
									) : (
										<input
											type={field.type === "list<number>" ? "number" : "text"}
											value={item}
											onChange={(e) =>
												updateField(field.id, (prev) => ({
													...prev,
													listValues: prev.listValues.map((entry, idx) =>
														idx === index ? e.target.value : entry,
													),
												}))
											}
											className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 outline-none ring-slate-200 transition focus:ring"
											placeholder={`Item ${index + 1}`}
										/>
									)}

									<button
										type="button"
										onClick={() => removeListInput(field.id, index)}
										className="rounded-md border border-slate-300 bg-white px-3 py-2 text-xs font-medium text-slate-600 hover:bg-slate-100"
									>
										Remove
									</button>
								</div>
							))}
						</div>
					</div>
				) : null}

				{isDictType ? (
					<div className="mt-4 space-y-3 rounded-lg border border-dashed border-slate-300 bg-white p-4">
						<div className="flex items-center justify-between">
							<p className="text-sm font-medium text-slate-700">Nested Fields</p>
							<button
								type="button"
								onClick={() => addNestedField(field.id)}
								className="rounded-md border border-slate-300 bg-white px-3 py-1 text-xs font-medium text-slate-600 hover:bg-slate-100"
							>
								Add Nested Field
							</button>
						</div>

						{field.dictFields.length === 0 ? (
							<p className="text-xs text-slate-600">No nested fields yet.</p>
						) : (
							<div className="space-y-3">
								{field.dictFields.map((nestedField, nestedIndex) =>
									renderFieldEditor(nestedField, nestedIndex, level + 1),
								)}
							</div>
						)}
					</div>
				) : null}
			</div>
		);
	}

	return (
		<main className="mx-auto w-full max-w-5xl px-6 py-10 sm:px-10">
			<Navbar />

			<div className="mt-8 rounded-2xl border border-slate-200 bg-white p-6 shadow-sm sm:p-8">
				<h1 className="text-3xl font-semibold tracking-tight text-slate-900">Create Record</h1>
				<p className="mt-2 text-sm text-slate-600">Endpoint: {endpoint}</p>

				<form className="mt-8 space-y-6" onSubmit={handleSubmit}>
					{fields.map((field, fieldIndex) => renderFieldEditor(field, fieldIndex, 0))}

					<div className="flex flex-wrap items-center gap-3">
						<button
							type="button"
							onClick={addField}
							className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50"
						>
							Add Another Field
						</button>

						<button
							type="submit"
							disabled={isSubmitting}
							className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
						>
							{isSubmitting ? "Submitting..." : "Create and Queue"}
						</button>

						<span className="text-sm text-slate-600">Status: {status}</span>
					</div>
				</form>

				{error ? <p className="mt-4 text-sm text-red-600">Error: {error}</p> : null}

				{responseText ? (
					<div className="mt-6 rounded-lg border border-slate-200 bg-slate-50 p-4">
						<p className="mb-2 text-sm font-semibold text-slate-700">Server Response</p>
						<pre className="overflow-x-auto whitespace-pre-wrap break-words text-xs text-slate-700">
							{responseText}
						</pre>
					</div>
				) : null}
			</div>
		</main>
	);
}
