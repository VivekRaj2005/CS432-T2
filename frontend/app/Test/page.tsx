"use client";

import { useState } from "react";
import Navbar from "../Components/Navbar";

type TestResponse = {
    success?: boolean;
    output?: string;
    errorOutput?: string;
    error?: string;
    message?: string;
};

const ACID_TESTS = [
    { id: "at.py", name: "Atomicity (at.py)" },
    { id: "cons.py", name: "Consistency (cons.py)" },
    { id: "iso.py", name: "Isolation (iso.py)" },
    { id: "dur.py", name: "Durability (dur.py)" },
];

export default function AcidTestPage() {
    const [status, setStatus] = useState("Idle");
    const [error, setError] = useState<string | null>(null);
    const [responseText, setResponseText] = useState("");
    const [isBusy, setIsBusy] = useState(false);
    
    const [selectedTests, setSelectedTests] = useState<Set<string>>(new Set());

    function handleToggleTest(testId: string) {
        const next = new Set(selectedTests);
        if (next.has(testId)) {
            next.delete(testId);
        } else {
            next.add(testId);
        }
        setSelectedTests(next);
    }

    async function runTests(testsToRun: string[]) {
        if (testsToRun.length === 0) {
            setError("Please select at least one test to run.");
            return;
        }

        setIsBusy(true);
        setError(null);
        setResponseText("");
        setStatus(`Running ${testsToRun.length} test(s)...`);

        try {
            const response = await fetch("http://localhost:8000/run-tests", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ tests: testsToRun }),
            });
            
            const data = (await response.json()) as TestResponse;
            
            if (!response.ok) {
                throw new Error(data.error ?? `Request failed: ${response.status}`);
            }

            setStatus(data.success ? "All tests passed!" : "Completed with test failures.");
            setResponseText(data.output || data.errorOutput || "No output returned.");
            
        } catch (err) {
            setStatus("Failed to execute");
            setError(err instanceof Error ? err.message : "Failed to run tests");
        } finally {
            setIsBusy(false);
        }
    }

    return (
        <main className="mx-auto w-full max-w-5xl px-6 py-10 sm:px-10">
            <Navbar />

            <section className="mt-8 rounded-2xl border border-slate-200 bg-white p-6 shadow-sm sm:p-8">
                <h1 className="text-3xl font-semibold tracking-tight text-slate-900">ACID Testing</h1>
                <p className="mt-2 text-sm text-slate-600">
                    Run database transaction tests for Atomicity, Consistency, Isolation, and Durability.
                </p>

                <div className="mt-6 rounded-xl border border-slate-200 bg-slate-50 p-4 sm:p-6">
                    <p className="mb-4 text-sm font-semibold tracking-wide text-slate-800 uppercase">
                        Select Test Suites
                    </p>
                    
                    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                        {ACID_TESTS.map((test) => (
                            <label 
                                key={test.id} 
                                className="flex cursor-pointer items-center space-x-3 rounded-lg border border-slate-200 bg-white p-3 shadow-sm hover:bg-slate-50"
                            >
                                <input
                                    type="checkbox"
                                    className="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-900"
                                    checked={selectedTests.has(test.id)}
                                    onChange={() => handleToggleTest(test.id)}
                                    disabled={isBusy}
                                />
                                <span className="text-sm font-medium text-slate-700">{test.name}</span>
                            </label>
                        ))}
                    </div>

                    <div className="mt-6 flex flex-wrap gap-4">
                        <button
                            type="button"
                            onClick={() => runTests(Array.from(selectedTests))}
                            disabled={isBusy || selectedTests.size === 0}
                            className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white transition-colors hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
                        >
                            {isBusy ? "Running..." : `Run Selected (${selectedTests.size})`}
                        </button>
                        
                        <button
                            type="button"
                            onClick={() => runTests(ACID_TESTS.map(t => t.id))}
                            disabled={isBusy}
                            className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:bg-slate-200"
                        >
                            Run All Tests
                        </button>
                    </div>
                </div>

                <div className="mt-5 rounded-lg border border-slate-200 bg-white px-4 py-3 text-sm text-slate-700">
                    <span className="font-semibold text-slate-900">Status: </span> {status}
                </div>

                {error ? <p className="mt-4 text-sm font-medium text-red-600">Error: {error}</p> : null}
            
            </section>
        </main>
    );
}