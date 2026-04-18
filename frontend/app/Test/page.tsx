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

// Removed the .py extensions from the display names
const ACID_TESTS = [
    { id: "at.py", name: "Atomicity" },
    { id: "cons.py", name: "Consistency" },
    { id: "iso.py", name: "Isolation" },
    { id: "dur.py", name: "Durability" },
];

export default function AcidTestPage() {
    const [status, setStatus] = useState("Idle");
    const [error, setError] = useState<string | null>(null);
    const [responseText, setResponseText] = useState("");
    const [isBusy, setIsBusy] = useState(false);
    const [activeTest, setActiveTest] = useState<string | null>(null);
    const [showConsole, setShowConsole] = useState(false);

    async function runSingleTest(testId: string, testName: string) {
        setIsBusy(true);
        setError(null);
        setResponseText("");
        setStatus(`Running ${testName} test...`);
        setActiveTest(testId);
        
        // Auto-hide console when starting a new test to keep it clean
        setShowConsole(false);

        try {
            const response = await fetch("http://localhost:8000/run-tests/", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ tests: [testId] }), // Send as single item array
            });
            
            const data = (await response.json()) as TestResponse;
            
            if (!response.ok) {
                throw new Error(data.error ?? `Request failed: ${response.status}`);
            }

            setStatus(data.success ? `${testName} tests passed!` : `${testName} tests failed.`);
            setResponseText(data.output || data.errorOutput || "No output returned.");
            
        } catch (err) {
            setStatus("Failed to execute");
            setError(err instanceof Error ? err.message : "Failed to run tests");
        } finally {
            setIsBusy(false);
            setActiveTest(null);
        }
    }

    return (
        <main className="mx-auto w-full max-w-5xl px-6 py-10 sm:px-10">
            <Navbar />

            <section className="mt-8 rounded-2xl border border-slate-200 bg-white p-6 shadow-sm sm:p-8">
                <h1 className="text-3xl font-semibold tracking-tight text-slate-900">ACID Testing</h1>
                <p className="mt-2 text-sm text-slate-600">
                    Click a property below to immediately run its database transaction test.
                </p>

                <div className="mt-6 rounded-xl border border-slate-200 bg-slate-50 p-4 sm:p-6">
                    <p className="mb-4 text-sm font-semibold tracking-wide text-slate-800 uppercase">
                        Run Test
                    </p>
                    
                    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                        {ACID_TESTS.map((test) => (
                            <button
                                key={test.id}
                                type="button"
                                onClick={() => runSingleTest(test.id, test.name)}
                                disabled={isBusy}
                                className={`flex items-center justify-center rounded-lg border py-3 px-4 text-sm font-semibold transition-all ${
                                    isBusy 
                                        ? "cursor-not-allowed opacity-60 bg-white border-slate-200 text-slate-400" 
                                        : "bg-white border-slate-200 text-slate-700 hover:border-slate-400 hover:bg-slate-100 hover:shadow-sm"
                                } ${
                                    activeTest === test.id 
                                        ? "!bg-slate-900 !text-white !border-slate-900 animate-pulse" 
                                        : ""
                                }`}
                            >
                                {activeTest === test.id ? "Running..." : test.name}
                            </button>
                        ))}
                    </div>
                </div>

                {/* Status & Console Toggle Bar */}
                <div className="mt-5 flex items-center justify-between rounded-lg border border-slate-200 bg-white px-4 py-3 text-sm text-slate-700">
                    <div>
                        <span className="font-semibold text-slate-900">Status: </span> 
                        <span className={status.includes("failed") || status.includes("Failed") ? "text-red-600 font-medium" : status.includes("passed") ? "text-green-600 font-medium" : ""}>
                            {status}
                        </span>
                    </div>
                    
                    {responseText && (
                        <button 
                            onClick={() => setShowConsole(!showConsole)}
                            className="rounded border border-slate-300 bg-slate-50 px-3 py-1 text-xs font-semibold text-slate-700 transition hover:bg-slate-200"
                        >
                            {showConsole ? "Hide Console" : "Show Console"}
                        </button>
                    )}
                </div>

                {error && <p className="mt-4 text-sm font-medium text-red-600">Error: {error}</p>}
                
                {/* Expandable Console Output */}
                {showConsole && responseText && (
                    <div className="mt-4 rounded-lg border border-slate-200 bg-slate-900 p-4">
                        <p className="mb-3 text-sm font-semibold text-white">Console Output</p>
                        <pre className="overflow-x-auto whitespace-pre-wrap break-words font-mono text-xs text-green-400">
                            {responseText}
                        </pre>
                    </div>
                )}
            </section>
        </main>
    );
}