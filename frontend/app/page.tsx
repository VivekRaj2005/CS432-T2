import Navbar from "./Components/Navbar";

export default function Home() {
  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top,_#f8fbff_0%,_#fbfdff_38%,_#ffffff_100%)] text-slate-800">
      <main className="mx-auto flex w-full max-w-6xl flex-col gap-10 px-6 py-8 sm:gap-12 sm:px-10 lg:px-12">
        <Navbar />

        <section className="relative py-4 sm:py-8">
          <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-slate-200 to-transparent" />
          <div className="pointer-events-none absolute inset-x-0 bottom-0 h-px bg-gradient-to-r from-transparent via-slate-200 to-transparent" />

          <div className="grid gap-8 lg:grid-cols-[1.3fr_0.7fr] lg:items-end">
            <div className="space-y-6">
              <div className="flex flex-wrap items-center gap-3">
                <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-semibold tracking-[0.16em] text-slate-700 uppercase">
                  IITGnDB Database Manager
                </span>
                <span className="rounded-full bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-700">
                  Enterprise Workflow
                </span>
              </div>

              <h1 className="max-w-4xl text-4xl leading-tight font-semibold tracking-tight text-slate-900 sm:text-5xl lg:text-6xl">
                Authoritative control over every schema and every migration.
              </h1>

              <p className="max-w-2xl text-base leading-8 text-slate-600 sm:text-lg">
                IITGnDB unifies create, update, delete, and schema visibility in
                one continuous operations surface designed for teams that need
                precision, traceability, and calm execution.
              </p>

              <div className="flex flex-wrap gap-3">
                <button
                  type="button"
                  className="rounded-lg bg-slate-900 px-5 py-3 text-sm font-semibold text-white transition-colors duration-200 hover:bg-slate-800"
                >
                  Launch Console
                </button>
                <button
                  type="button"
                  className="rounded-lg border border-slate-300 bg-white px-5 py-3 text-sm font-semibold text-slate-700 transition-colors duration-200 hover:bg-slate-50"
                >
                  Review Schema Policy
                </button>
              </div>
            </div>

            <aside className="space-y-4 text-sm text-slate-600 lg:pl-10">
              <p className="text-xs font-semibold tracking-[0.14em] text-slate-500 uppercase">
                Operating Principles
              </p>
              <p>Versioned structure changes with verified rollback paths.</p>
              <p>Clear ownership across create, update, and delete actions.</p>
              <p>Instant schema visibility before and after deployment.</p>
            </aside>
          </div>
        </section>

        <section className="space-y-1">
          {[
            {
              id: "create",
              title: "Create",
              description:
                "Define new entities and relations with guarded defaults, ownership tags, and release notes attached from day one.",
            },
            {
              id: "update",
              title: "Update",
              description:
                "Evolve live structures through reviewable revisions with impact previews before any production write path changes.",
            },
            {
              id: "delete",
              title: "Delete",
              description:
                "Retire tables and fields with dependency checks, archival options, and staged deprecation windows for safety.",
            },
            {
              id: "view-schema",
              title: "View Schema",
              description:
                "Inspect current and historical schema states in one timeline with index, key, and relation clarity.",
            },
          ].map((item) => (
            <article
              id={item.id}
              key={item.id}
              className="group py-6 sm:py-7"
            >
              <div className="mb-6 h-px bg-gradient-to-r from-slate-200 via-slate-200/60 to-transparent" />
              <div className="grid gap-3 sm:grid-cols-[180px_1fr] sm:gap-6">
                <p className="text-sm font-semibold tracking-[0.12em] text-slate-500 uppercase">
                  {item.title}
                </p>
                <p className="max-w-3xl text-lg leading-8 text-slate-700 transition-colors duration-200 group-hover:text-slate-900">
                  {item.description}
                </p>
              </div>
            </article>
          ))}
        </section>

        <section className="pt-2 pb-10">
          <div className="h-px bg-gradient-to-r from-transparent via-slate-200 to-transparent" />
          <div className="mt-8 flex flex-col gap-5 sm:flex-row sm:items-end sm:justify-between">
            <div className="space-y-2">
              <p className="text-sm font-semibold tracking-[0.12em] text-slate-500 uppercase">
                Governance First
              </p>
              <p className="max-w-2xl text-2xl font-semibold leading-tight text-slate-900">
                Execute schema lifecycle decisions with confidence, not guesswork.
              </p>
            </div>
            <button
              type="button"
              className="rounded-lg border border-slate-300 bg-slate-900 px-5 py-3 text-sm font-semibold text-white transition-colors duration-200 hover:bg-slate-800"
            >
              Open IITGnDB Workspace
            </button>
          </div>
        </section>
      </main>
    </div>
  );
}
