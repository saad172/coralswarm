"use client";

/* DEV-ONLY PREVIEW — delete before merge (or fold into a lasting fixture if
 * it turns out worth keeping; not part of PR #819 itself).
 *
 * Renders the REAL `PromoteModal` AND the REAL `PromoteJobsIndicator` (the
 * actual shipped components, same as app/dev/usage-page/page.tsx and
 * app/dev/out-of-credits/page.tsx do for theirs) against a fixture
 * reef/oceans, wrapped in the REAL `PromoteJobsProvider`, with `window.fetch`
 * shimmed to stand in for a running backend — no Postgres, no Clerk session,
 * no real job. Lives OUTSIDE the (app) route group deliberately: that
 * group's layout gates on Clerk, and this needs to render with nobody
 * signed in. The mocked header bar below is NOT the real `AppShell` (that's
 * (app)-gated) — it's just enough markup to show the indicator in roughly
 * the position it actually renders in.
 *
 * `?state=picker|progress|done|error|minimized|multi` on the URL lands
 * directly on one of this feature's states with ZERO manual clicks:
 *   - `picker` needs no mock and no auto-click: it's the modal's default
 *     idle render.
 *   - `progress`/`done`/`error` auto-select the one fixture target ocean and
 *     click Share on mount.
 *   - `minimized` does the same, then ALSO clicks the promoting panel's
 *     "Close" button — the exact scenario the persistent indicator exists
 *     for: the modal is gone, but the job is still running, and the pill in
 *     the (simulated) header is the only remaining sign of it.
 *   - `multi` does the same as `progress` for reef 1 (through the real
 *     `PromoteModal`/`POST /promote` flow), AND directly seeds a SECOND,
 *     synthetic reef's batch straight into the shared provider (bypassing
 *     the modal entirely — there's only one on screen) so the indicator has
 *     two concurrent, DIFFERENT-reef batches to render as its summary pill
 *     + popover, per this feature's "multiple concurrent jobs" requirement.
 * Every click above is a real simulated `.click()` on the real rendered
 * `<button>`s, so this exercises the actual event handlers, not a shortcut
 * around them. The mocked fetch only answers `POST /v1/nodes/{id}/promote`
 * and `GET /v1/jobs/{id}` — everything else (including Next's own internal
 * requests) passes through to the real `fetch` untouched.
 */

import { Suspense, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import { PromoteModal, type PromoteTarget } from "@/components/PromoteModal";
import PromoteJobsIndicator from "@/components/PromoteJobsIndicator";
import { PromoteJobsProvider, usePromoteJobs } from "@/lib/promoteJobsProvider";
import type { Graph, JobStatus, Reef, ReefAtom } from "@/lib/api";
import type { PromoteLeg } from "@/lib/promoteJob";

type DemoState = "picker" | "progress" | "done" | "error" | "minimized" | "multi";

const REEF: Reef = {
  node_id: "demo-reef-1",
  type: "decision",
  title: "Q3 roadmap priorities",
  scope: "graph",
  atom_count: 8,
  created_at: "2026-06-01T00:00:00Z",
  updated_at: "2026-09-01T00:00:00Z",
};

// Deliberately a handful, not literally 3,129 — this is what the (unchanged)
// coral picker renders in the DOM, and the picker's row count has nothing to
// do with the async job's `corals_requested`/progress numbers below, which
// mirror the real bug report (a 3,129-coral reef) regardless of how many
// rows are on screen.
const ATOMS: ReefAtom[] = [
  { atom_id: "demo-atom-0", source_type: "slack", source_id: "demo-source-0", timestamp: "2026-08-01T10:00:00Z", participants: ["demo-user-1"] },
  { atom_id: "demo-atom-1", source_type: "transcript", source_id: "demo-source-1", timestamp: "2026-08-02T14:30:00Z", participants: ["demo-user-2"] },
  { atom_id: "demo-atom-2", source_type: "slack", source_id: "demo-source-2", timestamp: "2026-08-03T09:15:00Z", participants: ["demo-user-1"] },
  { atom_id: "demo-atom-3", source_type: "notion", source_id: "demo-source-3", timestamp: "2026-08-04T11:00:00Z", participants: [] },
  { atom_id: "demo-atom-4", source_type: "transcript", source_id: "demo-source-4", timestamp: "2026-08-05T16:45:00Z", participants: ["demo-user-3"] },
  { atom_id: "demo-atom-5", source_type: "slack", source_id: "demo-source-5", timestamp: "2026-08-06T08:20:00Z", participants: ["demo-user-2"] },
  { atom_id: "demo-atom-6", source_type: "email", source_id: "demo-source-6", timestamp: "2026-08-07T13:10:00Z", participants: ["demo-user-3"] },
  { atom_id: "demo-atom-7", source_type: "slack", source_id: "demo-source-7", timestamp: "2026-08-08T17:05:00Z", participants: ["demo-user-1"] },
];

const PERSONAL: Graph = {
  id: "demo-personal",
  name: "Your ocean",
  kind: "personal",
  is_primary: true,
  created_at: "2026-01-01T00:00:00Z",
  team_id: null,
  my_team_role: null,
};

// The one promote target — matches the background story's real example
// ("Coralswarm Team · team · created Sep 8").
const TEAM: Graph = {
  id: "demo-team",
  name: "Coralswarm Team",
  kind: "team",
  is_primary: false,
  created_at: "2026-09-08T00:00:00Z",
  team_id: "demo-team-row",
  my_team_role: "member",
};

const OCEANS: Graph[] = [PERSONAL, TEAM];

// The `multi` state's second, synthetic reef/job — seeded directly into the
// shared provider (see the effect below), never through a real `PromoteModal`
// submit. Parked mid-flight forever, same as the first reef's job, so the
// summary pill reads a stable "2 promoting".
const REEF_2_NODE_ID = "demo-reef-2";
const REEF_2_TITLE = "Sales pipeline notes";
const REEF_2_JOB_ID = "demo-job-progress-2";

type JobState = "progress" | "done" | "error" | "minimized";

// `minimized`/`multi` share their (reef 1) job id/status with `progress` —
// it's the SAME running-forever mid-flight job, just observed with the modal
// closed (`minimized`) or alongside a second concurrent batch (`multi`).
const JOB_ID: Record<JobState, string> = {
  progress: "demo-job-progress",
  minimized: "demo-job-progress",
  done: "demo-job-done",
  error: "demo-job-error",
};

function jobStatusFor(state: JobState): JobStatus {
  const base = {
    id: JOB_ID[state],
    kind: "PromoteNode",
    created_at: "2026-09-08T18:00:00Z",
    started_at: "2026-09-08T18:00:01Z",
  };
  if (state === "progress" || state === "minimized") {
    // Parks here forever (every poll gets the same answer) — a stable,
    // screenshot-able mid-flight state rather than one that races to done.
    return { ...base, status: "running", progress_current: 1600, progress_total: 3129, error_message: null, finished_at: null };
  }
  if (state === "done") {
    return { ...base, status: "done", progress_current: 3129, progress_total: 3129, error_message: null, finished_at: "2026-09-08T18:04:00Z" };
  }
  return {
    ...base,
    status: "failed",
    progress_current: 1024,
    progress_total: 3129,
    error_message: "Batch coral copy failed after 3 retries — the target workspace's database connection was reset.",
    finished_at: "2026-09-08T18:02:00Z",
  };
}

/** Installs a `window.fetch` shim answering only the two endpoints
 *  `PromoteModal`/`PromoteJobsProvider` call for a "reef" promote;
 *  everything else (Next's own requests included) passes through to the
 *  real `fetch`. Returns the restore function. `multi` treats reef 1
 *  exactly like `progress` (real `POST /promote` flow) and additionally
 *  answers reef 2's job id — reef 2 itself is seeded straight into the
 *  provider (see the effect below), never via a `POST /promote` call, so
 *  only its `GET /v1/jobs/{id}` needs a response here. */
function installFetchMock(state: DemoState): () => void {
  const real = window.fetch.bind(window);
  if (state === "picker") return () => {};

  const reef1State: JobState = state === "multi" ? "progress" : state;
  const jobId = JOB_ID[reef1State];

  window.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    const method = (init?.method ?? "GET").toUpperCase();

    if (method === "POST" && /\/v1\/nodes\/[^/]+\/promote$/.test(url)) {
      const body = {
        source_node_id: REEF.node_id,
        target_graph_id: TEAM.id,
        target_node_id: "demo-target-node",
        created: true,
        corals_requested: 3129,
        job_id: jobId,
      };
      return new Response(JSON.stringify(body), { status: 201, headers: { "content-type": "application/json" } });
    }

    const jobMatch = url.match(/\/v1\/jobs\/([^/?]+)/);
    if (jobMatch) {
      if (state === "multi" && jobMatch[1] === REEF_2_JOB_ID) {
        const res: JobStatus = {
          id: REEF_2_JOB_ID,
          kind: "PromoteNode",
          status: "running",
          progress_current: 40,
          progress_total: 260,
          error_message: null,
          created_at: "2026-09-08T18:00:00Z",
          started_at: "2026-09-08T18:00:01Z",
          finished_at: null,
        };
        return new Response(JSON.stringify(res), { status: 200, headers: { "content-type": "application/json" } });
      }
      if (jobMatch[1] !== jobId) {
        return new Response(JSON.stringify({ error: "job not found" }), { status: 404, headers: { "content-type": "application/json" } });
      }
      return new Response(JSON.stringify(jobStatusFor(reef1State)), { status: 200, headers: { "content-type": "application/json" } });
    }

    return real(input, init);
  }) as typeof window.fetch;

  return () => {
    window.fetch = real;
  };
}

const TARGET: PromoteTarget = { kind: "reef", reef: REEF, atoms: ATOMS };

function PromoteProgressPreviewInner() {
  const params = useSearchParams();
  const state = (params.get("state") as DemoState | null) ?? "picker";
  const rootRef = useRef<HTMLDivElement>(null);
  // Real open/close state, unlike the earlier version of this harness (a
  // bare console.log): `minimized` needs the modal to ACTUALLY unmount so
  // the indicator is the only thing left on screen, which is the entire
  // point of that state.
  const [modalOpen, setModalOpen] = useState(true);
  const { startBatch } = usePromoteJobs();

  useEffect(() => {
    const restore = installFetchMock(state);
    return restore;
    // Re-installed only if `state` changes — this page is always loaded
    // fresh per demo URL, so in practice this runs exactly once.
  }, [state]);

  // `multi`'s second, synthetic reef — seeded directly into the shared
  // provider rather than through a real `PromoteModal` submit (there's only
  // one modal on screen, for reef 1). Demonstrates the indicator's "more
  // than one concurrent batch" summary-pill + popover path.
  useEffect(() => {
    if (state !== "multi") return;
    const leg: PromoteLeg = {
      oceanId: TEAM.id,
      oceanName: TEAM.name,
      phase: "job",
      jobId: REEF_2_JOB_ID,
      status: "running",
      current: 40,
      total: 260,
      error: null,
    };
    startBatch(REEF_2_NODE_ID, REEF_2_TITLE, [leg]);
  }, [state, startBatch]);

  // Zero-manual-click automation. `picker` needs none of this (it's the
  // default idle render); every other state selects the one fixture target
  // ocean, then clicks Share; `minimized` additionally waits for the
  // promoting panel to render, then clicks its "Close" button.
  useEffect(() => {
    if (state === "picker") return;
    const root = rootRef.current;
    if (!root) return;
    const byText = (needle: string) =>
      Array.from(root.querySelectorAll("button")).find((b) => b.textContent?.includes(needle));

    const timers: ReturnType<typeof setTimeout>[] = [];
    timers.push(
      setTimeout(() => {
        byText("Coralswarm Team")?.click();
        timers.push(
          setTimeout(() => {
            byText("Share")?.click();
            if (state !== "minimized") return;
            timers.push(
              setTimeout(() => {
                byText("Close")?.click();
                setModalOpen(false);
              }, 200),
            );
          }, 150),
        );
      }, 150),
    );
    return () => timers.forEach(clearTimeout);
  }, [state]);

  const oceans = useMemo(() => OCEANS, []);

  return (
    <div ref={rootRef} className="min-h-screen bg-bg-2">
      {/* Stand-in for AppShell's real 52px header — not the real component
          (that lives behind the (app) route group's Clerk gate), just enough
          to place <PromoteJobsIndicator> where it actually renders. */}
      <header className="flex h-[52px] items-center gap-3 border-b border-line-2 bg-bg px-[20px]">
        <span className="min-w-0 truncate text-[14px] font-medium tracking-[-0.01em] text-ink">
          Topic · Q3 roadmap priorities
        </span>
        <div className="ml-auto flex min-w-0 items-center gap-[8px]">
          <PromoteJobsIndicator />
        </div>
      </header>

      <div className="mx-auto max-w-[720px] p-10">
        <p className="mb-4 font-mono text-[11px] uppercase tracking-[0.13em] text-ink-4">
          promote progress preview (dev only) — state={state}
        </p>
        {modalOpen && (
          <PromoteModal
            target={TARGET}
            oceans={oceans}
            currentGraphId={PERSONAL.id}
            token="dev-preview-token"
            onClose={(completed) => {
              console.log("[promote-progress-preview] onClose", { completed });
              setModalOpen(false);
            }}
            onPromoted={(names, summary) => {
              console.log("[promote-progress-preview] onPromoted", { names, summary });
            }}
          />
        )}
        {!modalOpen && (
          <p className="font-mono text-[11px] text-ink-3">
            (modal closed — check the header pill above for the persistent indicator)
          </p>
        )}
      </div>
    </div>
  );
}

export default function PromoteProgressPreview() {
  return (
    <Suspense fallback={null}>
      <PromoteJobsProvider token="dev-preview-token">
        <PromoteProgressPreviewInner />
      </PromoteJobsProvider>
    </Suspense>
  );
}
