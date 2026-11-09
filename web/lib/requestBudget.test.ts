// W7 — the request-budget gate (docs/plans/request-reliability-and-scale.md
// §6.7, §7).
//
// THE INCIDENT THIS WOULD HAVE CAUGHT. The Reefs workbench issued one request
// per reef. At 3,440 reefs that is 3,440 concurrent fetches and
// `net::ERR_INSUFFICIENT_RESOURCES`. Nothing in review caught it and nothing
// in CI could have, because CI proves correctness and says nothing about
// cost. This file is that missing check: it drives the real API client
// against a mocked `fetch` at a declared ocean size and asserts how many
// network calls each whole-set loader makes.
//
// WHAT THIS IS AND IS NOT — read before trusting a green run:
//
//   * It measures the DATA-LOADING functions (`api.reefs.listAll`,
//     `api.graphs.atomsAll`, and the per-reef enrichment shape), not a
//     rendered screen. This repo deliberately has no component-test harness
//     (see web/vitest.config.ts), and standing up Playwright + Clerk session
//     auth is the larger, separate half of W7. So a screen could still add a
//     NEW loader this file does not know about and stay green.
//   * What it does prove is the arithmetic in §7's capacity table — which is
//     precisely the claim the plan got wrong. The original W3 acceptance
//     criterion ("≤25 requests") was arithmetically unreachable because
//     `listAll` alone is 35 requests at today's ocean size, and nobody had
//     counted. Counting is cheap; assuming is what cost us.
//   * Budgets are GENEROUS THRESHOLDS, not exact counts, so a benign change
//     of one request does not fail CI and erode trust in the gate (§11's
//     perf-gate-flakiness risk). They are tight enough that an O(rows)
//     regression cannot hide inside them.
//
// W3 HAS LANDED. The `page_size` ceilings rose from 100/200 to 1,000 and the
// per-reef fan-out is gone. The numbers below are the post-W3 measurements —
// this file was updated in that PR, because a budget that silently stays
// loose after the thing it measured got cheaper is how a gate rots.

import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "./api";
import { loadTopicReef, loadRelatedReefs } from "./topicLoad";
import { __resetTransportForTests } from "./transport";

/** Today's measured primary ocean (get_ocean_stats, 2026-07-26).
 *
 *  NOTE (fix/topics-derive-ocean): this conflicts with
 *  web/app/(app)/workspaces/page.tsx's "a workspace here holds 1,500+
 *  topics" comment — same repo, two different reef-count figures, neither
 *  one dated against the other. Left as-is (not this PR's job to
 *  reconcile), but any "Nx smaller" savings claim below is only ever
 *  relative to THIS number, not a verified current ocean size. */
const TODAY = { reefs: 3440, atoms: 3869 };

/** §7's target. The point of the drain budget is that cost stops tracking
 *  data volume once it binds — so the count here must NOT be ~65× today's. */
const TARGET = { reefs: 100_000, atoms: 250_000 };

/**
 * Mock `fetch` as a paginated backend: returns `min(page_size, remaining)`
 * rows and sets `X-Has-More` accordingly, exactly as `nodes.rs::list_nodes`
 * does. Counting calls against a REAL pager (rather than asserting on a
 * hand-computed number) means the test tracks the client's actual behaviour,
 * including its page-size choice — which is the variable W3 changes.
 */
function mockPaginatedBackend(total: number) {
  let calls = 0;
  const fetchMock = vi.fn(async (url: string) => {
    calls += 1;
    const u = new URL(url, "http://x");
    const page = Number(u.searchParams.get("page") ?? "1");
    const pageSize = Number(u.searchParams.get("page_size") ?? "20");
    const start = (page - 1) * pageSize;
    const n = Math.max(0, Math.min(pageSize, total - start));
    const hasMore = start + n < total;
    return {
      ok: true,
      status: 200,
      headers: { get: (k: string) => (k.toLowerCase() === "x-has-more" ? String(hasMore) : null) },
      json: async () => Array.from({ length: n }, (_, i) => rowFor(start + i)),
      clone() {
        return this;
      },
    };
  });
  vi.stubGlobal("fetch", fetchMock);
  return {
    get calls() {
      return calls;
    },
  };
}

function rowFor(i: number) {
  return {
    node_id: `3f346a2a-71ca-4779-9701-${String(i).padStart(12, "0")}`,
    atom_id: `3f346a2a-71ca-4779-9702-${String(i).padStart(12, "0")}`,
    title: `row ${i}`,
    type: "entity",
    atom_count: 1,
  };
}

afterEach(() => {
  __resetTransportForTests();
  vi.unstubAllGlobals();
});

describe("declared request budgets — whole-set loaders", () => {
  it("reefs.listAll at today's ocean size", async () => {
    const backend = mockPaginatedBackend(TODAY.reefs);
    await api.reefs.listAll("tok", "graph-1");

    // 3,440 reefs ÷ 1,000 per page = 4 requests. Was 35: the ceiling is the
    // backend's own `page_size.clamp(...)` in nodes.rs, NOT a client choice,
    // which is exactly why W3 had to raise it server-side — and why the
    // plan's original "≤25 total" gate was unreachable before it did.
    expect(backend.calls).toBe(4);
    // Budget, deliberately above the measured value: a benign +1 must not
    // fail CI, but a regression to per-row fetching cannot fit here.
    expect(backend.calls).toBeLessThanOrEqual(6);
  });

  it("graphs.atomsAll at today's ocean size", async () => {
    const backend = mockPaginatedBackend(TODAY.atoms);
    await api.graphs.atomsAll("tok", "graph-1");

    // 3,869 atoms ÷ 1,000 per page = 4 requests. Was 20.
    expect(backend.calls).toBe(4);
    expect(backend.calls).toBeLessThanOrEqual(6);
  });

  it("Reefs cold load is within the ≤25 budget at today's ocean size", async () => {
    // THE Phase 2 exit gate (plan §6.3), now measured rather than asserted.
    // Before W3 this was 3,440 fan-out requests + 58 paging. The corrected
    // gate splits by phase: ≤25 at 3,440 reefs here in Phase 2, and ≤25 at
    // 250k atoms only after W5 retires the whole-set drains.
    let total = 0;

    const reefsBackend = mockPaginatedBackend(TODAY.reefs);
    await api.reefs.listAll("tok", "g");
    total += reefsBackend.calls;
    vi.unstubAllGlobals();

    const atomsBackend = mockPaginatedBackend(TODAY.atoms);
    await api.graphs.atomsAll("tok", "g");
    total += atomsBackend.calls;
    vi.unstubAllGlobals();

    // The edge drain that REPLACED the 3,440-request fan-out. ~10k edges at
    // today's scale ÷ 1,000 = 10 requests.
    const edgeBackend = mockPaginatedBackend(10_000);
    await api.graphs.nodeAtomsAll("tok", "g");
    total += edgeBackend.calls;

    // 4 + 4 + 10 = 18, plus 3 base calls (sources, graphs.list, stats) = 21.
    total += 3;
    expect(total).toBeLessThanOrEqual(25);
  });

  it("the edge drain stops at its budget instead of tracking ocean size", async () => {
    // R4's acceptance criterion for this endpoint: past the budget, cost
    // stops growing with data. 20 pages × 1,000 = 20,000 edges, then it
    // stops and reports itself incomplete — which is what lets the UI say
    // the facets are partial rather than quietly rendering 3% as a total.
    const backend = mockPaginatedBackend(5_000_000);
    const { edges, complete } = await api.graphs.nodeAtomsAll("tok", "g");

    expect(backend.calls).toBe(20);
    expect(edges.length).toBe(20_000);
    expect(complete).toBe(false);
  });

  it("reports itself complete when the ocean fits inside the budget", async () => {
    // The other half of the same contract — a false "partial" banner on a
    // small ocean would be its own kind of lie.
    const backend = mockPaginatedBackend(2_500);
    const { edges, complete } = await api.graphs.nodeAtomsAll("tok", "g");

    expect(backend.calls).toBe(3);
    expect(edges.length).toBe(2_500);
    expect(complete).toBe(true);
  });
});

describe("the O(rows) regression this gate exists to catch", () => {
  // Kept after W3 as a LIVE regression test, not as history: `api.reefs.atoms`
  // still exists and is still correct for the single-reef detail surfaces
  // (peek drawer, promote). What must never come back is calling it once per
  // row in a list view — so this pins the cost of that shape.
  it("per-reef enrichment costs one request per reef — the shape W3 removed", async () => {
    const backend = mockPaginatedBackend(0);
    const reefIds = Array.from(
      { length: 500 },
      (_, i) => `3f346a2a-71ca-4779-9701-${String(i).padStart(12, "0")}`,
    );

    await Promise.allSettled(reefIds.map((id) => api.reefs.atoms("tok", id)));

    // Cost tracks data exactly. At the real 3,440 reefs this was 3,440
    // requests; at §7's 100,000-reef target it would be 100,000. The Reefs
    // workbench no longer does this — `scripts/check_no_unbounded_fanout.sh`
    // now fails on the shape, and its allowlist entry for that file was
    // deleted with W3.
    expect(backend.calls).toBe(reefIds.length);
  });
});

describe("scale behaviour — cost must stop tracking data volume", () => {
  it("today's drains scale LINEARLY with ocean size, which is the problem", async () => {
    // Not an aspiration — a measurement of the current design, recorded so
    // the W3 "after" number has a real "before" to be compared against.
    const small = mockPaginatedBackend(1_000);
    await api.reefs.listAll("tok", "g");
    const smallCalls = small.calls;
    vi.unstubAllGlobals();

    const large = mockPaginatedBackend(10_000);
    await api.reefs.listAll("tok", "g");

    // 10x the reefs, 10x the requests — still true for `listAll`, because
    // retiring the whole-set drains is W5, not W3. W3 raised the page size
    // (35 → 4 at today's scale) and removed the per-row fan-out; it did NOT
    // make this loader size-independent, and pretending otherwise is exactly
    // the overclaim the plan's original exit gate made.
    expect(large.calls / smallCalls).toBeCloseTo(10, 0);
  });

  it("records what the target ocean would cost today", async () => {
    const backend = mockPaginatedBackend(TARGET.reefs);
    await api.reefs.listAll("tok", "g");

    // 100,000 reefs ÷ 1,000 = 100 requests for the reef list ALONE (was
    // 1,000 before W3 raised the ceiling), before any atoms or edges. Better
    // by 10×, and still far past the ≤25 target — which is precisely why the
    // corrected plan puts "≤25 at 250k atoms" in Phase 3 behind W5, not here.
    expect(backend.calls).toBe(100);
  });
});

describe("/topics page load budget (fix/topics-derive-ocean)", () => {
  // THE SHAPE THIS REPLACED: the page resolved its ocean from ambient
  // session state, then had to search the WHOLE ocean's reef roster
  // (`api.reefs.listAll`) for the one node id it actually wanted — 4
  // requests at TODAY.reefs before a single pixel could render, PLUS this
  // reef's own atoms (1) = 5, and that 4 tracks ocean size exactly like
  // `reefs.listAll at today's ocean size` above measures.
  //
  // THE SHAPE NOW: the reef resolves directly by id (`api.reefs.get`, GET
  // /v1/nodes/{id} — 1 request, flat regardless of ocean size), its own
  // atoms fetch in parallel (1, unchanged), and the page asks for only the
  // up-to-5 same-type reefs its always-visible "related topics" rail needs
  // (`api.reefs.list`, bounded page_size — 1 request). The full roster
  // (`listAll`) still exists for `TopicSupersession`/`TopicMerge`, but only
  // fires once `TopicCurate` is opened — deliberately NOT part of this
  // load-path budget, the same way this file doesn't fold `reefs.atoms`
  // fan-out into the loaders above it measures instead.
  //
  // THIS CALLS THE PAGE'S OWN CODE, NOT A REPRODUCTION OF IT (CodeRabbit
  // finding on PR #859). `loadTopicReef`/`loadRelatedReefs` are exactly what
  // `app/(app)/topics/page.tsx`'s `load()` and its related-reefs effect call
  // — see `lib/topicLoad.ts`'s own header. Calling `api.reefs.get`/`.atoms`/
  // `.list` here directly, by hand, would only prove that hand-written
  // reproduction makes 3 requests; it would stay green even if the page
  // regressed back to draining the whole ocean via `listAll`, since nothing
  // it calls would be the page's own code. Verified below (search this
  // file's git history / the PR for the negative control): reintroducing a
  // `listAll` drain inside `loadTopicReef` fails this test, because this
  // test calls that exact function.
  it("resolves a topic + its related reefs in 3 flat requests, not an ocean-wide drain", async () => {
    const urls: string[] = [];
    const fetchMock = vi.fn(async (url: string) => {
      urls.push(url);
      const u = new URL(url, "http://x");
      if (/\/v1\/nodes\/[^/?]+\/atoms$/.test(u.pathname)) {
        return {
          ok: true,
          status: 200,
          headers: { get: () => null },
          json: async () => [],
          clone() {
            return this;
          },
        };
      }
      if (u.pathname === "/v1/nodes") {
        const pageSize = Number(u.searchParams.get("page_size") ?? "20");
        return {
          ok: true,
          status: 200,
          headers: { get: (k: string) => (k.toLowerCase() === "x-has-more" ? "false" : null) },
          json: async () => Array.from({ length: Math.min(pageSize, TODAY.reefs) }, (_, i) => rowFor(i)),
          clone() {
            return this;
          },
        };
      }
      // GET /v1/nodes/{id} — the direct-by-id lookup that replaced searching
      // `listAll`'s output for a matching node_id.
      return {
        ok: true,
        status: 200,
        headers: { get: () => null },
        json: async () => ({ ...rowFor(0), graph_id: "graph-1", can_edit: true }),
        clone() {
          return this;
        },
      };
    });
    vi.stubGlobal("fetch", fetchMock);

    const { reef } = await loadTopicReef("tok", "node-1");
    await loadRelatedReefs("tok", reef.graph_id, reef.type);

    // loadTopicReef (2 requests: reef.get + reef.atoms, in parallel) +
    // loadRelatedReefs (1, bounded) — flat, not 4 + 1 = 5 at TODAY.reefs
    // (and not 100 + 1 at TARGET.reefs; this number would be identical at
    // either ocean size, which is the point).
    expect(urls.length).toBe(3);
    expect(urls.length).toBeLessThanOrEqual(4);

    const listUrl = urls.find((u) => new URL(u, "http://x").pathname === "/v1/nodes");
    expect(listUrl).toBeDefined();
    const pageSize = Number(new URL(listUrl as string, "http://x").searchParams.get("page_size"));
    // Bounded at 5 — never the ~3,440-reef `listAll` drain this replaced.
    expect(pageSize).toBeLessThanOrEqual(5);
  });
});
