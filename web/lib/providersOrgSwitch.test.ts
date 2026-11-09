// @vitest-environment jsdom
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { createElement } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Graph, UsageSummary } from "./api";

/* An in-place org switch (Clerk `setActive`) changes `orgId` a render before
   the boot for the new org has minted its bearer. Every cached read must stay
   off the new org's key until it has the new org's token, or the old org's
   answer is cached as the new org's and served as fresh. */

const deferred = <T,>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
};

// The token Clerk hands out for the active org. After the switch it is held
// back (a pending promise) to model the boot's getToken landing late.
const clerk = vi.hoisted(() => {
  const state = {
    nextToken: (async () => "token-a") as () => Promise<string>,
    auth: {
      isLoaded: true,
      isSignedIn: true,
      getToken: async () => state.nextToken(),
      signOut: async () => {},
      orgId: "org_a" as string | null,
    },
    user: { user: { id: "user_a" } },
  };
  return state;
});
vi.mock("@clerk/nextjs", () => ({ useAuth: () => clerk.auth, useUser: () => clerk.user }));
vi.mock("./clientMetrics", () => ({ startClientMetrics: () => () => {} }));
vi.mock("./devBearer", () => ({ readDevBearer: () => null }));

const wire = vi.hoisted(() => ({ usage: vi.fn() }));
const orgOf = (token: string) => (token === "token-a" ? "org_a" : token === "token-b" ? "org_b" : "unknown");
vi.mock("./api", async (original) => {
  const actual = await original<typeof import("./api")>();
  return {
    ...actual,
    apiRequest: (path: string, token: string) => {
      if (path === "/v1/bootstrap?billing_banner=trial_v1") return api.bootstrap(token);
      if (path === "/v1/me") return api.me(token);
      if (path === "/v1/graphs") return api.graphs.list(token);
      if (path === "/v1/usage/summary") return wire.usage(token);
      throw new Error(`Unexpected page read ${path}`);
    },
    setTokenRefresher: () => {},
    api: {
      ...actual.api,
      me: vi.fn(async (t: string) => ({
        user_id: "u", account_id: `acct-${orgOf(t)}`, role: "owner", device_id: "d",
        org_teams_enabled: true, active_graph_id: null, is_personal_org: false,
      })),
      graphs: { ...actual.api.graphs, list: vi.fn(async (t: string) => [{ id: `g-${orgOf(t)}`, name: "Primary", kind: "org" } as Graph]) },
      // Boot's one read (lib/bootData.ts), answering for the bearer's org.
      bootstrap: vi.fn(async (t: string) => ({
        me: {
          user_id: "u", account_id: `acct-${orgOf(t)}`, role: "owner", device_id: "d",
          org_teams_enabled: true, active_graph_id: null, is_personal_org: false,
        },
        graphs: [{ id: `g-${orgOf(t)}`, name: "Primary", kind: "org" } as Graph],
        activities_enabled: true, gist_enabled: true, my_join_requests: [], billing_banner: null, join_requests_count: null,
      })),
      reportTimezone: vi.fn(async () => undefined),
      reportActiveOcean: vi.fn(async () => undefined),
    },
  };
});

import { api } from "./api";
import { AppProvider, useApp } from "./providers";
import { useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { usageSummaryQueryOptions } from "./appQueries";

function usageFor(org: string): UsageSummary {
  return { period_start: "2026-10-01T00:00:00Z", period_end: "2026-11-01T00:00:00Z", balance_credits: org === "org_a" ? 11 : 22,
    plan: null, credit_conversion_usd_per_credit: null, spend: { total_credits: 0, total_cost_usd: 0, by_axis: [] },
    by_workspace: [], top_askers: [], atoms_ingested: 0, has_ledger_history: false, ledger_coverage_start: null };
}

afterEach(() => cleanup());

describe("org switch", () => {
  it("never fetches under the new org's scope with the old org's token, nor caches old-org data there", async () => {
    // Every (scope, token) pair a read was issued with, and what it returned.
    const issued: { scope: string; token: string | null }[] = [];
    wire.usage.mockImplementation(async (t: string) => ({ model: "usage_summary", version: 1, payload: usageFor(orgOf(t)), computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null }));
    let qc!: QueryClient;
    let seen: { scope: string; data: unknown } = { scope: "", data: undefined };
    function Screen() {
      const app = useApp();
      qc = useQueryClient();
      const ctx = {
        getAuthToken: () => {
          const token = app.getAuthToken();
          issued.push({ scope: app.queryScope, token });
          return token;
        },
        scope: app.queryScope,
      };
      const q = useQuery({ ...usageSummaryQueryOptions(ctx), enabled: app.authReady });
      seen = { scope: app.queryScope, data: q.data };
      return null;
    }
    const view = render(createElement(AppProvider, null, createElement(Screen)));
    await waitFor(() => expect(seen).toEqual({ scope: "user_a::org_a", data: usageFor("org_a") }));

    // Switch in place: Clerk's orgId changes now; the new org's bearer lands later.
    const tokenB = deferred<string>();
    clerk.nextToken = () => tokenB.promise;
    clerk.auth = { ...clerk.auth, orgId: "org_b" };
    view.rerender(createElement(AppProvider, null, createElement(Screen)));
    await act(async () => {});
    // Something forces a read during the window (an invalidation, a remount).
    await act(async () => { await qc.invalidateQueries(); });

    await act(async () => { tokenB.resolve("token-b"); });
    await waitFor(() => expect(seen.scope).toBe("user_a::org_b"));
    await waitFor(() => expect(seen.data).toBeDefined());

    const leaked = issued.filter((r) => r.scope.endsWith("::org_b") && r.token === "token-a");
    expect(leaked).toEqual([]);
    // Every entry ever cached under the new org's scope holds the new org's data.
    for (const query of qc.getQueryCache().findAll({ queryKey: ["usageSummary", "user_a::org_b"] })) {
      expect(query.state.data).toEqual({ model: "usage_summary", version: 1, payload: usageFor("org_b"), computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null });
    }
    expect(seen).toEqual({ scope: "user_a::org_b", data: usageFor("org_b") });
  });
});
