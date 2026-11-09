// @vitest-environment jsdom
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { createElement } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { TrialBootstrapResponse, Graph } from "./api";

/* An in-place org switch while org A's GET /v1/bootstrap is still in flight.
   A's answer arrives AFTER B's boot has started (and finished). Nothing from
   A may reach the cache or the app state: not under B's scope, and not under
   A's either, since that boot was superseded. */

const deferred = <T,>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
};

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

const bootFor = (org: string, open: number): TrialBootstrapResponse => ({
  me: {
    user_id: "u", account_id: `acct-${org}`, role: "admin", device_id: "d",
    org_teams_enabled: true, active_graph_id: null, is_personal_org: false,
  },
  graphs: [{ id: `g-${org}`, name: "Primary", kind: "org" } as Graph],
  activities_enabled: true, gist_enabled: true, my_join_requests: [], billing_banner_mode: "trial_v1", trial_banner: { kind: "trial_banner_v1", trial: null }, join_requests_count: { open },
});
const bootA = deferred<TrialBootstrapResponse>();
vi.mock("./api", async (original) => {
  const actual = await original<typeof import("./api")>();
  return {
    ...actual,
    apiRequest: (path: string, token: string) => {
      if (path === "/v1/bootstrap?billing_banner=trial_v1") return api.bootstrap(token);
      if (path === "/v1/me") return api.me(token);
      if (path === "/v1/graphs") return api.graphs.list(token);
      throw new Error(`Unexpected page read ${path}`);
    },
    setTokenRefresher: () => {},
    api: {
      ...actual.api,
      // Org A's answer is held until the test releases it; org B answers at once.
      bootstrap: vi.fn((t: string) => (t === "token-a" ? bootA.promise : Promise.resolve(bootFor("org_b", 2)))),
      me: vi.fn(),
      graphs: { ...actual.api.graphs, list: vi.fn() },
      reportTimezone: vi.fn(async () => undefined),
      reportActiveOcean: vi.fn(async () => undefined),
    },
  };
});

import { api } from "./api";
import { AppProvider, useApp } from "./providers";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import { meQueryOptions, orgJoinRequestCountQueryOptions, trialBannerQueryOptions, billingSummaryQueryOptions } from "./appQueries";

afterEach(() => cleanup());

describe("org switch during boot", () => {
  it("never serves org A's bootstrap data after switching to org B", async () => {
    let qc!: QueryClient;
    let app!: ReturnType<typeof useApp>;
    function Probe() {
      app = useApp();
      qc = useQueryClient();
      return null;
    }
    const view = render(createElement(AppProvider, null, createElement(Probe)));
    await waitFor(() => expect(api.bootstrap).toHaveBeenCalledWith("token-a"));

    // Switch in place before A answers.
    clerk.nextToken = async () => "token-b";
    clerk.auth = { ...clerk.auth, orgId: "org_b" };
    view.rerender(createElement(AppProvider, null, createElement(Probe)));
    await waitFor(() => expect(app.ready).toBe(true));
    expect(app.queryScope).toBe("user_a::org_b");

    // A's answer lands late.
    await act(async () => { bootA.resolve(bootFor("org_a", 7)); });
    await act(async () => {});

    const ctxB = { getAuthToken: app.getAuthToken, scope: "user_a::org_b" };
    const ctxA = { getAuthToken: app.getAuthToken, scope: "user_a::org_a" };
    expect(qc.getQueryData(meQueryOptions(ctxB).queryKey)).toEqual(bootFor("org_b", 2).me);
    expect(qc.getQueryData(orgJoinRequestCountQueryOptions(ctxB).queryKey)).toBe(2);
    expect(qc.getQueryData(meQueryOptions(ctxA).queryKey)).toBeUndefined();
    expect(qc.getQueryData(orgJoinRequestCountQueryOptions(ctxA).queryKey)).toBeUndefined();
    expect(qc.getQueryData(trialBannerQueryOptions(ctxB).queryKey)).toEqual({ kind: "trial_banner_v1", trial: null });
    expect(qc.getQueryData(trialBannerQueryOptions(ctxA).queryKey)).toBeUndefined();
    expect(qc.getQueryData(billingSummaryQueryOptions(ctxB).queryKey)).toBeUndefined();
    expect(app.accountId).toBe("acct-org_b");
    expect(app.graphs.map((g) => g.id)).toEqual(["g-org_b"]);

    // And no cache entry anywhere holds A's data.
    const holdsA = qc.getQueryCache().getAll().filter((q) => JSON.stringify(q.state.data ?? null).includes("org_a") || q.state.data === 7);
    expect(holdsA).toEqual([]);
    expect(api.me).not.toHaveBeenCalled();
  });
});
