// @vitest-environment jsdom
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { createElement } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { BootstrapResponse, TrialBootstrapResponse, Graph, MeResponse } from "./api";

/* Boot issues ONE request, GET /v1/bootstrap, and seeds `me` and the
   join-request count into the cache under the booted identity's scope. A
   backend without the route (404, a deploy race) gets the previous boot:
   GET /v1/me and GET /v1/graphs together. */

// Clerk's functions and user are stable across renders, as in the real SDK.
const clerk = vi.hoisted(() => ({
  auth: { isLoaded: true, isSignedIn: true, getToken: async () => "bearer", signOut: async () => {}, orgId: "org_a" as string | null },
  user: { user: { id: "user_a" } },
}));
vi.mock("@clerk/nextjs", () => ({ useAuth: () => clerk.auth, useUser: () => clerk.user }));
vi.mock("./clientMetrics", () => ({ startClientMetrics: () => () => {} }));
vi.mock("./devBearer", () => ({ readDevBearer: () => null }));

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
      bootstrap: vi.fn(),
      me: vi.fn(),
      graphs: { ...actual.api.graphs, list: vi.fn() },
      orgs: { ...actual.api.orgs, joinRequests: { ...actual.api.orgs.joinRequests, count: vi.fn() } },
      reportTimezone: vi.fn(async () => undefined),
      reportActiveOcean: vi.fn(async () => undefined),
    },
  };
});

import { api, ApiError } from "./api";
import { AppProvider, useApp } from "./providers";
import { useQueryClient } from "@tanstack/react-query";
import {
  meQueryOptions,
  trialBannerQueryOptions,
  billingSummaryQueryOptions,
  orgJoinRequestCountQueryOptions,
  orgSelfserveActiveQueryOptions,
  orgSelfserveEnabledQueryOptions,
} from "./appQueries";

const meBody: MeResponse = {
  user_id: "u", account_id: "acct-a", role: "admin", device_id: "d",
  org_teams_enabled: true, active_graph_id: "g1", is_personal_org: false,
};
const graphList = [{ id: "g1", name: "Primary", kind: "org" } as Graph];

type Seen = {
  ready: boolean;
  bootError: string | null;
  scope: string;
  graphs: Graph[];
  me: unknown;
  count: unknown;
  selfserve: unknown;
  selfserveOrg: unknown;
  trialBanner: unknown;
  fullBilling: unknown;
  selfserveOrgGcTime: number | undefined;
};
function mount() {
  const seen: { current: Seen | null } = { current: null };
  function Probe() {
    const app = useApp();
    const qc = useQueryClient();
    const ctx = { getAuthToken: app.getAuthToken, scope: app.queryScope };
    seen.current = {
      ready: app.ready,
      bootError: app.bootError,
      scope: app.queryScope,
      graphs: app.graphs,
      me: qc.getQueryData(meQueryOptions(ctx).queryKey),
      trialBanner: qc.getQueryData(trialBannerQueryOptions(ctx).queryKey),
      fullBilling: qc.getQueryData(billingSummaryQueryOptions(ctx).queryKey),
      count: qc.getQueryData(orgJoinRequestCountQueryOptions(ctx).queryKey),
      selfserve: qc.getQueryData(orgSelfserveEnabledQueryOptions(ctx).queryKey),
      selfserveOrg: qc.getQueryData(orgSelfserveActiveQueryOptions(ctx).queryKey),
      selfserveOrgGcTime: qc.getQueryCache().find({ queryKey: orgSelfserveActiveQueryOptions(ctx).queryKey, exact: true })
        ?.gcTime,
    };
    return null;
  }
  render(createElement(AppProvider, null, createElement(Probe)));
  return seen;
}

beforeEach(() => {
  vi.mocked(api.bootstrap).mockReset();
  vi.mocked(api.me).mockReset();
  vi.mocked(api.graphs.list).mockReset();
  vi.mocked(api.orgs.joinRequests.count).mockReset();
});
afterEach(() => cleanup());

describe("boot", () => {
  it("makes one request and seeds me and the join-request count under the booted scope", async () => {
    const body: BootstrapResponse = { me: meBody, graphs: graphList, activities_enabled: true, gist_enabled: true, my_join_requests: [], billing_banner: null, join_requests_count: { open: 3 } };
    vi.mocked(api.bootstrap).mockResolvedValue(body);
    const seen = mount();
    await waitFor(() => expect(seen.current?.ready).toBe(true));

    expect(api.bootstrap).toHaveBeenCalledTimes(1);
    expect(api.me).not.toHaveBeenCalled();
    expect(api.graphs.list).not.toHaveBeenCalled();
    expect(api.orgs.joinRequests.count).not.toHaveBeenCalled();

    expect(seen.current?.scope).toBe("user_a::org_a");
    expect(seen.current?.graphs).toEqual(graphList);
    expect(seen.current?.me).toEqual(meBody);
    expect(seen.current?.count).toBe(3);
  });

  it("publishes the opted-in banner only after the successful identity boot", async () => {
    vi.mocked(api.bootstrap).mockResolvedValue({ me: meBody, graphs: graphList,
      activities_enabled: true, gist_enabled: false, my_join_requests: [], join_requests_count: null,
      billing_banner_mode: "trial_v1", trial_banner: { kind: "trial_banner_v1", trial: null },
    } satisfies TrialBootstrapResponse as unknown as BootstrapResponse);
    const seen = mount();
    await waitFor(() => expect(seen.current?.ready).toBe(true));
    expect(seen.current?.trialBanner).toEqual({ kind: "trial_banner_v1", trial: null });
    expect(seen.current?.fullBilling).toBeUndefined();
    expect(api.bootstrap).toHaveBeenCalledTimes(1);
  });

  it("seeds the self-serve sign-up flag from the same request (A2 #1533), false when absent", async () => {
    vi.mocked(api.bootstrap).mockResolvedValue({
      me: meBody,
      graphs: graphList,
      activities_enabled: true, gist_enabled: true, my_join_requests: [], billing_banner: null, join_requests_count: null,
      selfserve_signup_enabled: true,
    });
    const seen = mount();
    await waitFor(() => expect(seen.current?.ready).toBe(true));
    expect(api.bootstrap).toHaveBeenCalledTimes(1);
    expect(seen.current?.selfserve).toBe(true);
    cleanup();

    vi.mocked(api.bootstrap).mockResolvedValue({ me: meBody, graphs: graphList, activities_enabled: true, gist_enabled: true, my_join_requests: [], billing_banner: null, join_requests_count: null });
    const older = mount();
    await waitFor(() => expect(older.current?.ready).toBe(true));
    expect(older.current?.selfserve).toBe(false);
  });

  it("seeds whether the booted org is self-serve (#1540 review SF-a), null when the server cannot say", async () => {
    vi.mocked(api.bootstrap).mockResolvedValue({
      me: meBody,
      graphs: graphList,
      activities_enabled: true, gist_enabled: true, my_join_requests: [], billing_banner: null, join_requests_count: null,
      selfserve_org: true,
    });
    const seen = mount();
    await waitFor(() => expect(seen.current?.ready).toBe(true));
    expect(api.bootstrap).toHaveBeenCalledTimes(1);
    expect(seen.current?.selfserveOrg).toBe(true);
    // Kept for the tab's life: a People visit later does not re-read the
    // whole bootstrap for this one boolean (review SF-b).
    expect(seen.current?.selfserveOrgGcTime).toBe(Infinity);
    cleanup();

    for (const body of [
      { me: meBody, graphs: graphList, activities_enabled: true, gist_enabled: true, my_join_requests: [], billing_banner: null, join_requests_count: null, selfserve_org: false },
      { me: meBody, graphs: graphList, activities_enabled: true, gist_enabled: true, my_join_requests: [], billing_banner: null, join_requests_count: null, selfserve_org: null },
      { me: meBody, graphs: graphList, activities_enabled: true, gist_enabled: true, my_join_requests: [], billing_banner: null, join_requests_count: null },
    ]) {
      vi.mocked(api.bootstrap).mockResolvedValue(body);
      const next = mount();
      await waitFor(() => expect(next.current?.ready).toBe(true));
      expect(next.current?.selfserveOrg).toBe(body.selfserve_org === false ? false : null);
      cleanup();
    }
  });

  it("leaves the count entry empty when the server has none for this caller", async () => {
    vi.mocked(api.bootstrap).mockResolvedValue({ me: meBody, graphs: graphList, activities_enabled: true, gist_enabled: true, my_join_requests: [], billing_banner: null, join_requests_count: null });
    const seen = mount();
    await waitFor(() => expect(seen.current?.ready).toBe(true));
    expect(seen.current?.me).toEqual(meBody);
    expect(seen.current?.count).toBeUndefined();
  });

  it("falls back to GET /v1/me and GET /v1/graphs together when /v1/bootstrap is 404", async () => {
    vi.mocked(api.bootstrap).mockRejectedValue(new ApiError("HTTP 404", 404));
    // Held open so the test can see both fallback reads in flight at once.
    let resolveMe!: (v: MeResponse) => void;
    vi.mocked(api.me).mockImplementation(() => new Promise((r) => { resolveMe = r; }));
    vi.mocked(api.graphs.list).mockResolvedValue(graphList);
    const seen = mount();
    await waitFor(() => expect(api.me).toHaveBeenCalledTimes(1));
    expect(api.graphs.list).toHaveBeenCalledTimes(1);

    await act(async () => { resolveMe(meBody); });
    await waitFor(() => expect(seen.current?.ready).toBe(true));
    expect(api.bootstrap).toHaveBeenCalledTimes(1);
    expect(seen.current?.graphs).toEqual(graphList);
    expect(seen.current?.me).toEqual(meBody);
    // Boot did not read the count on this path; the badge fetches its own.
    expect(seen.current?.count).toBeUndefined();
  });

  it("does not fall back on any other failure: the boot fails as a failed /v1/me did", async () => {
    vi.mocked(api.bootstrap).mockRejectedValue(new ApiError("internal error", 500));
    const seen = mount();
    await waitFor(() => expect(seen.current?.bootError).not.toBeNull());
    expect(seen.current?.ready).toBe(false);
    expect(api.me).not.toHaveBeenCalled();
    expect(api.graphs.list).not.toHaveBeenCalled();
  });
});
