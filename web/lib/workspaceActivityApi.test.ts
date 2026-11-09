import { afterEach, expect, it, vi } from "vitest";
import { ApiError, setTokenRefresher } from "./api";
import { __resetTransportForTests } from "./transport";
import {
  workspaceActivityPath,
  workspaceHeadlineText,
  type WorkspaceActivity,
  type WorkspaceActivityParams,
} from "./pageSectionTypes/workspaceActivity";
import { workspaceActivityQueryOptions } from "./appQueries";

afterEach(() => {
  setTokenRefresher(null);
  vi.unstubAllGlobals();
  __resetTransportForTests();
});

async function read(params: WorkspaceActivityParams = {}) {
  const options = workspaceActivityQueryOptions({ scope: "fixture", getAuthToken: () => "tok" }, params);
  const value = options.select(await options.queryFn({ signal: new AbortController().signal }));
  if (!value) throw new Error("Expected computed fixture");
  return value;
}

const body: WorkspaceActivity = {
  workspaces: [
    {
      graph_id: "g1",
      name: "Engineering",
      kind: "org",
      last_activity_at: "2026-10-29T10:00:00Z",
      headline: { kind: "reef_updated", reef: { node_id: "n1", title: "Auth rewrite" }, coral_count: 14 },
      total_corals_window: 20,
    },
  ],
};

it("builds the path with only the params given and rejects out-of-range values", () => {
  expect(workspaceActivityPath()).toBe("/v1/home/workspace-activity");
  expect(workspaceActivityPath({ limit: 6, sinceDays: 7 })).toBe("/v1/home/workspace-activity?limit=6&since_days=7");
  expect(() => workspaceActivityPath({ limit: 0 })).toThrow();
  expect(() => workspaceActivityPath({ limit: 21 })).toThrow();
  expect(() => workspaceActivityPath({ sinceDays: 31 })).toThrow();
  expect(() => workspaceActivityPath({ sinceDays: 1.5 })).toThrow();
});

it("formats the one-line summary for each headline kind", () => {
  expect(workspaceHeadlineText(body.workspaces[0].headline)).toBe("Auth rewrite reef updated · 14 corals");
  expect(
    workspaceHeadlineText({ kind: "reef_added", reef: { node_id: "n", title: "EU launch plan" }, coral_count: 6 }),
  ).toBe("EU launch plan added · 6 corals");
  expect(workspaceHeadlineText({ kind: "corals_added", reef: null, coral_count: 1 })).toBe("1 new coral");
  expect(workspaceHeadlineText({ kind: "corals_added", reef: null, coral_count: 3 })).toBe("3 new corals");
});

it("fetches with the bearer and returns the typed body", async () => {
  const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({ model: "workspace_activity", version: 1, payload: body, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null })));
  vi.stubGlobal("fetch", fetch);
  const got = await read({ limit: 3, sinceDays: 14 });
  expect(got.workspaces[0].name).toBe("Engineering");
  const [url, init] = fetch.mock.calls[0];
  expect(String(url)).toContain("/v1/home/workspace-activity?limit=3&since_days=14");
  expect(new Headers(init.headers).get("Authorization")).toBe("Bearer tok");
});

it("retains a 404 refusal so the card can hide", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("{}", { status: 404 })));
  await expect(read()).rejects.toBeInstanceOf(ApiError);
});

it("keys the query by scope and params, never the bearer", () => {
  const ctx = { getAuthToken: () => "secret", scope: "u1:o1" };
  const a = workspaceActivityQueryOptions(ctx, { limit: 6, sinceDays: 7 });
  const b = workspaceActivityQueryOptions(ctx, { limit: 3 });
  expect(a.queryKey).toEqual(["home-workspace-activity", "u1:o1", expect.objectContaining({ limit: 6, sinceDays: 7 })]);
  expect(b.queryKey).not.toEqual(a.queryKey);
  expect(JSON.stringify(a.queryKey)).not.toContain("secret");
});
