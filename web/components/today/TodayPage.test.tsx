// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { QueryClientProvider, type QueryClient } from "@tanstack/react-query";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { createElement, type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { makeQueryClient } from "@/lib/queryClient";
import type { Stored } from "@/lib/pageSectionWire";
import type { JournalHour, JournalResponse } from "@/lib/api";

/* The data-layer contract, on the screen that made it visible (Today):
   returning paints the cached journal with no loading state, a bearer
   rotation is not a reason to refetch, and an identity change never shows the
   previous identity's journal. The page lives under app/, which vitest does
   not discover, so it is exercised from here.

   This is the feature-off path: bootstrap declares activities disabled, so the page renders the hour-row journal
   inside the new shell. The activities path is covered in
   TodayActivities.test.tsx. */

const wire = vi.hoisted(() => ({ read: vi.fn() }));
const app = vi.hoisted(() => ({
  token: "token-a",
  search: "d=2026-10-20",
  queryScope: "user-a::personal",
}));
vi.mock("@/lib/providers", () => ({
  useApp: () => ({
    authReady: true, activitiesEnabled: false, gistEnabled: false,
    getAuthToken: () => app.token,
    queryScope: app.queryScope,
    userId: "user_clerk_a",
    internalUserId: "u-me",
    currentGraph: null,
    graphs: [],
  }),
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn() }),
  useSearchParams: () => new URLSearchParams(app.search),
}));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  apiRequest: (path: string, token: string) => { const url = new URL(path, "http://wire.test"); if (url.pathname !== "/v1/journal") throw new Error(`Disabled model read ${path}`); return wire.read(path, token); },
}));
// The journal's own rendering is not under test: show which entries it got.
vi.mock("@/components/today/DayJournal", () => ({
  DayJournal: ({ hours }: { hours: JournalHour[] }) =>
    createElement("p", null, hours.map((h) => h.hour_start).join(",") || "empty"),
}));
vi.mock("@/components/today/JournalRail", () => ({ JournalRail: () => null }));

import TodayPage from "@/app/(app)/today/page";

const list = wire.read;
const journal = (hourStart: string): Stored<JournalResponse> => ({ model: "journal_rollups", version: 1,
  computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
  payload: { hours: [{ hour_start: hourStart, entries: [], aggregate: { atom_count: 1, asks: 0, blocks: [] } }] } as unknown as JournalResponse });
const LOADING = /reading the journal/;

/** One QueryClient for the whole test, as in the app: it outlives the page. */
function mount(client: QueryClient) {
  const wrapper = ({ children }: { children: ReactNode }) => createElement(QueryClientProvider, { client }, children);
  return render(createElement(TodayPage), { wrapper });
}

let client: QueryClient;
beforeEach(() => {
  app.token = "token-a";
  app.search = "d=2026-10-20";
  app.queryScope = "user-a::personal";
  client = makeQueryClient();
  list.mockResolvedValue(journal("A-hour"));
});
afterEach(() => {
  cleanup();
  client.clear();
  vi.resetAllMocks();
});

describe("Today fallback shell", () => {
  it("renders the hour-row journal under the new shell, with no Mine / Team toggle", async () => {
    mount(client);
    expect(await screen.findByText("A-hour")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Tuesday 20 October" })).toBeInTheDocument();
    expect(screen.getByRole("group", { name: "Last 14 days" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Week" })).toBeInTheDocument();
    expect(screen.queryByRole("tab", { name: "Team" })).not.toBeInTheDocument();
    // The old breadcrumb and prev / next cluster are gone.
    expect(screen.queryByRole("navigation")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Previous|Next/ })).not.toBeInTheDocument();
  });
});

describe("Today on the shared query cache", () => {
  it("renders the cached journal with no loading state when the user returns", async () => {
    const first = mount(client);
    expect(await screen.findByText("A-hour")).toBeInTheDocument();
    first.unmount();

    mount(client);
    // Painted in the very first render: no spinner, no request.
    expect(screen.queryByText(LOADING)).not.toBeInTheDocument();
    expect(screen.getByText("A-hour")).toBeInTheDocument();
    expect(list).toHaveBeenCalledTimes(1);
  });

  it("does not refetch or show a loading state when the bearer rotates", async () => {
    const view = mount(client);
    expect(await screen.findByText("A-hour")).toBeInTheDocument();

    app.token = "token-a-rotated";
    view.rerender(createElement(TodayPage));
    await act(async () => {});

    expect(screen.queryByText(LOADING)).not.toBeInTheDocument();
    expect(screen.getByText("A-hour")).toBeInTheDocument();
    expect(list).toHaveBeenCalledTimes(1);
  });

  it("never shows the previous identity's journal after an account switch", async () => {
    const view = mount(client);
    expect(await screen.findByText("A-hour")).toBeInTheDocument();

    let resolveB!: (value: Stored<JournalResponse>) => void;
    list.mockReturnValueOnce(new Promise((resolve) => { resolveB = resolve; }));
    app.token = "token-b";
    app.queryScope = "user-a::org_b";
    view.rerender(createElement(TodayPage));

    // The incoming identity has nothing cached: a loading state, never A's rows.
    expect(screen.queryByText("A-hour")).not.toBeInTheDocument();
    expect(screen.getByText(LOADING)).toBeInTheDocument();
    // The bootstrap flag selects rollups directly, without a disabled-model probe.
    await waitFor(() => expect(list).toHaveBeenLastCalledWith(expect.stringContaining("granularity=hour"), "token-b"));

    await act(async () => resolveB(journal("B-hour")));
    expect(await screen.findByText("B-hour")).toBeInTheDocument();
    expect(screen.queryByText("A-hour")).not.toBeInTheDocument();
  });
});

it.each(["week", "month"] as const)("feature off %s dispatches both rollups in parallel and retains independent readiness", async (zoom) => {
  app.search = `d=2026-10-20&z=${zoom}`;
  const releases = new Map<string, (value: unknown) => void>();
  list.mockImplementation((path: string) => new Promise((resolve) => { const granularity = new URL(path, "http://wire.test").searchParams.get("granularity"); if (!granularity) throw new Error("Missing rollup granularity"); releases.set(granularity, resolve); }));
  mount(client);
  await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
  const child = zoom === "week" ? "day" : "week";
  expect([...releases.keys()].sort()).toEqual([zoom, child].sort());
  const computed = { model: "journal_rollups", version: 1, payload: { periods: [] }, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null };
  await act(async () => releases.get(zoom)?.(computed));
  expect(screen.getByText(LOADING)).toBeInTheDocument();
  const states = client.getQueryCache().findAll({ predicate: (q) => q.meta?.model === "journal_rollups" }).map((q) => q.state.status).sort();
  expect(states).toEqual(["pending", "success"]);
  expect(screen.getByRole("button", { name: "Refresh Today" })).toBeDisabled();
  expect(screen.getByText("Refreshing…")).toBeInTheDocument();
  await act(async () => releases.get(child)?.(computed));
  await waitFor(() => expect(screen.queryByText(LOADING)).not.toBeInTheDocument());
  expect(screen.getByRole("button", { name: "Refresh Today" })).toBeEnabled();
  expect(list).toHaveBeenCalledTimes(2);
});
