// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, screen } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, expect, it, vi } from "vitest";
import AddPage from "@/app/(app)/add/page";
import { api, type ActivityEvent, type Graph } from "@/lib/api";

/* /add shows an "added recently" rail read from the cached activity feed.
   Releasing a note must refresh it, not leave it on the pre-ingest answer
   for the staleness window. */

const state = vi.hoisted(() => ({
  token: "token-a", authReady: true, queryScope: "user-a::personal",
  getAuthToken: () => "token-a", currentGraph: { id: "g1", name: "Primary", kind: "personal" } as Graph,
}));
vi.mock("@/lib/providers", () => ({ useApp: () => state }));
vi.mock("@/lib/engine/provider", () => ({ useEngine: () => ({ status: "idle", engine: null }) }));
vi.mock("@/lib/api", async (original) => {
  const actual = await original<typeof import("@/lib/api")>();
  return { ...actual, apiRequest: (path: string, token: string) => { const url = new URL(path, "http://wire.test"); if (url.pathname !== "/v1/activity") throw new Error(`Unexpected page read ${path}`); return api.activity.list(token, url.searchParams.get("graph_id") ?? undefined); }, api: { corals: { ingest: vi.fn() }, activity: { list: vi.fn() } } };
});

afterEach(() => { cleanup(); vi.resetAllMocks(); });

it("refreshes the added-recently rail after a release", async () => {
  let events: ActivityEvent[] = [];
  vi.mocked(api.activity.list).mockImplementation(async () => events);
  vi.mocked(api.corals.ingest).mockImplementation(async () => {
    events = [{ id: "e1", event_type: "atom_ingested", graph_id: "g1", payload: { source_type: "note" }, user_id: "u", created_at: new Date().toISOString() }];
    return { atom_ids: ["a1"], jobs_enqueued: 1, chunks_created: 1 };
  });
  render(<AddPage />);
  expect(await screen.findByText("Nothing ingested in the last 7 days.")).toBeInTheDocument();

  fireEvent.change(screen.getByPlaceholderText("Paste notes, a doc, or a decision…"), { target: { value: "We decided to ship Friday." } });
  await act(async () => { fireEvent.click(screen.getByRole("button", { name: /Release into Primary/ })); });

  expect(await screen.findByText("ingested")).toBeInTheDocument();
  expect(screen.queryByText("Nothing ingested in the last 7 days.")).not.toBeInTheDocument();
});
