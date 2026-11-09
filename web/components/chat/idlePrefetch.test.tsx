// @vitest-environment jsdom
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { QueryClientProvider, useQuery, type QueryClient } from "@tanstack/react-query";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { conversationKey, conversationQueryOptions } from "@/lib/appQueries";
import { makeQueryClient } from "@/lib/queryClient";
import type { ConversationSummary } from "@/lib/api";
import { PageLoadBoundary } from "@/components/PageLoadBoundary";
import { __resetTransportForTests, __transportStatsForTests } from "@/lib/transport";

const graph = "11111111-1111-4111-8111-111111111111";
const id = (n: number) => `66666666-6666-4666-8666-00000000000${n}`;
const live = vi.hoisted(() => ({ scope: "scope-A", query: "", path: "/home" }));
const getToken = () => "token";
vi.mock("@/lib/useEvidenceAdmission", () => ({
  useEvidenceAdmission: () => ({ key: live.scope, ready: true, getToken, graphs: [], currentGraphId: null, queryScope: live.scope }),
}));
vi.mock("@/lib/providers", () => ({ useApp: () => ({ ready: true, bootError: null, getAuthToken: getToken, authReady: true, queryScope: live.scope }) }));
vi.mock("next/navigation", () => ({ useSearchParams: () => new URLSearchParams(live.query), usePathname: () => live.path }));
import ChatHistory from "@/components/ChatHistory";

const summary = (n: number): ConversationSummary => ({ id: id(n), graph_id: graph, title: `Chat ${n}`, pinned: false, shared: false, mine: true, created_at: "2026-10-01", updated_at: `2026-10-${30 - n}` });
const six = [6, 3, 1, 5, 2, 4].map(summary);
const detail = (cid: string) => ({ model: "conversation_detail", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
  payload: { conversation: { id: cid, graph_id: graph, title: "Fixture chat", pinned: false, shared: false, mine: true, created_at: "2026-10-31T00:00:00Z", updated_at: "2026-10-31T00:00:00Z", library_scope: null, scope_status: "unscoped" },
    messages: [], messages_truncated: false, older_before_seq: null, resolved_scope: null } });
function deferred() { let resolve!: () => void; const promise = new Promise<void>((r) => { resolve = r; }); return { promise, resolve }; }
const starts: { path: string; signal: AbortSignal; priority: string | null; fetchPriority: RequestInit["priority"] }[] = [];
let delay = new Map<string, ReturnType<typeof deferred>>();
const reads = () => starts.filter((r) => r.path.startsWith("/v1/load/conversations/")).map((r) => r.path.split("/").at(-1));
function CriticalReader() {
  useQuery(conversationQueryOptions({ getAuthToken: getToken, scope: live.scope }, id(5)));
  return null;
}
function mount(qc: QueryClient, critical = false) {
  const ui = () => <QueryClientProvider client={qc}><PageLoadBoundary>{critical && <CriticalReader />}<ChatHistory /></PageLoadBoundary></QueryClientProvider>;
  const view = render(ui());
  return { ...view, again: () => view.rerender(ui()) };
}
function setVisibility(state: "visible" | "hidden") { Object.defineProperty(document, "visibilityState", { value: state, configurable: true }); }
beforeEach(() => {
  live.scope = "scope-A"; live.query = ""; live.path = "/home"; starts.length = 0; delay = new Map(); setVisibility("visible");
  vi.stubGlobal("fetch", vi.fn(async (url: string, init: RequestInit) => {
    const path = new URL(url, "http://wire.test").pathname;
    const signal = init.signal!;
    starts.push({ path, signal, priority: new Headers(init.headers).get("x-coralswarm-priority"), fetchPriority: init.priority });
    if (path === "/v1/conversations") return new Response(JSON.stringify({ model: "conversation_list", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { conversations: six, shared: [] } }));
    if (!path.startsWith("/v1/load/conversations/")) throw new Error(`Unexpected read ${path}`);
    const cid = path.split("/").at(-1)!;
    const hold = delay.get(cid);
    if (hold) await new Promise<void>((resolve, reject) => {
      void hold.promise.then(resolve);
      signal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
    });
    return new Response(JSON.stringify(detail(cid)));
  }));
});
afterEach(() => { cleanup(); __resetTransportForTests(); vi.unstubAllGlobals(); Reflect.deleteProperty(navigator, "connection"); });

it("warms exactly the three newest Stored details after the critical list settles", async () => {
  mount(makeQueryClient());
  await waitFor(() => expect(reads()).toEqual([id(1), id(2), id(3)]));
  expect(starts[0]).toMatchObject({ path: "/v1/conversations", priority: "critical" });
  expect(starts.slice(1).every((r) => r.priority === "background" && r.fetchPriority === "low")).toBe(true);
});
it("starts all three in parallel without waiting for the first response", async () => {
  for (const n of [1, 2, 3]) delay.set(id(n), deferred());
  mount(makeQueryClient());
  await waitFor(() => expect(reads()).toEqual([id(1), id(2), id(3)]));
  expect(__transportStatsForTests().inFlight).toBe(3);
  await act(async () => delay.forEach((d) => d.resolve()));
  await waitFor(() => expect(__transportStatsForTests().inFlight).toBe(0));
});
it("skips the open chat and an already fresh Stored cache entry", async () => {
  live.query = `id=${id(1)}`;
  const qc = makeQueryClient(); qc.setQueryData(conversationKey(live.scope, id(2)), detail(id(2)));
  mount(qc);
  await waitFor(() => expect(reads()).toEqual([id(3)]));
});
it("does nothing while hidden, then resumes on visibility", async () => {
  setVisibility("hidden"); mount(makeQueryClient());
  await screen.findByRole("link", { name: /Chat 1/ }); expect(reads()).toEqual([]);
  setVisibility("visible"); act(() => document.dispatchEvent(new Event("visibilitychange")));
  await waitFor(() => expect(reads()).toEqual([id(1), id(2), id(3)]));
});
it("honors the browser's save-data preference", async () => {
  Object.defineProperty(navigator, "connection", { configurable: true, value: { saveData: true } });
  mount(makeQueryClient()); await screen.findByRole("link", { name: /Chat 1/ }); expect(reads()).toEqual([]);
});
it("does not repeat warmers for the same page and newest set even after invalidation", async () => {
  const qc = makeQueryClient(); const view = mount(qc);
  await waitFor(() => expect(reads()).toHaveLength(3));
  await waitFor(() => expect(__transportStatsForTests().inFlight).toBe(0));
  for (const n of [1, 2, 3]) void qc.invalidateQueries({ queryKey: conversationKey(live.scope, id(n)), refetchType: "none" });
  view.again(); act(() => document.dispatchEvent(new Event("visibilitychange")));
  expect(reads()).toHaveLength(3); expect(__transportStatsForTests().queued).toBe(0);
});
it("isolates cache entries across an account change", async () => {
  const qc = makeQueryClient(); const view = mount(qc);
  await waitFor(() => expect(reads()).toHaveLength(3));
  await waitFor(() => expect(__transportStatsForTests().inFlight).toBe(0));
  live.scope = "scope-B"; view.again();
  await waitFor(() => expect(reads()).toHaveLength(6));
  for (const n of [1, 2, 3]) expect(qc.getQueryData(conversationKey("scope-B", id(n)))).toEqual(detail(id(n)));
});
it("queues all three while the user's critical detail is in flight, then dispatches together", async () => {
  delay.set(id(5), deferred()); mount(makeQueryClient(), true);
  await waitFor(() => expect(__transportStatsForTests().queued).toBe(3));
  expect(reads()).toEqual([id(5)]);
  expect(document.querySelector('[data-page-loaded="true"]')).toBeNull();
  await act(async () => delay.get(id(5))!.resolve());
  await waitFor(() => expect(reads()).toEqual([id(5), id(1), id(2), id(3)]));
});
it("cancels outstanding warmers on page navigation without leaking slots", async () => {
  for (const n of [1, 2, 3]) delay.set(id(n), deferred());
  const view = mount(makeQueryClient());
  await waitFor(() => expect(reads()).toHaveLength(3));
  const old = starts.slice(1); live.path = "/settings"; setVisibility("hidden"); view.again();
  await waitFor(() => expect(old.every((r) => r.signal.aborted)).toBe(true));
  await waitFor(() => expect(__transportStatsForTests()).toMatchObject({ inFlight: 0, queued: 0 }));
});
