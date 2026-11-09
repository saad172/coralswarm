// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { PageLoadBoundary } from "./PageLoadBoundary";
import { pageSectionQuery } from "@/lib/pageSections";
import { createPageReadRegistration, PageReadRegistrationContext, useCriticalPageReadLease } from "@/lib/pageReadRegistration";
import { makeQueryClient } from "@/lib/queryClient";
import { oceansIndexQueryOptions } from "@/lib/oceansIndex";
import { __resetTransportForTests, __setTransportConfigForTests, __transportStatsForTests } from "@/lib/transport";
const app = vi.hoisted(() => ({ ready: true, authReady: true, queryScope: "u:personal", bootError: null }));
vi.mock("@/lib/providers", () => ({ useApp: () => app }));
vi.mock("next/navigation", () => ({ usePathname: () => "/test-page", useSearchParams: () => new URLSearchParams() }));
const starts: { path: string; signal: AbortSignal; finish: (status?: number, changes?: Record<string, unknown>) => void }[] = [];
const labels = ["current", "one", "two", "three", "late", "restricted", "joined", "decoding", "shared-denied", "deferred"];
const idFor = (label: string) => {
  const index = labels.indexOf(label);
  if (index < 0) throw new Error(`Unknown test conversation ${label}`);
  return `11111111-1111-4111-8111-${String(index + 1).padStart(12, "0")}`;
};
const detailEnvelope = (id: string) => ({ model: "conversation_detail", version: 1,
  payload: { conversation: { id, title: "Fixture conversation", graph_id: "22222222-2222-4222-8222-222222222222", mine: true, pinned: false, shared: false,
    created_at: "2026-10-31T00:00:00Z", updated_at: "2026-10-31T00:00:00Z", library_scope: null, scope_status: "unscoped" },
    messages: [], messages_truncated: false, older_before_seq: null, resolved_scope: null },
  computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null });
const token = () => "token";
function Reader({ id, background = false }: { id: string; background?: boolean }) {
  useQuery(pageSectionQuery("conversation_detail", app.queryScope, { id: idFor(id) }, token, { priority: background ? "background" : "critical" }));
  return null;
}
function Tree({ late = false }: { late?: boolean }) {
  return <><Reader id="current" />{["one", "two", "three"].map((id) => <Reader key={id} id={id} background />)}{late && <Reader id="late" />}</>;
}
beforeEach(() => {
  starts.length = 0;
  vi.stubGlobal("fetch", vi.fn((url: string, init: RequestInit) => new Promise<Response>((resolve, reject) => {
    const path = new URL(url, "http://wire.test").pathname;
    if (!init.signal) throw new Error("Request is missing cancellation.");
    starts.push({ path, signal: init.signal, finish: (status = 200, changes = {}) => resolve(new Response(JSON.stringify({ ...detailEnvelope(path.split("/").at(-1) ?? ""), ...changes }), { status })) });
    init.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
  })));
});
afterEach(() => { cleanup(); vi.useRealTimers(); __resetTransportForTests(); vi.unstubAllGlobals(); });
it("genuine queued background section queries do not block readiness, then dispatch three in parallel", async () => {
  const qc = makeQueryClient();
  render(<QueryClientProvider client={qc}><PageLoadBoundary><Tree /></PageLoadBoundary></QueryClientProvider>);
  await waitFor(() => expect(starts).toHaveLength(1));
  expect(document.querySelector('[data-page-loaded="true"]')).toBeNull();
  await act(async () => starts[0].finish());
  await waitFor(() => expect(starts).toHaveLength(4));
  expect(document.querySelector('[data-page-loaded="true"]')).not.toBeNull();
  expect(starts.slice(1).every((r) => !r.signal.aborted)).toBe(true);
  await act(async () => starts.slice(1).forEach((row) => row.finish()));
  await waitFor(() => expect(__transportStatsForTests()).toMatchObject({ queued: 0, inFlight: 0 }));
});
it("a verified stale answer counts as loaded and releases queued background reads", async () => {
  const qc = makeQueryClient();
  render(<QueryClientProvider client={qc}><PageLoadBoundary><Tree /></PageLoadBoundary></QueryClientProvider>);
  await waitFor(() => expect(starts).toHaveLength(1));
  await act(async () => starts[0].finish(200, { stale: true, refresh_eta: new Date(Date.now() + 60_000).toISOString() }));
  await waitFor(() => expect(document.querySelector('[data-page-loaded="true"]')).not.toBeNull());
  await waitFor(() => expect(starts).toHaveLength(4));
  await act(async () => starts.slice(1).forEach((row) => row.finish()));
});
it("an empty certificate remains loading between polls and does not release background reads", async () => {
  const qc = makeQueryClient();
  render(<QueryClientProvider client={qc}><PageLoadBoundary><Tree /></PageLoadBoundary></QueryClientProvider>);
  await waitFor(() => expect(starts).toHaveLength(1));
  await act(async () => starts[0].finish(200, { computed_at: null, stale: true, refresh_eta: new Date(Date.now() + 60_000).toISOString() }));
  expect(document.querySelector('[data-page-loaded="true"]')).toBeNull();
  expect(starts).toHaveLength(1);
});
it.each([false, true])("Oceans fan-in counts stored stale neighbors as loaded but blocks for any withheld component (%s)", async (withheld) => {
  const qc = makeQueryClient();
  function Oceans() {
    useQuery(oceansIndexQueryOptions({ scope: app.queryScope, getAuthToken: token }));
    return <Reader id="one" background />;
  }
  render(<QueryClientProvider client={qc}><PageLoadBoundary><Oceans /></PageLoadBoundary></QueryClientProvider>);
  await waitFor(() => expect(starts).toHaveLength(1));
  const components = [false, withheld].map((pending, i) => ({ account_id: idFor(i ? 'two' : 'one'), anchor_graph_id: i ? idFor('two') : null,
    refresh_path: `/v1/load/oceans/component/refresh${i ? `?graph=${idFor('two')}` : ''}`,
    stored: { model: 'oceans_index', version: 1, computed_at: pending ? null : '2026-10-31T00:00:00Z', stale: true,
      refresh_eta: new Date(Date.now() + 60_000).toISOString(), payload: { oceans: [], members: [], team_members: {}, archived: null } } }));
  await act(async () => starts[0].finish(200, { components }));
  await waitFor(() => expect(document.querySelector('[data-page-loaded="true"]') !== null).toBe(!withheld));
  if (withheld) expect(starts).toHaveLength(1);
  else {
    await waitFor(() => expect(starts).toHaveLength(2));
    await act(async () => starts[1].finish());
  }
});
it("late critical work preempts background section queries and releases all slots", async () => {
  __setTransportConfigForTests({ maxInFlight: 3, maxPerCallerInFlight: 3 });
  const qc = makeQueryClient();
  const ui = (late: boolean) => <QueryClientProvider client={qc}><PageLoadBoundary><Tree late={late} /></PageLoadBoundary></QueryClientProvider>;
  const view = render(ui(false));
  await waitFor(() => expect(starts).toHaveLength(1)); await act(async () => starts[0].finish());
  await waitFor(() => expect(starts).toHaveLength(4));
  view.rerender(ui(true));
  await waitFor(() => expect(starts.some((r) => r.path.endsWith(`/${idFor("late")}`))).toBe(true));
  expect(starts.slice(1, 4).every((r) => r.signal.aborted)).toBe(true);
  const lateRead = starts.find((r) => r.path.endsWith(`/${idFor("late")}`));
  if (!lateRead) throw new Error("Late critical request never started.");
  await act(async () => lateRead.finish());
  await waitFor(() => expect(starts.length).toBe(8));
  await act(async () => starts.slice(5).forEach((r) => r.finish()));
  await waitFor(() => expect(__transportStatsForTests()).toMatchObject({ queued: 0, inFlight: 0 }));
});

it("a declared two-second deferred child cannot produce an early loaded marker or release background work", async () => {
  vi.useFakeTimers();
  const qc = makeQueryClient();
  function Deferred({ initialized }: { initialized: boolean }) {
    useCriticalPageReadLease("deferred account initialization", !initialized);
    return initialized ? <Reader id="deferred" /> : null;
  }
  const external = createPageReadRegistration();
  const ui = (initialized: boolean) => <QueryClientProvider client={qc}><PageReadRegistrationContext.Provider value={external}>
    <PageLoadBoundary><Deferred initialized={initialized} /></PageLoadBoundary>
  </PageReadRegistrationContext.Provider></QueryClientProvider>;
  const view = render(ui(false));
  await act(async () => { await vi.advanceTimersByTimeAsync(2_000); });
  expect(document.querySelector('[data-page-loaded="true"]')).toBeNull();
  expect(starts).toHaveLength(0);
  view.rerender(ui(true));
  await act(async () => { await vi.advanceTimersByTimeAsync(1); });
  expect(starts).toHaveLength(1);
  expect(document.querySelector('[data-page-loaded="true"]')).toBeNull();
  await act(async () => { starts[0].finish(); await vi.advanceTimersByTimeAsync(5); });
  expect(document.querySelector('[data-page-loaded="true"]')).not.toBeNull();
  view.unmount();
  expect(external.snapshot()).toBe(0);
  vi.useRealTimers();
});

it("terminal error settles as failed, then a verified retry can establish successful readiness", async () => {
  const qc = makeQueryClient();
  render(<QueryClientProvider client={qc}><PageLoadBoundary><Reader id="restricted" /></PageLoadBoundary></QueryClientProvider>);
  await waitFor(() => expect(starts).toHaveLength(1));
  await act(async () => starts[0].finish(403));
  await waitFor(() => expect(document.querySelector('[data-page-settled="true"][data-page-failed="true"]')).not.toBeNull());
  expect(document.querySelector('[data-page-loaded="true"]')).toBeNull();
  act(() => { void qc.invalidateQueries(); });
  await waitFor(() => expect(starts).toHaveLength(2));
  await act(async () => starts[1].finish());
  await waitFor(() => expect(document.querySelector('[data-page-loaded="true"][data-page-failed="false"]')).not.toBeNull());
});

it("promotes a TanStack-deduplicated queued prefetch when a critical observer opens it", async () => {
  const qc = makeQueryClient();
  const warm = qc.prefetchQuery(pageSectionQuery("conversation_detail", app.queryScope, { id: idFor("joined") }, token, { priority: "background" }));
  await act(async () => { await Promise.resolve(); });
  expect(starts).toHaveLength(0);
  render(<QueryClientProvider client={qc}><PageLoadBoundary><Reader id="joined" /></PageLoadBoundary></QueryClientProvider>);
  await waitFor(() => expect(starts).toHaveLength(1));
  await act(async () => starts[0].finish());
  await warm;
  await waitFor(() => expect(document.querySelector('[data-page-loaded="true"]')).not.toBeNull());
});

it("keeps a required critical registration pending through body decoding even if shared query metadata becomes background", async () => {
  vi.useFakeTimers();
  const qc = makeQueryClient();
  let complete!: () => void;
  const stream = new ReadableStream({ start(controller) { complete = () => {
    controller.enqueue(new TextEncoder().encode(JSON.stringify(detailEnvelope(idFor("decoding")))));
    controller.close();
  }; } });
  vi.stubGlobal("fetch", vi.fn(async () => new Response(stream)));
  const ui = (background: boolean) => <QueryClientProvider client={qc}><PageLoadBoundary><Reader id="decoding" />{background && <Reader id="decoding" background />}</PageLoadBoundary></QueryClientProvider>;
  const view = render(ui(false));
  await act(async () => { await vi.advanceTimersByTimeAsync(1); });
  view.rerender(ui(true));
  await act(async () => { await vi.advanceTimersByTimeAsync(1); });
  expect(qc.getQueryCache().findAll()[0].meta?.priority).toBe("background");
  await act(async () => { await vi.advanceTimersByTimeAsync(2_000); });
  expect(document.querySelector('[data-page-loaded="true"]')).toBeNull();
  await act(async () => { complete(); await vi.advanceTimersByTimeAsync(5); });
  expect(document.querySelector('[data-page-loaded="true"]')).not.toBeNull();
  vi.useRealTimers();
});

it("uses critical observer registrations for failure even when another observer last wrote background metadata", async () => {
  const qc = makeQueryClient();
  render(<QueryClientProvider client={qc}><PageLoadBoundary><Reader id="shared-denied" /><Reader id="shared-denied" background /></PageLoadBoundary></QueryClientProvider>);
  await waitFor(() => expect(starts).toHaveLength(1));
  expect(qc.getQueryCache().findAll()[0].meta?.priority).toBe("background");
  await act(async () => starts[0].finish(403));
  await waitFor(() => expect(document.querySelector('[data-page-settled="true"][data-page-failed="true"]')).not.toBeNull());
  expect(document.querySelector('[data-page-loaded="true"]')).toBeNull();
});
