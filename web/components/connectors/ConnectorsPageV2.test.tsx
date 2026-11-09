// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import type { ConnectorSummary } from "@/lib/connectorsApi";
import { CONNECTOR_KEYS, CONNECTOR_META } from "@/components/settings/providerMeta";

// A plain function behind the mock, not `vi.fn().mockRejectedValue`: the spy's
// own bookkeeping of a rejected promise surfaces as an unhandled rejection.
const mocks = vi.hoisted(() => ({ impl: (async () => ({ connectors: [] })) as () => Promise<unknown>, detail: (async () => { throw new Error("no detail"); }) as () => Promise<unknown> }));
vi.mock("@/lib/providers", () => ({
  useApp: () => ({ token: "tok", getAuthToken: () => "tok", authReady: true, accountId: "a", queryScope: "u::a" }),
}));
vi.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(""),
  useRouter: () => ({ push: vi.fn(), replace: vi.fn() }),
}));
vi.mock("next/link", () => ({
  default: ({ children, href, ...props }: React.AnchorHTMLAttributes<HTMLAnchorElement>) => (
    <a href={href} {...props}>
      {children}
    </a>
  ),
}));
vi.mock("@/lib/api", async (actual) => ({
  ...(await actual<typeof import("@/lib/api")>()),
  apiRequest: (path: string) => {
    if (path === "/v1/connectors") return mocks.impl();
    if (path === "/v1/connectors/slack") return mocks.detail();
    throw new Error(`Unexpected page read ${path}`);
  },
}));

import { ConnectorsPageV2 } from "./ConnectorsPageV2";
import { makeQueryClient } from "@/lib/queryClient";
import { ApiError } from "@/lib/api";

const TOOLKITS = ["linear", "jira", "confluence", "hubspot", "zendesk", "dart"];

function tenFresh(): ConnectorSummary[] {
  return CONNECTOR_KEYS.map((key) => ({
    key,
    name: CONNECTOR_META[key].label,
    beta: TOOLKITS.includes(key),
    status: "not_connected",
    sources_on: 0,
    sources_total: 1,
    last_captured_at: null,
    importing: null,
    problem: null,
    resume_step: null,
    sources: [],
  }));
}

function renderPage(qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })) {
  return render(
    <QueryClientProvider client={qc}>
      <ConnectorsPageV2 />
    </QueryClientProvider>,
  );
}

// jsdom has no canvas; SwarmStream draws nothing without a 2d context.
HTMLCanvasElement.prototype.getContext = (() => null) as typeof HTMLCanvasElement.prototype.getContext;

afterEach(() => { cleanup(); vi.useRealTimers(); });

it("retains the usable grid past recovery expiry with finite ETA polls, then clears it on refusal", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-10-31T00:00:00Z'));
  let reads = 0;
  mocks.impl = async () => { reads++; return { model: 'connectors_list', version: 1,
    computed_at: '2026-10-30T23:59:00Z', stale: true, refresh_eta: '2026-10-31T00:01:00Z', payload: { connectors: tenFresh() } }; };
  const qc = makeQueryClient();
  const { container } = renderPage(qc);
  await act(async () => { await vi.advanceTimersByTimeAsync(1); });
  expect(container.querySelectorAll('[data-connector]')).toHaveLength(10);
  await act(async () => { await vi.advanceTimersByTimeAsync(59_000); }); expect(reads).toBe(1);
  await act(async () => { await vi.advanceTimersByTimeAsync(61_000); }); expect(reads).toBe(4);
  expect(container.querySelectorAll('[data-connector]')).toHaveLength(10);
  expect(screen.queryByText('Failed to load connectors')).not.toBeInTheDocument();
  expect(screen.queryByText('Refreshing…')).not.toBeInTheDocument();
  await act(async () => { await vi.advanceTimersByTimeAsync(120_000); }); expect(reads).toBe(4);
  mocks.impl = async () => { throw new ApiError('Access revoked', 403); };
  await act(async () => { await qc.invalidateQueries(); await vi.advanceTimersByTimeAsync(1); });
  expect(container.querySelectorAll('[data-connector]')).toHaveLength(0);
  expect(screen.getByText('Access revoked')).toBeInTheDocument();
  qc.clear();
});

it("shows an availability error alongside a retained authorized grid", async () => {
  mocks.impl = async () => ({ model: 'connectors_list', version: 1, computed_at: '2026-10-31T00:00:00Z', stale: false, refresh_eta: null, payload: { connectors: tenFresh() } });
  const qc = makeQueryClient();
  const { container } = renderPage(qc);
  await waitFor(() => expect(container.querySelectorAll('[data-connector]')).toHaveLength(10));
  mocks.impl = async () => { throw new ApiError('Temporarily unavailable', 503); };
  await act(async () => { await qc.invalidateQueries(); });
  await waitFor(() => expect(screen.getByText('Temporarily unavailable')).toBeInTheDocument());
  expect(container.querySelectorAll('[data-connector]')).toHaveLength(10);
  qc.clear();
});

it("shows the head and the MCP band above the grid, then one card per connector", async () => {
  mocks.impl = async () => ({ model: "connectors_list", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { connectors: tenFresh() } });
  const { container } = renderPage();
  expect(screen.getByRole("heading", { name: "Connectors" })).toBeInTheDocument();
  expect(screen.getByText("loading connectors…")).toBeInTheDocument();
  await waitFor(() => expect(container.querySelectorAll("[data-connector]")).toHaveLength(10));
  const names = [...container.querySelectorAll("[data-connector]")].map((el) => el.getAttribute("data-connector"));
  expect(names).toEqual([...CONNECTOR_KEYS]);
  expect(screen.getAllByText("Beta")).toHaveLength(6);
  const band = screen.getByRole("heading", { name: "Use CoralSwarm anywhere" });
  const firstCard = container.querySelector("[data-connector]");
  expect(firstCard).not.toBeNull();
  expect(band.compareDocumentPosition(firstCard as Element) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
});

it("drops what the old page carried", async () => {
  mocks.impl = async () => ({ model: "connectors_list", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { connectors: tenFresh() } });
  const { container } = renderPage();
  await waitFor(() => expect(container.querySelectorAll("[data-connector]")).toHaveLength(10));
  const text = container.textContent ?? "";
  expect(text.toLowerCase()).not.toContain("composio");
  expect(text).not.toMatch(/other ways in/i);
  expect(text).not.toMatch(/coming soon/i);
  expect(within(container).queryByText("Zoom")).toBeNull();
  expect(within(container).queryByText(/desktop/i)).toBeNull();
});

it("says so when the list fails", async () => {
  mocks.impl = async () => {
    throw new Error("boom");
  };
  renderPage();
  expect(await screen.findByText("Failed to load connectors")).toBeInTheDocument();
});

it("reuses the catalog in onboarding and recommends Slack without duplicating MCP", async () => {
  mocks.impl = async () => ({ model: "connectors_list", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: { connectors: tenFresh() } });
  const { ConnectorGrid } = await import("./ConnectorsPageV2");
  const { container } = render(<QueryClientProvider client={new QueryClient()}><ConnectorGrid onboarding /></QueryClientProvider>);
  await waitFor(() => expect(container.querySelectorAll("[data-connector]")).toHaveLength(10));
  expect(screen.getByText("Start here")).toBeVisible();
  expect(screen.queryByText("Use CoralSwarm anywhere")).not.toBeInTheDocument();
});

it("closes the setup wizard when the member presses Close", async () => {
  const stored = (model: string, payload: unknown) => ({ model, version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload });
  const [slack] = tenFresh();
  mocks.impl = async () => stored("connectors_list", { connectors: tenFresh() });
  mocks.detail = async () => stored("connector_detail", { ...slack, account_label: null, connected_at: null, topics: [],
    captures: { items: [], next_cursor: null }, watched: { threads: [], next_cursor: null }, addin_tokens: { tokens: [] } });
  renderPage();
  fireEvent.click(await screen.findByRole("button", { name: "Connect Slack" }));
  const dialog = await screen.findByRole("dialog");
  const closes = within(dialog).getAllByRole("button", { name: "Close" });
  fireEvent.click(closes[closes.length - 1]);
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
});

it("keeps the setup wizard open while a save withholds the connector read", async () => {
  const stored = (model: string, payload: unknown) => ({ model, version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload });
  const [slack] = tenFresh();
  const detail = { ...slack, account_label: null, connected_at: null, topics: [],
    captures: { items: [], next_cursor: null }, watched: { threads: [], next_cursor: null }, addin_tokens: { tokens: [] } };
  mocks.impl = async () => stored("connectors_list", { connectors: tenFresh() });
  mocks.detail = async () => stored("connector_detail", detail);
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  renderPage(qc);
  fireEvent.click(await screen.findByRole("button", { name: "Connect Slack" }));
  await screen.findByRole("dialog");
  mocks.detail = async () => ({ model: "connector_detail", version: 1, computed_at: null, stale: true, refresh_eta: "2026-10-31T00:00:05Z", payload: detail });
  await qc.invalidateQueries({ queryKey: ["connector"] });
  await waitFor(() => expect(qc.getQueryCache().findAll({ queryKey: ["connector"] }).some((q) => (q.state.data as { computed_at: string | null } | undefined)?.computed_at === null)).toBe(true));
  expect(screen.getByRole("dialog")).toBeInTheDocument();
  expect(screen.queryByText("loading connector setup…")).not.toBeInTheDocument();
});
