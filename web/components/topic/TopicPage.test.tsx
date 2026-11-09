// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { render } from "@/lib/testing/queryRender";
import type { LibrarySearchResponse } from "@/lib/libraryMemoryApi";
import { searchLibrary } from "@/lib/libraryMemoryApi";
import { apiRequest } from "@/lib/api";
import { resolveP6NodeGraph } from "@/lib/p6EvidenceApi";
import { TopicPage } from "./TopicPage";

const push = vi.fn();
vi.mock("next/navigation", () => ({ useRouter: () => ({ push }) }));
vi.mock("next/link", () => ({ default: ({ children, href }: { children: React.ReactNode; href: string }) => <a href={href}>{children}</a> }));
vi.mock("@/lib/providers", () => ({ useApp: () => ({ authReady: true, queryScope: "acct", getAuthToken: () => "tok" }) }));
vi.mock("@/lib/api", async (actual) => ({ ...await actual<typeof import("@/lib/api")>(), apiRequest: vi.fn() }));
vi.mock("@/lib/useEvidenceAdmission", () => ({ useEvidenceAdmission: () => ({ key: "acct", queryScope: "acct", ready: true, getToken: () => "tok", currentGraphId: null }) }));
vi.mock("@/lib/p6EvidenceApi", () => ({ resolveP6NodeGraph: vi.fn() }));
vi.mock("@/lib/libraryMemoryApi", async (orig) => ({ ...(await orig<typeof import("@/lib/libraryMemoryApi")>()), searchLibrary: vi.fn() }));
// The sections below the header read their own endpoints; they are covered by
// their own tests, so the page test only checks they mount for this node.
vi.mock("./TopicDigest", () => ({ TopicDigest: ({ nodeId }: { nodeId: string }) => <div>digest for {nodeId}</div> }));
vi.mock("./TopicSessions", () => ({ TopicSessions: ({ nodeId }: { nodeId: string }) => <div>sessions for {nodeId}</div> }));
vi.mock("./TopicConnections", () => ({ TopicConnections: ({ nodeId }: { nodeId: string }) => <div>connections for {nodeId}</div> }));

const graph = "11111111-1111-4111-8111-111111111111";
const node = "22222222-2222-4222-8222-222222222222";
const counts = { source_support_count: 3, semantic_atom_count: 5, unmapped_legacy_chunk_count: 0 };

function response(over: Partial<LibrarySearchResponse["nodes"]["items"][number]> = {}): LibrarySearchResponse {
  const page = <T,>(items: T[]) => ({ items, total: items.length, next_cursor: null });
  return {
    scope: { graph_ids: [graph], node_id: node },
    sources: page([]),
    outcomes: page([]),
    nodes: page([
      {
        graph_id: graph, node_id: node, type: "reef", entity_type: "decision", title: "Rate limiter rewrite", contribution: counts, wider_context: counts,
        latest_captured_at: null, unresolved_disagreement: false, current_understanding_status: "evidence_only", synthetic_demo: false, ...over,
      },
    ]),
    prominent_work: [],
    facets: { services: [], content_kinds: [], connections: [], containers: [], unknown_origin_count: 0, truncated: false },
  };
}

function topicFixture(search = response()) {
  return { model: "topic_page", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
    payload: { node: search.nodes.items.length ? { node_id: node, graph_id: graph } : null, edit_requires_admin: false,
      digest: null, sessions: null, neighborhood: null, search } };
}

beforeEach(() => {
  vi.mocked(apiRequest).mockResolvedValue(topicFixture());
});
afterEach(() => {
  cleanup();
  vi.resetAllMocks();
});

it("reads one certified topic payload with its workspace, summary, sessions and connections", async () => {
  render(<TopicPage nodeId={node} />);
  expect(await screen.findByRole("heading", { name: "Rate limiter rewrite" })).toBeInTheDocument();
  expect(apiRequest).toHaveBeenCalledTimes(1);
  expect(vi.mocked(apiRequest).mock.calls[0][0]).toBe(`/v1/load/topics/${node}`);
  expect(resolveP6NodeGraph).not.toHaveBeenCalled();
  expect(searchLibrary).not.toHaveBeenCalled();
  expect(screen.getByText(`digest for ${node}`)).toBeInTheDocument();
  expect(screen.getByText(`sessions for ${node}`)).toBeInTheDocument();
  expect(screen.getByText(`connections for ${node}`)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Home" })).toHaveAttribute("href", "/home");
  // No Library chrome.
  expect(screen.queryByText(/Library/)).toBeNull();
});

it("Ask about this opens a chat in the topic's workspace", async () => {
  render(<TopicPage nodeId={node} />);
  fireEvent.click(await screen.findByRole("button", { name: "Ask about this" }));
  expect(push.mock.calls[0][0]).toContain(`graph_id=${graph}`);
  expect(push.mock.calls[0][0]).toContain("/chats?");
});

it("says so when the topic is not readable", async () => {
  vi.mocked(apiRequest).mockResolvedValue(topicFixture({ ...response(), nodes: { items: [], total: 0, next_cursor: null } }));
  render(<TopicPage nodeId={node} />);
  expect(await screen.findByText("Topic unavailable")).toBeInTheDocument();
});

it("offers the unresolved-disagreement toggle only when there is one", async () => {
  vi.mocked(apiRequest).mockResolvedValue(topicFixture(response({ unresolved_disagreement: true })));
  render(<TopicPage nodeId={node} />);
  expect(await screen.findByRole("button", { name: "Inspect unresolved disagreement" })).toBeInTheDocument();
});

it("treats the real computed-empty payload as unavailable rather than pending", async () => {
  vi.mocked(apiRequest).mockResolvedValue({ model: "topic_page", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
    payload: { node: null, edit_requires_admin: false, digest: null, sessions: null, neighborhood: null, search: null } });
  render(<TopicPage nodeId={node} />);
  expect(await screen.findByText("Topic unavailable")).toBeInTheDocument();
  expect(screen.queryByText(/verifying topic/)).not.toBeInTheDocument();
});
