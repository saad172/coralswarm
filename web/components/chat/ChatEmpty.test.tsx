// @vitest-environment jsdom
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { render } from "@/lib/testing/queryRender";
import { ChatEmpty } from "./ChatEmpty";
const deps = vi.hoisted(() => ({ scope: "a", read: vi.fn() }));
vi.mock("@clerk/nextjs", () => ({ useUser: () => ({ user: { firstName: "Dana" } }) }));
vi.mock("@/lib/useEvidenceAdmission", () => ({
  useEvidenceAdmission: () => ({ queryScope: deps.scope, ready: true, getToken: () => "token" }),
}));
vi.mock("@/lib/api", async (actual) => ({ ...await actual<typeof import("@/lib/api")>(),
  apiRequest: (path: string) => { if (!path.startsWith("/v1/load/chats/empty?")) throw new Error(`Unexpected page read ${path}`); return deps.read(); },
}));
vi.mock("./RevealAnswer", () => ({
  RevealAnswer: ({ children }: { children: React.ReactNode }) => children,
}));
afterEach(() => {
  cleanup();
  vi.resetAllMocks();
  deps.scope = "a";
});
it("asks immediately using available generic prompts while requests load", () => {
  deps.read.mockReturnValue(new Promise(() => {}));
  const ask = vi.fn();
  render(<ChatEmpty onAsk={ask} />);
  fireEvent.click(screen.getByRole("button", { name: /What changed this week/ }));
  expect(ask).toHaveBeenCalledWith("What changed this week?");
  expect(screen.getByRole("status").textContent).toContain("Finding recent");
});
it("falls back on failure with a retry and suppresses broad topics when narrowed", async () => {
  deps.read.mockRejectedValue(new Error("offline"));
  const view = render(<ChatEmpty onAsk={vi.fn()} />);
  await screen.findByRole("button", { name: "Retry recent topics" });
  view.rerender(<ChatEmpty onAsk={vi.fn()} narrowed />);
  expect(screen.queryByRole("button", { name: "Retry recent topics" })).toBeNull();
});
it("does not retain previous identity topics", async () => {
  deps.read.mockResolvedValue({ model: "chat_empty", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
    payload: { workspace_activity: { workspaces: [{ graph_id: "g", name: "Private name", headline: { reef: null } }] }, activity: [],
      connections: [], connections_status: "available", teammate_count: null, own_conversation_count: 0 } });
  const view = render(<ChatEmpty onAsk={vi.fn()} />);
  await screen.findByRole("button", { name: /Private name/ });
  deps.scope = "b";
  deps.read.mockReturnValue(new Promise(() => {}));
  view.rerender(<ChatEmpty onAsk={vi.fn()} />);
  await waitFor(() => expect(screen.queryByRole("button", { name: /Private name/ })).toBeNull());
});
