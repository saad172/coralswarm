// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { type OAuthConnection } from "@/lib/api";
import { FirstRunChecklist } from "@/components/chat/FirstRunChecklist";

const mocks = vi.hoisted(() => ({
  read: vi.fn(),
  payload: { workspace_activity: null, activity: [], connections: [] as OAuthConnection[], connections_status: "available" as "available" | "unavailable" | "pending", teammate_count: 0, own_conversation_count: 0 },
  hasOrg: true,
  isAdmin: true,
}));

vi.mock("@/lib/providers", () => ({
  useApp: () => ({
    authReady: true,
    getAuthToken: () => "tok",
    queryScope: "user-1:org_x",
    accountId: "acct-1",
    internalUserId: "user-1",
  }),
}));
vi.mock("@/components/org/useOrgAdmin", () => ({
  useOrgAdmin: () => ({
    loaded: true,
    hasOrg: mocks.hasOrg,
    isAdmin: mocks.isAdmin,
    orgId: "org_x",
    orgName: "Acme",
    orgSlug: "acme",
  }),
}));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  apiRequest: (path: string, token: string) => { if (!path.startsWith("/v1/load/chats/empty?")) throw new Error(`Unexpected checklist read ${path}`); return mocks.read(path, token); },
}));

function conn(provider: string, mine: boolean): OAuthConnection {
  return {
    provider,
    provider_workspace_id: null,
    scope: "",
    expires_at: null,
    connected_at: "2026-10-01T00:00:00Z",
    missing_scopes: [],
    scopes_unknown: false,
    credential_invalid_at: null,
    credential_invalid_reason: null,
    mine,
  };
}
function renderChecklist() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <FirstRunChecklist />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  mocks.hasOrg = true;
  mocks.isAdmin = true;
  mocks.payload = { workspace_activity: null, activity: [], connections: [], connections_status: "available", teammate_count: 0, own_conversation_count: 0 };
  mocks.read.mockResolvedValue({ model: "chat_empty", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null, payload: mocks.payload });
});
afterEach(cleanup);

describe("FirstRunChecklist", () => {
  it("shows connect, invite and ask, in that order, for a new org admin", async () => {
    renderChecklist();
    const connectLink = await screen.findByRole("link", { name: /open connectors/i });
    expect(connectLink).toHaveAttribute("href", "/connectors");
    expect(screen.getByRole("link", { name: /invite people/i })).toHaveAttribute(
      "href",
      "/settings/organization/members",
    );
    const titles = screen.getAllByText(/connect your sources|invite your teammates|ask your first question/i);
    expect(titles.map((t) => t.textContent)).toEqual([
      "Connect your sources",
      "Invite your teammates",
      "Ask your first question",
    ]);
  });

  it("counts a pending invite as the invite step done", async () => {
    mocks.payload.teammate_count = 1;
    renderChecklist();
    expect(await screen.findByText(/1 other person is in this organization or invited/i)).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /invite people/i })).not.toBeInTheDocument();
  });

  it("leaves out the invite step, and never reads the directory, for a plain member", async () => {
    mocks.isAdmin = false;
    renderChecklist();
    expect(await screen.findByText(/ask your first question/i)).toBeInTheDocument();
    expect(screen.queryByText(/invite your teammates/i)).not.toBeInTheDocument();
    expect(mocks.read.mock.calls.every(([path]) => path.startsWith("/v1/load/chats/empty?"))).toBe(true);
  });

  it("does not treat another member's shared connection as yours", async () => {
    mocks.payload.connections = [conn("slack", false)];
    mocks.payload.own_conversation_count = 1;
    renderChecklist();
    expect(await screen.findByRole("link", { name: /open connectors/i })).toBeInTheDocument();
    expect(screen.getByText(/slack is connected for the workspace/i)).toBeInTheDocument();
  });

  it("renders nothing once the admin has connected, invited and asked", async () => {
    mocks.payload.connections = [conn("google_gmail", true)];
    mocks.payload.own_conversation_count = 1;
    mocks.payload.teammate_count = 1;
    const { container } = renderChecklist();
    await waitFor(() => expect(mocks.read).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(container).toBeEmptyDOMElement());
  });

  it("only counts the caller's own conversations, not ones shared into the ocean", async () => {
    mocks.payload.connections = [conn("google_gmail", true)];
    mocks.payload.teammate_count = 1;
    mocks.payload.own_conversation_count = 0;
    renderChecklist();
    expect(await screen.findByText(/type it in the box below/i)).toBeInTheDocument();
  });

  it("renders nothing outside an organization and makes no request", async () => {
    mocks.hasOrg = false;
    const { container } = renderChecklist();
    expect(container).toBeEmptyDOMElement();
    expect(mocks.read).not.toHaveBeenCalled();
  });

  it("hide for now is a snooze with an expiry, scoped to this account and user", async () => {
    renderChecklist();
    fireEvent.click(await screen.findByRole("button", { name: /hide for now/i }));
    expect(screen.queryByText(/ask your first question/i)).not.toBeInTheDocument();
    const raw = localStorage.getItem("cs_first_run_snooze:acct-1:user-1");
    expect(raw).not.toBeNull();
    expect(Number(raw)).toBeGreaterThan(Date.now());
  });

  it("comes back after the snooze has expired", async () => {
    localStorage.setItem("cs_first_run_snooze:acct-1:user-1", String(Date.now() - 1000));
    renderChecklist();
    expect(await screen.findByText(/ask your first question/i)).toBeInTheDocument();
  });

  it("says connectors aren't on yet, not \"couldn't check\", when the org has no connector switched on", async () => {
    mocks.payload.connections_status = "unavailable";
    renderChecklist();
    expect(await screen.findByText("not on yet")).toBeInTheDocument();
    expect(screen.getByText(/connectors aren.t switched on for this workspace yet/i)).toBeInTheDocument();
    expect(screen.queryByText(/couldn.t check/i)).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /open connectors/i })).not.toBeInTheDocument();
  });

  it("disappears once the rest is done, when connecting doesn't apply", async () => {
    mocks.payload.connections_status = "unavailable";
    mocks.payload.own_conversation_count = 1;
    mocks.payload.teammate_count = 1;
    const { container } = renderChecklist();
    await waitFor(() => expect(mocks.read).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(container).toBeEmptyDOMElement());
  });
});
