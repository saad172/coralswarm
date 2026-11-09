// @vitest-environment jsdom
//
// End-to-end through the REAL `useMyJoinRequests` hook (not mocked) — only
// the boundary is faked: `@clerk/nextjs`, `@/lib/providers`, `@/lib/api`.
// Same convention as components/connectors/FollowedThreads.test.tsx, the
// nearest existing test for a component whose gate is "personal plane,
// signed in, identity settled". A real `QueryClientProvider` wraps every
// render since `useMyJoinRequests` reads/writes `@tanstack/react-query`'s
// shared cache (lib/joinRequestsQuery.ts) — that sharing is the whole point
// of routing this through react-query rather than local `useState` (see
// lib/useMyJoinRequests.ts's module comment), so a mock of the hook itself
// would test nothing about it.
import "@testing-library/jest-dom/vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { JoinRequestBanner } from "./JoinRequestBanner";
import { api, type MyJoinRequest } from "@/lib/api";

const identity = vi.hoisted(() => ({
  app: {
    getAuthToken: () => "TOKEN" as string | null,
    authReady: true,
    ready: true,
    accountId: "account-a" as string | null,
    isPersonalOrg: true,
    resolvedOrgId: null as string | null,
  },
  auth: {
    isLoaded: true,
    orgId: null as string | null,
    sessionId: "session-a" as string | null,
    userId: "user-a" as string | null,
  },
}));
vi.mock("@/lib/providers", () => ({ useApp: () => identity.app }));
vi.mock("@clerk/nextjs", () => ({ useAuth: () => identity.auth }));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  apiRequest: (path: string, token: string) => { if (path !== "/v1/me/join-requests") throw new Error(`Unexpected page read ${path}`); return list(token); },
  api: { myJoinRequests: vi.fn() },
}));

const list = vi.mocked(api.myJoinRequests);

function row(id: string, clerkOrgId: string, name: string | null): MyJoinRequest {
  return { id, org: { clerk_org_id: clerkOrgId, name }, requested_at: "2026-09-20T12:00:00Z" };
}

function renderBanner() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <JoinRequestBanner />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  identity.app = {
    getAuthToken: () => "TOKEN",
    authReady: true,
    ready: true,
    accountId: "account-a",
    isPersonalOrg: true,
    resolvedOrgId: null,
  };
  identity.auth = { isLoaded: true, orgId: null, sessionId: "session-a", userId: "user-a" };
  list.mockResolvedValue([]);
  window.sessionStorage.clear();
});
afterEach(() => {
  cleanup();
  vi.resetAllMocks();
});

describe("JoinRequestBanner", () => {
  it("renders one open request with the exact copy and no numerals", async () => {
    list.mockResolvedValue([row("r1", "org_acme", "Acme")]);
    renderBanner();
    expect(await screen.findByText("Your request to join Acme has been sent to your admin.")).toBeInTheDocument();
    expect(screen.getByText("You can keep using your own workspace meanwhile.")).toBeInTheDocument();
    expect(screen.getByRole("status").textContent).not.toMatch(/\d/);
  });

  it("renders nothing when the list is empty", async () => {
    renderBanner();
    await waitFor(() => expect(list).toHaveBeenCalled());
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("renders nothing when the fetch fails", async () => {
    list.mockRejectedValue(new Error("boom"));
    renderBanner();
    await waitFor(() => expect(list).toHaveBeenCalled());
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("shows two lines for two open requests", async () => {
    list.mockResolvedValue([row("r1", "org_acme", "Acme"), row("r2", "org_beta", "Beta")]);
    renderBanner();
    expect(await screen.findByText("Your request to join Acme has been sent to your admin.")).toBeInTheDocument();
    expect(screen.getByText("Your request to join Beta has been sent to your admin.")).toBeInTheDocument();
    expect(screen.getAllByRole("status")).toHaveLength(2);
  });

  it("dismisses per org per user, writing sessionStorage", async () => {
    list.mockResolvedValue([row("r1", "org_acme", "Acme")]);
    renderBanner();
    await screen.findByText("Your request to join Acme has been sent to your admin.");
    fireEvent.click(screen.getByRole("button", { name: "Dismiss join request notice for Acme" }));
    await waitFor(() => expect(screen.queryByRole("status")).not.toBeInTheDocument());
    // Keyed by user AND org (CodeRabbit, PR #1023) — an org-only key would
    // let one account's dismissal hide the next signed-in account's banner
    // for the same org within the same browser tab session.
    expect(window.sessionStorage.getItem("cs_join_request_dismissed:user-a:org_acme")).toBe("1");
  });

  it("does not leak a dismissal across signed-in users sharing a tab", async () => {
    list.mockResolvedValue([row("r1", "org_acme", "Acme")]);
    const { unmount } = renderBanner();
    await screen.findByText("Your request to join Acme has been sent to your admin.");
    fireEvent.click(screen.getByRole("button", { name: "Dismiss join request notice for Acme" }));
    await waitFor(() => expect(screen.queryByRole("status")).not.toBeInTheDocument());
    unmount();

    identity.auth = { isLoaded: true, orgId: null, sessionId: "session-b", userId: "user-b" };
    list.mockResolvedValue([row("r2", "org_acme", "Acme")]);
    renderBanner();
    expect(await screen.findByText("Your request to join Acme has been sent to your admin.")).toBeInTheDocument();
  });

  it("survives a sessionStorage write failure without throwing", async () => {
    list.mockResolvedValue([row("r1", "org_acme", "Acme")]);
    const setItem = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("private mode");
    });
    renderBanner();
    await screen.findByText("Your request to join Acme has been sent to your admin.");
    expect(() =>
      fireEvent.click(screen.getByRole("button", { name: "Dismiss join request notice for Acme" })),
    ).not.toThrow();
    await waitFor(() => expect(screen.queryByRole("status")).not.toBeInTheDocument());
    setItem.mockRestore();
  });

  it("does not fetch or render inside an ordinary org context", () => {
    identity.auth.orgId = "org_real";
    identity.app.resolvedOrgId = "org_real";
    identity.app.isPersonalOrg = false;
    renderBanner();
    expect(list).not.toHaveBeenCalled();
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });
});
