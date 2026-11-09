// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { OrgCreateResult, OrgSetupStatus } from "@/lib/api";
import { attemptByOrg, readAttempts, rememberAttempt, type AttemptStore, type KV } from "@/lib/orgSelfserve";
import type { TeamSignupDeps } from "./TeamSignupShell";
import { TeamReturn, resolveReturn } from "./TeamReturn";

const mocks = vi.hoisted(() => ({ status: vi.fn(), params: new URLSearchParams() }));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  apiRequest: (path: string, token: string) => { const match = /^\/v1\/orgs\/([^/]+)\/provisioning$/.exec(path); if (!match) throw new Error(`Unexpected page read ${path}`); return mocks.status(token, decodeURIComponent(match[1])); },
  api: { orgSelfserve: { status: mocks.status } },
}));
vi.mock("next/navigation", () => ({ useSearchParams: () => mocks.params }));

function memStore(): AttemptStore {
  const data = new Map<string, string>();
  const kv: KV = {
    getItem: (k) => data.get(k) ?? null,
    setItem: (k, v) => void data.set(k, v),
    removeItem: (k) => void data.delete(k),
  };
  return { kv, user: "user_1" };
}

function st(p: Partial<OrgSetupStatus>): OrgSetupStatus {
  return { org_id: "org_x", status: "provisioning", step: "migrate", waiting_for: null, retryable: false, resume_payment_until: null, message: "", ...p };
}

function created(orgId: string): OrgCreateResult {
  return {
    org_id: orgId,
    status: "awaiting_payment",
    checkout_url: "https://checkout.stripe.com/c/pay/cs_1",
    session_id: "cs_1",
    trial: true,
    seats: 3,
    trial_credits: 3000,
    trial_days: 90,
    resume_payment: false,
    replayed: true,
  };
}

function deps(store: AttemptStore, p: Partial<TeamSignupDeps> = {}): TeamSignupDeps {
  return {
    ctx: { getAuthToken: () => "personal-token", scope: "user::personal" },
    store,
    priceCents: 2000,
    create: vi.fn(),
    retry: vi.fn(),
    trialCheckout: vi.fn(),
    openTeam: vi.fn().mockResolvedValue(undefined),
    goToCheckout: vi.fn(),
    goTo: vi.fn(),
    returnUrl: (path, q) => `https://app.test${path}?${q}`,
    ...p,
  };
}

function at(query: string) {
  mocks.params = new URLSearchParams(query);
}

beforeEach(() => {
  mocks.status.mockResolvedValue(st({ status: "provisioning" }));
});
afterEach(() => {
  cleanup();
  mocks.status.mockReset();
});

describe("TeamReturn", () => {
  it("S1: a link carrying an attempt this browser never made does NOT create anything until a click", async () => {
    at("k=attacker-key-1&n=%3Cb%3EEvil%20Corp%3C%2Fb%3E&s=50");
    const store = memStore();
    const d = deps(store, { create: vi.fn().mockResolvedValue(created("org_evil01")) });
    render(<TeamReturn deps={d} page="return" />);
    // The name is plain text, markup and all.
    expect(await screen.findByRole("heading", { name: /Continue setting up “<b>Evil Corp<\/b>”\?/ })).toBeInTheDocument();
    await new Promise((r) => setTimeout(r, 50));
    expect(d.create).not.toHaveBeenCalled();
    expect(mocks.status).not.toHaveBeenCalled();
    expect(readAttempts(store)).toEqual([]);
    fireEvent.click(screen.getByRole("button", { name: /^continue$/i }));
    await waitFor(() => expect(d.create).toHaveBeenCalledTimes(1));
    expect((d.create as ReturnType<typeof vi.fn>).mock.calls[0][1]).toBe("attacker-key-1");
    // Confirmed: this browser now follows it.
    await waitFor(() => expect(attemptByOrg(store, "org_evil01")?.key).toBe("attacker-key-1"));
  });

  it("S1: 'Back to CoralSwarm' on the question creates nothing", async () => {
    at("k=attacker-key-1&n=Evil&s=2");
    const d = deps(memStore(), { create: vi.fn() });
    render(<TeamReturn deps={d} page="return" />);
    fireEvent.click(await screen.findByRole("button", { name: /back to coralswarm/i }));
    expect(d.goTo).toHaveBeenCalledWith("/chats");
    expect(d.create).not.toHaveBeenCalled();
  });

  it("this browser's own attempt (same key) replays by itself", async () => {
    at("k=my-key-0001&n=Acme&s=3");
    const store = memStore();
    rememberAttempt(store, { key: "my-key-0001", name: "Acme", seats: 3 });
    const d = deps(store, { create: vi.fn().mockResolvedValue(created("org_mine01")) });
    render(<TeamReturn deps={d} page="return" />);
    await waitFor(() => expect(d.create).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(attemptByOrg(store, "org_mine01")?.key).toBe("my-key-0001"));
  });

  it("S2: ?org= for another team neither clears this browser's sign-up nor switches", async () => {
    at("org=org_someoneElse1");
    mocks.status.mockResolvedValue(st({ org_id: "org_someoneElse1", status: "ready" }));
    const store = memStore();
    rememberAttempt(store, { key: "my-key-0001", name: "Acme", seats: 3, orgId: "org_mine01" });
    const d = deps(store);
    render(<TeamReturn deps={d} page="return" />);
    const open = await screen.findByRole("button", { name: /open this team/i });
    await new Promise((r) => setTimeout(r, 50));
    expect(d.openTeam).not.toHaveBeenCalled();
    // The real sign-up in progress is untouched.
    expect(attemptByOrg(store, "org_mine01")?.key).toBe("my-key-0001");
    fireEvent.click(open);
    await waitFor(() => expect(d.openTeam).toHaveBeenCalledWith("org_someoneElse1"));
    expect(attemptByOrg(store, "org_mine01")?.key).toBe("my-key-0001");
  });

  it("S2: ?org= for this browser's own team switches and forgets only that attempt", async () => {
    at("org=org_mine01");
    mocks.status.mockResolvedValue(st({ org_id: "org_mine01", status: "ready" }));
    const store = memStore();
    rememberAttempt(store, { key: "other-key-01", name: "Beta", seats: 2, orgId: "org_beta01" });
    rememberAttempt(store, { key: "my-key-0001", name: "Acme", seats: 3, orgId: "org_mine01" });
    const d = deps(store);
    render(<TeamReturn deps={d} page="return" />);
    await waitFor(() => expect(d.openTeam).toHaveBeenCalledWith("org_mine01"));
    expect(attemptByOrg(store, "org_mine01")).toBeNull();
    expect(attemptByOrg(store, "org_beta01")?.key).toBe("other-key-01");
  });

  it("S2: ?org= with a URL attempt whose key is ours but for another org asks first", () => {
    const store = memStore();
    rememberAttempt(store, { key: "my-key-0001", name: "Acme", seats: 3, orgId: "org_mine01" });
    const r = resolveReturn(new URLSearchParams("k=my-key-0001&n=Acme&s=3&org=org_notMine1"), store);
    expect(r.kind).toBe("confirm");
  });
});
