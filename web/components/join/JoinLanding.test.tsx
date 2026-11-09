// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { JoinLanding } from "./JoinLanding";
import { ApiError, api, type JoinRequestSentResult } from "@/lib/api";
import type { Instance } from "@/components/useInstanceSwitcher";

const push = vi.fn();
const nav = vi.hoisted(() => ({
  search: new URLSearchParams(""),
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push }),
  useSearchParams: () => nav.search,
}));

const identity = vi.hoisted(() => ({
  app: { authReady: true, getAuthToken: () => "TOKEN" as string | null },
}));
vi.mock("@/lib/providers", () => ({ useApp: () => identity.app }));

/** A fake join-notice JWT shaped like `mint_join_notice_token` produces —
 *  base64url header.payload.signature, signature never checked client-side
 *  (this test never verifies it) — carrying only what
 *  `orgNameFromInviteToken` reads out of the payload segment. */
function fakeJoinToken(payload: Record<string, unknown>): string {
  const b64url = (obj: unknown) =>
    Buffer.from(JSON.stringify(obj)).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return `${b64url({ alg: "HS256", typ: "JWT" })}.${b64url(payload)}.sig`;
}

const PERSONAL: Instance = { kind: "personal", id: "personal", name: "Personal" };

const switcher = vi.hoisted(() => ({
  state: {
    loaded: true,
    instances: [{ kind: "personal", id: "personal", name: "Personal" }] as Instance[],
    active: { kind: "personal", id: "personal", name: "Personal" } as Instance | null,
    soloInstance: true,
    switchTo: vi.fn(async () => {}),
  },
}));
vi.mock("@/components/useInstanceSwitcher", () => ({ useInstanceSwitcher: () => switcher.state }));

const joinRequests = vi.hoisted(() => ({
  state: { requests: [] as { id: string; org: { clerk_org_id: string; name: string | null }; requested_at: string }[], loading: false, refresh: vi.fn() },
}));
vi.mock("@/lib/useMyJoinRequests", () => ({ useMyJoinRequests: () => joinRequests.state }));

vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  api: { joinRequests: { create: vi.fn() } },
}));
const create = vi.mocked(api.joinRequests.create);

beforeEach(() => {
  push.mockClear();
  nav.search = new URLSearchParams("");
  identity.app = { authReady: true, getAuthToken: () => "TOKEN" };
  switcher.state = {
    loaded: true,
    instances: [PERSONAL],
    active: PERSONAL,
    soloInstance: true,
    switchTo: vi.fn(async () => {}),
  };
  joinRequests.state = { requests: [], loading: false, refresh: vi.fn() };
  create.mockResolvedValue({ status: "sent_to_admin" } satisfies JoinRequestSentResult);
});
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("JoinLanding", () => {
  it("asks to join, calling create with the route's org, and shows the sent sentence", async () => {
    render(<JoinLanding org="org_acme" />);
    fireEvent.click(screen.getByRole("button", { name: "Ask to join" }));
    expect(await screen.findByText("Your request has been sent to your admin.")).toBeInTheDocument();
    expect(create).toHaveBeenCalledWith("TOKEN", "org_acme", undefined);
    expect(joinRequests.state.refresh).toHaveBeenCalled();
  });

  it("forwards the invite token from ?t= so the request is matched_not_member", async () => {
    nav.search = new URLSearchParams("t=invite-tok");
    render(<JoinLanding org="org_acme" />);
    fireEvent.click(screen.getByRole("button", { name: "Ask to join" }));
    expect(await screen.findByText("Your request has been sent to your admin.")).toBeInTheDocument();
    expect(create).toHaveBeenCalledWith("TOKEN", "org_acme", "invite-tok");
  });

  it("shows the unavailable sentence on a 404, never 'doesn't exist' or 'is personal'", async () => {
    create.mockRejectedValue(new ApiError("not_found", 404));
    render(<JoinLanding org="org_missing" />);
    fireEvent.click(screen.getByRole("button", { name: "Ask to join" }));
    expect(await screen.findByText("That organization isn’t available.")).toBeInTheDocument();
    expect(screen.queryByText(/doesn.t exist/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/is personal/i)).not.toBeInTheDocument();
  });

  it("titles from the invite token's org_name when present (CS-441)", () => {
    nav.search = new URLSearchParams(`t=${fakeJoinToken({ org_name: "Acme & Co" })}`);
    render(<JoinLanding org="org_acme" />);
    expect(screen.getByRole("heading").textContent).toBe("Join Acme & Co on CoralSwarm?");
  });

  it("falls back to the generic title on a missing or unreadable token", () => {
    render(<JoinLanding org="org_acme" />);
    expect(screen.getByRole("heading").textContent).toBe("Join this organization on CoralSwarm?");

    cleanup();
    nav.search = new URLSearchParams("t=not-a-jwt");
    render(<JoinLanding org="org_acme" />);
    expect(screen.getByRole("heading").textContent).toBe("Join this organization on CoralSwarm?");
  });

  it("shows the already-member branch instead of a request button", () => {
    const acme: Instance = { kind: "org", id: "org_acme", name: "Acme", slug: "acme" };
    switcher.state = { ...switcher.state, instances: [PERSONAL, acme], active: acme };
    render(<JoinLanding org="org_acme" />);
    // `PageTitle` splits the italicized org name into its own <em>, so the
    // sentence isn't one text node — assert on the heading's full text.
    expect(screen.getByRole("heading").textContent).toBe("You’re already a member of Acme.");
    expect(screen.queryByRole("button", { name: "Ask to join" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Go to Acme" })).toBeInTheDocument();
  });
});
