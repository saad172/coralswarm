// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { membersAdminFixture } from "@/lib/testing/membersAdminFixture";
import type { MembersAdminPayload } from "@/lib/pageSectionTypes/membersAdmin";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { JoinRequestQueue } from "./JoinRequestQueue";
import { api, type JoinRequest, type SeatTier } from "@/lib/api";

const identity = vi.hoisted(() => ({
  app: { authReady: true, queryScope: "org-scope", getAuthToken: () => "TOKEN" as string | null },
}));
vi.mock("@/lib/providers", () => ({ useApp: () => identity.app }));

const orgAdmin = vi.hoisted(() => ({
  state: {
    loaded: true,
    hasOrg: true,
    isAdmin: true,
    orgId: "org_acme",
    orgName: "Acme" as string | null,
    orgSlug: "acme",
  },
}));
vi.mock("./useOrgAdmin", () => ({ useOrgAdmin: () => orgAdmin.state }));

const countRefresh = vi.hoisted(() => ({ fn: vi.fn() }));
vi.mock("./useJoinRequestCount", async (original) => {
  const actual = await original<typeof import("./useJoinRequestCount")>();
  return { ...actual, refreshJoinRequestCount: (qc: Parameters<typeof actual.refreshJoinRequestCount>[0]) => { countRefresh.fn(); actual.refreshJoinRequestCount(qc); } };
});

const wire = vi.hoisted(() => vi.fn());
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  apiRequest: (path: string) => { if (path !== "/v1/load/org/members") throw new Error(`Unexpected page read ${path}`); return wire(); },
  api: {
    orgs: { joinRequests: { list: vi.fn(), approve: vi.fn(), reject: vi.fn() } },
    billing: { seatTiers: vi.fn() },
  },
}));

const approve = vi.mocked(api.orgs.joinRequests.approve);
const reject = vi.mocked(api.orgs.joinRequests.reject);

let payload: MembersAdminPayload;
function serveMembers(changes: Partial<MembersAdminPayload> = {}) {
  payload = { ...payload, ...changes };
  wire.mockResolvedValue(membersAdminFixture(payload));
}

function makeRequest(overrides: Partial<JoinRequest> = {}): JoinRequest {
  return {
    id: "r1",
    user: { user_id: "u1", name: "Dana Reyes", email: "dana@acme.com" },
    source: "self_request",
    evidence: {},
    requested_at: "2026-09-20T12:00:00Z",
    decided_at: null,
    decided_by: null,
    decision: null,
    ...overrides,
  };
}

beforeEach(() => {
  identity.app = { authReady: true, queryScope: "org-scope", getAuthToken: () => "TOKEN" };
  orgAdmin.state = {
    loaded: true,
    hasOrg: true,
    isAdmin: true,
    orgId: "org_acme",
    orgName: "Acme",
    orgSlug: "acme",
  };
  countRefresh.fn.mockClear();
  wire.mockReset(); payload = membersAdminFixture().payload; serveMembers();
  serveMembers({ seat_tiers: [] });
  approve.mockResolvedValue({ user_id: "u1", slot: "reserved", tier: "standard" });
  reject.mockResolvedValue(undefined);
});
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("JoinRequestQueue", () => {
  it("shows the empty state when nothing is waiting", async () => {
    render(<JoinRequestQueue />);
    expect(await screen.findByText("No one is waiting")).toBeInTheDocument();
    expect(screen.getByText("Requests to join land here.")).toBeInTheDocument();
  });

  it("renders an open row with its source chip and the D8 sentence — no 'seat'", async () => {
    serveMembers({ join_requests: [makeRequest({ source: "pool_full" })] });
    render(<JoinRequestQueue />);
    expect(await screen.findByText("Dana Reyes")).toBeInTheDocument();
    expect(screen.getByText("Asked when no slot was free")).toBeInTheDocument();
    const sentence = screen.getByText(/can join Acme/);
    expect(sentence.textContent).toContain("Dana");
    expect(sentence.textContent).toContain("connects");
    expect(sentence.textContent).toContain("90 days");
    expect(sentence.textContent).not.toMatch(/\bseat\b/i);
  });

  it.each([
    ["matched_not_member", "Already uses CoralSwarm"],
    ["tombstoned_returning", "Was removed before"],
    ["unverified_domain", "Address not on a verified domain"],
  ] as const)("shows the plain-words chip for source=%s", async (source, label) => {
    serveMembers({ join_requests: [makeRequest({ source })] });
    render(<JoinRequestQueue />);
    expect(await screen.findByText(label)).toBeInTheDocument();
  });

  it("shows no source chip for a self_request", async () => {
    serveMembers({ join_requests: [makeRequest({ source: "self_request" })] });
    render(<JoinRequestQueue />);
    await screen.findByText("Dana Reyes");
    expect(screen.queryByText("Already uses CoralSwarm")).not.toBeInTheDocument();
    expect(screen.queryByText("Asked when no slot was free")).not.toBeInTheDocument();
  });

  it("approves with the default standard tier and shows the reserved-slot copy", async () => {
    serveMembers({ join_requests: [makeRequest()] });
    approve.mockResolvedValue({ user_id: "u1", slot: "reserved", tier: "standard" });
    render(<JoinRequestQueue />);
    await screen.findByText("Dana Reyes");
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    await waitFor(() => expect(approve).toHaveBeenCalledWith("TOKEN", "r1", "standard"));
    expect(await screen.findByText("Approved.")).toBeInTheDocument();
    expect(countRefresh.fn).toHaveBeenCalled();
  });

  it("shows the purchased-slot copy when approval buys a new slot", async () => {
    serveMembers({ join_requests: [makeRequest()] });
    approve.mockResolvedValue({ user_id: "u1", slot: "purchased", tier: "standard" });
    render(<JoinRequestQueue />);
    await screen.findByText("Dana Reyes");
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    expect(await screen.findByText("Approved — a new slot was added to your plan.")).toBeInTheDocument();
  });

  it("shows the API's own error and keeps the row when approve fails", async () => {
    serveMembers({ join_requests: [makeRequest()] });
    approve.mockRejectedValue(new (await import("@/lib/api")).ApiError("pool_exhausted", 409));
    render(<JoinRequestQueue />);
    await screen.findByText("Dana Reyes");
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    expect(await screen.findByText("pool_exhausted")).toBeInTheDocument();
    expect(screen.getByText("Dana Reyes")).toBeInTheDocument();
  });

  it("offers the fetched tiers and approves with the admin's selection", async () => {
    serveMembers({ join_requests: [makeRequest()] });
    serveMembers({ seat_tiers: [
      { code: "standard", credits: 0, price_usd_cents: 0 },
      { code: "pro", credits: 500, price_usd_cents: 2900 },
    ] satisfies SeatTier[] });
    render(<JoinRequestQueue />);
    await screen.findByText("Dana Reyes");
    await waitFor(() => expect(screen.getByRole("option", { name: "Pro — $29.00" })).toBeInTheDocument());
    fireEvent.change(screen.getByLabelText("Slot tier for Dana Reyes"), { target: { value: "pro" } });
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    await waitFor(() => expect(approve).toHaveBeenCalledWith("TOKEN", "r1", "pro"));
  });

  it("defaults an untouched row to the first fetched tier when 'standard' isn't offered (CodeRabbit, PR #1027)", async () => {
    serveMembers({ join_requests: [makeRequest()] });
    serveMembers({ seat_tiers: [
      { code: "growth", credits: 200, price_usd_cents: 1900 },
      { code: "enterprise", credits: 1000, price_usd_cents: 9900 },
    ] satisfies SeatTier[] });
    render(<JoinRequestQueue />);
    await screen.findByText("Dana Reyes");
    const select = (await screen.findByLabelText("Slot tier for Dana Reyes")) as HTMLSelectElement;
    await waitFor(() => expect(select.options.length).toBe(2));
    // The visible selection (the browser's default for an unmatched value is
    // its first <option>) and the tier this submits must agree.
    expect(select.value).toBe("growth");
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    await waitFor(() => expect(approve).toHaveBeenCalledWith("TOKEN", "r1", "growth"));
  });

  it("falls back to a single unpriced 'standard' option when the computed model has no active tiers", async () => {
    serveMembers({ join_requests: [makeRequest()] });
    serveMembers({ seat_tiers: [] });
    render(<JoinRequestQueue />);
    await screen.findByText("Dana Reyes");
    const select = screen.getByLabelText("Slot tier for Dana Reyes") as HTMLSelectElement;
    await waitFor(() => expect(select.options.length).toBe(1));
    expect(select.options[0].textContent).toBe("Standard");
  });

  it("rejects only after an inline confirm, with the durable-silence copy, and never notifies", async () => {
    serveMembers({ join_requests: [makeRequest()] });
    render(<JoinRequestQueue />);
    await screen.findByText("Dana Reyes");
    fireEvent.click(screen.getByRole("button", { name: "Reject" }));
    expect(reject).not.toHaveBeenCalled();
    expect(screen.getByText("They won’t be told, and can’t ask again.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Confirm reject" }));
    await waitFor(() => expect(reject).toHaveBeenCalledWith("TOKEN", "r1"));
    expect(countRefresh.fn).toHaveBeenCalled();
    expect(screen.queryByText(/notif/i)).not.toBeInTheDocument();
  });

  it("cancels an in-progress reject confirmation without calling reject", async () => {
    serveMembers({ join_requests: [makeRequest()] });
    render(<JoinRequestQueue />);
    await screen.findByText("Dana Reyes");
    fireEvent.click(screen.getByRole("button", { name: "Reject" }));
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(reject).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Reject" })).toBeInTheDocument();
  });

  it("lists recently decided requests read-only, with the tier recorded in evidence", async () => {
    serveMembers({ join_requests: [
      makeRequest({
        id: "r2",
        decision: "approved",
        decided_at: "2026-09-21T09:00:00Z",
        decided_by: "admin-1",
        evidence: { tier: "pro" },
      }),
      makeRequest({
        id: "r3",
        user: { user_id: "u2", name: "Sam Lee", email: "sam@acme.com" },
        decision: "rejected",
        decided_at: "2026-09-21T10:00:00Z",
        decided_by: "admin-1",
      }),
    ] });
    render(<JoinRequestQueue />);
    fireEvent.click(await screen.findByText("recently decided"));
    expect(screen.getByText("pro")).toBeInTheDocument();
    expect(screen.getByText("approved")).toBeInTheDocument();
    expect(screen.getByText("rejected")).toBeInTheDocument();
    expect(screen.getByText("Sam Lee")).toBeInTheDocument();
    // Both requests are already decided — the open queue above renders its
    // own empty state, never an Approve/Reject control for either row.
    expect(screen.getByText("No one is waiting")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Approve" })).not.toBeInTheDocument();
  });
});
