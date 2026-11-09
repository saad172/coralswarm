// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, type SlotPoolResult } from "@/lib/api";
import OrgInvitePage from "@/app/onboarding/org/invite/page";

const mocks = vi.hoisted(() => ({
  push: vi.fn(),
  create: vi.fn(),
  slotsGet: vi.fn(),
  seatQuote: vi.fn(),
  purchase: vi.fn(),
  org: { loaded: true, hasOrg: true, isAdmin: true, orgId: "org_x", orgName: "Acme", orgSlug: "acme" },
}));

vi.mock("next/navigation", () => ({ useRouter: () => ({ push: mocks.push }) }));
vi.mock("@/lib/providers", () => ({
  useApp: () => ({ ready: true, authReady: true, getAuthToken: () => "tok", queryScope: "user::org_x" }),
}));
vi.mock("@/components/org/useOrgAdmin", () => ({ useOrgAdmin: () => mocks.org }));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  apiRequest: (path: string, token: string) => { if (path !== "/v1/orgs/slots") throw new Error(`Unexpected page read ${path}`); return mocks.slotsGet(token); },
  api: {
    orgs: { invites: { create: mocks.create }, slots: { get: mocks.slotsGet, purchase: mocks.purchase } },
    billing: { seatQuote: mocks.seatQuote },
  },
}));

function pool(standardAvailable: number): SlotPoolResult {
  return {
    purchased: 3,
    used: 3 - standardAvailable,
    reserved: 0,
    available: standardAvailable,
    tier_breakdown: [{ tier: "standard", seats: 3 }],
    tiers: [{ code: "standard", purchased: 3, used: 3 - standardAvailable, reserved: 0, available: standardAvailable }],
  };
}

function addEmail(email: string) {
  const input = screen.getByLabelText(/invite by email/i);
  fireEvent.change(input, { target: { value: email } });
  fireEvent.keyDown(input, { key: "Enter" });
}

beforeEach(() => {
  vi.clearAllMocks();
  mocks.org = { loaded: true, hasOrg: true, isAdmin: true, orgId: "org_x", orgName: "Acme", orgSlug: "acme" };
  mocks.slotsGet.mockResolvedValue(pool(3));
  mocks.purchase.mockResolvedValue({});
  mocks.seatQuote.mockImplementation(async (_t: string, p: { quantity: number }) => ({
    tier: "standard",
    action: "add",
    quantity: p.quantity,
    current_quantity: 3,
    new_quantity: 3 + p.quantity,
    currency: "usd",
    proration_cents: 1200 * p.quantity,
    next_invoice_total_cents: 9000,
    recurring_cents_after: 9000,
    next_invoice_at: null,
  }));
  mocks.create.mockImplementation(async (_t: string, body: { email: string; role: string }) => ({
    user_id: `u-${body.email}`,
    clerk_user_id: "user_x",
    email: body.email,
    role: body.role,
    pending: true,
    email_sent: true,
  }));
});
afterEach(cleanup);

describe("onboarding invite step", () => {
  it("starts empty: no seeded addresses", async () => {
    render(<OrgInvitePage />);
    await waitFor(() => expect(mocks.slotsGet).toHaveBeenCalled());
    expect(screen.queryByText("dana@acme.com")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /send/i })).toBeDisabled();
  });

  it("says before sending that invites beyond the free seats need more, priced first", async () => {
    mocks.slotsGet.mockResolvedValue(pool(1));
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    addEmail("b@acme.com");
    addEmail("c@acme.com");
    expect(await screen.findByText(/1 free seat/i)).toBeInTheDocument();
    expect(screen.getByText(/1 invite uses it; the other 2 need more seats/i)).toBeInTheDocument();
    expect(screen.getByText(/see the price before anything is sent/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /send 3 invites · adds 2 seats/i })).toBeEnabled();
    expect(mocks.create).not.toHaveBeenCalled();
  });

  it("goes through the seat quote and confirm before sending past the seat count", async () => {
    mocks.slotsGet.mockResolvedValue(pool(1));
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    addEmail("b@acme.com");
    fireEvent.click(await screen.findByRole("button", { name: /send 2 invites · adds 1 seat/i }));
    const dialog = await screen.findByRole("dialog");
    expect(mocks.seatQuote).toHaveBeenCalledWith("tok", { quantity: 1, action: "add", tier: "standard" });
    expect(dialog).toHaveTextContent("3 → 4");
    expect(mocks.create).not.toHaveBeenCalled();
    fireEvent.click(within(dialog).getByRole("button", { name: "Add 1 seat" }));
    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(2));
    expect(mocks.purchase).toHaveBeenCalledTimes(1);
    expect(mocks.purchase.mock.invocationCallOrder[0]).toBeLessThan(mocks.create.mock.invocationCallOrder[0]);
  });

  it("sends nothing when the seat quote is cancelled", async () => {
    mocks.slotsGet.mockResolvedValue(pool(0));
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    fireEvent.click(await screen.findByRole("button", { name: /send 1 invite · adds 1 seat/i }));
    fireEvent.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(mocks.purchase).not.toHaveBeenCalled();
    expect(mocks.create).not.toHaveBeenCalled();
  });

  it("says nothing is charged when the org has no paid seats yet", async () => {
    mocks.slotsGet.mockResolvedValue({ ...pool(0), purchased: 0 });
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    expect(await screen.findByText(/no seats have been bought/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^send 1 invite$/i })).toBeEnabled();
  });

  it("sends each address through the real invite API with the chosen role", async () => {
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    addEmail("b@acme.com");
    fireEvent.change(screen.getByLabelText(/role/i), { target: { value: "admin" } });
    fireEvent.click(await screen.findByRole("button", { name: /send 2 invites/i }));
    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(2));
    expect(mocks.create).toHaveBeenNthCalledWith(1, "tok", { email: "a@acme.com", role: "admin", allow_purchase: false });
    expect(mocks.create).toHaveBeenNthCalledWith(2, "tok", { email: "b@acme.com", role: "admin", allow_purchase: false });
    expect(await screen.findByText(/2 invites sent/i)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /continue/i }));
    expect(mocks.push).toHaveBeenCalledWith("/chats");
  });

  it("shows a per-address error, keeps going, and lets the admin retry what failed", async () => {
    mocks.create.mockImplementation(async (_t: string, body: { email: string; role: string }) => {
      if (body.email === "a@acme.com") throw new ApiError("already_a_member", 409);
      return { user_id: "u", clerk_user_id: "c", email: body.email, role: body.role, pending: true, email_sent: true };
    });
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    addEmail("b@acme.com");
    fireEvent.click(await screen.findByRole("button", { name: /send 2 invites/i }));
    expect(await screen.findByText("That address already holds a slot here.")).toBeInTheDocument();
    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(2));
    expect(screen.getByText(/1 of 2 invites sent/i)).toBeInTheDocument();
  });

  it("stops the batch on a payment failure instead of trying every address", async () => {
    mocks.create.mockRejectedValue(new ApiError("seat_purchase_failed", 402, null, "card_declined"));
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    addEmail("b@acme.com");
    fireEvent.click(await screen.findByRole("button", { name: /send 2 invites/i }));
    expect(await screen.findByText(/declined/i)).toBeInTheDocument();
    expect(mocks.create).toHaveBeenCalledTimes(1);
    expect(screen.getByText(/not sent/i)).toBeInTheDocument();
  });

  it("on a 409 no_free_seat, re-reads the seats and prices them for the invites still unsent", async () => {
    // The cached count says 3 free; by the second invite the server has none.
    mocks.slotsGet.mockResolvedValueOnce(pool(3)).mockResolvedValue(pool(0));
    mocks.create.mockImplementation(async (_t: string, body: { email: string; role: string }) => {
      if (body.email !== "a@acme.com" && mocks.purchase.mock.calls.length === 0) {
        throw new ApiError("no free seat", 409, "no_free_seat");
      }
      return { user_id: "u", clerk_user_id: "c", email: body.email, role: body.role, pending: true, email_sent: true };
    });
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    addEmail("b@acme.com");
    addEmail("c@acme.com");
    fireEvent.click(await screen.findByRole("button", { name: /^send 3 invites$/i }));
    const dialog = await screen.findByRole("dialog");
    expect(mocks.seatQuote).toHaveBeenCalledWith("tok", { quantity: 2, action: "add", tier: "standard" });
    expect(mocks.create).toHaveBeenCalledTimes(2);
    expect(mocks.purchase).not.toHaveBeenCalled();
    fireEvent.click(within(dialog).getByRole("button", { name: "Add 2 seats" }));
    await waitFor(() => expect(mocks.create).toHaveBeenCalledTimes(4));
    expect(mocks.create).toHaveBeenNthCalledWith(3, "tok", { email: "b@acme.com", role: "member", allow_purchase: false });
    expect(mocks.create).toHaveBeenNthCalledWith(4, "tok", { email: "c@acme.com", role: "member", allow_purchase: false });
    expect(await screen.findByText(/3 invites sent/i)).toBeInTheDocument();
  });

  it("says the bought seats stay when an invite after the purchase fails", async () => {
    mocks.slotsGet.mockResolvedValue(pool(0));
    mocks.create.mockRejectedValue(new ApiError("billing_unavailable", 502));
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    addEmail("b@acme.com");
    fireEvent.click(await screen.findByRole("button", { name: /send 2 invites · adds 2 seats/i }));
    fireEvent.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Add 2 seats" }));
    expect(await screen.findByText("2 seats were added; sending again will use them.")).toBeInTheDocument();
  });

  it("keeps Send disabled while the seat count is loading", async () => {
    let resolve: (v: SlotPoolResult) => void = () => {};
    mocks.slotsGet.mockReturnValue(new Promise((r) => (resolve = r)));
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    expect(await screen.findByRole("button", { name: /checking seats/i })).toBeDisabled();
    resolve(pool(3));
    expect(await screen.findByRole("button", { name: /^send 1 invite$/i })).toBeEnabled();
  });

  it("flags an invite whose email didn't go out", async () => {
    mocks.create.mockResolvedValue({ user_id: "u", clerk_user_id: "c", email: "a@acme.com", role: "member", pending: true, email_sent: false });
    render(<OrgInvitePage />);
    addEmail("a@acme.com");
    fireEvent.click(await screen.findByRole("button", { name: /send 1 invite/i }));
    expect(await screen.findByText(/email didn.t go out/i)).toBeInTheDocument();
  });

  it("rejects an address that is not one email before any request", async () => {
    render(<OrgInvitePage />);
    addEmail("not-an-email");
    expect(await screen.findByText(/doesn't look like an email/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /send/i })).toBeDisabled();
  });

  it("refuses to send for a non-admin", async () => {
    mocks.org = { ...mocks.org, isAdmin: false };
    render(<OrgInvitePage />);
    expect(await screen.findByText(/only an org admin can invite/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/invite by email/i)).not.toBeInTheDocument();
  });

  it("refuses outside an organization", async () => {
    mocks.org = { ...mocks.org, hasOrg: false, isAdmin: false };
    render(<OrgInvitePage />);
    expect(await screen.findByText(/switch to your organization/i)).toBeInTheDocument();
    expect(mocks.slotsGet).not.toHaveBeenCalled();
  });
});
