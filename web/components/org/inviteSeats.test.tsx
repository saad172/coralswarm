// @vitest-environment jsdom
//
// Inviting past the seat count goes through the seat quote → confirm step
// first. The invite is sent only after the extra seat is bought; Cancel sends
// nothing; a quote that can't be priced says why instead of failing opaquely.
import "@testing-library/jest-dom/vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { membersAdminFixture } from "@/lib/testing/membersAdminFixture";
import { render } from "@/lib/testing/queryRender";

const members = vi.fn();
const wire = vi.hoisted(() => vi.fn());
const slots = vi.fn();
const create = vi.fn();
const seatQuote = vi.fn();
const purchase = vi.fn();

vi.mock("@/lib/providers", () => ({
  useApp: () => ({ getAuthToken: () => "tok", authReady: true, queryScope: "user::org", internalUserId: "u_admin" }),
}));
vi.mock("next/link", () => ({
  default: ({ children, href }: { children: React.ReactNode; href: string }) => <a href={href}>{children}</a>,
}));
vi.mock("@/components/org/OrgSettingsShell", () => ({
  OrgSettingsShell: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}));
vi.mock("@/components/org/DomainVerification", () => ({ DomainVerification: () => null }));
vi.mock("@/components/org/AutoAddSetting", () => ({ AutoAddSetting: () => null }));
vi.mock("@/components/org/JoinRequestQueue", () => ({ JoinRequestQueue: () => null }));
vi.mock("@/lib/api", async () => {
  const actual = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
  return {
    ...actual,
    apiRequest: (path: string) => { if (path !== "/v1/load/org/members") throw new Error(`Unexpected page read ${path}`); return wire(); },
    api: {
      orgs: {
        members: (...a: unknown[]) => members(...a),
        slots: { get: (...a: unknown[]) => slots(...a), purchase: (...a: unknown[]) => purchase(...a) },
        domains: { list: vi.fn().mockResolvedValue({ domains: [] }) },
        invites: { create: (...a: unknown[]) => create(...a), revoke: vi.fn(), resend: vi.fn() },
      },
      billing: { seatQuote: (...a: unknown[]) => seatQuote(...a) },
    },
  };
});

import { ApiError } from "@/lib/api";
import Page from "@/app/(app)/settings/organization/members/page";

const ADMIN = { user_id: "u_admin", name: "Ada", email: "ada@example.com", role: "org:admin", pending: false };
const OTHER = { user_id: "u_bob", name: "Bob", email: "bob@example.com", role: "org:member", pending: false };
// A team org's Checkout minimum: two seats, both taken.
const FULL = {
  purchased: 2,
  used: 2,
  reserved: 0,
  available: 0,
  tier_breakdown: [{ tier: "standard", seats: 2 }],
  tiers: [{ code: "standard", purchased: 2, used: 2, reserved: 0, available: 0 }],
};
const ROOM = { ...FULL, used: 1, available: 1, tiers: [{ code: "standard", purchased: 2, used: 1, reserved: 0, available: 1 }] };
const QUOTE = {
  tier: "standard",
  action: "add",
  quantity: 1,
  current_quantity: 2,
  new_quantity: 3,
  currency: "usd",
  proration_cents: 1200,
  next_invoice_total_cents: 6000,
  recurring_cents_after: 6000,
  next_invoice_at: "2026-11-27T00:00:00Z",
};
const INVITED = { user_id: "u_new", clerk_user_id: "user_x", email: "new@example.com", role: "member", pending: true, email_sent: true };

beforeEach(() => {
  for (const m of [members, slots, create, seatQuote, purchase]) m.mockReset();
  members.mockResolvedValue([ADMIN, OTHER]);
  wire.mockReset().mockResolvedValue(membersAdminFixture({ members: [ADMIN, OTHER], slots: FULL }));
  slots.mockResolvedValue(FULL);
  create.mockResolvedValue(INVITED);
  seatQuote.mockResolvedValue(QUOTE);
  purchase.mockResolvedValue({});
});
afterEach(cleanup);

async function invite(email: string) {
  render(<Page />);
  await screen.findByText(/every seat is taken|uses a free seat/);
  await waitFor(() => expect(wire).toHaveBeenCalled());
  fireEvent.change(screen.getByPlaceholderText("name@company.com"), { target: { value: email } });
  fireEvent.click(screen.getByRole("button", { name: "Add member" }));
}

describe("inviting past the seat count", () => {
  it("prices one more seat first and sends nothing until it is confirmed", async () => {
    await invite("new@example.com");
    const dialog = await screen.findByRole("dialog");
    expect(seatQuote).toHaveBeenCalledWith("tok", { quantity: 1, action: "add", tier: "standard" });
    expect(dialog).toHaveTextContent("2 → 3");
    expect(dialog).toHaveTextContent(/every seat is taken/i);
    expect(create).not.toHaveBeenCalled();

    fireEvent.click(within(dialog).getByRole("button", { name: "Add 1 seat" }));
    await waitFor(() => expect(purchase).toHaveBeenCalledWith("tok", expect.objectContaining({ tier: "standard", quantity: 1 })));
    await waitFor(() => expect(create).toHaveBeenCalledWith("tok", { email: "new@example.com", role: "member", allow_purchase: false }));
    expect(purchase.mock.invocationCallOrder[0]).toBeLessThan(create.mock.invocationCallOrder[0]);
  });

  it("sends nothing on Cancel", async () => {
    await invite("new@example.com");
    const dialog = await screen.findByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(purchase).not.toHaveBeenCalled();
    expect(create).not.toHaveBeenCalled();
  });

  it("says why when the seats can't be priced, and sends nothing", async () => {
    seatQuote.mockRejectedValue(new ApiError("no active team subscription", 409, "no_subscription"));
    await invite("new@example.com");
    expect(await screen.findByText(/no active subscription yet/i)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Open Billing" })).toHaveAttribute("href", "/settings/organization/billing");
    expect(create).not.toHaveBeenCalled();
  });

  it("on a 409 no_free_seat, re-reads the seats and prices one before inviting again", async () => {
    // The cached count says a seat is free; the server says otherwise.
    wire.mockResolvedValue(membersAdminFixture({ members: [ADMIN, OTHER], slots: ROOM }));
    slots.mockResolvedValue(FULL);
    create.mockRejectedValueOnce(new ApiError("no free seat", 409, "no_free_seat")).mockResolvedValue(INVITED);
    await invite("new@example.com");
    const dialog = await screen.findByRole("dialog");
    expect(wire).toHaveBeenCalledTimes(1);
    expect(slots).toHaveBeenCalledTimes(1);
    expect(dialog).toHaveTextContent(/every seat is taken/i);
    expect(create).toHaveBeenCalledTimes(1);
    expect(purchase).not.toHaveBeenCalled();
    fireEvent.click(within(dialog).getByRole("button", { name: "Add 1 seat" }));
    await waitFor(() => expect(create).toHaveBeenCalledTimes(2));
    expect(create).toHaveBeenLastCalledWith("tok", { email: "new@example.com", role: "member", allow_purchase: false });
    expect(purchase.mock.invocationCallOrder[0]).toBeLessThan(create.mock.invocationCallOrder[1]);
  });

  it("says the bought seat stays when the invite after it fails", async () => {
    create.mockRejectedValue(new ApiError("billing_unavailable", 502));
    await invite("new@example.com");
    fireEvent.click(within(await screen.findByRole("dialog")).getByRole("button", { name: "Add 1 seat" }));
    expect(await screen.findByText(/1 seat was added; sending again will use it\./)).toBeInTheDocument();
  });

  it("withholds member actions until the combined authoritative payload arrives", async () => {
    let resolve: (v: ReturnType<typeof membersAdminFixture>) => void = () => {};
    wire.mockReturnValue(new Promise((r) => (resolve = r)));
    render(<Page />);
    expect(screen.queryByPlaceholderText("name@company.com")).toBeNull();
    expect(create).not.toHaveBeenCalled();
    resolve(membersAdminFixture({ members: [ADMIN, OTHER], slots: ROOM }));
    fireEvent.change(await screen.findByPlaceholderText("name@company.com"), { target: { value: "new@example.com" } });
    expect(await screen.findByRole("button", { name: "Add member" })).toBeEnabled();
  });

  it("invites straight away while a seat is free", async () => {
    wire.mockResolvedValue(membersAdminFixture({ members: [ADMIN, OTHER], slots: ROOM }));
    await invite("new@example.com");
    await waitFor(() => expect(create).toHaveBeenCalledWith("tok", { email: "new@example.com", role: "member", allow_purchase: false }));
    expect(seatQuote).not.toHaveBeenCalled();
  });
});
