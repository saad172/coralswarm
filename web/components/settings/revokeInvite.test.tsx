// @vitest-environment jsdom
//
// Revoking a pending invite is destructive (it frees the seat and kills the
// emailed link), so it is gated behind a confirmation that names the invitee;
// Cancel must do nothing, and a failed call must show its error.
import "@testing-library/jest-dom/vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { membersAdminFixture } from "@/lib/testing/membersAdminFixture";
import { render } from "@/lib/testing/queryRender";

const members = vi.fn();
const wire = vi.hoisted(() => vi.fn());
const revoke = vi.fn();

vi.mock("@/lib/providers", () => ({
  useApp: () => ({ getAuthToken: () => "tok", authReady: true, queryScope: "user::org" }),
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
        slots: { get: vi.fn().mockResolvedValue({ purchased: 3, used: 2, reserved: 1, available: 0 }) },
        domains: { list: vi.fn().mockResolvedValue({ domains: [] }) },
        invites: { revoke: (...a: unknown[]) => revoke(...a), resend: vi.fn() },
      },
    },
  };
});

import { ApiError } from "@/lib/api";
import Page from "@/app/(app)/settings/organization/members/page";

const INVITEE = {
  user_id: "u_invited",
  name: null,
  email: "new.person@example.com",
  role: "org:member",
  pending: true,
};
const ADMIN = { user_id: "u_admin", name: "Ada", email: "ada@example.com", role: "org:admin", pending: false };

beforeEach(() => {
  members.mockReset();
  revoke.mockReset();
  wire.mockReset().mockResolvedValue(membersAdminFixture({ members: [ADMIN, INVITEE], slots: { purchased: 3, used: 2, reserved: 1, available: 0, tier_breakdown: [], tiers: [] } }));
});
afterEach(cleanup);

async function openRevoke() {
  render(<Page />);
  fireEvent.click(await screen.findByRole("button", { name: "Revoke" }));
  return screen.getByRole("dialog");
}

describe("revoke invite confirmation", () => {
  it("names the invitee and does not call the API until confirmed", async () => {
    revoke.mockResolvedValue(undefined);
    const dialog = await openRevoke();
    expect(dialog).toHaveTextContent("new.person@example.com");
    expect(revoke).not.toHaveBeenCalled();
    fireEvent.click(within(dialog).getByRole("button", { name: "Revoke invite" }));
    await waitFor(() => expect(revoke).toHaveBeenCalledWith("tok", "u_invited"));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  });

  it("does nothing on Cancel", async () => {
    const dialog = await openRevoke();
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(revoke).not.toHaveBeenCalled();
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  });

  it("shows the error in the dialog and keeps it open when revoke fails", async () => {
    revoke.mockRejectedValue(new ApiError("seat service down", 502));
    const dialog = await openRevoke();
    fireEvent.click(within(dialog).getByRole("button", { name: "Revoke invite" }));
    expect(await within(screen.getByRole("dialog")).findByText(/seat service down/)).toBeInTheDocument();
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });
});
