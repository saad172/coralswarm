// @vitest-environment jsdom
//
// Remove and Change role on Organization -> Members: admins only, never on
// their own row or the owner's, a confirm before anything is sent, and the
// server's refusal shown in plain words inside the dialog.
import "@testing-library/jest-dom/vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { membersAdminFixture } from "@/lib/testing/membersAdminFixture";
import { render } from "@/lib/testing/queryRender";

const members = vi.fn();
const wire = vi.hoisted(() => vi.fn());
const remove = vi.fn();
const setRole = vi.fn();
let selfId = "u_admin";

vi.mock("@/lib/providers", () => ({
  useApp: () => ({ getAuthToken: () => "tok", authReady: true, queryScope: "user::org", internalUserId: selfId }),
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
        slots: { get: vi.fn().mockResolvedValue({ purchased: 3, used: 2, reserved: 0, available: 1 }) },
        domains: { list: vi.fn().mockResolvedValue({ domains: [] }) },
        invites: { revoke: vi.fn(), resend: vi.fn() },
        member: { remove: (...a: unknown[]) => remove(...a), setRole: (...a: unknown[]) => setRole(...a) },
      },
    },
  };
});

import { ApiError } from "@/lib/api";
import Page from "@/app/(app)/settings/organization/members/page";

const row = (user_id: string, name: string, role: string) => ({
  user_id,
  name,
  email: `${name.toLowerCase()}@example.com`,
  role,
  pending: false,
});
const ME = row("u_admin", "Ada", "org:admin");
const OWNER = { ...row("u_owner", "Olive", "org:admin"), is_owner: true };
const LEGACY_OWNER = row("u_legacy", "Lee", "owner");
const BOB = row("u_bob", "Bob", "org:member");
const CY = row("u_cy", "Cy", "org:admin");

beforeEach(() => {
  members.mockReset();
  remove.mockReset();
  setRole.mockReset();
  selfId = "u_admin";
  wire.mockReset().mockResolvedValue(membersAdminFixture({ members: [OWNER, LEGACY_OWNER, ME, BOB, CY], slots: { purchased: 3, used: 2, reserved: 0, available: 1, tier_breakdown: [], tiers: [] } }));
});
afterEach(cleanup);

describe("who sees the controls", () => {
  it("shows them for other non-owner members only, to an admin", async () => {
    render(<Page />);
    await screen.findByText("bob@example.com");
    expect(screen.getAllByRole("button", { name: /^Remove / }).map((b) => b.getAttribute("aria-label"))).toEqual([
      "Remove Bob",
      "Remove Cy",
    ]);
    expect(screen.queryByRole("button", { name: "Remove Ada" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Remove Olive" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Remove Lee" })).toBeNull();
    expect(screen.getByRole("button", { name: "Make admin: Bob" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Make member: Cy" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /: Ada$/ })).toBeNull();
    expect(screen.queryByRole("button", { name: /: Olive$/ })).toBeNull();
    expect(screen.queryByRole("button", { name: /: Lee$/ })).toBeNull();
  });

  it("shows none to a regular member", async () => {
    selfId = "u_bob";
    render(<Page />);
    await screen.findByText("bob@example.com");
    expect(screen.queryByRole("button", { name: /^Remove / })).toBeNull();
    expect(screen.queryByRole("button", { name: /^Make (admin|member)/ })).toBeNull();
  });
});

async function openRemove() {
  render(<Page />);
  fireEvent.click(await screen.findByRole("button", { name: "Remove Bob" }));
  return screen.getByRole("dialog");
}

describe("remove", () => {
  it("says what happens and sends nothing until confirmed, then refreshes", async () => {
    remove.mockResolvedValue({ removed: true, user_id: "u_bob" });
    const dialog = await openRemove();
    expect(dialog).toHaveTextContent("loses access to this organization right away");
    expect(dialog).toHaveTextContent("archived, not deleted");
    expect(remove).not.toHaveBeenCalled();
    const calls = wire.mock.calls.length;
    fireEvent.click(within(dialog).getByRole("button", { name: "Remove member" }));
    await waitFor(() => expect(remove).toHaveBeenCalledWith("tok", "u_bob"));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    await waitFor(() => expect(wire.mock.calls.length).toBeGreaterThan(calls));
  });

  it("does nothing on Cancel", async () => {
    const dialog = await openRemove();
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(remove).not.toHaveBeenCalled();
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  });

  const cases: [string, number, string, RegExp][] = [
    ["cannot_remove_self", 400, "x", /can't remove yourself/],
    ["cannot_remove_owner", 400, "x", /owner can't be removed/],
    ["member_not_found", 404, "x", /no longer a member/],
    ["last_admin", 409, "x", /needs at least one admin\. Make someone else an admin first\./],
    ["clerk_error", 502, "x", /briefly unavailable/],
    ["clerk_misconfigured", 503, "x", /briefly unavailable/],
    ["local_cleanup_failed", 500, "x", /safe to try again/],
    ["admin only", 403, "x", /Only an admin/],
    ["something_new", 400, "The server says no.", /The server says no\./],
  ];
  it.each(cases)("shows copy for %s and keeps the dialog open", async (code, status, serverMessage, copy) => {
    remove.mockRejectedValue(new ApiError(code, status, null, null, serverMessage));
    const dialog = await openRemove();
    fireEvent.click(within(dialog).getByRole("button", { name: "Remove member" }));
    expect(await within(screen.getByRole("dialog")).findByText(copy)).toBeInTheDocument();
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });
});

describe("change role", () => {
  it("promotes a member with the right body and refreshes", async () => {
    setRole.mockResolvedValue({ role: "admin", changed: true });
    render(<Page />);
    fireEvent.click(await screen.findByRole("button", { name: "Make admin: Bob" }));
    const dialog = screen.getByRole("dialog");
    expect(setRole).not.toHaveBeenCalled();
    const calls = wire.mock.calls.length;
    fireEvent.click(within(dialog).getByRole("button", { name: "Make admin" }));
    await waitFor(() => expect(setRole).toHaveBeenCalledWith("tok", "u_bob", "admin"));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    await waitFor(() => expect(wire.mock.calls.length).toBeGreaterThan(calls));
  });

  it("demotes an admin to member", async () => {
    setRole.mockResolvedValue({ role: "member", changed: true });
    render(<Page />);
    fireEvent.click(await screen.findByRole("button", { name: "Make member: Cy" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Make member" }));
    await waitFor(() => expect(setRole).toHaveBeenCalledWith("tok", "u_cy", "member"));
  });

  it("shows the last-admin copy in the dialog", async () => {
    setRole.mockRejectedValue(new ApiError("last_admin", 409));
    render(<Page />);
    fireEvent.click(await screen.findByRole("button", { name: "Make member: Cy" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Make member" }));
    expect(await within(screen.getByRole("dialog")).findByText(/at least one admin/)).toBeInTheDocument();
  });

  it("does nothing on Cancel", async () => {
    render(<Page />);
    fireEvent.click(await screen.findByRole("button", { name: "Make admin: Bob" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Cancel" }));
    expect(setRole).not.toHaveBeenCalled();
  });
});
