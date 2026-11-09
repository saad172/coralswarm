// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { DeleteAccountZone } from "./DeleteAccountZone";
import { ApiError } from "@/lib/api";

const h = vi.hoisted(() => ({ role: "personal", assign: vi.fn(), verify: vi.fn() }));

vi.mock("@/lib/api", async () => {
  const actual = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
  return { ...actual, apiRequest: (path: string, token: string) => { if (path !== "/v1/me/delete-account") throw new Error(`Unexpected page read ${path}`); return api.meDeleteAccount.get(token); }, api: { meDeleteAccount: { get: vi.fn(), post: vi.fn() } } };
});
vi.mock("@/lib/providers", () => ({
  useApp: () => ({ getAuthToken: () => "tok", authReady: true, queryScope: "u::p" }),
}));
// Emulates Clerk: a fetcher result carrying a reverification hint opens the
// verify modal (h.verify), then the fetcher runs once more.
vi.mock("@clerk/nextjs", () => ({
  useReverification: (fn: (...a: unknown[]) => Promise<unknown>) => async (...a: unknown[]) => {
    const r = await fn(...a);
    if (r && typeof r === "object" && "clerk_error" in r) {
      await h.verify();
      return fn(...a);
    }
    return r;
  },
}));
vi.mock("./useSettingsRole", () => ({
  useSettingsRole: () => ({ role: h.role, loaded: true, orgName: null, orgGoverned: false, isAdmin: false }),
}));

import { api } from "@/lib/api";
const getMock = api.meDeleteAccount.get as unknown as ReturnType<typeof vi.fn>;
const postMock = api.meDeleteAccount.post as unknown as ReturnType<typeof vi.fn>;

const eligible = {
  eligible: true,
  email: "Ada@Example.com",
  reverification_required: false,
  blocking_organizations: [],
  shared_account_members: [],
  deletion: null,
};
const started = { id: "d1", status: "grace", deletion: {} };

beforeEach(() => {
  h.role = "personal";
  h.verify.mockResolvedValue(undefined);
  Object.defineProperty(window, "location", { configurable: true, value: { assign: h.assign } });
});
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

async function openDialog() {
  getMock.mockResolvedValue(eligible);
  render(<DeleteAccountZone />);
  fireEvent.click(await screen.findByRole("button", { name: "Delete account" }));
}
const confirmBtn = () => screen.getByRole("button", { name: "Schedule deletion" });
const type = (v: string) => fireEvent.change(screen.getByRole("textbox"), { target: { value: v } });
const submit = async () => {
  type("ada@example.com");
  fireEvent.click(confirmBtn());
};

describe("DeleteAccountZone", () => {
  it("is hidden for team-org members and does not fetch", () => {
    h.role = "org-member";
    render(<DeleteAccountZone />);
    expect(screen.queryByText("Delete account")).toBeNull();
    expect(getMock).not.toHaveBeenCalled();
  });

  it("lists blocking orgs and offers no button when ineligible", async () => {
    getMock.mockResolvedValue({
      ...eligible,
      eligible: false,
      blocking_organizations: [
        { account_id: "1", clerk_org_id: "o1", slug: "acme", role: "owner" },
        { account_id: "2", clerk_org_id: "o2", slug: "globex", role: "member" },
      ],
    });
    render(<DeleteAccountZone />);
    expect(await screen.findByText("acme")).toBeInTheDocument();
    expect(screen.getByText("globex")).toBeInTheDocument();
    expect(screen.getByText(/transfer ownership or delete the organization/)).toBeInTheDocument();
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("lists co-members of a shared personal account when ineligible", async () => {
    getMock.mockResolvedValue({
      ...eligible,
      eligible: false,
      shared_account_members: [{ account_id: "a", user_id: "u1", email: "bob@x.co", name: "Bob", role: "member" }],
    });
    render(<DeleteAccountZone />);
    expect(await screen.findByText(/Other people use this account: Bob\. Remove them first\./)).toBeInTheDocument();
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("explains the scheduled deletion in the dialog", async () => {
    await openDialog();
    expect(screen.getByText(/connected apps and devices are signed out/)).toBeInTheDocument();
    expect(screen.getByText(/current billing period is not refunded/)).toBeInTheDocument();
    expect(screen.getByText(/In 7 days/)).toBeInTheDocument();
    expect(screen.getByText(/extra usage you bought is forfeited/)).toBeInTheDocument();
  });

  it("keeps confirm disabled until the typed email matches (case/trim-insensitive)", async () => {
    await openDialog();
    expect(confirmBtn()).toBeDisabled();
    type("nope@example.com");
    expect(confirmBtn()).toBeDisabled();
    type("  ada@example.COM ");
    expect(confirmBtn()).toBeEnabled();
  });

  it("goes to the scheduled page after a 202 without signing out", async () => {
    postMock.mockResolvedValue(started);
    await openDialog();
    await submit();
    await waitFor(() => expect(h.assign).toHaveBeenCalledWith("/account/delete/pending"));
    expect(postMock).toHaveBeenCalledWith("tok", "ada@example.com");
  });

  it("re-verifies on reverification_required, then retries once", async () => {
    postMock
      .mockRejectedValueOnce(new ApiError("x", 403, "reverification_required"))
      .mockResolvedValueOnce(started);
    await openDialog();
    await submit();
    await waitFor(() => expect(h.assign).toHaveBeenCalledWith("/account/delete/pending"));
    expect(h.verify).toHaveBeenCalledOnce();
    expect(postMock).toHaveBeenCalledTimes(2);
  });

  it("shows an error when re-verification is cancelled", async () => {
    postMock.mockRejectedValue(new ApiError("x", 403, "reverification_required"));
    h.verify.mockRejectedValue(Object.assign(new Error("cancelled"), { code: "reverification_cancelled" }));
    await openDialog();
    await submit();
    expect(await screen.findByText(/Verification was cancelled/)).toBeInTheDocument();
    expect(h.assign).not.toHaveBeenCalled();
  });

  it.each([
    [new ApiError("x", 400, "confirmation_mismatch"), /doesn't match/],
    [new ApiError("x", 403, "session_required"), /sign out and sign back in/],
    [new ApiError("x", 409, "team_org_membership", null, null, { organizations: [] }), /still a member of an organization/],
    [
      new ApiError("x", 409, "shared_personal_account", null, null, {
        members: [{ account_id: "a", user_id: "u", email: "bob@x.co", name: "Bob", role: "member" }],
      }),
      /Other people use this account: Bob\. Remove them first/,
    ],
    [new ApiError("x", 500, null), /Couldn't schedule the deletion/],
  ])("renders the %# error in the dialog", async (error, text) => {
    postMock.mockRejectedValue(error);
    await openDialog();
    await submit();
    expect(await screen.findByText(text)).toBeInTheDocument();
    expect(h.assign).not.toHaveBeenCalled();
    expect(confirmBtn()).toBeEnabled();
  });
});
