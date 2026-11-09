// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "@/lib/api";
import AddinApprovePage from "@/app/(app)/addin/approve/page";

const m = vi.hoisted(() => ({ query: new URLSearchParams(), approve: vi.fn(), pairInfo: vi.fn(), approveMicrosoft: vi.fn() }));
vi.mock("next/navigation", () => ({ useSearchParams: () => m.query }));
vi.mock("@clerk/nextjs", () => ({ useUser: () => ({ user: { primaryEmailAddress: { emailAddress: "ada@acme.test" } } }) }));
vi.mock("@/lib/providers", () => ({ useApp: () => ({ authReady: true, getAuthToken: () => "clerk-token" }) }));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  apiRequest: (path: string, token: string) => { const url = new URL(path, "http://wire.test"); if (url.pathname !== "/v1/addin/pair/info") throw new Error(`Unexpected page read ${path}`); return m.pairInfo(token, url.searchParams.get("code")); },
  api: { addin: { approve: m.approve, pairInfo: m.pairInfo, approveMicrosoft: m.approveMicrosoft } },
}));

const workspaces = [{ account_id: "acct-p", name: "Personal", kind: "personal" }, { account_id: "acct-o", name: "Acme ISD", kind: "org" }];

beforeEach(() => { vi.clearAllMocks(); m.query = new URLSearchParams(); });
afterEach(cleanup);

describe("Gmail branch is unchanged", () => {
  it("renders the Gmail copy and approves with the code alone (no workspace, no pair/info call)", async () => {
    m.query = new URLSearchParams("code=GMAIL123");
    m.approve.mockResolvedValue({ approved: true });
    render(<AddinApprovePage />);
    expect(await screen.findByText("Connect the Gmail Add-on")).toBeVisible();
    expect(screen.getByText("GMAIL123")).toBeVisible();
    expect(screen.getByText("Approving as ada@acme.test")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    expect(await screen.findByText(/Go back to Gmail and tap/)).toBeVisible();
    expect(m.approve).toHaveBeenCalledWith("clerk-token", "GMAIL123");
    expect(m.pairInfo).not.toHaveBeenCalled();
    expect(m.approveMicrosoft).not.toHaveBeenCalled();
  });

  it("treats an unknown kind as Gmail", async () => {
    m.query = new URLSearchParams("code=X1&kind=something-else");
    render(<AddinApprovePage />);
    expect(await screen.findByText("Connect the Gmail Add-on")).toBeVisible();
    expect(m.pairInfo).not.toHaveBeenCalled();
  });
});

describe("kind=microsoft", () => {
  it("loads pair/info, makes the user pick a workspace, then approves with account_id", async () => {
    m.query = new URLSearchParams("code=MSFT1234&kind=microsoft");
    m.pairInfo.mockResolvedValue({ kind: "microsoft", workspaces });
    m.approveMicrosoft.mockResolvedValue({ approved: true });
    render(<AddinApprovePage />);
    expect(await screen.findByText("Connect CoralSwarm in Teams and Outlook")).toBeVisible();
    expect(m.pairInfo).toHaveBeenCalledWith("clerk-token", "MSFT1234");
    expect(screen.getByText("MSFT1234")).toBeVisible();
    const approve = await screen.findByRole("button", { name: "Approve" });
    expect(approve).toBeDisabled(); // two workspaces: an explicit pick is required
    fireEvent.click(screen.getByRole("radio", { name: /Acme ISD/ }));
    expect(screen.getByText("Approving as ada@acme.test · Acme ISD")).toBeVisible();
    fireEvent.click(approve);
    expect(await screen.findByText(/Approved for ada@acme.test in Acme ISD/)).toBeVisible();
    expect(m.approveMicrosoft).toHaveBeenCalledWith("clerk-token", "MSFT1234", "acct-o");
    expect(m.approve).not.toHaveBeenCalled();
  });

  it("preselects the only workspace", async () => {
    m.query = new URLSearchParams("code=ONE&kind=microsoft");
    m.pairInfo.mockResolvedValue({ kind: "microsoft", workspaces: [workspaces[0]] });
    render(<AddinApprovePage />);
    expect(await screen.findByRole("button", { name: "Approve" })).toBeEnabled();
  });

  it("says an expired code is expired", async () => {
    m.query = new URLSearchParams("code=OLD&kind=microsoft");
    m.pairInfo.mockRejectedValue(new ApiError("gone", 404));
    render(<AddinApprovePage />);
    expect(await screen.findByText(/pairing code has expired/)).toBeVisible();
    expect(screen.queryByRole("button", { name: "Approve" })).not.toBeInTheDocument();
  });

  it("refuses a code that belongs to another kind of pairing", async () => {
    m.query = new URLSearchParams("code=GM&kind=microsoft");
    m.pairInfo.mockResolvedValue({ kind: "gmail", workspaces: [] });
    render(<AddinApprovePage />);
    expect(await screen.findByText(/isn't for the Teams or Outlook add-ins/)).toBeVisible();
    expect(screen.queryByRole("button", { name: "Approve" })).not.toBeInTheDocument();
  });

  it("shows the backend's message when approval fails", async () => {
    m.query = new URLSearchParams("code=FAIL&kind=microsoft");
    m.pairInfo.mockResolvedValue({ kind: "microsoft", workspaces: [workspaces[1]] });
    m.approveMicrosoft.mockRejectedValue(new ApiError("not a member of that workspace", 403));
    render(<AddinApprovePage />);
    fireEvent.click(await screen.findByRole("button", { name: "Approve" }));
    expect(await screen.findByText("not a member of that workspace")).toBeVisible();
  });
});
