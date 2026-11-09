// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { DeletionStatus } from "./DeletionStatus";
import { ApiError } from "@/lib/api";

vi.mock("@/lib/api", async () => {
  const actual = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
  return { ...actual, apiRequest: (path: string, token: string) => { if (path !== "/v1/me/delete-account") throw new Error(`Unexpected page read ${path}`); return api.meDeleteAccount.get(token); }, api: { meDeleteAccount: { get: vi.fn(), cancel: vi.fn() } } };
});
const getToken = vi.fn();
vi.mock("@clerk/nextjs", () => ({ useAuth: () => ({ isLoaded: true, isSignedIn: true, getToken }) }));

import { api } from "@/lib/api";
const getMock = api.meDeleteAccount.get as unknown as ReturnType<typeof vi.fn>;
const cancelMock = api.meDeleteAccount.cancel as unknown as ReturnType<typeof vi.fn>;

const base = { eligible: false, email: "a@b.co", reverification_required: false, blocking_organizations: [], shared_account_members: [] };
const grace = { id: "d", state: "grace", purge_after: "2026-11-02T12:00:00Z", can_cancel: true, current_step: null, steps: [], retrying: false };

beforeEach(() => getToken.mockResolvedValue("tok"));
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("DeletionStatus", () => {
  it("shows the scheduled state with the date and cancels", async () => {
    getMock.mockResolvedValue({ ...base, deletion: grace });
    cancelMock.mockResolvedValue({ status: "cancelled" });
    render(<DeletionStatus />);
    expect(await screen.findByText("Deletion scheduled")).toBeInTheDocument();
    expect(screen.getByText(/2026/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Cancel deletion" }));
    expect(await screen.findByText(/Your account is restored\. Your subscription stays cancelled/)).toBeInTheDocument();
    expect(cancelMock).toHaveBeenCalledWith("tok");
  });

  it("explains a too-late cancel", async () => {
    getMock.mockResolvedValue({ ...base, deletion: grace });
    cancelMock.mockRejectedValue(new ApiError("x", 409, "too_late"));
    render(<DeletionStatus />);
    fireEvent.click(await screen.findByRole("button", { name: "Cancel deletion" }));
    await waitFor(() => expect(screen.getByText(/too late to cancel/)).toBeInTheDocument());
  });

  it("shows a running deletion with no cancel button", async () => {
    getMock.mockResolvedValue({ ...base, deletion: { ...grace, state: "running", can_cancel: false } });
    render(<DeletionStatus />);
    expect(await screen.findByText("Your account is being deleted")).toBeInTheDocument();
    expect(screen.queryByRole("button")).toBeNull();
  });
});
