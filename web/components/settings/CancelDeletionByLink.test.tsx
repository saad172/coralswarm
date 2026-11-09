// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { CancelDeletionByLink } from "./CancelDeletionByLink";
import { ApiError } from "@/lib/api";

vi.mock("@/lib/api", async () => {
  const actual = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
  return { ...actual, api: { accountDeletion: { cancelWithToken: vi.fn() } } };
});
import { api } from "@/lib/api";
const cancelMock = api.accountDeletion.cancelWithToken as unknown as ReturnType<typeof vi.fn>;

const open = (url: string) => window.history.replaceState({}, "", url);
const clickCancel = async () => fireEvent.click(await screen.findByRole("button", { name: "Cancel deletion" }));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  open("/");
});

describe("CancelDeletionByLink", () => {
  it("does not cancel on page load; only after a click, with the hash token", async () => {
    open("/account/delete/cancel#token=abc");
    cancelMock.mockResolvedValue({ status: "cancelled" });
    render(<CancelDeletionByLink />);
    expect(await screen.findByText("Cancel account deletion?")).toBeInTheDocument();
    expect(cancelMock).not.toHaveBeenCalled();
    await clickCancel();
    expect(await screen.findByText(/Your account is restored\. Your subscription stays cancelled — resubscribe from Billing\./)).toBeInTheDocument();
    expect(cancelMock).toHaveBeenCalledTimes(1);
    expect(cancelMock).toHaveBeenCalledWith("abc");
  });

  it("clears the token from the URL after reading it", async () => {
    open("/account/delete/cancel#token=abc");
    render(<CancelDeletionByLink />);
    await screen.findByText("Cancel account deletion?");
    expect(window.location.hash).toBe("");
    expect(window.location.href).not.toContain("abc");
  });

  it("does not accept a ?token= query string", async () => {
    open("/account/delete/cancel?token=abc");
    render(<CancelDeletionByLink />);
    expect(await screen.findByText(/isn't valid or has already been used/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Cancel deletion" })).toBeNull();
    expect(cancelMock).not.toHaveBeenCalled();
  });

  it.each([
    ["invalid_token", 400, /isn't valid or has already been used/],
    ["too_late", 409, /too late to cancel/],
    ["no_pending_deletion", 404, /no deletion to cancel/],
  ])("renders %s after the click", async (code, status, text) => {
    open("/account/delete/cancel#token=abc");
    cancelMock.mockRejectedValue(new ApiError("x", status, code));
    render(<CancelDeletionByLink />);
    await clickCancel();
    expect(await screen.findByText(text)).toBeInTheDocument();
  });

  it("treats a missing token as an invalid link", async () => {
    open("/account/delete/cancel");
    render(<CancelDeletionByLink />);
    expect(await screen.findByText(/isn't valid or has already been used/)).toBeInTheDocument();
    expect(cancelMock).not.toHaveBeenCalled();
  });
});
