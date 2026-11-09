// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type OrgSetupStatus } from "@/lib/api";
import { TeamSetupStatus, type TeamSetupStatusProps } from "./TeamSetupStatus";

const mocks = vi.hoisted(() => ({ status: vi.fn() }));
vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  apiRequest: (path: string, token: string) => { const match = /^\/v1\/orgs\/([^/]+)\/provisioning$/.exec(path); if (!match) throw new Error(`Unexpected page read ${path}`); return mocks.status(token, decodeURIComponent(match[1])); },
  api: { orgSelfserve: { status: mocks.status } },
}));

function st(p: Partial<OrgSetupStatus>): OrgSetupStatus {
  return {
    org_id: "org_new",
    status: "provisioning",
    step: "migrate",
    waiting_for: null,
    retryable: false,
    resume_payment_until: null,
    message: "",
    ...p,
  };
}

function setup(p: Partial<TeamSetupStatusProps> = {}) {
  const props: TeamSetupStatusProps = {
    ctx: { getAuthToken: () => "personal-token", scope: "user::personal" },
    orgId: "org_new",
    attempt: { key: "test-key-1", name: "Acme", seats: 3 },
    page: "return",
    priceCents: 2000,
    retry: vi.fn(),
    trialCheckout: vi.fn(),
    openTeam: vi.fn().mockResolvedValue(undefined),
    goToCheckout: vi.fn(),
    goTo: vi.fn(),
    returnUrl: (path, q) => `https://app.test${path}?${q}`,
    onReady: vi.fn(),
    ...p,
  };
  render(<TeamSetupStatus {...props} />);
  return props;
}

afterEach(() => {
  cleanup();
  mocks.status.mockReset();
});

describe("TeamSetupStatus", () => {
  it("polls with the CURRENT session's token and shows the step", async () => {
    mocks.status.mockResolvedValue(st({ status: "provisioning", step: "create_database" }));
    setup();
    expect(await screen.findByText(/Creating its own private database/)).toBeInTheDocument();
    expect(mocks.status).toHaveBeenCalledWith("personal-token", "org_new");
  });

  it("ready: switches into the team once", async () => {
    mocks.status.mockResolvedValue(st({ status: "ready" }));
    const p = setup();
    await waitFor(() => expect(p.openTeam).toHaveBeenCalledWith("org_new"));
    expect(p.onReady).toHaveBeenCalledTimes(1);
    expect(p.openTeam).toHaveBeenCalledTimes(1);
  });

  it("ready but the switch fails: says so and offers to open it", async () => {
    mocks.status.mockResolvedValue(st({ status: "ready" }));
    const openTeam = vi.fn().mockRejectedValueOnce(new Error("clerk")).mockResolvedValueOnce(undefined);
    setup({ openTeam });
    expect(await screen.findByText(/couldn't switch you into it/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /^open/i }));
    await waitFor(() => expect(openTeam).toHaveBeenCalledTimes(2));
  });

  it("failed + retryable: Retry calls the retry route", async () => {
    mocks.status.mockResolvedValue(st({ status: "failed", retryable: true }));
    const retry = vi.fn().mockResolvedValue(st({ status: "provisioning", step: "queued" }));
    setup({ retry });
    fireEvent.click(await screen.findByRole("button", { name: /retry setup/i }));
    await waitFor(() => expect(retry).toHaveBeenCalledWith("org_new"));
  });

  it("failed + retryable: a rate-limited retry says when", async () => {
    mocks.status.mockResolvedValue(st({ status: "failed", retryable: true }));
    const retry = vi.fn().mockRejectedValue(new ApiError("x", 429, "retry_rate_limited", null, null, { retry_after_s: 300 }));
    setup({ retry });
    fireEvent.click(await screen.findByRole("button", { name: /retry setup/i }));
    expect(await screen.findByText(/try again in about 5 minutes/)).toBeInTheDocument();
  });

  it("cancel page + awaiting_payment: Resume payment opens a new Checkout for the same seats", async () => {
    mocks.status.mockResolvedValue(
      st({ status: "awaiting_payment", step: null, resume_payment_until: new Date(Date.now() + 3600_000).toISOString() }),
    );
    const trialCheckout = vi.fn().mockResolvedValue({
      checkout_url: "https://checkout.stripe.com/c/pay/cs_3",
      session_id: "cs_3",
      trial: true,
      seats: 3,
      trial_credits: 3000,
      trial_days: 90,
    });
    const p = setup({ page: "canceled", trialCheckout });
    fireEvent.click(await screen.findByRole("button", { name: /resume payment/i }));
    await waitFor(() => expect(p.goToCheckout).toHaveBeenCalledWith("https://checkout.stripe.com/c/pay/cs_3"));
    expect(trialCheckout.mock.calls[0][0]).toBe("org_new");
    expect(trialCheckout.mock.calls[0][1]).toMatchObject({ seats: 3 });
    expect(trialCheckout.mock.calls[0][1].success_url).toContain("/onboarding/team/return?k=test-key-1");
  });

  it("Resume payment uses the status body's seats, even with no attempt (?org= page)", async () => {
    mocks.status.mockResolvedValue(
      st({ status: "awaiting_payment", step: null, seats: 7, resume_payment_until: new Date(Date.now() + 3600_000).toISOString() }),
    );
    const trialCheckout = vi.fn().mockResolvedValue({
      checkout_url: "https://checkout.stripe.com/c/pay/cs_4",
      session_id: "cs_4",
      trial: true,
      seats: 7,
      trial_credits: 3000,
      trial_days: 90,
    });
    const p = setup({ page: "canceled", trialCheckout, attempt: null });
    fireEvent.click(await screen.findByRole("button", { name: /resume payment/i }));
    await waitFor(() => expect(p.goToCheckout).toHaveBeenCalledWith("https://checkout.stripe.com/c/pay/cs_4"));
    expect(trialCheckout.mock.calls[0][1]).toMatchObject({ seats: 7 });
    expect(trialCheckout.mock.calls[0][1].success_url).toContain("org=org_new");
  });

  it("Resume payment prefers the server's seats over the stored attempt", async () => {
    mocks.status.mockResolvedValue(
      st({ status: "awaiting_payment", step: null, seats: 5, resume_payment_until: new Date(Date.now() + 3600_000).toISOString() }),
    );
    const trialCheckout = vi.fn().mockReturnValue(new Promise(() => {}));
    setup({ page: "canceled", trialCheckout });
    fireEvent.click(await screen.findByRole("button", { name: /resume payment/i }));
    await waitFor(() => expect(trialCheckout).toHaveBeenCalled());
    expect(trialCheckout.mock.calls[0][1]).toMatchObject({ seats: 5 });
  });

  it("payment_abandoned: Start again, no Resume", async () => {
    mocks.status.mockResolvedValue(st({ status: "payment_abandoned", step: null }));
    const p = setup();
    fireEvent.click(await screen.findByRole("button", { name: /start again/i }));
    expect(p.goTo).toHaveBeenCalledWith("/onboarding/team");
    expect(screen.queryByRole("button", { name: /resume payment/i })).toBeNull();
  });

  it("waiting_for=update says we're updating", async () => {
    mocks.status.mockResolvedValue(st({ status: "provisioning", waiting_for: "update" }));
    setup();
    expect(await screen.findByText(/We're updating, this takes a few minutes/)).toBeInTheDocument();
  });

  it("S2: ready on a team this browser didn't start never switches by itself", async () => {
    mocks.status.mockResolvedValue(st({ status: "ready" }));
    const p = setup({ owned: false, attempt: null });
    const open = await screen.findByRole("button", { name: /open this team/i });
    expect(p.openTeam).not.toHaveBeenCalled();
    expect(p.onReady).not.toHaveBeenCalled();
    fireEvent.click(open);
    await waitFor(() => expect(p.openTeam).toHaveBeenCalledWith("org_new"));
  });

  it("S2: awaiting_payment on a team this browser didn't start offers no payment", async () => {
    mocks.status.mockResolvedValue(
      st({ status: "awaiting_payment", step: null, seats: 3, resume_payment_until: new Date(Date.now() + 3600_000).toISOString() }),
    );
    const p = setup({ owned: false, attempt: null, page: "canceled" });
    expect(await screen.findByText(/can't be finished here/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /resume payment/i })).toBeNull();
    expect(screen.getByRole("button", { name: /open this team/i })).toBeInTheDocument();
    expect(p.trialCheckout).not.toHaveBeenCalled();
  });

  it("no_pending_checkout says there's nothing to resume, not that it's paid", async () => {
    mocks.status.mockResolvedValue(
      st({ status: "awaiting_payment", step: null, seats: 3, resume_payment_until: new Date(Date.now() + 3600_000).toISOString() }),
    );
    const trialCheckout = vi.fn().mockRejectedValue(new ApiError("x", 409, "no_pending_checkout"));
    setup({ page: "canceled", trialCheckout });
    fireEvent.click(await screen.findByRole("button", { name: /resume payment/i }));
    expect(await screen.findByText(/no sign-up waiting for payment/)).toBeInTheDocument();
    expect(screen.queryByText(/went through/)).toBeNull();
  });

  it("a 401 stops polling", async () => {
    mocks.status.mockRejectedValue(new ApiError("signed out", 401, null));
    setup();
    expect(await screen.findByText(/You're signed out/)).toBeInTheDocument();
    const calls = mocks.status.mock.calls.length;
    await new Promise((r) => setTimeout(r, 2_000));
    expect(mocks.status.mock.calls.length).toBe(calls);
  });

  it("a 404 is a plain not-yours message", async () => {
    mocks.status.mockRejectedValue(new ApiError("organization not found", 404, "org_not_found"));
    setup();
    expect(await screen.findByText(/can't show this team's setup/)).toBeInTheDocument();
  });
});
