// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import type { BillingSummary } from "@/lib/api";

const summary = vi.fn();
const portal = vi.fn();
const subscribe = vi.fn();
const seatQuote = vi.fn();
const update = vi.fn();
const topUp = vi.fn();
const purchase = vi.fn();
const reduce = vi.fn();
const goToStripe = vi.fn();

vi.mock("@/lib/providers", () => ({
  useApp: () => ({ getAuthToken: () => "tok", authReady: true, queryScope: "user::org" }),
}));
vi.mock("./navigate", () => ({
  goToStripe: (...a: unknown[]) => goToStripe(...a),
  returnUrl: (path: string, query?: string) => `https://app.test${path}${query ? `?${query}` : ""}`,
}));
vi.mock("@/lib/api", async () => {
  const actual = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
  return {
    ...actual,
    apiRequest: (path: string, token: string) => { if (path !== "/v1/billing/summary") throw new Error(`Unexpected page read ${path}`); return summary(token); },
    api: {
      billing: {
        summary: (...a: unknown[]) => summary(...a),
        portal: (...a: unknown[]) => portal(...a),
        subscribe: (...a: unknown[]) => subscribe(...a),
        seatQuote: (...a: unknown[]) => seatQuote(...a),
        extraUsage: { update: (...a: unknown[]) => update(...a), topUp: (...a: unknown[]) => topUp(...a) },
      },
      orgs: { slots: { purchase: (...a: unknown[]) => purchase(...a), reduce: (...a: unknown[]) => reduce(...a) } },
    },
  };
});

import { ApiError } from "@/lib/api";
import { BillingView } from "./BillingView";

const EXTRA_OFF = {
  enabled: false,
  balance_micro_usd: 0,
  monthly_limit_micro_usd: null,
  spent_this_period_micro_usd: 0,
  member_cap_seat_multiple: null,
  auto_reload: { enabled: false, threshold_micro_usd: null, amount_micro_usd: null },
};

const ORG: BillingSummary = {
  account_kind: "org",
  plan: { code: "team_standard", name: "Team" },
  seats: { used: 4, total: 5 },
  credits: { allowance_used: 1200, allowance_total: 15000, resets_at: "2026-11-15T12:00:00Z" },
  extra_usage: {
    ...EXTRA_OFF,
    enabled: true,
    balance_micro_usd: 42_500_000,
    monthly_limit_micro_usd: 100_000_000,
    spent_this_period_micro_usd: 7_250_000,
    member_cap_seat_multiple: 1,
  },
  subscription: { status: "active", has_billing_account: true, renews_at: "2026-11-15T12:00:00Z", cancel_at_period_end: false, cancel_at: null },
  payment_method: { type: "card", brand: "visa", last4: "4242", exp_month: 4, exp_year: 2028 },
};

const PRO: BillingSummary = {
  ...ORG,
  account_kind: "personal",
  plan: { code: "pro", name: "Pro" },
  seats: { used: 1, total: 1 },
  extra_usage: { ...EXTRA_OFF, enabled: false },
};

const FREE: BillingSummary = {
  ...PRO,
  plan: null,
  extra_usage: EXTRA_OFF,
  subscription: { status: null, has_billing_account: false, renews_at: null, cancel_at_period_end: false, cancel_at: null },
  payment_method: null,
};

const FROZEN: BillingSummary = {
  ...PRO,
  plan: { code: "free", name: "Free" },
  extra_usage: { ...EXTRA_OFF, enabled: true, balance_micro_usd: 30_000_000 },
  subscription: { status: "canceled", has_billing_account: true, renews_at: null, cancel_at_period_end: false, cancel_at: null },
};

const CANCELLING: BillingSummary = {
  ...PRO,
  subscription: { status: "active", has_billing_account: true, renews_at: "2026-11-15T12:00:00Z", cancel_at_period_end: true, cancel_at: "2026-11-15T12:00:00Z" },
};

/** A team in its free trial: the mirror says `active` (Stripe's `trialing` is
 *  stored as `active`) and there is no plan allowance yet (#1557). */
const TRIALING: BillingSummary = {
  ...ORG,
  seats: { used: 2, total: 2 },
  credits: { allowance_used: 0, allowance_total: 0, resets_at: "2026-11-28T12:00:00Z" },
  extra_usage: EXTRA_OFF,
  subscription: { status: "active", has_billing_account: true, renews_at: "2027-01-26T00:00:00Z", cancel_at_period_end: false, cancel_at: null },
  trial: {
    state: "trialing",
    seats: 2,
    credits_total: 3000,
    credits_used: 1200,
    ends_at: "2027-01-26T00:00:00Z",
    payment_failed: false,
    seat_price_usd_cents: 4000,
    seat_monthly_credits: 3000,
  },
};

const TRIALING_TRIAL = TRIALING.trial as NonNullable<BillingSummary["trial"]>;

beforeEach(() => {
  for (const m of [summary, portal, subscribe, seatQuote, update, topUp, purchase, reduce, goToStripe]) m.mockReset();
  window.history.replaceState(null, "", "/");
});
afterEach(cleanup);

async function show(s: BillingSummary, path = "/settings/organization/billing") {
  summary.mockResolvedValue(s);
  render(<BillingView path={path} />);
  await screen.findByText("credits");
}

describe("rendering", () => {
  it("org admin: plan, seats, credits, extra usage and card", async () => {
    await show(ORG);
    expect(screen.getByText("Team")).toBeInTheDocument();
    expect(screen.getByText("Renews on November 15, 2026.")).toBeInTheDocument();
    expect(screen.getByText("of 5 seats in use")).toBeInTheDocument();
    expect(screen.getByText("of 15,000 plan credits used")).toBeInTheDocument();
    expect(screen.getByText("Resets on November 15, 2026.")).toBeInTheDocument();
    expect(screen.getByText("$42.50")).toBeInTheDocument();
    expect(screen.getByText("$7.25")).toBeInTheDocument();
    expect(screen.getByLabelText(/Monthly limit/)).toHaveValue("100");
    expect(screen.getByLabelText(/Per-member limit/)).toHaveValue("1");
    expect(screen.getByText("Visa ending 4242")).toBeInTheDocument();
    expect(screen.getByText("· expires 04/2028")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Manage billing" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Subscribe" })).toBeNull();
  });

  it("trialing org: shows the free trial, credits used of 3,000, the rule and the price after", async () => {
    await show(TRIALING);
    expect(screen.getByText("free trial")).toBeInTheDocument();
    expect(screen.queryByText("active")).toBeNull();
    expect(screen.queryByText(/Renews on/)).toBeNull();
    expect(
      screen.getByText(
        "You're on a free trial. It converts to a paid subscription when the trial credits run out, or on January 26, 2027, whichever comes first.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("of 3,000 trial credits used")).toBeInTheDocument();
    expect(screen.getByText("1,200")).toBeInTheDocument();
    expect(screen.getByText("1,800 trial credits left.")).toBeInTheDocument();
    expect(screen.queryByText(/0 of 0|plan credits used/)).toBeNull();
    expect(
      screen.getByText("After the trial: 2 seats at $40.00 per seat per month, $80.00 a month. Each seat then gets 3,000 credits a month."),
    ).toBeInTheDocument();
  });

  it("trialing org: omits the price line when the seat price is unknown", async () => {
    await show({ ...TRIALING, trial: { ...TRIALING_TRIAL, seat_price_usd_cents: null } });
    expect(screen.getByText("free trial")).toBeInTheDocument();
    expect(screen.queryByText(/After the trial/)).toBeNull();
  });

  it("trialing org: prices the seats bought during the trial, not the Checkout's count", async () => {
    await show({ ...TRIALING, seats: { used: 3, total: 3 } });
    expect(
      screen.getByText("After the trial: 3 seats at $40.00 per seat per month, $120.00 a month. Each seat then gets 3,000 credits a month."),
    ).toBeInTheDocument();
  });

  it("trialing org with a scheduled cancel: no free-trial conversion copy", async () => {
    await show({ ...TRIALING, subscription: { ...TRIALING.subscription!, cancel_at: "2026-12-01T00:00:00Z" } });
    expect(screen.queryByText("free trial")).toBeNull();
    expect(screen.queryByText(/converts to a paid subscription/)).toBeNull();
    expect(screen.queryByText(/After the trial/)).toBeNull();
  });

  it("a converted trial reads as an ordinary active plan", async () => {
    await show({ ...ORG, trial: { ...TRIALING_TRIAL, state: "converted" } });
    expect(screen.getByText("active")).toBeInTheDocument();
    expect(screen.getByText("of 15,000 plan credits used")).toBeInTheDocument();
    expect(screen.queryByText("free trial")).toBeNull();
  });

  it("renders a bank account default as the bank name and last four", async () => {
    await show({ ...PRO, payment_method: { type: "us_bank_account", bank_name: "Example Bank", last4: "4242" } }, "/settings/billing");
    expect(screen.getByText("Example Bank •••• 4242")).toBeInTheDocument();
    expect(screen.queryByText(/No payment method on file/)).toBeNull();
    expect(screen.queryByText(/expires/)).toBeNull();
  });

  it("renders a Link default as Link with its email", async () => {
    await show({ ...PRO, payment_method: { type: "link", email: "a@example.com" } }, "/settings/billing");
    expect(screen.getByText("Link")).toBeInTheDocument();
    expect(screen.getByText("· a@example.com")).toBeInTheDocument();
  });

  it("Pro: no seats section and no member limit", async () => {
    await show(PRO, "/settings/billing");
    expect(screen.getByText("Pro")).toBeInTheDocument();
    expect(screen.queryByText("seats")).toBeNull();
    expect(screen.getByRole("switch", { name: "Extra usage" })).toHaveAttribute("aria-checked", "false");
    expect(screen.queryByLabelText(/Per-member limit/)).toBeNull();
  });

  it("Free: offers Subscribe, hides extra-usage controls, no card", async () => {
    await show(FREE, "/settings/billing");
    expect(screen.getByText("Free")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Subscribe to Pro" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Manage billing" })).toBeNull();
    expect(screen.getByText(/Extra usage is available on paid plans/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Save extra usage" })).toBeNull();
    expect(screen.getByText("No payment method on file.")).toBeInTheDocument();
  });

  it("frozen: shows the held balance and pauses extra usage until resubscribing", async () => {
    await show(FROZEN, "/settings/billing");
    expect(screen.getByText(/\$30\.00 of extra-usage credits are on hold/)).toBeInTheDocument();
    expect(screen.getByText("Extra usage is paused until you resubscribe.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Add funds" })).toBeNull();
    expect(screen.getByRole("button", { name: "Subscribe to Pro" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Manage billing" })).toBeInTheDocument();
  });

  it("cancel scheduled: says when it ends and what happens to credits", async () => {
    await show(CANCELLING, "/settings/billing");
    expect(screen.getByText("Ends on November 15, 2026.")).toBeInTheDocument();
    expect(screen.getByText("ending")).toBeInTheDocument();
    expect(screen.getByText(/set to end on November 15, 2026/)).toBeInTheDocument();
    expect(screen.getByText(/come back if you resubscribe/)).toBeInTheDocument();
    expect(screen.queryByText("active")).toBeNull();
  });

  it("uses no ocean, reef or coral words", async () => {
    await show(ORG);
    expect(document.body.textContent).not.toMatch(/ocean|reef|coral/i);
  });

  it("explains a members' 403 in plain words", async () => {
    summary.mockRejectedValue(new ApiError("forbidden", 403, "admin_required"));
    render(<BillingView path="/x" />);
    expect(await screen.findByText("Only an admin can see billing.")).toBeInTheDocument();
  });
});

describe("Stripe redirects", () => {
  it("Subscribe starts Checkout for one standard seat (org) and redirects", async () => {
    await show({ ...ORG, subscription: FREE.subscription, plan: null });
    subscribe.mockResolvedValue({ checkout_url: "https://stripe.test/c", session_id: "s" });
    fireEvent.click(screen.getByRole("button", { name: "Subscribe" }));
    await waitFor(() => expect(goToStripe).toHaveBeenCalledWith("https://stripe.test/c"));
    expect(subscribe).toHaveBeenCalledWith("tok", {
      tier: "standard",
      successUrl: "https://app.test/settings/organization/billing?checkout=success",
      cancelUrl: "https://app.test/settings/organization/billing",
    });
  });

  it("Subscribe buys Pro for a personal account", async () => {
    await show(FREE, "/settings/billing");
    subscribe.mockResolvedValue({ checkout_url: "https://stripe.test/c", session_id: "s" });
    fireEvent.click(screen.getByRole("button", { name: "Subscribe to Pro" }));
    await waitFor(() => expect(subscribe).toHaveBeenCalledWith("tok", expect.objectContaining({ tier: "pro" })));
  });

  it("Manage billing opens the portal and returns here", async () => {
    await show(PRO, "/settings/billing");
    portal.mockResolvedValue({ url: "https://stripe.test/portal" });
    fireEvent.click(screen.getByRole("button", { name: "Manage billing" }));
    await waitFor(() => expect(goToStripe).toHaveBeenCalledWith("https://stripe.test/portal"));
    expect(portal).toHaveBeenCalledWith("tok", "https://app.test/settings/billing");
  });

  it.each([
    [409, "no_billing_account", /no billing account yet/],
    [503, "portal_not_configured", /portal isn't available/],
    [502, "billing_unavailable", /briefly unavailable/],
  ])("portal %s %s shows plain copy", async (status, code, copy) => {
    await show(PRO, "/settings/billing");
    portal.mockRejectedValue(new ApiError("raw", status, code));
    fireEvent.click(screen.getByRole("button", { name: "Manage billing" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(copy);
    expect(goToStripe).not.toHaveBeenCalled();
  });

  it("thanks people returning from Checkout", async () => {
    window.history.replaceState(null, "", "/?checkout=success");
    await show(PRO, "/settings/billing");
    expect(await screen.findByText(/subscription is being set up/)).toBeInTheDocument();
  });
});

const QUOTE_ADD = {
  tier: "standard",
  action: "add" as const,
  quantity: 2,
  current_quantity: 5,
  new_quantity: 7,
  currency: "usd",
  proration_cents: 1840,
  next_invoice_total_cents: 21840,
  recurring_cents_after: 20000,
  next_invoice_at: "2026-11-15T12:00:00Z",
};

describe("seat changes", () => {
  it("add: quote, then confirm shows proration and next invoice, and only then buys", async () => {
    await show(ORG);
    seatQuote.mockResolvedValue(QUOTE_ADD);
    purchase.mockResolvedValue({});
    fireEvent.change(screen.getByLabelText("Number of seats"), { target: { value: "2" } });
    fireEvent.click(screen.getByRole("button", { name: "Add seats" }));
    const dialog = await screen.findByRole("dialog");
    expect(seatQuote).toHaveBeenCalledWith("tok", { quantity: 2, action: "add", tier: "standard" });
    expect(purchase).not.toHaveBeenCalled();
    expect(dialog).toHaveTextContent("5 → 7");
    expect(dialog).toHaveTextContent("Prorated charge$18.40");
    expect(dialog).toHaveTextContent("Next invoice, November 15, 2026$218.40");
    expect(dialog).toHaveTextContent("Each invoice after that$200.00");
    expect(dialog).toHaveTextContent("Nothing is charged right now");
    fireEvent.click(within(dialog).getByRole("button", { name: "Add 2 seats" }));
    await waitFor(() => expect(purchase).toHaveBeenCalledTimes(1));
    expect(purchase).toHaveBeenCalledWith("tok", { tier: "standard", quantity: 2, idempotencyKey: expect.any(String) });
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(screen.getByText("Added 2 seats. You now have 7.")).toBeInTheDocument();
  });

  it("cancel in the dialog changes nothing", async () => {
    await show(ORG);
    seatQuote.mockResolvedValue(QUOTE_ADD);
    fireEvent.click(screen.getByRole("button", { name: "Add seats" }));
    const dialog = await screen.findByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    expect(purchase).not.toHaveBeenCalled();
    expect(reduce).not.toHaveBeenCalled();
  });

  it("reduce: a credit is described as a credit and uses the reduce endpoint", async () => {
    await show(ORG);
    seatQuote.mockResolvedValue({
      ...QUOTE_ADD, action: "reduce", quantity: 1, new_quantity: 4, proration_cents: -920, next_invoice_total_cents: 19080, recurring_cents_after: 16000,
    });
    reduce.mockResolvedValue({});
    fireEvent.click(screen.getByRole("button", { name: "Reduce seats" }));
    const dialog = await screen.findByRole("dialog");
    expect(seatQuote).toHaveBeenCalledWith("tok", expect.objectContaining({ action: "reduce" }));
    expect(dialog).toHaveTextContent("Prorated credit$9.20");
    fireEvent.click(within(dialog).getByRole("button", { name: "Remove 1 seat" }));
    await waitFor(() => expect(reduce).toHaveBeenCalledWith("tok", expect.objectContaining({ tier: "standard", quantity: 1 })));
    expect(purchase).not.toHaveBeenCalled();
  });

  it("seats in use: quote error says how many are free", async () => {
    await show(ORG);
    const e = new ApiError("raw", 409, "seats_in_use");
    e.available = 1;
    seatQuote.mockRejectedValue(e);
    fireEvent.click(screen.getByRole("button", { name: "Reduce seats" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Only 1 seat is free to give back");
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("keeps the dialog open and shows the error when the purchase fails", async () => {
    await show(ORG);
    seatQuote.mockResolvedValue(QUOTE_ADD);
    purchase.mockRejectedValue(new ApiError("seat_purchase_failed", 402, null, "card_declined"));
    fireEvent.click(screen.getByRole("button", { name: "Add seats" }));
    const dialog = await screen.findByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "Add 2 seats" }));
    expect(await within(dialog).findByRole("alert")).toHaveTextContent("The card was declined");
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("rejects a bad quantity before asking for a quote", async () => {
    await show(ORG);
    fireEvent.change(screen.getByLabelText("Number of seats"), { target: { value: "0" } });
    fireEvent.click(screen.getByRole("button", { name: "Add seats" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("between 1 and 500");
    expect(seatQuote).not.toHaveBeenCalled();
  });

  it("without a subscription there are no seat controls", async () => {
    await show({ ...ORG, plan: null, subscription: FREE.subscription });
    expect(screen.queryByRole("button", { name: "Add seats" })).toBeNull();
    expect(screen.getByText(/Subscribe to add seats\. You can give/)).toBeInTheDocument();
  });
});

describe("extra usage", () => {
  it("saves limits converted from dollars to micro-USD", async () => {
    await show(ORG);
    update.mockResolvedValue({ extra_usage: ORG.extra_usage });
    fireEvent.change(screen.getByLabelText(/Monthly limit/), { target: { value: "250.50" } });
    fireEvent.change(screen.getByLabelText(/Per-member limit/), { target: { value: "2.5" } });
    fireEvent.click(screen.getByRole("button", { name: "Save extra usage" }));
    await waitFor(() => expect(update).toHaveBeenCalledTimes(1));
    expect(update).toHaveBeenCalledWith("tok", {
      enabled: true,
      monthly_limit_micro_usd: 250_500_000,
      member_cap_seat_multiple: 2.5,
      auto_reload: { enabled: false },
    });
    expect(await screen.findByText("Saved.")).toBeInTheDocument();
  });

  it("enabling on Pro with blank limit lets the server apply its default", async () => {
    await show(PRO, "/settings/billing");
    update.mockResolvedValue({ extra_usage: PRO.extra_usage });
    fireEvent.click(screen.getByRole("switch", { name: "Extra usage" }));
    fireEvent.click(screen.getByRole("button", { name: "Save extra usage" }));
    await waitFor(() => expect(update).toHaveBeenCalledWith("tok", { enabled: true, auto_reload: { enabled: false } }));
  });

  it("auto-reload is off by default and sends dollars as micro-USD when turned on", async () => {
    await show(ORG);
    expect(screen.getByRole("switch", { name: "Auto-reload" })).toHaveAttribute("aria-checked", "false");
    update.mockResolvedValue({ extra_usage: ORG.extra_usage });
    fireEvent.click(screen.getByRole("switch", { name: "Auto-reload" }));
    fireEvent.change(screen.getByLabelText(/When the balance drops to/), { target: { value: "10" } });
    fireEvent.change(screen.getByLabelText(/^Add \(\$\)/), { target: { value: "50" } });
    fireEvent.click(screen.getByRole("button", { name: "Save extra usage" }));
    await waitFor(() => expect(update).toHaveBeenCalled());
    expect(update.mock.calls[0][1].auto_reload).toEqual({ enabled: true, threshold_micro_usd: 10_000_000, amount_micro_usd: 50_000_000 });
  });

  it("validates before calling the API and shows the message beside the field", async () => {
    await show(ORG);
    fireEvent.change(screen.getByLabelText(/Monthly limit/), { target: { value: "12.345" } });
    fireEvent.click(screen.getByRole("button", { name: "Save extra usage" }));
    expect(await screen.findByText(/must be in whole cents/)).toBeInTheDocument();
    expect(update).not.toHaveBeenCalled();
  });

  it("maps a server validation code onto its field in plain words", async () => {
    await show(ORG);
    update.mockRejectedValue(new ApiError("raw", 400, "limit_out_of_range"));
    fireEvent.click(screen.getByRole("button", { name: "Save extra usage" }));
    expect(await screen.findByText("The monthly limit must be between $1 and $100,000.")).toBeInTheDocument();
  });

  it("maps whole-request errors to a banner", async () => {
    await show(ORG);
    update.mockRejectedValue(new ApiError("raw", 403, "admin_required"));
    fireEvent.click(screen.getByRole("button", { name: "Save extra usage" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Only an admin can change billing.");
  });

  it("tells a bank-account customer to add a card in Manage billing when auto-reload is refused", async () => {
    await show(ORG);
    update.mockRejectedValue(new ApiError("raw", 409, "auto_reload_requires_card"));
    fireEvent.click(screen.getByRole("switch", { name: "Auto-reload" }));
    fireEvent.change(screen.getByLabelText(/When the balance drops to/), { target: { value: "10" } });
    fireEvent.change(screen.getByLabelText(/^Add \(\$\)/), { target: { value: "50" } });
    fireEvent.click(screen.getByRole("button", { name: "Save extra usage" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Auto-reload can only charge a card");
    expect(alert).toHaveTextContent("Add a card from Manage billing");
  });

  it("top-up sends micro-USD and redirects to Checkout", async () => {
    await show(ORG);
    topUp.mockResolvedValue({ checkout_url: "https://stripe.test/topup", session_id: "cs_1" });
    fireEvent.change(screen.getByLabelText("Add funds ($)"), { target: { value: "75.25" } });
    fireEvent.click(screen.getByRole("button", { name: "Add funds" }));
    await waitFor(() => expect(goToStripe).toHaveBeenCalledWith("https://stripe.test/topup"));
    expect(topUp).toHaveBeenCalledWith("tok", {
      amountMicroUsd: 75_250_000,
      successUrl: "https://app.test/settings/organization/billing?topup=success",
      cancelUrl: "https://app.test/settings/organization/billing",
    });
  });

  it("top-up refuses amounts outside $5 to $1,000 without calling the API", async () => {
    await show(ORG);
    fireEvent.change(screen.getByLabelText("Add funds ($)"), { target: { value: "4" } });
    fireEvent.click(screen.getByRole("button", { name: "Add funds" }));
    expect(await screen.findByText("Add between $5 and $1,000 at a time.")).toBeInTheDocument();
    expect(topUp).not.toHaveBeenCalled();
  });

  it("top-up maps a frozen-credits refusal", async () => {
    await show(ORG);
    topUp.mockRejectedValue(new ApiError("raw", 409, "credits_frozen"));
    fireEvent.click(screen.getByRole("button", { name: "Add funds" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("on hold because the subscription ended");
    expect(goToStripe).not.toHaveBeenCalled();
  });

  it("top-up is disabled while extra usage is off", async () => {
    await show(PRO, "/settings/billing");
    expect(screen.getByRole("button", { name: "Add funds" })).toBeDisabled();
  });
});
