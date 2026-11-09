// @vitest-environment jsdom
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { apiRequest, ApiError, type BillingSummary, type BillingTrial, type TrialBannerData } from "./api";
import { fetchBootData, seedBootCache } from "./bootData";
import { billingSummaryQueryOptions, trialBannerQueryOptions, invalidateBillingQueries } from "./appQueries";
import { makeQueryClient } from "./queryClient";
import { TrialBanner } from "@/components/billing/TrialBanner";
import { QueryCacheIdentityBoundary } from "./queryCacheIdentityBoundary";
import { isBillingSummary, parseTrialBanner } from "./trialBannerData";

const state = vi.hoisted(() => ({ scope: "u::org_a", user: "u", org: "org_a" as string | null, admin: true,
  facts: { trialBannerMode: "trial_v1" } as { trialBannerMode: "trial_v1" | "legacy" } | null }));
vi.mock("./api", async (original) => ({ ...await original<typeof import("./api")>(), apiRequest: vi.fn() }));
vi.mock("./providers", () => ({ useApp: () => ({ authReady: true, queryScope: state.scope, getAuthToken: () => state.scope, bootstrapFacts: state.facts }) }));
vi.mock("@clerk/nextjs", () => ({ useAuth: () => ({ orgId: state.org }), useUser: () => ({ user: { id: state.user } }) }));
vi.mock("@/components/org/useOrgAdmin", () => ({ useOrgAdmin: () => ({ loaded: true, hasOrg: !!state.org, isAdmin: state.admin, orgId: state.org }) }));
vi.mock("next/link", () => ({ default: ({ children, href }: { children: React.ReactNode; href: string }) => <a href={href}>{children}</a> }));
const read = vi.mocked(apiRequest);
const trial: BillingTrial = { state: "trialing", seats: 2, credits_total: 3000, credits_used: 100, ends_at: null, payment_failed: false, seat_price_usd_cents: 2000, seat_monthly_credits: 5000 };
const banner: TrialBannerData = { kind: "trial_banner_v1", trial };
const full: BillingSummary = { account_kind: "org", plan: null, seats: { used: 2, total: 2 },
  credits: { allowance_used: 100, allowance_total: 3000, resets_at: null },
  extra_usage: { enabled: false, balance_micro_usd: 0, monthly_limit_micro_usd: null, spent_this_period_micro_usd: 0, member_cap_seat_multiple: null,
    auto_reload: { enabled: false, threshold_micro_usd: null, amount_micro_usd: null } },
  subscription: { status: "active", has_billing_account: true, renews_at: null, cancel_at_period_end: false, cancel_at: null },
  payment_method: { type: "card", brand: "visa", last4: "4242", exp_month: 12, exp_year: 2030 }, trial };
const core = { me: { user_id: "internal", account_id: "acct-a", role: "admin", device_id: "d", org_teams_enabled: true, active_graph_id: null, is_personal_org: false },
  graphs: [], activities_enabled: true, gist_enabled: false, my_join_requests: [], join_requests_count: null };
let client = makeQueryClient();
const ctx = () => ({ scope: state.scope, getAuthToken: () => state.scope });
beforeEach(() => { client = makeQueryClient(); read.mockReset(); state.scope = "u::org_a"; state.org = "org_a"; state.user = "u"; state.admin = true; state.facts = { trialBannerMode: "trial_v1" }; });
afterEach(() => { cleanup(); client.clear(); });
function mount(children: React.ReactNode = <TrialBanner />) { return render(<QueryClientProvider client={client}><QueryCacheIdentityBoundary>{children}</QueryCacheIdentityBoundary></QueryClientProvider>); }
async function boot(raw: unknown) {
  read.mockResolvedValueOnce(raw);
  const data = await fetchBootData("token");
  seedBootCache(client, ctx(), data);
  return data;
}

it("opts in once, seeds only the banner, and Billing subsequently obtains complete payment data", async () => {
  await boot({ ...core, billing_banner_mode: "trial_v1", trial_banner: banner });
  expect(read.mock.calls[0][0]).toBe("/v1/bootstrap?billing_banner=trial_v1");
  expect(client.getQueryData(billingSummaryQueryOptions(ctx()).queryKey)).toBeUndefined();
  const view = mount();
  await waitFor(() => expect(view.getByText(/Free trial:.*credits left/)).toBeTruthy());
  expect(read).toHaveBeenCalledTimes(1);
  read.mockResolvedValueOnce(full);
  await expect(client.fetchQuery(billingSummaryQueryOptions(ctx()))).resolves.toEqual(full);
  expect(read.mock.calls[1][0]).toBe("/v1/billing/summary");
  expect(client.getQueryData(trialBannerQueryOptions(ctx()).queryKey)).toEqual(banner);
  expect(client.getQueryData(billingSummaryQueryOptions(ctx()).queryKey)).toEqual(full);
});

it("learns legacy mode from an old backend in the same request and preserves the full cache", async () => {
  const data = await boot({ ...core, billing_banner: full });
  expect(data.trialBannerMode).toBe("legacy");
  state.facts = { trialBannerMode: "legacy" };
  mount();
  expect(read).toHaveBeenCalledTimes(1);
  expect(client.getQueryData(billingSummaryQueryOptions(ctx()).queryKey)).toEqual(full);
  read.mockResolvedValueOnce({ ...full, trial: null });
  await act(async () => { await invalidateBillingQueries(client); });
  expect(read.mock.calls[1][0]).toBe("/v1/billing/summary");
  expect(client.getQueryData(trialBannerQueryOptions(ctx()).queryKey)).toEqual({ kind: "trial_banner_v1", trial: null });
});

it.each([null, { kind: "trial_banner_v1" }, { kind: "trial_banner_v1", trial: { state: "trialing" } }])(
  "unavailable or malformed optional data stays silent without a mount read; explicit invalidation recovers: %j", async (value) => {
    await boot({ ...core, billing_banner_mode: "trial_v1", trial_banner: value });
    const view = mount();
    expect(view.queryByText(/Free trial:/)).toBeNull();
    expect(read).toHaveBeenCalledTimes(1);
    expect(client.getQueryData(trialBannerQueryOptions(ctx()).queryKey)).toBeNull();
    read.mockResolvedValueOnce(banner);
    await act(async () => { await invalidateBillingQueries(client); });
    await waitFor(() => expect(view.getByText(/Free trial:/)).toBeTruthy());
    expect(read.mock.calls[1][0]).toBe("/v1/billing/trial-banner");
    expect(client.getQueryData(billingSummaryQueryOptions(ctx()).queryKey)).toBeUndefined();
  },
);

it("rejects a partial legacy summary instead of populating either cache with fabricated fields", async () => {
  await boot({ ...core, billing_banner: { trial } });
  expect(client.getQueryData(billingSummaryQueryOptions(ctx()).queryKey)).toBeUndefined();
  expect(client.getQueryData(trialBannerQueryOptions(ctx()).queryKey)).toBeNull();
  expect(isBillingSummary(banner)).toBe(false);
  expect(isBillingSummary({ ...full, payment_method: undefined })).toBe(false);
  expect(parseTrialBanner(full)).toBeNull();
});

it.each(["member", "personal"])("%s does not dispatch a banner read", async (kind) => {
  state.admin = kind !== "member";
  if (kind === "personal") state.org = null;
  await boot({ ...core, billing_banner_mode: "trial_v1", trial_banner: null });
  mount();
  await act(async () => { await invalidateBillingQueries(client); });
  expect(read).toHaveBeenCalledTimes(1);
});

it("shares one canonical banner read among observers and refuses old data after 403", async () => {
  client.setQueryData(trialBannerQueryOptions(ctx()).queryKey, null);
  function Observer() { useQuery({ ...trialBannerQueryOptions(ctx()), enabled: true, refetchOnMount: false }); return null; }
  const view = mount(<><TrialBanner /><Observer /></>);
  read.mockResolvedValueOnce(banner);
  await act(async () => { await invalidateBillingQueries(client); });
  expect(read).toHaveBeenCalledTimes(1);
  await waitFor(() => expect(view.getByText(/Free trial:/)).toBeTruthy());
  read.mockRejectedValueOnce(new ApiError("removed", 403));
  await act(async () => { await invalidateBillingQueries(client); });
  await waitFor(() => expect(view.queryByText(/Free trial:/)).toBeNull());
  expect(client.getQueryData(trialBannerQueryOptions(ctx()).queryKey)).toBeUndefined();
});

it.each(["org", "user"])("late banner completion cannot repopulate a switched %s", async (axis) => {
  client.setQueryData(trialBannerQueryOptions(ctx()).queryKey, null);
  let resolve!: (value: unknown) => void;
  read.mockImplementationOnce(() => new Promise((yes) => { resolve = yes; }));
  const view = mount();
  let pending!: Promise<unknown>;
  act(() => { pending = invalidateBillingQueries(client); });
  await waitFor(() => expect(read).toHaveBeenCalledTimes(1));
  act(() => { if (axis === "org") state.org = "org_b"; else state.user = "other"; state.scope = `${state.user}::${state.org}`; });
  view.rerender(<QueryClientProvider client={client}><QueryCacheIdentityBoundary><TrialBanner /></QueryCacheIdentityBoundary></QueryClientProvider>);
  read.mockResolvedValue({ kind: "trial_banner_v1", trial: null });
  await act(async () => { resolve(banner); await pending; });
  expect(view.queryByText(/Free trial:/)).toBeNull();
  expect(client.getQueryData(["billingTrialBanner", "u::org_a"])).toBeUndefined();
  expect(JSON.stringify(client.getQueryCache().getAll().map((q) => q.state.data))).not.toContain('"credits_used":100');
});

for (const mode of ["trial_v1", "legacy"] as const) {
  it.each([401, 403, 404])("full billing refusal withdraws " + mode + " banner: %s", async (status) => {
    await boot(mode === "legacy" ? { ...core, billing_banner: full } :
      { ...core, billing_banner_mode: "trial_v1", trial_banner: banner });
    state.facts = { trialBannerMode: mode };
    const view = mount();
    await waitFor(() => expect(view.queryByText(/Free trial:/)).not.toBeNull());
    read.mockRejectedValueOnce(new ApiError("billing refused", status));
    await act(async () => { await client.fetchQuery({ ...billingSummaryQueryOptions(ctx()), staleTime: 0 }).catch(() => {}); });
    expect(read.mock.lastCall?.[0]).toBe("/v1/billing/summary");
    expect(state.admin).toBe(true); // Clerk has not yet learned the server's refusal.
    await waitFor(() => expect(view.queryByText(/Free trial:/)).toBeNull());
    expect(client.getQueryData(trialBannerQueryOptions(ctx(), mode).queryKey)).toBeUndefined();
    expect(client.getQueryState(billingSummaryQueryOptions(ctx()).queryKey)?.error).toMatchObject({ status });
  });

  it("malformed " + mode + " refresh withdraws old data before a verified recovery", async () => {
    await boot(mode === "legacy" ? { ...core, billing_banner: full } :
      { ...core, billing_banner_mode: "trial_v1", trial_banner: banner });
    state.facts = { trialBannerMode: mode };
    const view = mount();
    await waitFor(() => expect(view.queryByText(/Free trial:/)).not.toBeNull());
    read.mockResolvedValueOnce(mode === "legacy" ? { trial } : { kind: "trial_banner_v1" });
    await act(async () => { await invalidateBillingQueries(client); });
    await waitFor(() => expect(view.queryByText(/Free trial:/)).toBeNull());
    expect(client.getQueryData(trialBannerQueryOptions(ctx(), mode).queryKey)).toBeUndefined();
    expect(client.getQueryState(trialBannerQueryOptions(ctx(), mode).queryKey)?.error).toBeInstanceOf(SyntaxError);
    expect(client.getQueryData(billingSummaryQueryOptions(ctx()).queryKey)).toBeUndefined();
    read.mockRejectedValueOnce(new ApiError("temporarily unavailable", 503));
    await act(async () => { await invalidateBillingQueries(client); });
    expect(view.queryByText(/Free trial:/)).toBeNull();
    expect(client.getQueryData(trialBannerQueryOptions(ctx(), mode).queryKey)).toBeUndefined();
    read.mockResolvedValueOnce(mode === "legacy" ? full : banner);
    await act(async () => { await invalidateBillingQueries(client); });
    await waitFor(() => expect(view.queryByText(/Free trial:/)).not.toBeNull());
    expect(client.getQueryData(billingSummaryQueryOptions(ctx()).queryKey)).toBeUndefined();
  });
}

it.each(["summary", "banner"] as const)("a %s refusal fences an abort-ignoring peer completion", async (refusing) => {
  await boot({ ...core, billing_banner_mode: "trial_v1", trial_banner: banner });
  const summary = billingSummaryQueryOptions(ctx());
  const trialOptions = trialBannerQueryOptions(ctx());
  read.mockResolvedValueOnce(full);
  await client.fetchQuery(summary);
  const view = mount();
  await waitFor(() => expect(view.queryByText(/Free trial:/)).not.toBeNull());
  read.mockReset();
  let resolve!: (value: unknown) => void;
  let rawAttempt!: Promise<unknown>;
  read.mockImplementationOnce(() => new Promise((yes) => { resolve = yes; }));
  const waiting = refusing === "summary" ? trialOptions : summary;
  const denied = refusing === "summary" ? summary : trialOptions;
  let pending!: Promise<unknown>;
  act(() => {
    pending = client.fetchQuery<unknown>({ ...waiting, staleTime: 0, queryFn: (context) => {
      rawAttempt = waiting.queryFn(context);
      return rawAttempt;
    } }).catch(() => undefined);
  });
  await waitFor(() => expect(read).toHaveBeenCalledTimes(1));
  try {
    read.mockRejectedValueOnce(new ApiError("billing refused", 403));
    await act(async () => { await client.fetchQuery<unknown>({ ...denied, staleTime: 0 }).catch(() => {}); });
    await waitFor(() => expect(view.queryByText(/Free trial:/)).toBeNull());
    expect(client.getQueryData(summary.queryKey)).toBeUndefined();
    expect(client.getQueryData(trialOptions.queryKey)).toBeUndefined();
  } finally {
    await act(async () => {
      resolve(refusing === "summary" ? banner : full);
      await rawAttempt.catch(() => {});
      await pending;
    });
  }
  expect(client.getQueryData(summary.queryKey)).toBeUndefined();
  expect(client.getQueryData(trialOptions.queryKey)).toBeUndefined();
  expect(view.queryByText(/Free trial:/)).toBeNull();
});

it.each(["trial_v1", "legacy"] as const)("a late %s boot seed cannot renew a billing refusal", async (mode) => {
  const data = await boot(mode === "legacy" ? { ...core, billing_banner: full } :
    { ...core, billing_banner_mode: "trial_v1", trial_banner: banner });
  const summary = billingSummaryQueryOptions(ctx());
  const trialOptions = trialBannerQueryOptions(ctx(), mode);
  read.mockResolvedValueOnce(full);
  await client.fetchQuery({ ...summary, staleTime: 0 });
  read.mockResolvedValueOnce(mode === "legacy" ? full : banner);
  await client.fetchQuery({ ...trialOptions, staleTime: 0 });
  read.mockRejectedValueOnce(new ApiError("billing refused", 403));
  await client.fetchQuery({ ...summary, staleTime: 0 }).catch(() => {});
  expect(client.getQueryData(summary.queryKey)).toBeUndefined();
  expect(client.getQueryData(trialOptions.queryKey)).toBeUndefined();
  expect(() => seedBootCache(client, ctx(), data)).not.toThrow();
  expect(client.getQueryData(summary.queryKey)).toBeUndefined();
  expect(client.getQueryData(trialOptions.queryKey)).toBeUndefined();
});

it("fences an unobserved banner seed without refusing another scope", async () => {
  const other = { scope: "u::org_b", getAuthToken: () => "u::org_b" };
  const otherSummary = billingSummaryQueryOptions(other);
  const otherBanner = trialBannerQueryOptions(other);
  read.mockResolvedValueOnce(full).mockResolvedValueOnce(banner);
  await client.fetchQuery(otherSummary);
  await client.fetchQuery(otherBanner);
  await boot({ ...core, billing_banner_mode: "trial_v1", trial_banner: banner });
  read.mockRejectedValueOnce(new ApiError("billing refused", 403));
  await client.fetchQuery(billingSummaryQueryOptions(ctx())).catch(() => {});
  expect(client.getQueryData(trialBannerQueryOptions(ctx()).queryKey)).toBeUndefined();
  expect(client.getQueryData(otherSummary.queryKey)).toEqual(full);
  expect(client.getQueryData(otherBanner.queryKey)).toEqual(banner);
});
