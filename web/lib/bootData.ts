import { PLAIN_LOAD_PATHS as plain, plainPageSectionQuery } from "./pageShell";
// The app shell's boot read, and the cache entries it fills.
//
// Boot used to issue GET /v1/me and GET /v1/graphs (in parallel, #1443), and
// the sidebar then issued GET /v1/orgs/join-requests/count for its Settings
// badge. GET /v1/bootstrap answers all three in one request, each part
// byte-identical to its own endpoint (backend/src/routes/bootstrap.rs).
//
// DEPLOY RACE. Web and backend deploy separately, so a new web build can talk
// to a backend that has no /v1/bootstrap yet. That backend answers 404, and
// only then does boot fall back to the separate reads it made before. Any
// other failure (401, 403, 5xx, network) is the boot's failure, exactly as a
// failed GET /v1/me was: retrying it as three requests would hide it.

import type { QueryClient } from "@tanstack/react-query";
import { ApiError, type Graph, type MeResponse, type BootstrapResponse, type TrialBootstrapResponse, type TrialBannerData, type TrialBannerMode } from "./api";
import {
  billingSummaryQueryOptions,
  trialBannerQueryOptions,
  meQueryOptions,
  orgJoinRequestCountQueryOptions,
  orgSelfserveEnabledQueryOptions,
  orgSelfservePendingQueryOptions,
  orgSelfserveActiveQueryOptions,
  type AuthTokenGetter,
} from "./appQueries";
import { myJoinRequestsQueryOptions } from "./joinRequestsQuery";
import { parseSelfserveOrg } from "./teamPanel";
import { bannerFromSummary, isBillingSummary, parseTrialBanner } from "./trialBannerData";
import { pageReadAuthorityRevision } from "./pageReadAuthority";

export interface BootData {
  me: MeResponse;
  /** The reachable oceans. A promise because on the fallback path the list
   *  is still in flight when `me` lands; boot's ocean decision awaits it. */
  graphs: Promise<Graph[]>;
  /** `undefined`: boot did not read it (fallback path), so leave the cache
   *  alone and let the badge's own query fetch. `null`: the server has no
   *  count for this caller (not an org admin) or could not read it. */
  joinRequestCount: number | null | undefined;
  /** Self-serve team sign-up on (A2 #1533). `undefined` on the fallback
   *  path: leave the cache alone. A bootstrap without the field (an older
   *  backend) reads as `false`. */
  selfserveSignupEnabled?: boolean;
  /** Clerk ids of the caller's self-serve teams that aren't ready (#1540).
   *  `undefined` when the backend doesn't send the field. */
  selfservePendingOrgs?: string[];
  /** Whether the booted org is a self-serve team org (#1540 review SF-a):
   *  `null` when the server could not say or does not send the field (the
   *  People page then keeps Clerk's profile; only `true` swaps it out);
   *  `undefined` on the fallback path (leave the cache alone). */
  selfserveOrg?: boolean | null;
  activitiesEnabled?: boolean;
  gistEnabled?: boolean;
  myJoinRequests?: BootstrapResponse["my_join_requests"];
  /** Complete legacy summary only; never the banner DTO. */
  billingBanner?: BootstrapResponse["billing_banner"];
  trialBanner?: TrialBannerData | null;
  trialBannerMode?: TrialBannerMode;
  source: "bootstrap" | "fallback";
}

export async function fetchBootData(token: string): Promise<BootData> {
  try {
    const boot = await plainPageSectionQuery<BootstrapResponse | TrialBootstrapResponse>(plain.bootstrap, "boot", { billing_banner: "trial_v1" }, () => token).queryFn({ signal: new AbortController().signal });
    const opted = "billing_banner_mode" in boot && boot.billing_banner_mode === "trial_v1";
    const legacy = !opted && "billing_banner" in boot && isBillingSummary(boot.billing_banner) ? boot.billing_banner : null;
    const banner = opted && "trial_banner" in boot ? parseTrialBanner(boot.trial_banner) : legacy ? bannerFromSummary(legacy) : null;
    return {
      me: boot.me,
      activitiesEnabled: boot.activities_enabled,
      gistEnabled: boot.gist_enabled,
      myJoinRequests: boot.my_join_requests,
      billingBanner: legacy,
      trialBanner: banner,
      trialBannerMode: opted ? "trial_v1" : "legacy",
      graphs: Promise.resolve(boot.graphs),
      joinRequestCount: boot.join_requests_count ? boot.join_requests_count.open : null,
      selfserveSignupEnabled: boot.selfserve_signup_enabled === true,
      selfservePendingOrgs: Array.isArray(boot.pending_orgs)
        ? boot.pending_orgs.map((o) => o?.org_id).filter((id): id is string => typeof id === "string")
        : undefined,
      selfserveOrg: parseSelfserveOrg(boot.selfserve_org),
      source: "bootstrap",
    };
  } catch (err) {
    if (!(err instanceof ApiError && err.status === 404)) throw err;
  }
  // The pre-bootstrap boot: both reads together, one round trip. The list's
  // rejection is observed here so a failing `me` (which ends boot first)
  // cannot leave it unhandled; the caller still awaits the original.
  const graphs = plainPageSectionQuery<Graph[]>(plain.graphs, "boot", {}, () => token).queryFn({ signal: new AbortController().signal });
  graphs.catch(() => {});
  const me = await plainPageSectionQuery<MeResponse>(plain.me, "boot", {}, () => token).queryFn({ signal: new AbortController().signal });
  return { me, graphs, joinRequestCount: undefined, source: "fallback" };
}

/** Put what boot read into the cache, under exactly the keys the screens'
 *  factories use (lib/appQueries.ts), so their first paint makes no request.
 *
 *  `scope` MUST be the identity this boot resolved (`identityScope` of the
 *  user and the org the boot started for), never the live Clerk org: on an
 *  in-place org switch the live org changes before the new org's boot runs,
 *  and seeding under it would file the old org's answer as the new org's.
 *  The caller also skips this entirely for a superseded boot. */
export function seedBootCache(
  queryClient: QueryClient,
  ctx: { scope: string; getAuthToken: AuthTokenGetter },
  data: Pick<BootData, "me" | "joinRequestCount" | "selfserveSignupEnabled" | "selfservePendingOrgs" | "selfserveOrg" | "myJoinRequests" | "billingBanner" | "trialBanner" | "trialBannerMode">,
): void {
  queryClient.setQueryData(meQueryOptions(ctx).queryKey, data.me);
  // Null means the optional bootstrap read was unavailable; never seed a
  // fabricated empty or partial billing response.
  if (data.myJoinRequests !== undefined && data.myJoinRequests !== null) {
    queryClient.setQueryData(myJoinRequestsQueryOptions({ ...ctx, enabled: true }).queryKey, data.myJoinRequests);
  }
  const summary = billingSummaryQueryOptions(ctx);
  // Seeded, unobserved entries must participate in the same refusal fence.
  // Bootstrap seeds never renew authority after a canonical billing refusal.
  if (isBillingSummary(data.billingBanner) && pageReadAuthorityRevision(queryClient, summary.meta.readIdentity) === 0) {
    queryClient.getQueryCache().build(queryClient, summary);
    queryClient.setQueryData(summary.queryKey, data.billingBanner);
  }
  if (data.trialBanner !== undefined) {
    const banner = trialBannerQueryOptions(ctx, data.trialBannerMode);
    if (pageReadAuthorityRevision(queryClient, banner.meta.readIdentity) === 0) {
      queryClient.getQueryCache().build(queryClient, banner);
      queryClient.setQueryData(banner.queryKey, data.trialBanner);
    }
  }
  if (typeof data.selfserveSignupEnabled === "boolean") {
    queryClient.setQueryData(orgSelfserveEnabledQueryOptions(ctx).queryKey, data.selfserveSignupEnabled);
  }
  if (data.selfservePendingOrgs) {
    queryClient.setQueryData(orgSelfservePendingQueryOptions(ctx).queryKey, data.selfservePendingOrgs);
  }
  if (data.selfserveOrg !== undefined) {
    // Built with the factory's gcTime first: a plain setQueryData would file
    // the seed under the default 5-minute gcTime, and a People visit after
    // that would re-read the whole bootstrap for this one boolean.
    const active = orgSelfserveActiveQueryOptions(ctx);
    queryClient.getQueryCache().build(queryClient, { queryKey: active.queryKey, gcTime: active.gcTime });
    queryClient.setQueryData(active.queryKey, data.selfserveOrg);
  }
  // Only a real count is seeded. `null` leaves the entry empty: for a
  // non-admin the badge's query is disabled anyway, and for an admin whose
  // count the server could not read the badge retries on its own.
  if (typeof data.joinRequestCount === "number") {
    queryClient.setQueryData(orgJoinRequestCountQueryOptions(ctx).queryKey, data.joinRequestCount);
  }
}
