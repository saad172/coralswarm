import { oceansIndexQueryOptions } from "./oceansIndex";
import { sectionCalendarDay } from "./pageSectionWire";
import { conversationList, type ConversationListPayload, type ChatEmptyPayload } from "./pageSectionTypes/chats";
import { pageSectionQuery, type Stored } from "./pageSections";
import { PLAIN_LOAD_PATHS as plain, plainPageSectionQuery } from "./pageShell";
// The query-option factories for every app read that is not a Library list
// (lib/libraryQueries.ts) or the Reefs workbench (lib/queryKeys.ts).
//
// WHY. Screens used to fetch in `useEffect` + `useState(true)`, so leaving a
// page threw its data away and coming back showed a spinner and refetched.
// On TanStack Query the read lives in the shared cache: a return inside the
// staleness window (`STALE.READ`, lib/queryClient.ts) paints instantly with
// no request, and past it paints the cached value while a background refetch
// swaps in fresh data. Screens gate their spinner on `isPending` (no data at
// all), never on `isFetching`.
//
// THE THREE RULES, same as lib/libraryQueries.ts:
//
// 1. The bearer is never a key or a dependency. Every `queryFn` calls the
//    `getAuthToken` getter at fetch time; Clerk rotates the string about once
//    a minute without anything the read depends on changing.
//
// 2. Every key starts `[resource, scope, …]`, where `scope` is
//    `AppState.queryScope` — the live Clerk (user, org) pair the bearer was
//    minted for. An account or org switch therefore lands on a different key
//    and can never be answered from the previous identity's entry, on top of
//    `QueryCacheIdentityBoundary` clearing the cache. Scope is second rather
//    than first so a mutation can invalidate `[resource]` without knowing it.
//
// 3. A mutation invalidates a key PREFIX (`appInvalidation` below), never a
//    fully specified key, so it reaches every param variant on screen.
//
// Sidebar destinations are warmed on link hover/focus by `prefetchDestination`
// at the bottom, with the exact factories the pages use.

import { pagePayloadQuery } from "./pageSections";
import { infiniteQueryOptions, isCancelledError, queryOptions, type QueryClient } from "@tanstack/react-query";
import { type ConnectorDetail, type ConnectorList } from "./connectorsApi";
import {
  api,
  ApiError,
  transientFailure,
  type ActivityEvent,
  type AddinToken,
  type ArchivedTeam,
  type AtlasGraph,
  type AutoAddSettings,
  type CandidateCoral,
  type ConnectorHealthResponse,
  type CoralDetail,
  type EscrowRecoveryEvent,
  type GithubInstall,
  type GithubReposResponse,
  type GoogleChatSpacesResponse,
  type JournalPeriod,
  type JoinRequest,
  type JournalHourItemsResponse,
  type JournalResponse,
  type MailboxProvider,
  type MsChat,
  type MsTeam,
  type MsWatched,
  type DeleteAccountEligibility,
  type OAuthConnection,
  type OceanStats,
  type OrgDirectoryMember,
  type OrgDomain,
  type SeatTier,
  type SlackChannelImpact,
  type SlackChannelsResponse,
  type SlackOnboardingStatus,
  type SlotPoolResult,
  type Reef,
  type ReefType,
  type RetractionEntry,
  type SupersessionEdge,
  type TeamMemberRow,
  type UsageSummary,
  type BillingMe,
  type BillingSummary,
  type TrialBannerData,
  type TrialBannerMode,
  type MembershipEvent,
  type MeResponse,
  type OrgSetupStatus,
  type BootstrapResponse,
} from "./api";
import { type ConnectorStatusResponse } from "./pageSectionTypes/connectorStatus";
import { keys } from "./queryKeys";
import { parseSelfserveOrg } from "./teamPanel";
import { bannerFromSummary, isBillingSummary, parseTrialBanner } from "./trialBannerData";
import { OPENED_GC_TIME, STALE } from "./queryClient";
import type { ComposioStatus, ComposioToolkit, ComposioToolkitSummary } from "@/components/connectors/composioModel";
import { readWatchedPage } from "@/components/connectors/followedThreadsModel";
import type { WorkspaceSurfaceFlags } from "@/components/connectors/googleWorkspaceModel";
import type { ConversationDetailPayload } from "./pageSectionTypes/conversationDetail";
import { refusePageRead } from "./pageReadAuthority";
import { pageSectionPath } from "./pageSectionWire";
import type { RequestPriority } from "./transport";
import { hasCriticalPageObserver } from "./pageReadPriority";
import { type PublicShare } from "./shares";
import { browserTimeZone, type HomeMetrics, type HomeMetricsParams } from "./pageSectionTypes/home";
import {
  type WorkspaceActivity,
  type WorkspaceActivityParams,
} from "./pageSectionTypes/workspaceActivity";
import { formatDayParam, localDayToUtcRange, parseDayParam, zoomRequestRange } from "./journal";

/** Read inside a `queryFn`, never captured into a key. May throw (an
 *  evidence-admission getter does when the identity is no longer admitted);
 *  the query then fails like any other request. */
export type AuthTokenGetter = () => string | null;

/** What every factory needs: how to get the bearer, and whose cache entry
 *  this is (`AppState.queryScope`). */
export interface QueryCtx {
  getAuthToken: AuthTokenGetter;
  scope: string;
}

function bearer(ctx: QueryCtx): string {
  const token = ctx.getAuthToken();
  if (!token) throw new Error("Not authenticated");
  return token;
}

// ── Account ─────────────────────────────────────────────────────────────

/** `GET /v1/me`. Settings/account and Profile read the internal ids from it;
 *  boot reads it separately (lib/providers.tsx) because boot gates the app. */
export function meQueryOptions(ctx: QueryCtx, priority: RequestPriority = "critical") {
  return plainPageSectionQuery<MeResponse>(plain.me, ctx.scope, {}, ctx.getAuthToken, { priority, queryKey: ["me", ctx.scope] });
}

/** `GET /v1/me/delete-account` — Settings → Account danger zone. */
export function deleteAccountQueryOptions(ctx: QueryCtx) {
  return plainPageSectionQuery<DeleteAccountEligibility>(plain.deleteAccount, ctx.scope, {}, ctx.getAuthToken, { queryKey: ["deleteAccount", ctx.scope] });
}

/** `GET /v1/billing/me` — the signed-in user's own credits and extra usage
 *  (Settings → Account "Your usage"). */
export function billingMeQueryOptions(ctx: QueryCtx) {
  return plainPageSectionQuery<BillingMe>(plain.billingMe, ctx.scope, {}, ctx.getAuthToken, { queryKey: ["billingMe", ctx.scope] });
}

/** `GET /v1/usage/summary` — the same document Settings → Usage and the org
 *  Usage and Billing pages read, so the three can never disagree. */
export function usageSummaryQueryOptions(ctx: QueryCtx) {
  return pagePayloadQuery<UsageSummary>("usage_summary", ctx.scope, {}, ctx.getAuthToken,
    { queryKey: ["usageSummary", ctx.scope, sectionCalendarDay().slice(0, 7)] });
}

/** `GET /v1/billing/summary` — the Billing pages' one read. Org: admin-only. */
export function billingSummaryQueryOptions(ctx: QueryCtx) {
  return plainPageSectionQuery<BillingSummary>(plain.billingSummary, ctx.scope, {}, ctx.getAuthToken, { queryKey: ["billingSummary", ctx.scope] });
}

/** The banner cache is nullable for an unavailable optional bootstrap read.
 * Keep an active observer so explicit mutations can refresh even that null.
 * Legacy mode is learned from the same boot response: no rollout probe. */
export function trialBannerQueryOptions(ctx: QueryCtx, mode: TrialBannerMode = "trial_v1") {
  return plainPageSectionQuery<TrialBannerData | null>(mode === "legacy" ? plain.billingSummary : plain.trialBanner,
    ctx.scope, {}, ctx.getAuthToken, {
      queryKey: ["billingTrialBanner", ctx.scope],
      authorityPath: plain.billingSummary,
      parse: (raw) => {
        if (mode === "legacy") {
          if (!isBillingSummary(raw)) throw new SyntaxError("Invalid billing summary");
          return bannerFromSummary(raw);
        }
        const banner = parseTrialBanner(raw);
        if (!banner) throw new SyntaxError("Invalid trial banner");
        return banner;
      },
    });
}

/** Every successful billing mutation/checkout return invalidates both views. */
export function invalidateBillingQueries(client: QueryClient) {
  return Promise.all([
    client.invalidateQueries({ queryKey: appInvalidation.billingSummary() }),
    client.invalidateQueries({ queryKey: appInvalidation.trialBanner() }),
  ]);
}

/** `GET /v1/oauth/connections` — Settings → Connections's connector list. */
export function oauthConnectionsQueryOptions(ctx: QueryCtx) {
  return plainPageSectionQuery<OAuthConnection[]>(plain.oauthConnections, ctx.scope, {}, ctx.getAuthToken, { queryKey: ["oauthConnections", ctx.scope] });
}

/** `GET /v1/connectors` — the redesigned Connectors page's grid. */
export function connectorsQueryOptions(ctx: QueryCtx, priority: RequestPriority = "critical") {
  return pagePayloadQuery<ConnectorList>("connectors_list", ctx.scope, {}, ctx.getAuthToken,
    { priority, queryKey: ["connectors", ctx.scope] });
}

/** `GET /v1/connectors/{provider}` — one connector's detail. */
export function connectorQueryOptions(ctx: QueryCtx, provider: string) {
  return pagePayloadQuery<ConnectorDetail>("connector_detail", ctx.scope, { provider }, ctx.getAuthToken,
    { queryKey: ["connector", ctx.scope, provider] });
}

// ── Chats ───────────────────────────────────────────────────────────────

/** The caller's conversations, own and shared, across every ocean. One entry
 *  serves the sidebar history and the Chats landing list, which used to issue
 *  two `GET /v1/conversations` on a cold load of /chats. Read through the
 *  verified reader (lib/scopedChatApi.ts) so both surfaces get the same
 *  shape checks. */
export function conversationsQueryOptions(ctx: QueryCtx, priority: RequestPriority = "critical") {
  return { ...pageSectionQuery<ConversationListPayload>("conversation_list", ctx.scope, {}, ctx.getAuthToken,
    { priority, queryKey: ["conversations", ctx.scope] }),
    select: (stored: Stored<ConversationListPayload>) => stored.computed_at === null ? undefined : conversationList(stored.payload) };
}

/** One shared empty-chat read supplies prompts and the organization checklist. */
export function chatEmptyQueryOptions(ctx: QueryCtx) {
  return pagePayloadQuery<ChatEmptyPayload>("chat_empty", ctx.scope, { limit: 6, since_days: 7 }, ctx.getAuthToken,
    { queryKey: ["chatEmpty", ctx.scope, sectionCalendarDay()] });
}

/** One opened chat's entry. Its own prefix, not under `["conversations"]`:
 *  a list change must not evict every opened thread. */
export function conversationKey(scope: string, conversationId: string) {
  return ["conversation", scope, conversationId] as const;
}

/** One saved chat, as the verified reader returns it (lib/scopedChatApi.ts).
 *  Opening it paints this entry at once and re-reads in the background; a
 *  row warms it on hover (`prefetchConversation`). */
export function conversationQueryOptions(ctx: QueryCtx, conversationId: string, priority: RequestPriority = "critical") {
  return {
    ...pagePayloadQuery<ConversationDetailPayload>("conversation_detail", ctx.scope, { id: conversationId }, ctx.getAuthToken,
      { queryKey: conversationKey(ctx.scope, conversationId), priority }),
    staleTime: STALE.OPENED,
    gcTime: OPENED_GC_TIME,
    // The thread owns offline (its own note, a re-read on `online`), so this
    // read fails like the direct fetch it replaced instead of pausing.
    networkMode: "always" as const,
  };
}

/** True when a read of a chat failed in a way that says the reader lost it
 *  (revoked, deleted, failed verification), not that the server was briefly
 *  unreachable. A cancelled read says nothing either way. */
export function conversationRefused(error: unknown): boolean {
  return !isCancelledError(error) && !transientFailure(error);
}

/** A refused read of a chat (`conversationRefused`) drops the thread the
 *  entry held, so a reopen shows the refusal at once instead of painting the
 *  lost thread for a round trip. The entry then holds only the refusal, which
 *  ages out with `OPENED_GC_TIME`, so a hover does not re-ask the server
 *  every `STALE.OPENED`. Only `prefetchConversation` honours the refusal:
 *  opening the chat still reads the server. */
export function forgetRefusedConversation(queryClient: QueryClient, ctx: QueryCtx, conversationId: string, error: unknown): void {
  const refusal = error instanceof Error ? error : new Error("This chat could not be verified.");
  void refusePageRead(queryClient, JSON.stringify([ctx.scope, pageSectionPath("conversation_detail", { id: conversationId })]), refusal);
}

/** Warm one chat (a hovered row, or an idle warm of the newest). A fresh entry
 *  issues no request; an in-flight read is joined, not repeated. A chat whose
 *  last read was refused is not re-asked, and a refusal found by a warm drops
 *  the entry's thread (`forgetRefusedConversation`). */
export function prefetchConversation(queryClient: QueryClient, ctx: QueryCtx & { authReady: boolean }, conversationId: string, signal?: AbortSignal): Promise<void> {
  if (!ctx.authReady || signal?.aborted) return Promise.resolve();
  const options = conversationQueryOptions(ctx, conversationId, "background");
  const state = queryClient.getQueryState(options.queryKey);
  if (state?.status === "error" && state.fetchStatus === "idle" && conversationRefused(state.error)) return Promise.resolve();
  const cancel = () => { void queryClient.cancelQueries({ queryKey: options.queryKey, exact: true,
    predicate: (query) => !hasCriticalPageObserver(query) }, { revert: false }); };
  signal?.addEventListener("abort", cancel, { once: true });
  return queryClient.fetchQuery(options).then(
    () => undefined,
    () => undefined, // The shared indexed query already fenced terminal failures.
  ).finally(() => signal?.removeEventListener("abort", cancel));
}

// ── Today journal ───────────────────────────────────────────────────────

// No `keepPreviousData` for the journal: a key is a different calendar
// period, and painting yesterday's entries under today's header is wrong, not
// merely stale. A period visited before paints from cache; a new one shows
// the loading state.

/** One local calendar day of the hourly journal. `dayKey` is `?d=`'s
 *  `YYYY-MM-DD`; the UTC window is derived inside so the key stays the day. */
export function journalDayQueryOptions(ctx: QueryCtx, dayKey: string, priority: RequestPriority = "critical") {
  const { from, to } = localDayToUtcRange(parseDayParam(dayKey));
  return pagePayloadQuery<JournalResponse>("journal_rollups", ctx.scope,
    { granularity: "hour", from: from.toISOString(), to: to.toISOString() }, ctx.getAuthToken,
    { priority, queryKey: ["journal", ctx.scope, "day", dayKey, sectionCalendarDay()] });
}

/** Each parent/child period retains its own Stored authority and polling. */
export function journalRollupsQueryOptions(ctx: QueryCtx, zoom: "day" | "week" | "month", rangeZoom: "week" | "month", anchorKey: string) {
  const { from, to } = zoomRequestRange(rangeZoom, parseDayParam(anchorKey));
  return pagePayloadQuery<{ periods: JournalPeriod[] }>("journal_rollups", ctx.scope,
    { granularity: zoom, from: from.toISOString(), to: to.toISOString() }, ctx.getAuthToken,
    { queryKey: ["journal", ctx.scope, rangeZoom, anchorKey, zoom, sectionCalendarDay()] });
}

/** The first page of one hour's "everything this hour" drill-down. Later
 *  pages ("load more") are appended locally by `HourDrilldown`. */
export function journalHourItemsQueryOptions(ctx: QueryCtx, hourStart: string, graphId: string | null) {
  return queryOptions<JournalHourItemsResponse>({
    queryKey: ["journal", ctx.scope, "hour", hourStart, { graphId }] as const,
    queryFn: () => api.journal.hourItems(bearer(ctx), hourStart, { graphId: graphId ?? undefined }),
  });
}

// ── Activity ────────────────────────────────────────────────────────────

/** `GET /v1/activity`, optionally narrowed to one ocean. */
export function activityQueryOptions(ctx: QueryCtx, graphId: string | null) {
  return plainPageSectionQuery<ActivityEvent[]>(plain.activity, ctx.scope, { graph_id: graphId }, ctx.getAuthToken, { queryKey: ["activity", ctx.scope, { graphId }] });
}

// ── Home ────────────────────────────────────────────────────────────────

/** `GET /v1/home/metrics`: KPI tiles, ingested-per-day matrix, oceans by
 *  volume. `days`/`tz` are part of the key, so a range change is a different
 *  entry. A 404 (`HomeDashboardDisabled`) means the server flag is off; it is
 *  not retried. */
export function homeMetricsQueryOptions(ctx: QueryCtx, params: HomeMetricsParams = {}, priority: RequestPriority = "critical") {
  return pagePayloadQuery<HomeMetrics>("home_metrics", ctx.scope, { days: params.days ?? 30, tz: params.tz ?? browserTimeZone() ?? "UTC" }, ctx.getAuthToken,
    { priority, queryKey: ["home-metrics", ctx.scope, { days: params.days ?? 30, tz: params.tz ?? browserTimeZone() ?? "UTC", calendarDay: sectionCalendarDay(params.tz ?? browserTimeZone() ?? "UTC") }] });
}

/** `GET /v1/home/workspace-activity`: the Home "From your workspaces" card.
 *  `limit`/`sinceDays` are part of the key. A 404 (`WorkspaceActivityDisabled`)
 *  means the server flag is off; it is not retried. */
export function workspaceActivityQueryOptions(ctx: QueryCtx, params: WorkspaceActivityParams = {}) {
  return pagePayloadQuery<WorkspaceActivity>("workspace_activity", ctx.scope, { limit: params.limit ?? 5, since_days: params.sinceDays ?? 30 }, ctx.getAuthToken,
    { queryKey: ["home-workspace-activity", ctx.scope, { limit: params.limit ?? 5, sinceDays: params.sinceDays ?? 30, calendarDay: sectionCalendarDay() }] });
}

// ── Workspaces ──────────────────────────────────────────────────────────

/** The first page of one workspace's topic grid, optionally filtered by kind.
 *  Later pages ("load more") append locally on the screen. */
export function workspaceReefsQueryOptions(
  ctx: QueryCtx,
  graphId: string,
  kind: ReefType | null,
  pageSize: number,
) {
  const options = pageSectionQuery<{ nodes: Reef[]; has_more: boolean }>("reef_page", ctx.scope,
    { graph: graphId, type: kind, page: 1, page_size: pageSize }, ctx.getAuthToken,
    { queryKey: ["workspaceReefs", ctx.scope, graphId, { kind, pageSize }] });
  return { ...options, select: (stored: Stored<{ nodes: Reef[]; has_more: boolean }>) => stored.computed_at === null ? undefined :
    { data: stored.payload.nodes, hasMore: stored.payload.has_more, computedAt: stored.computed_at } };

}

/** One workspace's counts for its index card. Under the Reefs workbench's
 *  `["graph", id]` prefix (lib/queryKeys.ts), so any reef mutation's
 *  ingest/promote invalidation (`invalidationFor`) reaches it; the identity
 *  scope rides last. */
export function graphStatsQueryOptions(ctx: QueryCtx, graphId: string) {
  return queryOptions<OceanStats>({
    queryKey: [...keys.stats(graphId), ctx.scope] as const,
    queryFn: ({ signal }) => api.graphs.stats(bearer(ctx), graphId, signal),
  });
}

/** `GET /v1/teams/archived`. A 403 is the expected answer for anyone without
 *  account-level authority and must render as nothing, so every failure is
 *  folded into `null` ("hidden") here rather than surfacing as an error. */
export function archivedTeamsQueryOptions(ctx: QueryCtx) {
  return plainPageSectionQuery<ArchivedTeam[]>(plain.archivedTeams, ctx.scope, {}, ctx.getAuthToken, { queryKey: ["archivedTeams", ctx.scope] });
}

/** A shared workspace's roster (`teams.id`, NOT the ocean id — see
 *  `Graph.team_id`). One entry serves the workspace page's summary and the
 *  manage page, so opening Manage paints the roster already on screen. */
export function teamRosterQueryOptions(ctx: QueryCtx, teamId: string) {
  return plainPageSectionQuery<TeamMemberRow[]>(plain.teamRoster, ctx.scope, { team_id: teamId }, ctx.getAuthToken, { queryKey: ["teamRoster", ctx.scope, teamId] });
}

/** Page 1 of a workspace's membership audit trail (admins only — the
 *  endpoint 403s for anyone else, so callers gate `enabled` on that). */
export function teamEventsQueryOptions(ctx: QueryCtx, teamId: string, pageSize: number) {
  return plainPageSectionQuery<MembershipEvent[]>(plain.teamEvents, ctx.scope, { team_id: teamId, page: 1, page_size: pageSize }, ctx.getAuthToken, { queryKey: ["teamEvents", ctx.scope, teamId, { pageSize }] });
}

/** `GET /v1/orgs/members` — the org directory the add-member picker offers. */
export function orgDirectoryQueryOptions(ctx: QueryCtx) {
  return queryOptions<OrgDirectoryMember[]>({
    queryKey: ["orgDirectory", ctx.scope] as const,
    queryFn: () => api.orgs.members(bearer(ctx)),
  });
}

/** `GET /v1/orgs/slots` — the org's seat pool. */
export function orgSlotsQueryOptions(ctx: QueryCtx) {
  return plainPageSectionQuery<SlotPoolResult>(plain.orgSlots, ctx.scope, {}, ctx.getAuthToken, { queryKey: ["orgSlots", ctx.scope] });
}

/** `GET /v1/orgs/domains` — read once for both DomainVerification and
 *  AutoAddSetting on the org Members page. */
export function orgDomainsQueryOptions(ctx: QueryCtx) {
  return queryOptions<OrgDomain[]>({
    queryKey: ["orgDomains", ctx.scope] as const,
    queryFn: async () => (await api.orgs.domains.list(bearer(ctx))).domains,
  });
}

/** `GET /v1/orgs/auto-add`. */
export function orgAutoAddQueryOptions(ctx: QueryCtx) {
  return queryOptions<AutoAddSettings>({
    queryKey: ["orgAutoAdd", ctx.scope] as const,
    queryFn: () => api.orgs.autoAdd.get(bearer(ctx)),
  });
}

/** `GET /v1/billing/seat-tiers`. Callers fall back to their own default
 *  tier list on failure, silently, so this is never an error surface. */
export function seatTiersQueryOptions(ctx: QueryCtx) {
  return plainPageSectionQuery<SeatTier[]>(plain.seatTiers, ctx.scope, {}, ctx.getAuthToken, { queryKey: ["seatTiers", ctx.scope] });
}

/** Whether self-serve team sign-up is open: `GET /v1/bootstrap` →
 *  `selfserve_signup_enabled` (A2 #1533). Boot seeds this entry from its own
 *  bootstrap read (lib/bootData.ts `seedBootCache`), so the app shell makes
 *  no extra request; the fetch here runs only when the entry is missing
 *  (e.g. the sign-up pages after a fallback boot).
 *
 *  A bootstrap that answers without the field (an older backend) is `false`.
 *  A bootstrap that FAILS (including the fallback-boot 404) is an error, not
 *  `false`: "unknown" and "off" lead to different screens (the return pages
 *  carry on when unknown, so a flaky read never locks a creator out of a
 *  team they paid for), and an error is retried shortly instead of being
 *  cached as "off" for ten minutes. */
export function orgSelfserveEnabledQueryOptions(ctx: QueryCtx) {
  const source = plainPageSectionQuery<BootstrapResponse>(plain.bootstrap, ctx.scope, {}, ctx.getAuthToken);
  return queryOptions<boolean>({
    ...source,
    queryKey: ["orgSelfserveEnabled", ctx.scope] as const,
    queryFn: async (context) => (await source.queryFn(context)).selfserve_signup_enabled === true,
    // A deploy setting: it does not change under an open tab. Applies to a
    // real answer only; a failed read is retried.
    staleTime: 10 * 60_000,
  });
}

/** The caller's self-serve teams that aren't ready (`GET /v1/bootstrap` →
 *  `pending_orgs`, issue #1540): Clerk org ids. Seeded by boot only; the
 *  switcher reads it with `enabled: false`, so a backend without the field
 *  costs no request (the entry just stays empty). */
export function orgSelfservePendingQueryOptions(ctx: QueryCtx) {
  const source = plainPageSectionQuery<BootstrapResponse>(plain.bootstrap, ctx.scope, {}, ctx.getAuthToken);
  return queryOptions<string[]>({
    ...source,
    queryKey: ["orgSelfservePending", ctx.scope] as const,
    queryFn: async (context) => (await source.queryFn(context)).pending_orgs?.map((o) => o.org_id) ?? [],
    staleTime: 10 * 60_000,
  });
}

/** Whether the active org is a self-serve team org (`GET /v1/bootstrap` →
 *  `selfserve_org`, #1540 review SF-a). `null` when the server could not
 *  say (or does not send the field). Seeded by boot; fetched only when the
 *  cache has no entry (the fallback boot path, or a boot that could not say).
 *  Scoped by identity, so an org switch reads the new org's answer.
 *
 *  Why the whole bootstrap: no cheaper endpoint carries this flag (it is
 *  computed only in the bootstrap handler). So the entry is kept for the life
 *  of the tab per org scope (`gcTime: Infinity`; whether an org was made by
 *  self-serve sign-up never changes) and a real answer is never re-read
 *  (`staleTime: Infinity`), so People visits cost no request. A `null`
 *  ("could not say") is re-asked once it is a minute old. */
export function orgSelfserveActiveQueryOptions(ctx: QueryCtx) {
  const source = plainPageSectionQuery<BootstrapResponse>(plain.bootstrap, ctx.scope, {}, ctx.getAuthToken);
  return queryOptions<boolean | null>({
    ...source,
    queryKey: ["orgSelfserveActive", ctx.scope] as const,
    queryFn: async (context) => {
      return parseSelfserveOrg((await source.queryFn(context)).selfserve_org);
    },
    staleTime: (q) => (typeof q.state.data === "boolean" ? Infinity : 60_000),
    gcTime: Infinity,
  });
}

/** `GET /v1/orgs/{org_id}/provisioning` — the sign-up return page's poll.
 *  Live on every read (`STALE.MUTABLE`): the page acts on each answer. The
 *  page adds its own `refetchInterval` (the backoff in lib/orgSelfserve.ts). */
export function orgSetupStatusQueryOptions(ctx: QueryCtx, orgId: string) {
  return plainPageSectionQuery<OrgSetupStatus>(plain.provisioning, ctx.scope, { org_id: orgId }, ctx.getAuthToken, { queryKey: ["orgSetupStatus", ctx.scope, orgId] });
}

/** The org's join queue: open requests, then those decided within
 *  `decidedDays` days (one read covers both — see `api.orgs.joinRequests`). */
export function orgJoinRequestsQueryOptions(ctx: QueryCtx, decidedDays: number) {
  return queryOptions<JoinRequest[]>({
    queryKey: ["orgJoinRequests", ctx.scope, { decidedDays }] as const,
    queryFn: () => api.orgs.joinRequests.list(bearer(ctx), decidedDays),
  });
}

/** The open join-request count behind the admin badge (sidebar + org rail). */
export function orgJoinRequestCountQueryOptions(ctx: QueryCtx) {
  const query = plainPageSectionQuery<{ open: number }>(plain.orgJoinRequestCount, ctx.scope, {}, ctx.getAuthToken);
  return { ...query, queryKey: ["orgJoinRequests", ctx.scope, "count"] as const,
    queryFn: async (context: { signal: AbortSignal }) => (await query.queryFn(context)).open };
}

// ── Org escrow recovery ─────────────────────────────────────────────────

/** Whether org key recovery is set up. A 404 is the answer "not set up";
 *  any other failure (a 5xx, the network) stays an error, which the
 *  panel renders as still-loading, as before. */
export function escrowStatusQueryOptions(ctx: QueryCtx) {
  return queryOptions<{ kind: "none" } | { kind: "configured"; threshold: number; adminCount: number }>({
    queryKey: ["escrowStatus", ctx.scope] as const,
    queryFn: async () => {
      try {
        const r = await api.escrow.getRecoveryPubkey(bearer(ctx));
        return { kind: "configured", threshold: r.threshold, adminCount: r.admin_count };
      } catch (e) {
        if (e instanceof ApiError && e.status === 404) return { kind: "none" };
        throw e;
      }
    },
  });
}

export function escrowRecoveryLogQueryOptions(ctx: QueryCtx) {
  return queryOptions<EscrowRecoveryEvent[]>({
    queryKey: ["escrowRecoveryLog", ctx.scope] as const,
    queryFn: async () => (await api.escrow.listRecoveryLog(bearer(ctx))).events,
  });
}

/** One page of the retraction history. Paging keeps the current page on
 *  screen until the next lands; changing the "not fully removed" filter does
 *  NOT — rows that contradict the selected filter must never be shown under
 *  it (the race the retraction page once guarded by hand). */
export function retractionsQueryOptions(ctx: QueryCtx, page: number, pageSize: number, unreachedOnly: boolean) {
  return plainPageSectionQuery<{ entries: RetractionEntry[] }>(plain.retractions, ctx.scope,
    { page, page_size: pageSize, unreached_only: unreachedOnly || undefined }, ctx.getAuthToken,
    { queryKey: ["retractions", ctx.scope, { page, pageSize, unreachedOnly }] });
}

// ── Connectors ──────────────────────────────────────────────────────────

/** The GitHub App installations this account can see. */
export function githubInstallsQueryOptions(ctx: QueryCtx) {
  return queryOptions<GithubInstall[]>({
    queryKey: ["githubInstalls", ctx.scope] as const,
    queryFn: async () => (await api.connectors.githubInstalls(bearer(ctx))).installs,
  });
}

/** One installation's granted repos plus this account's destination options. */
export function githubReposQueryOptions(ctx: QueryCtx, installationId: string) {
  return queryOptions<GithubReposResponse>({
    queryKey: ["githubRepos", ctx.scope, installationId] as const,
    queryFn: () => api.connectors.githubRepos(bearer(ctx), installationId),
  });
}

/** The Google Chat spaces CoralSwarm reads — a LIVE `spaces.list`
 *  server-side, so callers refetch through `useGuardedRefetch`'s floor. A 404
 *  ("not connected") stays an error here: the page decides whether it is the
 *  page's answer or merely a failed refresh of a table already shown. */
export function googleChatSpacesQueryOptions(ctx: QueryCtx) {
  return queryOptions<GoogleChatSpacesResponse>({
    queryKey: ["googleChatSpaces", ctx.scope] as const,
    queryFn: () => api.connectors.googleChatSpaces(bearer(ctx)),
  });
}

/** Slack setup progress for the onboarding checklist — a LIVE
 *  `conversations.list` server-side, refetched only through
 *  `useGuardedRefetch`'s floor. */
export function slackOnboardingQueryOptions(ctx: QueryCtx) {
  return queryOptions<SlackOnboardingStatus>({
    queryKey: ["slackOnboarding", ctx.scope] as const,
    queryFn: () => api.connectors.slackOnboarding(bearer(ctx)),
  });
}

/** The Slack channels CoralSwarm reads — a LIVE `conversations.list`
 *  server-side; same refetch floor and error handling as Google Chat. */
export function slackChannelsQueryOptions(ctx: QueryCtx) {
  return queryOptions<SlackChannelsResponse>({
    queryKey: ["slackChannels", ctx.scope] as const,
    queryFn: () => api.connectors.slackChannels(bearer(ctx)),
  });
}

/** One Composio toolkit's capture status (connections, label, content kinds). */
export function composioStatusQueryOptions(ctx: QueryCtx, toolkit: ComposioToolkit) {
  return queryOptions<ComposioStatus>({
    queryKey: ["composioStatus", ctx.scope, toolkit] as const,
    queryFn: () => api.composio.status(bearer(ctx), toolkit),
  });
}

/** The Composio capture recipes offered as links on /connectors. */
export function composioToolkitsQueryOptions(ctx: QueryCtx) {
  return queryOptions<ComposioToolkitSummary[]>({
    queryKey: ["composioToolkits", ctx.scope] as const,
    queryFn: async () => (await api.composio.toolkits(bearer(ctx))).toolkits,
  });
}

/** Current connector degradations for an org (admins only). Silent on
 *  failure by design: any error reads as "nothing degraded". */
export function connectorHealthQueryOptions(ctx: QueryCtx) {
  return queryOptions<ConnectorHealthResponse>({
    queryKey: ["connectorHealth", ctx.scope] as const,
    queryFn: () =>
      api.orgs.connectorHealth(bearer(ctx)).catch(() => ({ degradations: [], directory_attribution: null })),
  });
}

/** Per-connection health for the Home Connectors card and panel
 *  (`GET /v1/connectors/status`). Errors propagate: the card shows "could not
 *  load", never a made-up 0/0. A Sync now / Reconnect action invalidates
 *  `appInvalidation.connectorStatus()`. */
export function connectorStatusQueryOptions(ctx: QueryCtx, priority: RequestPriority = "critical") {
  return pagePayloadQuery<ConnectorStatusResponse>("connector_status", ctx.scope, {}, ctx.getAuthToken,
    { priority, queryKey: ["connectorStatus", ctx.scope] });
}

/** A mailbox's followed (watched) threads, cursor-paged 50 at a time. A
 *  malformed page, or a cursor that does not advance, is an error — never
 *  "nothing is followed". */
export function watchedThreadsQueryOptions(ctx: QueryCtx, provider: MailboxProvider) {
  return infiniteQueryOptions({
    queryKey: ["watchedThreads", ctx.scope, provider] as const,
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam }) => {
      const page = readWatchedPage(await api.connectors.watchedThreads(bearer(ctx), provider, pageParam));
      if (page.next_cursor !== null && page.next_cursor === pageParam) {
        throw new Error("Watched-thread cursor did not advance");
      }
      return page;
    },
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });
}

/** The caller's paired Gmail add-on tokens (personal plane only — the
 *  endpoint 400s elsewhere, so callers gate `enabled`). */
export function addinTokensQueryOptions(ctx: QueryCtx) {
  return queryOptions<AddinToken[]>({
    queryKey: ["addinTokens", ctx.scope] as const,
    queryFn: async () => (await api.addin.listTokens(bearer(ctx))).tokens,
  });
}

/** Which Google Workspace surfaces this account may connect. */
export function googleWorkspaceSurfacesQueryOptions(ctx: QueryCtx) {
  return queryOptions<WorkspaceSurfaceFlags>({
    queryKey: ["googleWorkspaceSurfaces", ctx.scope] as const,
    queryFn: async () => (await api.oauth.googleWorkspaceSurfaces(bearer(ctx))).surfaces,
  });
}

/** The confirmation dialog's real numbers for reading a Slack channel into
 *  an ocean (reach, seats). Must be live: someone confirms on these, so
 *  nothing is served from cache (STALE.MUTABLE) or kept once the dialog
 *  closes (gcTime 0). */
export function slackChannelImpactQueryOptions(ctx: QueryCtx, channelId: string, targetGraphId: string | undefined) {
  return queryOptions<SlackChannelImpact>({
    queryKey: ["slackChannelImpact", ctx.scope, channelId, { targetGraphId: targetGraphId ?? null }] as const,
    queryFn: () => api.connectors.slackChannelImpact(bearer(ctx), channelId, targetGraphId),
    staleTime: STALE.MUTABLE,
    gcTime: 0,
  });
}

/** A member's Microsoft Teams teams, chats and current capture watches,
 *  read together. A malformed list is an error, never an empty one. */
export function msTeamsListsQueryOptions(ctx: QueryCtx) {
  return queryOptions<{ teams: MsTeam[]; chats: MsChat[]; watched: MsWatched[] }>({
    queryKey: ["msTeamsLists", ctx.scope] as const,
    queryFn: async () => {
      const auth = bearer(ctx);
      const [teams, chats, watched] = await Promise.all([
        api.msTeams.teams(auth),
        api.msTeams.chats(auth),
        api.msTeams.watched(auth),
      ]);
      if (!Array.isArray(teams.teams) || !Array.isArray(chats.chats) || !Array.isArray(watched.watched))
        throw new Error("Teams returned an invalid list. Try again.");
      return { teams: teams.teams, chats: chats.chats, watched: watched.watched };
    },
  });
}

/** The member's per-meeting Teams transcript captures (the meeting tab). */
export function meetingCapturesQueryOptions(ctx: QueryCtx) {
  return { ...pageSectionQuery<{ captures: { capture_id: string; meeting_id: string; created_at: string; status: "active" }[] }>("teams_transcript_captures", ctx.scope, {}, ctx.getAuthToken,
    { queryKey: ["meetingCaptures", ctx.scope] }),
    select: (stored: Stored<{ captures: { capture_id: string; meeting_id: string; created_at: string; status: "active" }[] }>) => {
      if (stored.computed_at === null) return undefined;
      if (!Array.isArray(stored.payload.captures)) throw new Error("Unable to verify your meeting selections.");
      return stored.payload.captures;
    } };
}

/** The /graph canvas's atlas for one ocean. Changing the co-occurrence or
 *  anchor knobs keeps the current graph up (same ocean) until the new lands. */
export function atlasQueryOptions(ctx: QueryCtx, graphId: string, opts: { cooccur: number | null; allAnchors: boolean }) {
  return pagePayloadQuery<AtlasGraph>("atlas", ctx.scope,
    { graph_id: graphId, cooccur: opts.cooccur, anchors: opts.allAnchors ? "all" : "top" }, ctx.getAuthToken,
    { queryKey: [...keys.graph(graphId), "atlas", ctx.scope, opts] });
}

// ── Coral ───────────────────────────────────────────────────────────────

export interface CoralSectionPayload { atom: CoralDetail | null; source_inventory: import("./api").SourceItemsPage | null }
export function coralQueryOptions(ctx: QueryCtx, atomId: string) {
  return pagePayloadQuery<CoralSectionPayload>("coral_detail", ctx.scope, { atom: atomId }, ctx.getAuthToken,
    { queryKey: ["coral", ctx.scope, atomId] });
}

// ── Topic curation ──────────────────────────────────────────────────────

/** Suggested corals for a topic. The endpoint 400s when the topic has no
 *  atoms to seed retrieval from; any failure reads as "no suggestions". */
export function topicCandidatesQueryOptions(ctx: QueryCtx, nodeId: string) {
  return queryOptions<CandidateCoral[]>({
    queryKey: ["topicCandidates", ctx.scope, nodeId] as const,
    queryFn: () => api.reefs.candidates(bearer(ctx), nodeId, 8).catch(() => []),
  });
}

export function supersessionsQueryOptions(ctx: QueryCtx, nodeId: string) {
  return queryOptions<{ as_superseder: SupersessionEdge[]; as_superseded: SupersessionEdge[] }>({
    queryKey: ["supersessions", ctx.scope, nodeId] as const,
    queryFn: () => api.reefs.supersessions(bearer(ctx), nodeId),
  });
}

// ── Outside the app shell ───────────────────────────────────────────────

/** A meeting share (`/shared?t=`): the PUBLIC endpoint first (any "link"
 *  share, no sign-in); if that fails and the viewer is signed in, the AUTHED
 *  endpoint, which also resolves "team" shares. `null` means unavailable. */
export function sharedMeetingQueryOptions(ctx: QueryCtx, shareToken: string) {
  const publicRead = plainPageSectionQuery<PublicShare>(plain.publicShare, "public", { token: shareToken }, () => null, { public: true });
  const memberRead = plainPageSectionQuery<PublicShare>(plain.sharedMeeting, ctx.scope, { token: shareToken }, ctx.getAuthToken);
  return { ...publicRead, queryKey: ["sharedMeeting", ctx.scope, shareToken] as const,
    queryFn: async (context: { signal: AbortSignal }): Promise<PublicShare | null> => {
      // Both reads are fully determined by the link and verified identity.
      // Start together: a public 404 never creates a serial mount request.
      const [publicResult, memberResult] = await Promise.allSettled([
        publicRead.queryFn(context), ctx.getAuthToken() ? memberRead.queryFn(context) : Promise.resolve(null),
      ]);
      if (publicResult.status === "fulfilled") return publicResult.value;
      if (!(publicResult.reason instanceof ApiError) || publicResult.reason.status !== 404) throw publicResult.reason;
      if (memberResult.status === "fulfilled") return memberResult.value;
      if (memberResult.reason instanceof ApiError && [403, 404].includes(memberResult.reason.status)) return null;
      throw memberResult.reason;
    } };
}

/** Sign-up's `?pt=` prefill: an opaque token resolved to the address it was
 *  minted for (`null` on any failure — the form just starts empty). The
 *  address is not kept once the form unmounts (gcTime 0). */
export function emailPrefillQueryOptions(prefillToken: string) {
  const options = plainPageSectionQuery<{ email: string | null }>(plain.emailPrefill, "public", { t: prefillToken }, () => null, { public: true });
  return { ...options, queryKey: ["emailPrefill", prefillToken] as const,
    queryFn: async (context: { signal: AbortSignal }) => (await options.queryFn(context).catch(() => ({ email: null }))).email,
    gcTime: 0 };
}

// ── Invalidation ────────────────────────────────────────────────────────

/** Prefixes for `queryClient.invalidateQueries({ queryKey })`. Each reaches
 *  every scope and param variant of its resource. */
export const appInvalidation = {
  me: () => ["me"] as const,
  oauthConnections: () => ["oauthConnections"] as const,
  connectors: () => ["connectors"] as const,
  connector: () => ["connector"] as const,
  connectorStatus: () => ["connectorStatus"] as const,
  usageSummary: () => ["usageSummary"] as const,
  billingSummary: () => ["billingSummary"] as const,
  trialBanner: () => ["billingTrialBanner"] as const,
  conversations: () => ["conversations"] as const,
  journal: () => ["journal"] as const,
  homeMetrics: () => ["home-metrics"] as const,
  activity: () => ["activity"] as const,
  homeWorkspaceActivity: () => ["home-workspace-activity"] as const,
  /** Home's launchpad reads (lib/homeLaunchpadApi.ts), one resource each. */
  homeContinue: () => ["homeContinue"] as const,
  homeWhatsNew: () => ["homeWhatsNew"] as const,
  homeAttention: () => ["homeAttention"] as const,
  workspaceReefs: () => ["workspaceReefs"] as const,
  archivedTeams: () => ["archivedTeams"] as const,
  oceansIndex: () => ["oceansIndex"] as const,
  teamRoster: () => ["teamRoster"] as const,
  teamEvents: () => ["teamEvents"] as const,
  orgDirectory: () => ["orgDirectory"] as const,
  orgSlots: () => ["orgSlots"] as const,
  orgDomains: () => ["orgDomains"] as const,
  orgAutoAdd: () => ["orgAutoAdd"] as const,
  escrowStatus: () => ["escrowStatus"] as const,
  escrowRecoveryLog: () => ["escrowRecoveryLog"] as const,
  /** The queue AND the badge count: every decision changes both. */
  orgJoinRequests: () => ["orgJoinRequests"] as const,
  coral: () => ["coral"] as const,
  retractions: () => ["retractions"] as const,
  githubInstalls: () => ["githubInstalls"] as const,
  githubRepos: () => ["githubRepos"] as const,
  topicCandidates: () => ["topicCandidates"] as const,
  supersessions: () => ["supersessions"] as const,
} as const;

type KeyPrefix = readonly unknown[];

/**
 * What each mutation must invalidate, declared once rather than remembered at
 * every call site. A missed invalidation is silent (the screen just keeps
 * showing the old answer for `STALE.READ`), so the call sites name the
 * mutation and this table names the keys.
 *
 * The workspace list itself is `AppState.graphs` (`refreshGraphs()`), not a
 * cached query, so it is not listed here; callers still refresh it.
 *
 * Deliberately absent, because nothing in the web app mutates them: the
 * journal (compiled server-side) and the retraction history (written by
 * retract calls made from other clients). Their changes land within
 * `STALE.READ` of happening. Ingest and promote still invalidate the journal
 * and activity, since both feed them.
 *
 * Home's three launchpad reads re-read on every visit (`refetchOnMount:
 * "always"`, lib/homeLaunchpadApi.ts), so a change made on another page needs
 * no entry for them: Home is not mounted then, and the next visit reads it.
 * They are listed only where the change can land while Home is on screen.
 */
export const invalidationFor = {
  /** Rename. The audit trail gains a `renamed` event. */
  workspaceRenamed: (): KeyPrefix[] => [appInvalidation.teamEvents(), appInvalidation.oceansIndex()],
  /** Archive. The workspace moves into the Archived section (where Restore
   *  lives), and its roster/audit trail stop being reachable. */
  workspaceArchived: (): KeyPrefix[] => [
    appInvalidation.oceansIndex(),
    appInvalidation.archivedTeams(),
    appInvalidation.teamRoster(),
    appInvalidation.teamEvents(),
  ],
  /** Restore. Leaves the Archived section; roster and audit trail return. */
  workspaceRestored: (): KeyPrefix[] => [
    appInvalidation.oceansIndex(),
    appInvalidation.archivedTeams(),
    appInvalidation.teamRoster(),
    appInvalidation.teamEvents(),
  ],
  /** Add, remove, role change, or leaving. Roster and audit trail move. */
  workspaceMembersChanged: (): KeyPrefix[] => [appInvalidation.teamRoster(), appInvalidation.teamEvents(), appInvalidation.oceansIndex()],
  /** A connector connected, reconnected, re-scoped, repaired or disconnected
   *  (an MCP client is a connector too). This is where Home's Fix leads, so
   *  it also marks Home's attention read (`has_source`, `has_mcp`, the first
   *  broken connector) stale. */
  connectorChanged: (): KeyPrefix[] => [appInvalidation.connectors(), appInvalidation.connector(), appInvalidation.homeAttention()],
  /** A chat deleted. The sidebar's chat history deletes while Home is on
   *  screen, so Home's Continue must not keep listing it. */
  chatDeleted: (): KeyPrefix[] => [appInvalidation.homeContinue()],
  /** New context ingested into one ocean (/add). The ingest call writes the
   *  coral and its `atom_ingested` activity row synchronously; reef
   *  extraction and the journal follow asynchronously, so those may still be
   *  up to `STALE.READ` behind after this refetch. */
  ingest: (graphId: string): KeyPrefix[] => [
    keys.graph(graphId),
    appInvalidation.workspaceReefs(),
    appInvalidation.activity(),
    appInvalidation.journal(),
  ],
  /** Reefs or a session promoted into other oceans. Membership changes in
   *  every DESTINATION, which is the view most likely to be stale. (A promote
   *  can land while Home is open, but What's new may be minutes stale by
   *  contract, so Home picks it up on its next visit, not mid-visit.) */
  promote: (destGraphIds: readonly string[]): KeyPrefix[] => [
    ...destGraphIds.map((id) => keys.graph(id)),
    appInvalidation.workspaceReefs(),
    appInvalidation.activity(),
    appInvalidation.journal(),
  ],
  /** A suggested relationship confirmed or rejected on the atlas. */
  edgeReviewed: (graphId: string): KeyPrefix[] => [keys.graph(graphId)],
} as const;

/** Invalidate every prefix a mutation declared. `refetchType: "none"` marks
 *  them stale without refetching what is on screen, for a screen that has
 *  already applied the change locally (the atlas canvas). */
export function invalidate(
  queryClient: QueryClient,
  prefixes: readonly KeyPrefix[],
  opts?: { refetchType?: "active" | "none" },
): Promise<unknown> {
  return Promise.all(
    prefixes.map((queryKey) =>
      queryClient.invalidateQueries({ queryKey: [...queryKey], refetchType: opts?.refetchType ?? "active" }),
    ),
  );
}

// ── Hover prefetch (sidebar) ────────────────────────────────────────────

/** Warm a sidebar destination's first paint on link hover/focus, with the
 *  exact factories (and so the exact keys) the destination reads. Only reads
 *  fully determined by the href are warmed; anything else is a no-op. `prefetchQuery`
 *  honours `staleTime`, so hovering a destination that is already fresh
 *  issues no request. */
export function prefetchDestination(
  queryClient: QueryClient,
  href: string,
  ctx: QueryCtx & {
    authReady: boolean;
    /** `AppState.graphs` — the workspace index renders a card per ocean. */
    graphIds: string[];
    activitiesEnabled?: boolean;
  },
) {
  if (!ctx.authReady) return;
  switch (href) {
    case "/home":
      // Nothing to warm: Home re-reads its three launchpad reads on every
      // visit, so a warm read on hover would be a second read of the visit.
      return;
    case "/today":
      // The same day key Today derives from an absent `?d=`.
      if (ctx.activitiesEnabled === false) void queryClient.prefetchQuery(journalDayQueryOptions(ctx, formatDayParam(parseDayParam(null)), "background"));
      return;
    case "/chats":
      void queryClient.prefetchQuery(conversationsQueryOptions(ctx, "background"));
      return;
    case "/workspaces":
      void queryClient.prefetchQuery(oceansIndexQueryOptions(ctx, "background"));
      return;
    case "/connectors":
      void queryClient.prefetchQuery(connectorsQueryOptions(ctx, "background"));
      return;
    case "/settings":
      // Account is the landing page; it reads /v1/me.
      void queryClient.prefetchQuery(meQueryOptions(ctx, "background"));
      return;
  }
}
