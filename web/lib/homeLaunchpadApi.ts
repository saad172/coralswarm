// Home launchpad reads: the three requests Home makes per visit.
//
//   GET /v1/home/continue    the viewer's own recent items
//   GET /v1/home/whats-new   up to 5 reefs formed in the last 7 days
//   GET /v1/home/attention   has_content, has_source, has_mcp, has_asked + the first broken connector
//
// Contract: the Home launchpad API contract shared by the backend and web PRs
// (`ContinueResponse`, `WhatsNewResponse`, `AttentionResponse` below mirror it
// field for field). Product decisions: Home is a launchpad, minutes-stale,
// never polls.
//
// FRESHNESS, and why these are plain reads rather than stored page sections:
// a stored section polls every 2s while its model is pending, and Home's
// owner decision is "about 3 requests per visit, no polling". So each read is
// a plain page read (`plainPageSectionQuery`, declared in PLAIN_LOAD_PATHS so
// the page-load budget recognises it) with:
//   - `refetchOnMount: "always"`: every Home visit re-reads all three once,
//     so a visit is always fresh and always exactly 3 requests. A cached
//     answer paints at once meanwhile (no skeleton), marked `refreshing…`;
//   - `staleTime` 5 min with `refetchOnWindowFocus: true` (the owner's
//     decision): a tab return while Home is open refetches only once the
//     data is older than 5 min, since TanStack refetches only a STALE query
//     on focus;
//   - no `refetchInterval`, and no sidebar hover prefetch (a warm read would
//     be a second read of the visit).
// Because a visit always re-reads, a change made elsewhere in the app needs no
// invalidation of these keys: Home is not mounted while it happens, and the
// next visit reads it. Only what can change while Home is on screen
// invalidates them (`invalidationFor` in lib/appQueries.ts): the connector
// repair Home's Fix leads to, and a chat deleted from the sidebar.
// A section never drops back to a skeleton once it has data: the key is
// stable for an identity, so a refetch (mount, focus or the Refresh control)
// keeps `data` while `isFetching` is true, and a failed refetch keeps it too.
// `placeholderData: keepPreviousData` is deliberately NOT used: the only time
// the key changes is an account/org switch, and carrying the previous
// identity's rows onto the new identity's key would show one account's work
// under another.

import type { QueryCtx } from "./appQueries";
import { PLAIN_LOAD_PATHS, plainPageSectionQuery } from "./pageShell";
import type { RequestPriority } from "./transport";

// ── Contract types ──────────────────────────────────────────────────────

export type ContinueKind = "chat" | "coding" | "agent" | "meeting" | "workspace";

export interface ContinueItem {
  kind: ContinueKind;
  /** What to open: chat id, latest session id in the group, the meeting's
   *  transcript `source_id`, workspace id. */
  id: string;
  /** Chat title, "<repo> · <branch>", meeting title, workspace name. */
  title: string;
  /** One context line (chat ocean, newest session's harness/last activity,
   *  meeting date/attendees, workspace activity). Never the group count:
   *  that is `count`, shown as a pill. */
  context: string | null;
  /** Grouped sessions: the number in the group (>= 2); otherwise null. */
  count: number | null;
  /** The ocean, where the opening route needs it (chats and meetings); null otherwise. */
  graph_id: string | null;
  /** RFC 3339, last activity. */
  at: string;
}

export interface ContinueResponse {
  items: ContinueItem[];
  generated_at: string;
}

/** `decision` and `thread` come first server-side, then entities. */
export type ReefKind = "decision" | "thread" | "entity" | (string & {});

export interface WhatsNewItem {
  reef_id: string;
  kind: ReefKind;
  title: string;
  ocean: { graph_id: string; name: string; kind: "private" | "org" | "team" | (string & {}) };
  /** RFC 3339. */
  formed_at: string;
}

export interface WhatsNewResponse {
  items: WhatsNewItem[];
  window_days: number;
  generated_at: string;
}

export interface HomeAttention {
  connector_id: string;
  /** "slack", "github", ... */
  provider: string;
  /** "Slack needs you to sign in again". */
  label: string;
  reason: "reauth" | "failing";
}

export interface AttentionResponse {
  /** The viewer can see at least one coral or reef in any ocean. Drives State B. */
  has_content: boolean;
  /** A non-MCP connector is connected (any health): setup step 1 is done. */
  has_source: boolean;
  /** An MCP client is connected: setup step 2 is done. */
  has_mcp: boolean;
  /** The viewer has asked at least one question (any conversation with a
   *  user message, any surface): setup step 3 is done. Read a missing field
   *  as false (`=== true`) until every backend serving Home sends it. */
  has_asked: boolean;
  /** The FIRST broken connector only; null when every connector is healthy. */
  attention: HomeAttention | null;
  generated_at: string;
}

// ── Query options ───────────────────────────────────────────────────────

/** How long a Home read counts as fresh for a tab return: focus refetches
 *  only past it. A visit (mount) always re-reads. */
const HOME_LAUNCHPAD_STALE_MS = 5 * 60_000;

const FRESHNESS = {
  refetchOnMount: "always",
  staleTime: HOME_LAUNCHPAD_STALE_MS,
  refetchOnWindowFocus: true,
} as const;

/** Each read is its own resource, `[resource, scope]` like every app key, so
 *  a mutation invalidates exactly the read it changes
 *  (`appInvalidation.homeContinue()` and friends). */
const homeLaunchpadKeys = {
  continue: (scope: string) => ["homeContinue", scope] as const,
  whatsNew: (scope: string) => ["homeWhatsNew", scope] as const,
  attention: (scope: string) => ["homeAttention", scope] as const,
};

export function homeContinueQueryOptions(ctx: QueryCtx, priority: RequestPriority = "critical") {
  return {
    ...plainPageSectionQuery<ContinueResponse>(PLAIN_LOAD_PATHS.homeContinue, ctx.scope, {}, ctx.getAuthToken,
      { priority, queryKey: [...homeLaunchpadKeys.continue(ctx.scope)] }),
    ...FRESHNESS,
  };
}

export function homeWhatsNewQueryOptions(ctx: QueryCtx, priority: RequestPriority = "critical") {
  return {
    ...plainPageSectionQuery<WhatsNewResponse>(PLAIN_LOAD_PATHS.homeWhatsNew, ctx.scope, {}, ctx.getAuthToken,
      { priority, queryKey: [...homeLaunchpadKeys.whatsNew(ctx.scope)] }),
    ...FRESHNESS,
  };
}

export function homeAttentionQueryOptions(ctx: QueryCtx, priority: RequestPriority = "critical") {
  return {
    ...plainPageSectionQuery<AttentionResponse>(PLAIN_LOAD_PATHS.homeAttention, ctx.scope, {}, ctx.getAuthToken,
      { priority, queryKey: [...homeLaunchpadKeys.attention(ctx.scope)] }),
    ...FRESHNESS,
  };
}
