import { plainPageSectionQuery, PLAIN_LOAD_PATHS } from "./pageShell";
// perf/library-tab-cache — the query-key/option builders for every Library
// list (Topics, Sources, Coding/Agent sessions, People).
//
// WHY THIS FILE EXISTS. Every Library screen used to be a hand-rolled
// `useEffect` + `useState` fetch keyed on the raw Clerk bearer `token`
// (`lib/providers.tsx`'s `AppState.token` doc explicitly warns against this —
// the session re-mints ~every 40s and on every focus/visibilitychange, so an
// effect keyed on it re-runs that often). That reset every Library tab to
// "surfacing…" on a routine token rotation, not a real navigation, and
// refetched from scratch on every revisit no matter how recently it had
// loaded. `lib/queryClient.ts` (W4) already ships a `QueryClient` for
// exactly this, and `lib/queryKeys.ts` already declares the Reefs-workbench
// half of the key hierarchy — this file is the Library half, plus the
// concrete `queryFn`s the Reefs half didn't need (Library's fetches are
// paginated/filtered lists, not a single ocean-wide drain).
//
// THE RULE, same one `lib/providers.tsx` states for `getAuthToken`: a query
// key must never contain the token — Clerk rotates it independently of
// whether the underlying LIST changed, so a token-keyed cache entry would
// miss on every rotation, which is the exact bug this file exists to close.
// Every `queryFn` below takes a `getAuthToken` GETTER and calls it at fetch
// time instead.
//
// CACHE CONTRACT (decided, not discovered): `staleTime` 60s, so revisiting a
// tab within a minute costs zero requests; past a minute, `placeholderData:
// keepPreviousData` paints the last-known rows immediately while a background
// refetch (no spinner — see `LIBRARY_STALE_TIME`'s callers, which all gate
// their loading UI on `isPending`, never `isFetching`) swaps in fresh ones.
// `refetchOnWindowFocus` is off for the same reason `lib/queryClient.ts`'s
// module default is: a screen holding several of these should not issue a
// burst of requests every time the tab regains focus.

import { keepPreviousData, queryOptions, type QueryClient } from "@tanstack/react-query";
import {
  api,
  SESSION_KIND_CODING,
  type AgentSessionsPage,
  type CodingSessionsPage,
  type EntityKind,
  type LibraryPage,
  type Member,
  type SessionListFilters,
  type SourceGroupDetail,
  type SourceGroupRow,
} from "./api";

/** Rows per page for every Library list that paginates by page number
 *  (Topics) or by page size (sessions' `limit`). Kept as one constant so a
 *  future change to "how many rows" cannot drift between the page and its
 *  hover-prefetch (`prefetchLibraryTab` below), which must request the exact
 *  same page to land a cache hit. */
export const LIBRARY_PAGE_SIZE = 20;

/** Every Library list's staleness budget — see the module comment. */
export const LIBRARY_STALE_TIME = 60_000;

/** A stable getter for the current bearer, exactly `AppState.getAuthToken` —
 *  read INSIDE a `queryFn`, never captured as (or derived into) part of a
 *  `queryKey`. Re-declared locally rather than imported from
 *  `lib/providers.tsx` so this module has no dependency on the provider
 *  (and so `libraryQueries.test.ts` can pass a plain closure). */
type AuthTokenGetter = () => string | null;

function libraryDefaults() {
  return {
    staleTime: LIBRARY_STALE_TIME,
    placeholderData: keepPreviousData,
    refetchOnWindowFocus: false,
  } as const;
}

// ── Topics (`/library`) ─────────────────────────────────────────────────

export interface TopicsParams {
  /** The resolved `ws` filter. `null` means "every workspace" — NEVER the
   *  string `"all"` the URL uses for that: the URL's own sentinel is resolved
   *  to `null` before it ever reaches the key, so `router.replace`'s
   *  canonical-URL rewrite (dropping `?ws=all` back down to no param at all)
   *  can never produce a SECOND key for the same resolved filters. That
   *  resolve-before-keying step is what fixes the double `GET /v1/library`
   *  the URL-normalizing `router.replace` used to cause. */
  graphId: string | null;
  entityType: EntityKind | null;
  q: string;
  page: number;
}

export function topicsQueryKey(params: TopicsParams) {
  return [
    "library",
    "topics",
    { graphId: params.graphId, entityType: params.entityType, q: params.q, page: params.page },
  ] as const;
}

export function topicsQueryOptions(args: {
  getAuthToken: AuthTokenGetter;
  enabled: boolean;
  params: TopicsParams;
}) {
  const { params } = args;
  return queryOptions<LibraryPage>({
    queryKey: topicsQueryKey(params),
    enabled: args.enabled,
    queryFn: async ({ signal }) => {
      const token = args.getAuthToken();
      if (!token) throw new Error("Not authenticated");
      return api.library.list(
        token,
        {
          graphId: params.graphId ?? undefined,
          entityType: params.entityType ?? undefined,
          q: params.q || undefined,
          page: params.page,
          pageSize: LIBRARY_PAGE_SIZE,
        },
        signal,
      );
    },
    ...libraryDefaults(),
  });
}

// ── Sources (`/library/sources`) ────────────────────────────────────────
//
// perf/library-source-groups: two queries, matching the two `source-groups`
// backend routes — no more client fold over `graphs.sources` +
// `reefs.listAll`, and no more ocean-wide `graphs.nodeAtomsAll` edge drain
// just to title "settled into" chips (the detail route returns `reefs`
// pre-joined). `sourceGroupsQueryOptions` is the list — one row per channel,
// server-aggregated. `sourceGroupQueryOptions` is one channel's full detail
// (unpaged `source_ids`/`reefs`, one page of `corals`), keyed on `page` too
// so paging in never evicts an earlier page's cache entry.

export function sourceGroupsQueryKey(graphId: string) {
  return ["library", "sources", "groups", { graphId }] as const;
}

export function sourceGroupsQueryOptions(args: {
  getAuthToken: AuthTokenGetter;
  enabled: boolean;
  graphId: string | undefined;
}) {
  const graphId = args.graphId ?? "";
  return queryOptions<{ groups: SourceGroupRow[] }>({
    queryKey: sourceGroupsQueryKey(graphId),
    enabled: args.enabled && !!args.graphId,
    queryFn: async ({ signal }) => {
      const token = args.getAuthToken();
      if (!token || !args.graphId) throw new Error("Not authenticated");
      return api.graphs.sourceGroups(token, args.graphId, signal);
    },
    ...libraryDefaults(),
  });
}

export function sourceGroupQueryKey(graphId: string, channelKey: string, page: number) {
  return ["library", "sources", "group", { graphId, channelKey, page }] as const;
}

export function sourceGroupQueryOptions(args: {
  getAuthToken: AuthTokenGetter;
  enabled: boolean;
  graphId: string | undefined;
  channelKey: string;
  page: number;
}) {
  const graphId = args.graphId ?? "";
  return queryOptions<SourceGroupDetail>({
    queryKey: sourceGroupQueryKey(graphId, args.channelKey, args.page),
    enabled: args.enabled && !!args.graphId && !!args.channelKey,
    queryFn: async ({ signal }) => {
      const token = args.getAuthToken();
      if (!token || !args.graphId) throw new Error("Not authenticated");
      return api.graphs.sourceGroup(token, args.graphId, args.channelKey, { page: args.page }, signal);
    },
    ...libraryDefaults(),
  });
}

// ── Sessions (`/library/sessions`, `/agent-sessions`) ──
//
// Only the FIRST page (`cursor` unset) is cached here — that is the one a tab
// revisit needs to skip, and the one the hover-prefetch below warms. "Load
// more" stays a direct, guarded fetch on each page (see the pages themselves):
// it appends rather than replaces, which is a poor fit for a single cached
// query value, and it is a deliberate per-visit user action rather than the
// "did I already have this" case the cache exists for.

export type LibrarySessionKind = "coding" | "agent";

/** Empty-filters sentinel every "first page" key/prefetch uses, so the
 *  hover-prefetch (which never knows a page's live filter selection) always
 *  targets the same key an UNfiltered visit would land on. */
export const EMPTY_SESSION_FILTER_ARGS: SessionListFilters = {};

export function sessionsQueryKey(kind: LibrarySessionKind, graphId: string, filters: SessionListFilters) {
  return ["library", "sessions", kind, graphId, filters] as const;
}

export function codingSessionsQueryOptions(args: {
  getAuthToken: AuthTokenGetter;
  enabled: boolean;
  graphId: string | undefined;
  filters: SessionListFilters;
}) {
  const graphId = args.graphId ?? "";
  return queryOptions<CodingSessionsPage>({
    queryKey: sessionsQueryKey("coding", graphId, args.filters),
    enabled: args.enabled && !!args.graphId,
    queryFn: async ({ signal }) => {
      const token = args.getAuthToken();
      if (!token || !args.graphId) throw new Error("Not authenticated");
      return api.sessions.list(
        token,
        { graphId: args.graphId, kind: SESSION_KIND_CODING, limit: LIBRARY_PAGE_SIZE, ...args.filters },
        signal,
      );
    },
    ...libraryDefaults(),
  });
}

export function agentSessionsQueryOptions(args: {
  getAuthToken: AuthTokenGetter;
  enabled: boolean;
  graphId: string | undefined;
  filters: SessionListFilters;
}) {
  const graphId = args.graphId ?? "";
  return queryOptions<AgentSessionsPage>({
    queryKey: sessionsQueryKey("agent", graphId, args.filters),
    enabled: args.enabled && !!args.graphId,
    queryFn: async ({ signal }) => {
      const token = args.getAuthToken();
      if (!token || !args.graphId) throw new Error("Not authenticated");
      return api.sessions.listAgent(
        token,
        { graphId: args.graphId, limit: LIBRARY_PAGE_SIZE, ...args.filters },
        signal,
      );
    },
    ...libraryDefaults(),
  });
}

// ── People (`/library/people`) ──────────────────────────────────────────
//
// `api.teams.members` is account-wide, not ocean-scoped — there is no
// `graphId` to key on. `scopeId` is the internal control-plane `accountId`
// (`AppState.accountId`, from `GET /v1/me`), which IS the right axis here:
// it is the thing that actually changes when the caller's tenancy changes
// (switching between a personal account and an org), distinct from `token`
// (rotates every ~40s with no tenancy change) and from `graphId` (this list
// isn't ocean-scoped at all).

export function peopleQueryKey(scopeId: string | null) {
  return ["library", "people", scopeId] as const;
}

export function peopleQueryOptions(args: {
  getAuthToken: AuthTokenGetter;
  enabled: boolean;
  scopeId: string | null;
}) {
  return { ...plainPageSectionQuery<Member[]>(PLAIN_LOAD_PATHS.people, args.scopeId ?? "pending", {}, args.getAuthToken,
    { queryKey: peopleQueryKey(args.scopeId) }), enabled: args.enabled, ...libraryDefaults() };
}

// ── Invalidation ─────────────────────────────────────────────────────────
//
// Same rule `lib/queryKeys.ts` states for the Reefs half: a mutation
// invalidates a KEY PREFIX, never a fully-specified key, or it silently
// misses whatever page/filter combination the caller happens to be viewing.
// `queryClient.invalidateQueries({ queryKey: <prefix> })` matches by prefix
// by default, so `["library","topics"]` reaches every `{graphId, entityType,
// q, page}` variant without enumerating them.

export const libraryInvalidation = {
  /** Every Topics list/facet, across every workspace and filter — the target
   *  for any mutation that can change which topics exist or what they're
   *  named (e.g. Sources' promote-to-ocean, which can mint new topics in a
   *  destination the caller isn't necessarily viewing). */
  topics: () => ["library", "topics"] as const,
  /** The channel-rollup list for one ocean — never a sibling ocean's. */
  sourceGroups: (graphId: string) => ["library", "sources", "groups", { graphId }] as const,
  /** Every channel-detail entry for one ocean, across every `channelKey` AND
   *  `page` — the target object below only names `graphId`, and
   *  `partialMatchKey` only compares the keys IT names, so this reaches
   *  `{graphId, channelKey, page}` for any `channelKey`/`page` without
   *  enumerating them (same "prefix reaches every variant" contract the
   *  array-shaped keys above use, expressed through an object instead). */
  sourceGroup: (graphId: string) => ["library", "sources", "group", { graphId }] as const,
  sessions: (kind: LibrarySessionKind, graphId: string) => ["library", "sessions", kind, graphId] as const,
  people: () => ["library", "people"] as const,
} as const;

// ── Hover prefetch (LibraryNav) ────────────────────────────────────────
//
// The tab a pointer is hovering (or focusing, for keyboard nav) is warmed
// with its FIRST page, using the exact same key + queryFn the destination
// page itself uses — so by the time the click lands, the query is either
// already resolved or already in flight, and a genuinely fresh visit (cache
// past `LIBRARY_STALE_TIME`) starts its fetch a click earlier than it
// otherwise would. `prefetchQuery` does not honour `enabled`, so callers must
// still be gated at the call site (`authReady`, a resolved `graphId`) —
// this function does that itself rather than trusting each caller to.
// It reuses the exact same `...libraryDefaults()` (same `staleTime`) as the
// page's own query, so hovering an already-fresh tab issues no request at
// all — TanStack Query treats the prefetch as satisfied by the cached entry.

export type LibraryTab = "topics" | "sources" | "sessions" | "agent-sessions" | "people";

export function prefetchLibraryTab(
  queryClient: QueryClient,
  tab: LibraryTab,
  ctx: {
    getAuthToken: AuthTokenGetter;
    authReady: boolean;
    /** The ambient ocean (`AppState.currentGraphId`) — what Sources/Sessions
     *  facets scope to. `null` when no ocean has resolved yet. */
    graphId: string | null;
    /** `AppState.accountId` — what the People list scopes to. */
    accountId: string | null;
  },
) {
  if (!ctx.authReady) return;
  switch (tab) {
    case "topics":
      void queryClient.prefetchQuery(
        topicsQueryOptions({
          getAuthToken: ctx.getAuthToken,
          enabled: true,
          params: { graphId: null, entityType: null, q: "", page: 1 },
        }),
      );
      return;
    case "sources":
      if (!ctx.graphId) return;
      void queryClient.prefetchQuery(
        sourceGroupsQueryOptions({ getAuthToken: ctx.getAuthToken, enabled: true, graphId: ctx.graphId }),
      );
      return;
    case "sessions":
      if (!ctx.graphId) return;
      void queryClient.prefetchQuery(
        codingSessionsQueryOptions({
          getAuthToken: ctx.getAuthToken,
          enabled: true,
          graphId: ctx.graphId,
          filters: EMPTY_SESSION_FILTER_ARGS,
        }),
      );
      return;
    case "agent-sessions":
      if (!ctx.graphId) return;
      void queryClient.prefetchQuery(
        agentSessionsQueryOptions({
          getAuthToken: ctx.getAuthToken,
          enabled: true,
          graphId: ctx.graphId,
          filters: EMPTY_SESSION_FILTER_ARGS,
        }),
      );
      return;
    case "people":
      void queryClient.prefetchQuery(
        peopleQueryOptions({ getAuthToken: ctx.getAuthToken, enabled: true, scopeId: ctx.accountId }),
      );
      return;
  }
}
