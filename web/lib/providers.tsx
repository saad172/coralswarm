"use client";

import { PageLoadBoundary } from "@/components/PageLoadBoundary";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { api, setTokenRefresher, type Graph, type BootstrapResponse, type TrialBannerMode } from "./api";
import { bootErrorMessage } from "./bootErrorCopy";
import { startClientMetrics } from "./clientMetrics";
import { QueryClientProvider, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { makeQueryClient } from "./queryClient";
import { identityScope, QueryCacheIdentityBoundary } from "./queryCacheIdentityBoundary";
import { fetchBootData, seedBootCache } from "./bootData";
import { decideOceanBootstrap, effectivePlane, UnexpectedEmptyOrgError, type Plane } from "./oceanBootstrap";
import { pickActiveOcean, shouldReportActiveOcean } from "./activeOcean";
import { readDevBearer } from "./devBearer";
import { credentialScope } from "./credentialScope";
import { beginInitialPageReads } from "./initialPageReads";
import {
  bootFailureActions,
  bootFailureOutcome,
  bootRecordForFailure,
  isGatingAttempt,
  shouldAttemptBoot,
  type BootRecord,
} from "./bootGate";

// How long the initial-boot chain (getToken → GET /v1/bootstrap → ocean bootstrap) gets before
// the watchdog gives up and surfaces a retryable error instead of hanging.
// A backgrounded tab's network can be throttled, so this can't be too
// tight — 15s is generous enough to ride out that throttling on a live
// connection while still bounding the worst case (see the boot-gate RCA:
// previously this chain had no timeout at all and could hang until an app
// restart).
const BOOT_TIMEOUT_MS = 15_000;

/** Races `promise` against a timeout, rejecting with a labeled error if the
 *  timeout wins. Always clears its timer, so a slow-but-eventually-successful
 *  `promise` doesn't leak a dangling timeout. */
function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

interface AppState {
  /** Optional shell facts from this identity's completed bootstrap. Null
   * parts omit their banner; they never authorize a fallback mount request. */
  bootstrapFacts?: { myJoinRequests: BootstrapResponse["my_join_requests"]; billingBanner: BootstrapResponse["billing_banner"]; trialBannerMode?: TrialBannerMode; joinRequestCount: number | null } | null;
  ready: boolean; // auth resolved + (when signed in) oceans bootstrapped
  /** Set when the gating (initial or identity-change) boot attempt failed or
   *  timed out — lets the app shell offer a retry instead of hanging on
   *  "surfacing…" forever. */
  bootError: string | null;
  /** Manually re-run the boot chain (e.g. from the error affordance). */
  retryBoot: () => void;
  /** The current bearer.
   *
   *  DO NOT put this in a dependency array of a data-loading effect. Clerk
   *  sessions last ~60s and the provider re-mints on a 40s interval (plus
   *  every focus/visibilitychange), so the STRING changes about once a
   *  minute. An effect keyed on it re-runs that often — which is what made
   *  the topic page reset to "surfacing this topic" while the user was
   *  reading it, re-draining `reefs.listAll` and re-billing an LLM digest
   *  each time. Use `getAuthToken()` + `authReady` instead. */
  token: string | null;
  /** Stable getter for the current bearer — read it INSIDE the request, not
   *  as a dependency. Identity never changes, so it is safe in a dep array.
   *  Same pattern the client-metrics reporter already uses below. */
  getAuthToken: () => string | null;
  /** The identity every cached server read belongs to: the (user, org) pair
   *  the current boot RESOLVED, i.e. the one the bearer `getAuthToken()`
   *  returns was minted for. Query factories put it in their keys
   *  (lib/appQueries.ts), so an account or org switch can never be answered
   *  from another identity's cache entry. Stable across token rotations.
   *
   *  NOT the live Clerk org. On an in-place org switch Clerk's `orgId`
   *  changes a render before the boot for it runs, while `getAuthToken()`
   *  still returns the OLD org's bearer. Keying on the live org would let a
   *  mounted query refetch under the new org's key with the old org's token
   *  and cache the old org's answer as the new org's. Until the boot for the
   *  live identity completes this is a `#pending` scope no real entry uses,
   *  and `authReady` is false. See `bootScope` below. */
  queryScope: string;
  /** Whether a bearer exists AND the boot for the live identity has
   *  completed, so `getAuthToken()` belongs to `queryScope`. This is the
   *  value to gate reads on: it flips on sign-in, sign-out and around an org
   *  switch, never on a routine rotation. */
  authReady: boolean;
  userId: string | null;
  /** The CONTROL-PLANE internal user id (`GET /v1/me`'s `user_id`) — a UUID,
   *  and DIFFERENT from `userId` above (Clerk's `"user_xyz"` id). Every
   *  server-side roster/membership row (`TeamMemberRow.user_id`,
   *  `MembershipEvent.actor_user_id`/`target_user_id`, `Member.user_id`) is
   *  keyed by THIS id, not Clerk's — so "is this row me?" must compare
   *  against `internalUserId`, never `userId`. Null until the boot chain's
   *  `api.me()` call resolves (same lifecycle as `accountId`, which it rides
   *  alongside). */
  internalUserId: string | null;
  /** Server capability from `GET /v1/me`: can a shared workspace be created
   *  under THIS context? `null` = not in an org (irrelevant), `false` = in an
   *  org with `ORG_TEAMS_ENABLED` off (creation will 403), `true` = allowed.
   *  Null until the boot chain's `api.me()` resolves. */
  orgTeamsEnabled: boolean | null;
  activitiesEnabled: boolean;
  gistEnabled: boolean;
  /** `GET /v1/me`'s `is_personal_org` (control migration 0208): true when the
   *  Clerk organization currently active is actually a PERSONAL ORG — one
   *  that stands for this user's own ocean, not a real organization. Clerk's
   *  `orgId` alone cannot tell the two apart (a personal org is an ordinary
   *  Clerk org in every way Clerk cares about), which is the whole bug this
   *  field exists to close: every org-gated screen that reduced "am I in an
   *  org" to `orgId != null` was showing a one-person org the full
   *  organization experience. `useOrgAdmin()` is the one place that combines
   *  this with `orgId` — read `hasOrg` from there, not `orgId` directly.
   *
   *  Defaults to `false` (not null) before boot completes: every consumer of
   *  this field is mounted only once `ready` is true (`app/(app)/layout.tsx`
   *  gates on it), and `ready` cannot flip true until the SAME boot pass that
   *  resolves this value has already set it — see `attemptBoot` below, where
   *  it is set in the same synchronous block as `orgTeamsEnabled`, strictly
   *  before `setBooted(true)`. So no gated screen ever reads the pre-boot
   *  default. */
  isPersonalOrg: boolean;
  /** The Clerk org id (or `null`, the personal plane) that `isPersonalOrg`
   *  above was actually resolved FOR — i.e. `identityKey` from the boot pass
   *  that set it, pinned at the moment `GET /v1/me` was fetched.
   *
   *  A screen that reads `organization.id`/`orgId` directly (Clerk's
   *  `useAuth`/`useOrganization`) can observe a NEW active org one render
   *  before the identity effect below has reacted to it — Clerk updates its
   *  resources synchronously with the render, but the effect that resets
   *  `booted`/re-fetches `GET /v1/me` for the new identity only runs after
   *  that render commits. In that one-render window `ready` can still read
   *  `true` and `isPersonalOrg` can still describe the PREVIOUS org. Comparing
   *  this field against the live Clerk org id closes that window without
   *  waiting for `ready` to catch up — see `lib/orgAdmin.ts`'s
   *  `isPersonalOrgKnownForActiveOrg`, the one consumer that needs this
   *  (`app/(app)/settings/account/page.tsx`'s "Leave organization": the cost
   *  of getting this wrong is `membership.destroy()` on the wrong org). */
  resolvedOrgId: string | null;
  accountId: string | null;
  graphs: Graph[];
  currentGraphId: string | null;
  currentGraph: Graph | null;
  setCurrentGraphId: (id: string) => void;
  login: (token: string, userId: string, accountId?: string) => Promise<void>;
  logout: () => void;
  refreshGraphs: () => Promise<void>;
}

const Ctx = createContext<AppState | null>(null);

/** The LEGACY per-browser active-ocean key. Demoted, as of claim-based tenancy
 *  decision 8, from "where the active ocean lives" to "the fallback consulted
 *  when the server has no pointer for this (user, account) yet" — see
 *  `lib/activeOcean.ts` for the precedence and why it is that way round.
 *
 *  It is ONE GLOBAL KEY and nothing clears it on an org switch (`resetOcean`
 *  below clears React state only), so it genuinely holds org A's ocean id
 *  during org B's boot. `pickActiveOcean` intersecting it with the reachable
 *  list is the only thing that has ever stopped that from mattering. Kept, not
 *  removed: without it every existing user would be bounced off their current
 *  selection exactly once, on their first boot after the server-side pointer
 *  ships and before anything has populated it. */
const GRAPH_KEY = "cs_graph_id";

// ── Shared ocean (graph) state ────────────────────────────────────────────────
// The list of reachable oceans + the active selection, driven by whatever auth
// token the surrounding provider supplies.
function useOceanState(token: string | null) {
  const [graphs, setGraphs] = useState<Graph[]>([]);
  const [currentGraphId, setCurrentGraphIdState] = useState<string | null>(null);

  // The CURRENT bearer, read at call time rather than captured. Same pattern
  // (and the same reason) as `tokenRef` in `ClerkAuthProvider` below: Clerk
  // sessions last ~60s, so putting `token` in `setCurrentGraphId`'s dependency
  // array would change that callback's identity about once a minute and
  // re-fire any effect a consumer keyed on it.
  const tokenRef = useRef<string | null>(null);
  tokenRef.current = token;

  /** Persist the chosen ocean in both places: the server (authoritative, per
   *  (user, account) — claim-based tenancy decision 8) and the legacy
   *  per-browser key (the fallback for a boot where the server has no answer
   *  yet).
   *
   *  `tok` IS AN EXPLICIT PARAMETER, NOT READ FROM `tokenRef`, and that is
   *  load-bearing. `attemptBoot` calls `setToken(t)` and then immediately
   *  `bootstrap(t, …)`; `setToken` is a React state update, so `tokenRef` is
   *  not refreshed until the next render and is still `null` throughout that
   *  first `bootstrap`. Reading the ref here would silently skip the server
   *  write on the FIRST boot — precisely the boot that populates the pointer
   *  for a user who has none. Requiring the caller to hand over the token it
   *  already holds makes that mistake unavailable rather than merely fixed.
   *
   *  Fire-and-forget on the server leg, deliberately: this is called from the
   *  boot path, and a failed preference write must never fail a boot or a
   *  navigation. The server no-ops an unchanged value, so re-reporting is
   *  cheap, and the next boot reports again anyway — a dropped report costs at
   *  most one stale restore on another device, never correctness. */
  const persistGraphId = useCallback((id: string, tok: string | null) => {
    localStorage.setItem(GRAPH_KEY, id);
    if (tok) void api.reportActiveOcean(tok, id).catch(() => {});
  }, []);

  // Records a deliberate ocean SWITCH. NOTE FOR WHOEVER WIRES THE SWITCHER:
  // this had zero callers before claim-based tenancy decision 8 — it was
  // declared, defined, returned and exposed on the provider value, and never
  // invoked anywhere in `web/`. So the only active-ocean write that has ever
  // actually run in production is `bootstrap`'s, below. It now persists
  // server-side too, which is what makes a switch stick across devices.
  const setCurrentGraphId = useCallback(
    (id: string) => {
      setCurrentGraphIdState(id);
      persistGraphId(id, tokenRef.current);
    },
    [persistGraphId],
  );

  const bootstrap = useCallback(async (
    tok: string,
    plane: Plane,
    serverPreferred: string | null,
    // The boot chain already has the list (from GET /v1/bootstrap, or in
    // flight beside GET /v1/me on the 404 fallback) and hands it over here;
    // any other caller lets bootstrap issue it itself.
    listed: Promise<Graph[]> = api.graphs.list(tok),
  ) => {
    // What to do with an empty list is PLANE-DEPENDENT — see
    // `lib/oceanBootstrap.ts` for the full account of why (short version: the
    // backend auto-provisions a private Primary ocean on first Clerk auth on
    // the PERSONAL plane, so an empty list there falls back to a legacy
    // first-run create; a fresh org tenant legitimately has zero
    // oceans too, and creating a personal one under an org account is refused
    // by the backend outright — ADR-0007 D2).
    let list = await listed;
    const decision = decideOceanBootstrap(plane, list.length);
    switch (decision.type) {
      case "use":
        break;
      case "create-personal": {
        const personal = await api.graphs.create(tok, "Personal");
        list = [personal];
        break;
      }
      case "unexpected-empty-org":
        throw new UnexpectedEmptyOrgError();
    }
    setGraphs(list);
    // Which ocean to open: the server's per-(user, account) pointer first,
    // then this browser's legacy `cs_graph_id`, then the first reachable one —
    // and BOTH preferences are intersected with `list`, which is the
    // authority. See `lib/activeOcean.ts` for why that order, and for why the
    // intersection (not any server-side check) is what makes an unreachable
    // pointer harmless.
    const pick = pickActiveOcean(list, serverPreferred, localStorage.getItem(GRAPH_KEY));
    setCurrentGraphIdState(pick.id);
    // Tell the server unless it already knows. This is what populates the
    // pointer for every user who existed before it did, and what clears a
    // pointer to an ocean they can no longer open.
    if (shouldReportActiveOcean(pick.id, serverPreferred)) {
      persistGraphId(pick.id, tok);
    } else {
      // Nothing to report, but the local fallback still wants to match — a
      // device that has never seen this account should not go on preferring
      // whatever it last held.
      localStorage.setItem(GRAPH_KEY, pick.id);
    }
  }, [persistGraphId]);

  // Reads the bearer at call time (see `tokenRef`) so this callback's
  // identity does not change on every Clerk rotation.
  const refreshGraphs = useCallback(async () => {
    const tok = tokenRef.current;
    if (tok) setGraphs(await api.graphs.list(tok));
  }, []);

  const resetOcean = useCallback(() => {
    setGraphs([]);
    setCurrentGraphIdState(null);
  }, []);

  const currentGraph = graphs.find((g) => g.id === currentGraphId) ?? null;
  return {
    graphs,
    currentGraphId,
    currentGraph,
    setCurrentGraphId,
    bootstrap,
    refreshGraphs,
    resetOcean,
  };
}

// ── Clerk-backed provider (web) ───────────────────────────────────────────────
// Auth is Clerk; the backend verifies the Clerk session JWT (ADR-0007 D4). We
// keep the short-lived session token in state for the Bearer header, refreshing
// it on sign-in, on org switch (active account changes server-side), on an
// interval, and on window focus.
function ClerkAuthProvider({ children }: { children: ReactNode }) {
  // `orgId` is the ONLY tenancy input: claim-based tenancy names the tenant by
  // the JWT's `org_id` claim alone (docs/plans/claim-based-tenancy.md), so
  // there is no host, path, or slug for it to be reconciled against. `orgSlug`
  // used to be read alongside it to pick an API hostname; PR 2.1 removed that.
  const { isLoaded, isSignedIn, getToken, signOut, orgId } = useAuth();
  const { user } = useUser();
  const queryClient = useQueryClient();
  // Read by the boot chain when it seeds the `me` cache entry below; a ref so
  // the chain's callback identity does not change when Clerk's user loads.
  const userIdRef = useRef<string | null | undefined>(user?.id);
  userIdRef.current = user?.id;
  // Loopback JWKS bearer (localStorage.cs_dev_token). Off in prod — see
  // lib/devBearer.ts. Lets /graph boot against a laptop extract without a
  // live Clerk session whose user ids do not match the dump.
  const loopbackBearer = readDevBearer();
  const signedIn = !!isSignedIn || !!loopbackBearer;
  const [token, setToken] = useState<string | null>(null);
  const [accountId, setAccountId] = useState<string | null>(null);
  const [internalUserId, setInternalUserId] = useState<string | null>(null);
  const [orgTeamsEnabled, setOrgTeamsEnabled] = useState<boolean | null>(null);
  const [isPersonalOrg, setIsPersonalOrg] = useState(false);
  const [todayFeatures, setTodayFeatures] = useState({ activitiesEnabled: false, gistEnabled: false });
  const [bootstrapFacts, setBootstrapFacts] = useState<NonNullable<AppState["bootstrapFacts"]> | null>(null);
  // The Clerk org id (`identityKey`, `null` for the personal plane) that
  // `isPersonalOrg`/`accountId` above were actually resolved FOR — see
  // `resolvedOrgId`'s doc comment on `AppState`.
  const [resolvedOrgId, setResolvedOrgId] = useState<string | null>(null);
  const [booted, setBooted] = useState(false);
  const [bootError, setBootError] = useState<string | null>(null);
  const ocean = useOceanState(token);
  const { bootstrap, resetOcean } = ocean;

  // Stale-run / stale-closure guards for the boot chain below (this file has
  // a history of generation-guard ordering bugs, so these are deliberate):
  // - `bootGenRef` is bumped synchronously, before any `await`, at the start
  //   of every `attemptBoot` call AND on every re-run/unmount of the identity
  //   effect. Any async continuation checks it's still the current
  //   generation before touching state — this generalizes the old
  //   per-effect `active` boolean to also cover the two other call sites
  //   below (focus self-heal, manual retry) that can race the main effect.
  // - `bootLastSettledRef` remembers the identity + outcome (success OR
  //   error) of the last boot attempt that reached a terminal result. This is
  //   the fix for a real production incident: the identity effect used to
  //   decide whether to (re-)attempt a boot from a ref written on SUCCESS
  //   ONLY, so an identity that never succeeds (an org host whose bootstrap
  //   request the backend rejects, see `lib/oceanBootstrap.ts`) had no memory
  //   of "already tried" — any re-fire of that effect for ANY reason kept
  //   re-arming a doomed attempt, and each attempt's failure path called
  //   `setBootError` again, a state update, which is exactly the kind of
  //   update-triggers-effect-triggers-update cycle React's nested-update
  //   counter flags as "Maximum update depth exceeded". See
  //   `lib/bootGate.ts`'s module doc for the full account and the two pure
  //   predicates (`shouldAttemptBoot`, `isGatingAttempt`) this ref feeds.
  // - `bootFailedRef` tracks whether the last *gating* boot attempt is
  //   currently in a failed/timed-out state, for the focus self-heal below.
  const bootGenRef = useRef(0);
  // The live Clerk org, for async continuations that must not act on a token
  // minted before an org switch (the token refresh below).
  const liveOrgRef = useRef<string | null>(orgId ?? null);
  liveOrgRef.current = orgId ?? null;
  const bootLastSettledRef = useRef<BootRecord | null>(null);
  const bootFailedRef = useRef(false);
  const initialReadsRef = useRef<ReturnType<typeof beginInitialPageReads> | null>(null);
  const cancelInitialReads = useCallback(() => {
    initialReadsRef.current?.cancel();
    initialReadsRef.current = null;
  }, []);


  const attemptBoot = useCallback(
    async (identityKey: string, gating: boolean) => {
      const myGen = ++bootGenRef.current;
      cancelInitialReads();
      // The plane is a pure function of the identity: `identityKey` is
      // `orgId ?? "__personal__"` at every call site below, so re-deriving
      // it here (rather than threading a separate parameter through
      // `attemptBoot`'s callers) keeps the identity and the plane it implies
      // from ever disagreeing. See `lib/oceanBootstrap.ts` for why plane
      // comes from Clerk's `orgId` rather than the host.
      const plane: Plane = identityKey === "__personal__" ? "personal" : "org";
      let gotToken = false;
      try {
        // Time-box the ENTIRE chain (getToken → bootstrap → api.me) — a
        // backgrounded tab's network can be throttled, so any of these
        // can stall indefinitely instead of rejecting. Without a watchdog
        // that hangs the `ready` gate on "surfacing…" until an app restart.
        await withTimeout(
          (async () => {
            const t = readDevBearer() ?? (await getToken());
            if (bootGenRef.current !== myGen) return;
            // No token despite being signed-in (transient: token not yet
            // minted / clock skew) — handled after the race below so we
            // don't mark this identity as successfully booted.
            if (!t) return;
            gotToken = true;
            setToken(t);
            const claims = credentialScope(t);
            const bootUser = userIdRef.current;
            const bootOrg = identityKey === "__personal__" ? null : identityKey;
            // Claims can only refuse a mismatched credential. Each early GET
            // still verifies authority on the server; the existing layout
            // admission gate continues to withhold the entire page tree.
            if (gating && bootUser && claims?.subject === bootUser && claims.org === bootOrg) {
              initialReadsRef.current = beginInitialPageReads(queryClient, window.location.pathname, {
                scope: identityScope(bootUser, bootOrg),
                getAuthToken: () => {
                  if (bootGenRef.current !== myGen || userIdRef.current !== bootUser || liveOrgRef.current !== bootOrg)
                    throw new Error("The page account changed. Verify the current account.");
                  return t;
                },
              }, window.location.search);
            }
            // Resolve the internal active-account id (e.g. for connector OAuth),
            // the internal user id (e.g. matching "is this row me?" against a
            // roster/membership-event row — see `internalUserId`'s doc comment),
            // and this account's stored active ocean.
            //
            // ONE request: GET /v1/bootstrap answers `me`, the reachable ocean
            // list and the sidebar's join-request count together, each part
            // byte-identical to its own endpoint. Against a backend that has
            // no such route yet (a deploy race: web and backend ship
            // separately) it 404s and `fetchBootData` falls back to `me` ∥
            // `graphs.list`, the boot this replaced. See lib/bootData.ts.
            const boot = await fetchBootData(t);
            if (bootGenRef.current !== myGen) return;
            const me = boot.me;
            // `me.is_personal_org` OVERRIDES the Clerk-derived `plane` above,
            // never the reverse — see `lib/oceanBootstrap.ts`'s
            // `effectivePlane` for the full one-directional argument. Clerk's
            // `orgId` cannot distinguish a real organization from a personal
            // org, so `plane` alone would send a personal-org caller with
            // zero oceans down `bootstrap`'s "unexpected-empty-org" branch,
            // which throws.
            const resolvedPlane = effectivePlane(plane, me.is_personal_org);
            await bootstrap(t, resolvedPlane, me.active_graph_id ?? null, boot.graphs);
            // Still set AFTER `bootstrap`, deliberately, even though `me` now
            // resolves before it: today a failed boot leaves all three null,
            // and reordering the fetch must not quietly change that to
            // "account id populated under a boot that errored".
            if (bootGenRef.current === myGen) {
              setAccountId(me.account_id);
              setInternalUserId(me.user_id);
              setOrgTeamsEnabled(me.org_teams_enabled ?? null);
              setTodayFeatures({ activitiesEnabled: boot.activitiesEnabled === true, gistEnabled: boot.gistEnabled === true });
              setBootstrapFacts(boot.source === "bootstrap" ? { myJoinRequests: boot.myJoinRequests ?? null,
                billingBanner: boot.billingBanner ?? null, trialBannerMode: boot.trialBannerMode, joinRequestCount: boot.joinRequestCount ?? null } : null);
              // Set in the SAME synchronous block as `setBooted(true)` below
              // (no `await` between here and there) so every screen gated on
              // `ready` sees a resolved value, never the pre-boot `false`
              // default — see `isPersonalOrg`'s doc comment on `AppState`.
              setIsPersonalOrg(me.is_personal_org);
              // `identityKey`, not the live `orgId` closed over by this
              // callback — they agree at the top of THIS attempt, but only
              // `identityKey` is pinned to the boot pass that produced
              // `me.is_personal_org`. See `resolvedOrgId`'s doc comment.
              setResolvedOrgId(identityKey === "__personal__" ? null : identityKey);
              // Boot already holds `me` and (from /v1/bootstrap) the
              // join-request count; hand them to the screens that read them
              // (Profile, Settings → Account, the sidebar badge) so their
              // first paint needs no second request. Scoped to the identity
              // THIS boot resolved and skipped for a superseded boot (the
              // generation check above), so an org switch can never file the
              // old org's answer under the new org's key.
              seedBootCache(
                queryClient,
                { getAuthToken: () => tokenRef.current, scope: identityScope(userIdRef.current, identityKey === "__personal__" ? null : identityKey) },
                boot,
              );
            }
            // Boot-time IANA timezone report (Today-journal tier feature) —
            // fire-and-forget, never awaited and never allowed to affect the
            // boot outcome: the server validates and no-ops when unchanged,
            // so calling this on every boot is cheap, and a failure here
            // (offline, a transient 5xx) must never block sign-in or retry
            // the whole boot chain. No UI, no state — see `api.me.
            // reportTimezone`'s doc comment for the read side of this.
            try {
              const resolvedTz = Intl.DateTimeFormat().resolvedOptions().timeZone;
              if (resolvedTz) {
                void api.reportTimezone(t, resolvedTz).catch(() => {});
              }
            } catch {
              // `Intl.DateTimeFormat` itself throwing (should not happen in
              // any supported browser) is not worth failing boot over either.
            }
          })(),
          BOOT_TIMEOUT_MS,
          "boot",
        );
        if (bootGenRef.current !== myGen) return;
        if (!gotToken) {
          // Don't hang the `ready` gate — mark booted so the route guard
          // runs (it redirects to /login on a null token, recoverable). Not
          // a settled outcome for this identity either way — a missing token
          // is transient (clock skew / not yet minted), not a verdict on
          // whether THIS identity's boot works, so leave
          // `bootLastSettledRef` untouched and let the next real attempt
          // (retry / focus / a later effect fire for a genuinely new
          // identity) decide.
          bootFailedRef.current = false;
          if (gating) setBooted(true);
          return;
        }
        bootLastSettledRef.current = { identityKey, outcome: "success" };
        bootFailedRef.current = false;
        setBootError(null);
        setBooted(true);
      } catch (err) {
        if (bootGenRef.current !== myGen) return;
        cancelInitialReads();
        // THE REDIRECT-GATED-ON-`ready`-TRAP (observed in production
        // 2026-09-09): this catch used to unconditionally call
        // `setBootError` and NEVER `setBooted(true)`. `ready` (below) is
        // `isLoaded && booted`, and `app/(app)/layout.tsx`'s only redirect
        // to `/login` is `if (ready && !token) router.replace("/login")` —
        // gated on `ready`. That's fine for a real sign-out (the identity
        // effect's `!isSignedIn` branch above sets `booted` separately), but
        // when Clerk still thinks the session is valid and the BACKEND
        // rejects the bearer outright — a 401 that survived `lib/api.ts`'s
        // one refresh-and-retry (`req()`'s `if (res.status === 401 &&
        // !retried && token && tokenRefresher)`) — `ready` stayed false
        // forever: the user was stuck on the boot-error screen, whose only
        // control ("retry") just re-ran the identical, permanently-failing
        // boot. No path back to /login. `bootFailureOutcome` (see its doc in
        // `lib/bootGate.ts`) is the pure classification of which failures
        // are that terminal signed-out verdict versus an ordinary retryable
        // one.
        const outcome = bootFailureOutcome(err);
        const actions = bootFailureActions(outcome);
        if (outcome.type === "signed-out") {
          // A 401 surviving the retry already ruled out "merely stale
          // token" — it's a genuine, terminal auth failure. Settle the
          // identity so the identity effect's loop fix (`lib/bootGate.ts`)
          // refuses to re-arm this doomed boot — Clerk still reports
          // `isSignedIn` throughout this whole branch (that's the entire
          // premise: the BACKEND disagrees with Clerk, not Clerk itself), so
          // that effect's `!isSignedIn` early-return above does NOT run for
          // us; only this write blocks a re-fire.
          bootLastSettledRef.current = bootRecordForFailure(identityKey, outcome);
          // Not a failure to SELF-HEAL from: there is nothing transient left
          // to retry — the next window focus must not re-attempt a boot for
          // an identity that just settled as signed out.
          bootFailedRef.current = false;
          // Show the honest copy (bootErrorCopy's SIGNED_OUT_ERROR for a
          // 401) while the sign-out below is in flight.
          setBootError(bootErrorMessage(err));
          // DO NOT `setToken(null)` / `setBooted(true)` HERE — that was the
          // obvious-looking fix and it is an infinite redirect loop:
          // `app/login/page.tsx`'s `WebLogin` redirects `/login` -> `/home`
          // whenever `isSignedIn` is true, and `isSignedIn` stays true for
          // this whole branch (see above). Faking the signed-out UI state
          // while `isSignedIn` stays true sends layout to `/login`, and
          // `/login` immediately bounces back to `/home` — forever, and
          // worse than the stuck error screen this fix replaces. Ending the
          // Clerk session for real is what actually flips `isSignedIn`
          // false; once it does, the identity effect's `!isSignedIn` branch
          // above sets `token`/`booted` for us, and `/login` renders
          // `<SignIn>` instead of redirecting away from itself. If `signOut`
          // itself fails (e.g. offline), the user stays on today's screen
          // with this honest copy and a working retry — no regression.
          // `actions.markBooted` is always `false` (see
          // `BootFailureActions`'s doc) — asserting that this catch may
          // never set `booted` itself is exactly what keeps this loop shut.
          if (actions.signOutClerk) void signOut().catch(() => {});
          return;
        }
        bootFailedRef.current = true;
        if (gating) {
          // Initial boot / a real identity change stalled or errored —
          // record this identity as TERMINALLY settled (error) so the
          // identity effect never automatically re-arms it (see
          // `lib/bootGate.ts`), and surface a retryable error instead of
          // hanging on "surfacing…" forever. The focus self-heal effect
          // below, and the manual retry button, both call `attemptBoot`
          // directly and so can still retry this identity — the settled
          // record only blocks the AUTOMATIC identity-effect path.
          bootLastSettledRef.current = bootRecordForFailure(identityKey, outcome);
          setBootError(bootErrorMessage(err));
        }
        // A background (non-gating) revalidation failing quietly is fine —
        // keep the already-booted UI (and this identity's "success" settled
        // record) up; the focus self-heal retries it.
      }
    },
    [getToken, bootstrap, signOut, queryClient, cancelInitialReads],
  );

  useEffect(() => {
    if (!isLoaded) return;
    if (!signedIn) {
      bootGenRef.current += 1; // invalidate any in-flight attempt
      cancelInitialReads();
      bootFailedRef.current = false;
      bootLastSettledRef.current = null;
      setToken(null);
      setAccountId(null);
      setInternalUserId(null);
      setOrgTeamsEnabled(null);
      setIsPersonalOrg(false);
      setResolvedOrgId(null);
      resetOcean();
      setBootError(null);
      setBooted(true);
      return;
    }
    const identityKey = orgId ?? "__personal__";
    // THE LOOP FIX: only invoke `attemptBoot` from this effect when
    // `identityKey` has never settled before — i.e. a genuinely new identity
    // (first boot, org switch, different user). `shouldAttemptBoot` is false
    // for a repeat fire of this effect against an identity that already
    // settled, WHETHER THAT SETTLEMENT WAS SUCCESS OR ERROR. That "or error"
    // half is the actual fix: the previous version only remembered success,
    // so an identity that keeps failing (an org whose bootstrap request the
    // backend rejects) had no record of "already tried", and any
    // unrelated re-fire of this effect (e.g. `attemptBoot`'s own identity
    // churning because it depends on Clerk's `getToken`) re-armed a doomed
    // attempt from scratch — see `lib/bootGate.ts` for the full incident.
    // Reaching a settled outcome is a one-way door for THIS effect; only the
    // focus self-heal and the manual retry button (below) can try again.
    if (!shouldAttemptBoot(bootLastSettledRef.current, identityKey)) return;
    const gating = isGatingAttempt(bootLastSettledRef.current, identityKey);
    if (gating) {
      setBooted(false);
      setBootError(null);
    }
    void attemptBoot(identityKey, gating);
    return () => {
      // Invalidate this attempt if the effect re-runs (deps changed) or the
      // component unmounts before it resolves — the stale-run guard.
      bootGenRef.current += 1;
      cancelInitialReads();
    };
    // Re-run when the active org changes — that is a different active account.
    // `orgId` is the WHOLE tenancy input now (claim-based tenancy PR 2.1):
    // `orgSlug` used to be in these deps because it selected the API host, and
    // no hostname carries a tenant any more. Any OTHER re-fire
    // (attemptBoot/resetOcean identity churn) is a guaranteed no-op via
    // `shouldAttemptBoot` above, by construction.
  }, [isLoaded, signedIn, orgId, attemptBoot, resetOcean, cancelInitialReads]);

  // Self-heal: if the last gating boot attempt failed/timed out (e.g. a
  // tab backgrounded during a network blip), retry on the next
  // window focus/visibilitychange rather than leaving the user stuck at
  // "surfacing…" (or a stale boot error) until they manually restart.
  useEffect(() => {
    if (!signedIn) return;
    const retryIfFailed = () => {
      if (!bootFailedRef.current) return;
      // Deliberately bypasses `shouldAttemptBoot` — this is an intentional,
      // human/foreground-event-triggered retry of an identity that already
      // settled as an error, not the identity effect re-arming itself.
      const identityKey = orgId ?? "__personal__";
      const gating = isGatingAttempt(bootLastSettledRef.current, identityKey);
      void attemptBoot(identityKey, gating);
    };
    window.addEventListener("focus", retryIfFailed);
    document.addEventListener("visibilitychange", retryIfFailed);
    return () => {
      window.removeEventListener("focus", retryIfFailed);
      document.removeEventListener("visibilitychange", retryIfFailed);
    };
  }, [signedIn, orgId, attemptBoot]);

  // Manual retry (e.g. a button on the "surfacing…" error affordance).
  const retryBoot = useCallback(() => {
    const identityKey = orgId ?? "__personal__";
    setBooted(false);
    setBootError(null);
    void attemptBoot(identityKey, true);
  }, [orgId, attemptBoot]);

  // Keep the (short-lived) session token fresh for outgoing requests, and let the
  // API client mint a fresh bearer on demand (it retries once on a 401). Webviews
  // throttle background timers, so a request can still race a just-expired token —
  // the on-demand refresher (skipCache) is the reliable backstop.
  useEffect(() => {
    if (!signedIn) {
      setTokenRefresher(null);
      return;
    }
    // A refresh that started under one org and resolves after a switch holds
    // the PREVIOUS org's bearer; storing it would put that token behind the
    // new org's `queryScope`. Only a token minted under the org still active
    // is kept.
    const refresh = async () => {
      const startedFor = liveOrgRef.current;
      const t = readDevBearer() ?? (await getToken());
      if (t && liveOrgRef.current === startedFor) setToken(t);
    };
    // Force a brand-new token (bypass Clerk's cache) for the 401-retry path.
    setTokenRefresher(async () => {
      const startedFor = liveOrgRef.current;
      const t = readDevBearer() ?? (await getToken({ skipCache: true }));
      if (t && liveOrgRef.current === startedFor) setToken(t);
      return t;
    });
    const id = window.setInterval(refresh, 40_000);
    window.addEventListener("focus", refresh);
    document.addEventListener("visibilitychange", refresh);
    return () => {
      window.clearInterval(id);
      window.removeEventListener("focus", refresh);
      document.removeEventListener("visibilitychange", refresh);
      setTokenRefresher(null);
    };
  }, [signedIn, getToken]);

  // W6 — start the client metrics reporter
  // (docs/plans/request-reliability-and-scale.md §6.6).
  //
  // Mounted here because this is where the session token lives, and the
  // reporter needs a GETTER rather than a token: Clerk sessions last ~60s, so
  // capturing one would leave the reporter permanently unauthenticated within
  // a minute. `tokenRef` is read at flush time, so it is always current.
  //
  // The events it ships are the ones no server-side metric can see —
  // `ERR_INSUFFICIENT_RESOURCES`, client timeouts, requests refused by the
  // transport queue. Those were invisible during the incident this plan
  // exists to close out.
  //
  // Runs regardless of `isSignedIn`: the reporter buffers while
  // unauthenticated and flushes once a token appears, so events from the
  // sign-in window are not lost.
  const tokenRef = useRef<string | null>(null);
  tokenRef.current = token;
  useEffect(() => startClientMetrics(() => tokenRef.current), []);

  // The same getter, exposed to consumers. Screens had no way to read the
  // CURRENT token without depending on it, so they all put `token` in their
  // dependency arrays and re-fetched on every rotation.
  const getAuthToken = useCallback(() => tokenRef.current, []);

  const logout = useCallback(() => {
    // logout is fire-and-forget; contain sign-out failures.
    signOut().catch(() => {});
  }, [signOut]);

  // Clerk drives sign-in via <SignIn>; kept for interface parity.
  const login = useCallback(async () => {}, []);

  const ready = isLoaded && booted;
  const { queryScope, authReady } = bootScope({ userId: user?.id, liveOrgId: orgId, booted, hasToken: token !== null, resolvedOrgId });
  useEffect(() => {
    if (!authReady) return;
    initialReadsRef.current?.admit();
    initialReadsRef.current = null;
  }, [authReady]);

  return (
    <Ctx.Provider
      value={{
        ready,
        bootError,
        retryBoot,
        token,
        getAuthToken,
        queryScope,
        authReady,
        bootstrapFacts: authReady ? bootstrapFacts : null,
        userId: user?.id ?? null,
        // Both resolved from GET /v1/me (the backend maps the JWT org_id to
        // the internal account); used by flows that need the internal
        // account/user UUIDs directly — see `internalUserId`'s doc comment
        // for why it is NOT the same value as `userId` above.
        accountId,
        internalUserId,
        orgTeamsEnabled,
        ...todayFeatures,
        isPersonalOrg,
        resolvedOrgId,
        graphs: ocean.graphs,
        currentGraphId: ocean.currentGraphId,
        currentGraph: ocean.currentGraph,
        setCurrentGraphId: ocean.setCurrentGraphId,
        login,
        logout,
        refreshGraphs: ocean.refreshGraphs,
      }}
    >
      {children}
    </Ctx.Provider>
  );
}

export function AppProvider({ children }: { children: ReactNode }) {
  // W4 (docs/plans/request-reliability-and-scale.md §6.4). One QueryClient per
  // browser session, created in a ref rather than at module scope: a
  // module-level client would be shared across requests during SSR/prerender,
  // leaking one user's cached data into another's render. This is the standard
  // Next.js App Router hazard with TanStack Query and it is a data-leak bug,
  // not a performance one.
  const queryClientRef = useRef<QueryClient | null>(null);
  if (queryClientRef.current === null) queryClientRef.current = makeQueryClient();

  // OUTSIDE ClerkAuthProvider so the cache survives a token refresh. Clerk
  // sessions are ~60s; remounting the provider on each refresh would throw
  // away every cached query about once a minute, which is worse than having
  // no cache at all.
  return (
    <QueryClientProvider client={queryClientRef.current}>
      <ClerkAuthProvider>
        <QueryCacheIdentityBoundary><PageLoadBoundary>{children}</PageLoadBoundary></QueryCacheIdentityBoundary>
      </ClerkAuthProvider>
    </QueryClientProvider>
  );
}

/** `AppState.queryScope`/`authReady` from the boot state. The scope is the
 *  live identity only once the boot that resolved it is the boot for the live
 *  org (`resolvedOrgId` is set in the same pass as the boot's token); before
 *  that it is a `#pending` scope with `authReady` false, so a query neither
 *  runs nor lands in the incoming identity's entry with the outgoing
 *  identity's bearer. A user switch always passes through sign-out (token
 *  `null`), so the org is the only axis that changes with a token held. */
export function bootScope(args: {
  userId: string | null | undefined;
  liveOrgId: string | null | undefined;
  booted: boolean;
  hasToken: boolean;
  resolvedOrgId: string | null;
}): { queryScope: string; authReady: boolean } {
  const live = identityScope(args.userId, args.liveOrgId);
  const authReady = args.booted && args.hasToken && args.resolvedOrgId === (args.liveOrgId ?? null);
  return { queryScope: authReady ? live : `${live}#pending`, authReady };
}

export function useApp(): AppState {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useApp must be used within AppProvider");
  return ctx;
}
