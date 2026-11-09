"use client";

// CS-439 (org-onboarding plan §3 item 39, D3/D13) — the requester's own OPEN
// join requests, for the waiting-state banner (components/join/
// JoinRequestBanner.tsx) and the /join/[org] landing page
// (components/join/JoinLanding.tsx).
//
// Gate mirrors components/connectors/FollowedThreads.tsx's (the nearest
// existing "fetch once per personal-plane identity" component): a signed-in
// user's own open requests only exist on the personal plane (D19 — an
// ordinary org's member is, by definition, already in; personal orgs never
// receive join requests either way), so this skips entirely whenever the
// active context is a real org. `isPersonalOrgKnownForActiveOrg` closes the
// one-render window right after an org switch where the live Clerk `orgId`
// has updated but `useApp()`'s own `isPersonalOrg`/`resolvedOrgId` haven't
// caught up yet — see that function's own doc comment (lib/orgAdmin.ts).
//
// Caching is `@tanstack/react-query`'s job, not this hook's: every caller
// shares one cache entry (lib/joinRequestsQuery.ts's `myJoinRequestsQueryKey`),
// so `refresh()` called from ANY mounted consumer (the landing page, after a
// successful `create`) invalidates the data every OTHER mounted consumer (the
// shell's persistent banner) is reading too.

import { useCallback } from "react";
import { useAuth } from "@clerk/nextjs";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useApp } from "./providers";
import { isPersonalOrgKnownForActiveOrg } from "./orgAdmin";
import { myJoinRequestsQueryKey, myJoinRequestsQueryOptions } from "./joinRequestsQuery";
import type { MyJoinRequest } from "./api";

export interface UseMyJoinRequests {
  requests: MyJoinRequest[];
  loading: boolean;
  /** Invalidate the shared cache entry and refetch. Safe to call whenever —
   *  a no-op while ineligible (personal-plane gate closed) rather than an
   *  error, since a caller mid org-switch has no request to be wrong about. */
  refresh: () => void;
}

export function useMyJoinRequests(bootstrapOnly = false): UseMyJoinRequests {
  const { getAuthToken, authReady, ready, accountId, isPersonalOrg, resolvedOrgId, queryScope, bootstrapFacts } = useApp();
  const { isLoaded, orgId, sessionId, userId } = useAuth();
  const qc = useQueryClient();

  const known = isPersonalOrgKnownForActiveOrg({ ready, resolvedOrgId, activeOrgId: orgId ?? null });
  // Only the personal plane has open requests to show (see module comment) —
  // `orgId && !isPersonalOrg` is an ordinary org, the one context this must
  // never fetch (or render) in.
  const eligible =
    isLoaded && known && authReady && !!accountId && !!sessionId && !!userId && (!orgId || isPersonalOrg);

  const enabled = eligible && !(bootstrapOnly && bootstrapFacts != null);
  const query = useQuery(myJoinRequestsQueryOptions({ getAuthToken, scope: queryScope, enabled }));

  const refresh = useCallback(() => {
    void qc.invalidateQueries({ queryKey: myJoinRequestsQueryKey() });
  }, [qc]);

  if (!eligible) return { requests: [], loading: false, refresh };
  if (bootstrapOnly && bootstrapFacts && bootstrapFacts.myJoinRequests === null) return { requests: [], loading: false, refresh };
  // `query.data` stays `undefined` on a failed fetch (retry is off — see
  // lib/joinRequestsQuery.ts) — `?? []` is what makes a failure read
  // identically to "no open requests" for every consumer, per this hook's
  // own silent-on-error contract.
  return { requests: query.data ?? [], loading: enabled && query.isPending, refresh };
}
