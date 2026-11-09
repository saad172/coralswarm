"use client";

import { useRef, type ReactNode } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { useQueryClient } from "@tanstack/react-query";

/**
 * Drop every cached query when the signed-in Clerk user changes.
 *
 * The `QueryClient` deliberately outlives `ClerkAuthProvider` so the cache
 * survives a ~60s token refresh — but that same longevity is a data-leak
 * hazard across a sign-out/sign-in. Query keys are scoped by `graphId`, not by
 * user (`keys.graphList()` is the bare `["graphs"]`), so if this shell ever
 * swaps identity without remounting, the incoming user would read the previous
 * user's cached oceans and ocean-scoped data until something happened to
 * refetch.
 *
 * Clearing on identity change is preferred over threading a user id into every
 * key: a key that forgets the user id fails OPEN (it serves the wrong user's
 * data), whereas a clear that fires too often merely costs a refetch. Given
 * the failure modes are that asymmetric, the blunt instrument is the right one.
 *
 * `queryClient.clear()` rather than `invalidateQueries()`: invalidation marks
 * data stale but KEEPS it, so a component could still render the previous
 * user's rows for one frame before the refetch lands. One frame of another
 * user's oceans is exactly the thing being prevented.
 */
export function QueryCacheIdentityBoundary({ children }: { children: ReactNode }) {
  const { user } = useUser();
  const { orgId } = useAuth();
  const qc = useQueryClient();
  const seenId = useRef<string | null | undefined>(undefined);

  // Identity is (user, ORG) — not user alone. Switching org keeps the same
  // Clerk user, so a `user.id`-only check never fires, and the incoming org
  // reads the previous org's cached oceans and ocean-scoped data. That is the
  // same leak as the cross-user one on a different axis, and on this product
  // it is the LIKELIER of the two: org switching is a normal in-session
  // action, sign-out/sign-in is not.
  //
  // It matters more here than a key-scoping fix would suggest, because the
  // org also changes which BACKEND the requests go to — `apiBase()` resolves
  // per-org under tenancy topology T1 — so cached data can belong to an
  // entirely different tenant database.
  const currentId = identityScope(user?.id, orgId);
  // CLEARED DURING RENDER, NOT IN AN EFFECT.
  //
  // An effect runs AFTER commit, so with the clear there a user A -> user B
  // swap could paint one frame of A's cached oceans under B's identity before
  // the effect fired. One frame of another user's data is exactly what this
  // exists to prevent, so the clear has to happen before anything renders
  // under the new identity.
  //
  // Clearing during render is safe here specifically because `qc.clear()`
  // touches the query cache, not React state — it schedules no update, so it
  // cannot loop. The ref keeps it idempotent: it fires once per identity
  // change, not on every render, so a normal mount never discards a warm
  // cache.
  if (seenId.current !== undefined && seenId.current !== currentId) {
    qc.clear();
  }
  seenId.current = currentId;

  return <>{children}</>;
}

/** The (user, org) identity string, from one definition so `queryScope` and the
 *  cache-clearing boundary can never disagree about who "the same identity" is. */
export function identityScope(userId: string | null | undefined, orgId: string | null | undefined): string {
  return `${userId ?? "anon"}::${orgId ?? "personal"}`;
}
