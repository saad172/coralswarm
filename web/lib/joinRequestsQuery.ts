// CS-439 (org-onboarding plan §3 item 39) — the requester's own OPEN
// join requests. One `useQuery` key shared by every consumer:
//
//   - components/join/JoinRequestBanner.tsx — the waiting-state banner,
//     mounted once in AppShell for the whole signed-in session.
//   - components/join/JoinLanding.tsx — the /join/[org] landing page, which
//     invalidates this key right after a successful `api.joinRequests.create`
//     so the banner (already mounted, if the shell is on screen) picks up
//     the new request on ITS next render without a page reload or any
//     bespoke pub/sub — `@tanstack/react-query`'s cache IS the shared state
//     (see lib/queryClient.ts's W4 module comment for why this dependency
//     exists at all).
//
// Same shape as lib/libraryQueries.ts's `AuthTokenGetter`/`queryOptions`
// convention — re-declared locally rather than imported so this module has
// no dependency on the provider.

import { plainPageSectionQuery, PLAIN_LOAD_PATHS } from "./pageShell";
import type { MyJoinRequest } from "./api";
import { STALE } from "./queryClient";
type AuthTokenGetter = () => string | null;
/** Unscoped use invalidates every identity; reads always pass their scope. */
export const myJoinRequestsQueryKey = (scope?: string) => scope ? ["myJoinRequests", scope] as const : ["myJoinRequests"] as const;
export function myJoinRequestsQueryOptions(args: { getAuthToken: AuthTokenGetter; enabled: boolean; scope?: string }) {
  return { ...plainPageSectionQuery<MyJoinRequest[]>(PLAIN_LOAD_PATHS.myJoinRequests, args.scope ?? "pending", {}, args.getAuthToken,
    { queryKey: myJoinRequestsQueryKey(args.scope) }), enabled: args.enabled, staleTime: STALE.READ };
}
