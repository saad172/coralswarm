"use client";

import { useQuery, type QueryClient } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { appInvalidation, orgJoinRequestCountQueryOptions } from "@/lib/appQueries";
import { useOrgAdmin } from "./useOrgAdmin";
import { pagePayloadQuery } from "@/lib/pageSections";
import type { MembersAdminPayload } from "@/lib/pageSectionTypes/membersAdmin";

/* The open join-request count for the admin badge (CS-438, org-onboarding
   plan §3 item 5, D13) — shared by the Organization rail's Members row
   (OrgSettingsShell) and the sidebar's Settings entry (AppSidebar), so both
   surfaces agree on one cached number rather than each
   rolling its own.

   Admin-only, cached (no polling, no refetch-on-focus — a badge a few
   minutes stale is fine for a daily-digest-paced feature), and
   SILENT on error: a failed count read must never surface a visible error
   state on every page's sidebar. `null` covers "not loaded yet", "not an
   admin", "no active org", and "the fetch failed" — the caller only ever
   asks "is this a positive number worth rendering a pill for", never why it
   isn't. */

/* CS-441 — the badge and the members-page approval queue have to agree:
   approving/rejecting a request from the queue drops the rail pill without a
   page reload. Both now read the cache (lib/appQueries.ts), so a decision
   invalidates the `orgJoinRequests` prefix, which reaches the queue and every
   mounted badge at once. */

/** Call after an approve/reject decision so every mounted badge (and the
 *  queue) refetches. */
export function refreshJoinRequestCount(queryClient: QueryClient): void {
  void queryClient.invalidateQueries({ queryKey: appInvalidation.orgJoinRequests() }, { cancelRefetch: false });
  void queryClient.invalidateQueries({ queryKey: ["members_admin"] }, { cancelRefetch: false });
}

export function useJoinRequestCount(): number | null {
  const { authReady, getAuthToken, queryScope, bootstrapFacts } = useApp();
  const { loaded, hasOrg, isAdmin } = useOrgAdmin();
  const eligible = authReady && loaded && hasOrg && isAdmin;
  // Silent by design — see this hook's own doc comment: a failed read is
  // `null`, never an error state.
  const query = useQuery({ ...orgJoinRequestCountQueryOptions({ getAuthToken, scope: queryScope }), enabled: eligible && bootstrapFacts == null });
  const members = useQuery({ ...pagePayloadQuery<MembersAdminPayload>("members_admin", queryScope, {}, getAuthToken), enabled: false });
  if (eligible && members.data) return members.data.join_requests.filter((row) => row.decision === null).length;
  if (members.error || members.dataUpdatedAt > 0 || bootstrapFacts?.joinRequestCount === null) return null;
  return eligible && query.data !== undefined ? query.data : null;
}
