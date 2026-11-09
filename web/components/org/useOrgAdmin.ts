"use client";

import { useAuth, useOrganization } from "@clerk/nextjs";
import { useApp } from "@/lib/providers";
import { resolveOrgAdmin } from "@/lib/orgAdmin";

/* Admin-gating for the turn-3 organization surfaces.
   Mirrors the isAdmin check the other org screens use (orgRole is a Clerk
   role string like "org:admin" / "org:owner" / "org:member") so every org
   screen agrees on what "admin" means. Turn-3 screens are admin-only: gate on
   this and HIDE (via OrgGate below) rather than disable for non-admins. */
export interface OrgAdminState {
  /** Clerk auth + organization both resolved. */
  loaded: boolean;
  /** An organization is the active context at all (vs. the personal plane).
   *
   *  Clerk's `orgId` alone cannot answer this: a PERSONAL ORG — a Clerk
   *  organization that stands for one person's own ocean (control migration
   *  0208) — sets `orgId` exactly like a real organization does. Excluding
   *  `isPersonalOrg` here (`useApp()`'s `GET /v1/me` signal, via
   *  `lib/orgAdmin.ts`'s `resolveOrgAdmin`) is what keeps a one-person org
   *  out of every "Organization" surface this hook gates: member rosters,
   *  invite flows, org settings. */
  hasOrg: boolean;
  isAdmin: boolean;
  orgId: string | null;
  orgName: string | null;
  orgSlug: string | null;
}

export function useOrgAdmin(): OrgAdminState {
  const { isLoaded: authLoaded, orgId, orgRole } = useAuth();
  const { isLoaded: orgLoaded, organization } = useOrganization();
  const { isPersonalOrg } = useApp();
  const { hasOrg, isAdmin } = resolveOrgAdmin({ orgId, orgRole, isPersonalOrg });

  return {
    loaded: authLoaded && orgLoaded,
    hasOrg,
    isAdmin,
    orgId: orgId ?? null,
    orgName: organization?.name ?? null,
    orgSlug: organization?.slug ?? null,
  };
}
