"use client";

import type { ReactNode } from "react";
import { Loading, Empty } from "@/components/ui";
import { useOrgAdmin } from "./useOrgAdmin";
import { useCriticalPageReadLease } from "@/lib/pageReadRegistration";

/* Wraps every organization page. Admin-only, and HIDDEN rather than disabled
   for anyone else: a non-admin never sees the matrix, the policy switches or
   the billing numbers, not even greyed out. A member who lands here sees
   "Managed by your organization admin". */
export function OrgGate({ children }: { children: ReactNode }) {
  const { loaded, hasOrg, isAdmin } = useOrgAdmin();
  useCriticalPageReadLease("organization role verification", !loaded);

  if (!loaded) return <Loading what="checking your organization role" />;

  if (!hasOrg) {
    return (
      <Empty
        title="No organization is active"
        hint="Organization settings only apply when an org is your active context. Switch into one from the org switcher, or ask an admin to invite you."
      />
    );
  }

  if (!isAdmin) {
    return (
      <Empty
        title="Managed by your organization admin"
        hint="Only organization admins can view or change this. Ask an admin if you need something changed."
      />
    );
  }

  return <>{children}</>;
}
