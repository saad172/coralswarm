"use client";

import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { orgSelfserveEnabledQueryOptions } from "@/lib/appQueries";

/** Whether "Create a team" may be offered (and the switcher may send a
 *  team still being set up to its setup page). `false` while unknown, when
 *  the server says off, and on a failed read: the entry point is hidden
 *  unless the server says sign-up is open. */
export function useSelfserveEnabled(): boolean {
  const { authReady, getAuthToken, queryScope } = useApp();
  const query = useQuery({
    ...orgSelfserveEnabledQueryOptions({ getAuthToken, scope: queryScope }),
    enabled: authReady,
  });
  return query.data === true;
}
