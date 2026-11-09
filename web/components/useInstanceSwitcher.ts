"use client";

import { useEffect } from "react";
import { useOrganization, useOrganizationList } from "@clerk/nextjs";
import { performInstanceSwitch } from "./instanceSwitch";

/* Headless account-instance switching: your personal CoralSwarm vs each
 * organization you belong to.
 *
 * THIS IS NOT AN OCEAN PICKER. An instance is a whole tenant — its own
 * database, its own members, its own policy. Oceans/workspaces live INSIDE
 * one, and are chosen from the Workspaces rail. Conflating the two in one
 * control was wrong: switching ocean re-scopes a query, switching instance
 * changes which company's data you are looking at.
 *
 * Extracted from components/OrgSwitcher.tsx so the sidebar header can present
 * this in one place.
 *
 * SWITCHING IS NOW `setActive` + a reload (claim-based tenancy PR 2.1, and
 * decision 7). Every org used to live on its own hostname, so a switch did a
 * REAL cross-origin document navigation via
 * `lib/orgSwitcherNav.ts#decideSwitchNav` — otherwise an org token minted on
 * the apex host was refused by the host-based binding gate. One apex now
 * serves every tenant and the gate binds from the token's `org_id` claim, so
 * there is nowhere to navigate TO; that module is deleted and this falls
 * through to the in-page `setActive` path it already had for localhost. That path is now the only one, so it also has to do explicitly
 * what the cross-host navigation used to do for free: reload the document
 * once Clerk has persisted the switch (decision 7). The ordering that makes
 * that correct lives in ./instanceSwitch.ts, which is testable; this file
 * only wires the real `setActive` and `window.location.reload` into it. */

export type Instance =
  | { kind: "personal"; id: "personal"; name: string }
  | { kind: "org"; id: string; name: string; slug: string | null };

export interface InstanceSwitcher {
  loaded: boolean;
  instances: Instance[];
  active: Instance | null;
  /** True when there is nothing to switch between (personal-only user). */
  soloInstance: boolean;
  /** Hand the target instance to Clerk, then reload the document (see
   *  instanceSwitch.ts). Async because the reload must wait for Clerk to
   *  persist the switch; rejects, without reloading, if the switch fails. */
  switchTo: (target: Instance) => Promise<void>;
}

export function useInstanceSwitcher(): InstanceSwitcher {
  const { organization } = useOrganization();
  const { isLoaded, userMemberships, setActive } = useOrganizationList({
    userMemberships: { infinite: true },
  });

  // `infinite: true` paginates; without draining the pages a user in more orgs
  // than one page holds would silently miss some. Before any early return, to
  // satisfy the rules of hooks.
  useEffect(() => {
    if (userMemberships?.hasNextPage && !userMemberships.isFetching) {
      userMemberships.fetchNext?.();
    }
  }, [userMemberships?.hasNextPage, userMemberships?.isFetching, userMemberships?.fetchNext]);

  const personal: Instance = { kind: "personal", id: "personal", name: "Personal" };

  if (!isLoaded) {
    return {
      loaded: false,
      instances: [],
      active: null,
      soloInstance: true,
      switchTo: async () => {},
    };
  }

  const orgs: Instance[] = (userMemberships.data ?? []).map((m) => ({
    kind: "org",
    id: m.organization.id,
    name: m.organization.name,
    slug: m.organization.slug,
  }));

  const instances = [personal, ...orgs];
  const active = organization
    ? (orgs.find((o) => o.id === organization.id) ?? {
        kind: "org" as const,
        id: organization.id,
        name: organization.name,
        slug: organization.slug,
      })
    : personal;

  const switchTo = (target: Instance): Promise<void> =>
    performInstanceSwitch(target, {
      setActive,
      reload: () => window.location.reload(),
    });

  return { loaded: true, instances, active, soloInstance: orgs.length === 0, switchTo };
}
