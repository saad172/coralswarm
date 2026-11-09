"use client";

import { useOrgAdmin } from "@/components/org/useOrgAdmin";

/* Which settings you get depends on which of three situations you're in.
 * Settings is where that difference is most consequential, so name it once
 * rather than re-deriving `hasOrg && isAdmin` at every call site.
 *
 *   personal    — your own account. You answer every question, and there is
 *                 no admin above you. That cuts both ways: it's also why the
 *                 recovery passphrase is unskippable, since nobody can
 *                 recover the account for you.
 *   org-member  — you're inside someone's organization. Answer-mode, model
 *                 tier and recording are set org-wide; a member may choose
 *                 something STRICTER than the org default, never looser.
 *   org-admin   — the same, plus the Organization surface (members,
 *                 permissions, policy, connectors, usage, storage, billing).
 */

export type SettingsRole = "personal" | "org-member" | "org-admin";

export interface SettingsRoleState {
  role: SettingsRole;
  /** Clerk auth + org state resolved. Gate destructive/role-specific UI on
   *  this so nothing flashes the wrong shape mid-load. */
  loaded: boolean;
  orgName: string | null;
  /** True when an organization sets the defaults this account inherits. */
  orgGoverned: boolean;
  isAdmin: boolean;
}

export function useSettingsRole(): SettingsRoleState {
  const { loaded, hasOrg, isAdmin, orgName } = useOrgAdmin();
  const role: SettingsRole = !hasOrg ? "personal" : isAdmin ? "org-admin" : "org-member";
  return { role, loaded, orgName, orgGoverned: hasOrg, isAdmin };
}

/** One-line description of who decides, shown under the Settings title.
 *  Kept beside the role so the three wordings can't drift apart. */
export function roleSummary(state: SettingsRoleState): string {
  switch (state.role) {
    case "personal":
      return "Your account, your rules. Nobody else can change these.";
    case "org-member":
      return `These are your own settings. ${state.orgName ?? "Your organization"} manages the organization settings, and only an org admin can change them.`;
    case "org-admin":
      return `You administer ${state.orgName ?? "this organization"}. These are your own settings; the organization settings are under the Organization heading.`;
  }
}
