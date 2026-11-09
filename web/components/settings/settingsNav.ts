import type { SettingsRole } from "./useSettingsRole";

/* The single Settings rail. Everyone gets the same four personal pages
   (Account is the landing page). Usage and Billing join them for personal accounts and
   personal orgs. Admins of a team org additionally get an "Organization"
   heading. Members of a team org never see the org pages at all.

   Labels use plain words: no ocean / reef / coral. */

export interface SettingsNavItem {
  href: string;
  label: string;
  sub: string;
}

export interface SettingsNavSection {
  /** Heading above the rows. Omitted for the unlabelled top section. */
  heading?: string;
  items: SettingsNavItem[];
}

export const ACCOUNT_HREF = "/settings";
export const MEMBERS_HREF = "/settings/organization/members";

const EVERYONE: SettingsNavItem[] = [
  { href: ACCOUNT_HREF, label: "Account", sub: "you · sign-in" },
  { href: "/settings/connections", label: "Connections", sub: "AI assistants · connected sources" },
  { href: "/settings/privacy", label: "Privacy", sub: "what is processed · retractions" },
];

const PERSONAL_USAGE: SettingsNavItem = {
  href: "/settings/usage",
  label: "Usage",
  sub: "credits · spend · plan",
};

const PERSONAL_BILLING: SettingsNavItem = {
  href: "/settings/billing",
  label: "Billing",
  sub: "plan · extra usage · payment",
};

export const ORG_NAV: SettingsNavItem[] = [
  { href: MEMBERS_HREF, label: "Members", sub: "invites · joining · seats" },
  { href: "/settings/organization/permissions", label: "Permissions", sub: "admins · members" },
  { href: "/settings/organization/connectors", label: "Connectors", sub: "sources connected here" },
  { href: "/settings/organization/usage", label: "Usage", sub: "questions · spend · activity" },
  { href: "/settings/organization/billing", label: "Billing", sub: "plan · seats · extra usage" },
];

/** Rail sections for a role. Until the role has loaded, only the pages that
 *  are the same for everyone render, so nothing flashes the wrong shape. */
export function settingsNavFor(role: SettingsRole, loaded: boolean): SettingsNavSection[] {
  const top = [...EVERYONE];
  if (loaded && role === "personal") top.push(PERSONAL_USAGE, PERSONAL_BILLING);
  const sections: SettingsNavSection[] = [{ items: top }];
  if (loaded && role === "org-admin") sections.push({ heading: "Organization", items: ORG_NAV });
  return sections;
}

/** Old route -> new route, served by client-side `router.replace` stubs. */
export const LEGACY_SETTINGS_ROUTES = {
  "/settings/access": "/settings/connections",
  "/settings/retraction-history": "/settings/privacy",
  "/settings/account": "/settings",
  "/settings/answers": "/settings/privacy",
  "/settings/capture": "/settings/privacy",
  "/settings/organization/storage": "/settings/organization/usage",
  "/settings/organization/policy": "/settings/organization/permissions",
} as const;
