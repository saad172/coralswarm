import type { Hue } from "@/components/ui";

/* Shared connector metadata — the hue/letter/description every screen that
   renders a provider tile agrees on (personal Connectors, org Connectors,
   Settings · Access's read-only connector list). Slack, Microsoft and GitHub
   are live (GitHub behind its own rollout flag, below); Gmail and Zoom are
   shown as "coming soon" per the redesign. */
export const KNOWN_PROVIDERS = ["slack", "microsoft", "github"] as const;
export type KnownProvider = (typeof KNOWN_PROVIDERS)[number];

export function isKnownProvider(s: string | null): s is KnownProvider {
  return KNOWN_PROVIDERS.includes(s as KnownProvider);
}

export interface ProviderMeta {
  label: string;
  description: string;
  hue: Hue;
  letter: string;
  comingSoon?: boolean;
  /** Pull-based connectors expose a "Sync now" action once connected. */
  supportsSync?: boolean;
  /** Providers with an org-wide admin-consent flow show an admin-enable link. */
  supportsAdminConsent?: boolean;
}

export const PROVIDER_META: Record<KnownProvider | "gmail" | "zoom", ProviderMeta> = {
  slack: {
    label: "Slack",
    description: "Captures channels you add the bot to.",
    hue: "amber",
    letter: "S",
  },
  microsoft: {
    label: "Microsoft",
    description:
      "Calendar syncs automatically; email is opt-in per thread (never your whole inbox).",
    hue: "plum",
    letter: "M",
    supportsSync: true,
    supportsAdminConsent: true,
  },
  github: {
    label: "GitHub",
    description: "Pull requests, reviews, issues, releases and commits from repositories you select. Never your source code.",
    // "G" is already Gmail's; "H" alone would read as a different product,
    // so this is the one entry in the table with a two-character fallback
    // glyph (ProviderMark's non-brand letter box has room for it — see its
    // own comment). "pink" is the one hue left unclaimed by slack (amber),
    // microsoft (plum), gmail (coral) and zoom (teal).
    hue: "pink",
    letter: "GH",
  },
  gmail: {
    label: "Gmail",
    description: "Save threads from the Gmail add-on; email is opt-in per thread.",
    hue: "coral",
    letter: "G",
  },
  zoom: {
    label: "Zoom",
    description: "Meeting transcripts and participant metadata.",
    hue: "teal",
    letter: "Z",
    comingSoon: true,
  },
};

/* ── GitHub tile rollout gate (docs/connectors/github.md §3.12) ─────────────
   Build-time web flag: a `NEXT_PUBLIC_*` const evaluated once per
   deployment — there is no runtime state to re-check, so this is a module
   const rather than a hook. Default OFF: the connector doc's own rollout
   plan is "enabled first for the founder's own org", not a general release. */

/** Pure decision, taking the raw env STRING rather than reading
 *  `process.env` internally. `vitest` never populates `NEXT_PUBLIC_*` — a
 *  module-scope read is `undefined` under test regardless of
 *  what's actually configured, so a test asserting against the CONST below
 *  would pass no matter what this function did. Taking the value as a
 *  parameter is what makes the assertion mean something. */
export function githubConnectorTileEnabled(envValue: string | undefined): boolean {
  return envValue === "true";
}

export const GITHUB_CONNECTOR_TILE = githubConnectorTileEnabled(
  process.env.NEXT_PUBLIC_GITHUB_CONNECTOR_TILE,
);

/** The subset of `KNOWN_PROVIDERS` a tile grid should actually render —
 *  every known provider except `github` while its rollout flag is off.
 *  Pure (takes the flag as a parameter, same reason as above) so the GATING
 *  itself is testable, not just the constant. Both the personal Connectors
 *  grid (`app/(app)/connectors/page.tsx`) and the org Connectors settings
 *  page (`app/(app)/settings/organization/connectors/page.tsx`) call this
 *  rather than filtering inline, so the two surfaces can never disagree
 *  about whether GitHub is visible — see the brief's own note to mirror
 *  Slack's handling there. */
export function visibleProviders(
  providers: readonly KnownProvider[],
  githubTileEnabled: boolean,
): KnownProvider[] {
  return providers.filter((p) => p !== "github" || githubTileEnabled);
}

/* ── The connectors redesign's ten (docs/design/connectors-redesign/PLAN.md) ──
   One table for the new page, the wizard and the detail page. Separate from
   PROVIDER_META on purpose: that one keys the old per-provider routes
   (`KnownProvider`), and widening it would put the new connectors into the old
   grid. Keys are the `key` of `GET /v1/connectors`, in its display order.

   Hues: Slack amber, Microsoft plum, Google coral, GitHub pink (as above);
   toolkits take the same five: Jira and Confluence teal, Linear plum, HubSpot
   amber, Zendesk pink, Dart coral.

   `captures` / `never` are the design's copy (connectors-kit.js): what is
   taken, and, where it matters, what is not. */
import { type ConnectorKey } from "@/lib/connectorKeys";
export { CONNECTOR_KEYS, isConnectorKey, type ConnectorKey } from "@/lib/connectorKeys";

export interface ConnectorMeta {
  label: string;
  hue: Hue;
  letter: string;
  /** What CoralSwarm captures, as a sentence. */
  captures: string;
  /** What it never captures, as a noun phrase ("DMs"); read after "Never ". */
  never?: string;
}

export const CONNECTOR_META: Record<ConnectorKey, ConnectorMeta> = {
  slack: {
    label: "Slack",
    hue: PROVIDER_META.slack.hue,
    letter: PROVIDER_META.slack.letter,
    captures: "Messages and threads in channels you pick.",
    never: "DMs",
  },
  microsoft: {
    label: "Microsoft",
    hue: PROVIDER_META.microsoft.hue,
    letter: PROVIDER_META.microsoft.letter,
    captures: "Outlook mail, Calendar, Teams chats, channels and meetings.",
  },
  google: {
    label: "Google",
    hue: "coral",
    letter: "G",
    captures: "Gmail, Calendar, Meet and Chat.",
  },
  github: {
    label: "GitHub",
    hue: PROVIDER_META.github.hue,
    letter: PROVIDER_META.github.letter,
    captures: "Pull requests, reviews, issues, releases and commits.",
    never: "your source code",
  },
  linear: {
    label: "Linear",
    hue: "plum",
    letter: "L",
    captures: "Issues, projects and comments across your workspace.",
  },
  jira: {
    label: "Jira",
    hue: "teal",
    letter: "J",
    captures: "Issues, comments and status changes across your site.",
  },
  confluence: {
    label: "Confluence",
    hue: "teal",
    letter: "C",
    captures: "Pages and comments in the spaces you can see.",
  },
  hubspot: {
    label: "HubSpot",
    hue: "amber",
    letter: "H",
    captures: "Deals, notes and logged emails on your records.",
  },
  zendesk: {
    label: "Zendesk",
    hue: "pink",
    letter: "Z",
    captures: "Tickets with their public and internal replies.",
  },
  dart: {
    label: "Dart",
    hue: "coral",
    letter: "D",
    captures: "Tasks, docs and comments in your workspace.",
  },
};

/** The card's "what it takes" line: "Messages and threads in channels you pick. Never DMs." */
export function connectorBlurb(meta: ConnectorMeta): string {
  return meta.never ? `${meta.captures} Never ${meta.never}.` : meta.captures;
}
