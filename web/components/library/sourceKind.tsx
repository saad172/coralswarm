// Shared source-type taxonomy — label, hue and glyph — for every screen that
// renders raw captures by `source_type` (Topic detail's timeline/source mix,
// Library · Sources, and the facet lists on Sessions/Meetings/People). One
// mapping so a Slack coral reads identically (amber, the same glyph) whether
// it's shown in a topic's timeline or a source-group row in the Library.
//
// Hue semantics (docs/design/DESIGN_SYSTEM.md): amber = sources/candidates
// (Slack), teal = ingested/structured content (meetings), plum = systems
// (docs, MCP, coding sessions), pink = people (email — a person-to-person
// channel), coral = human-authored notes/decisions.

import type { Hue } from "@/components/ui";

export type SourceIconKind = "slack" | "meeting" | "doc" | "email" | "note" | "code" | "github" | "default";

interface SourceKindMeta {
  label: string;
  hue: Hue;
  icon: SourceIconKind;
}

const SOURCE_META: Record<string, SourceKindMeta> = {
  slack: { label: "Slack", hue: "amber", icon: "slack" },
  email: { label: "Email", hue: "pink", icon: "email" },
  transcript: { label: "Meeting", hue: "teal", icon: "meeting" },
  note: { label: "Note", hue: "coral", icon: "note" },
  notion: { label: "Doc", hue: "plum", icon: "doc" },
  mcp: { label: "System", hue: "plum", icon: "doc" },
  coding_session: { label: "Coding session", hue: "plum", icon: "code" },
  agent_session: { label: "Agent session", hue: "plum", icon: "code" },
  work_session: { label: "Work session", hue: "coral", icon: "default" },
  meeting_session: { label: "Meeting session", hue: "teal", icon: "meeting" },
  // GitHub connector (docs/connectors/github.md §3.5) — six distinct
  // `source_type`s, one per atom kind, all sharing the provider's own hue
  // and glyph the same way Slack's single `source_type` does. `pink` is the
  // hue `components/settings/providerMeta.ts` gives the GitHub provider tile
  // — kept identical here so a GitHub coral reads as the same product
  // wherever it's shown.
  github_pr: { label: "Pull request", hue: "pink", icon: "github" },
  github_issue: { label: "Issue", hue: "pink", icon: "github" },
  github_release: { label: "Release", hue: "pink", icon: "github" },
  github_commit: { label: "Commit", hue: "pink", icon: "github" },
  github_code: { label: "Code", hue: "pink", icon: "github" },
  github_code_summary: { label: "Code summary", hue: "pink", icon: "github" },
};

// The backend normalizes several provider strings onto these families
// (backend/src/routes/queries.rs): gmail/outlook/mail → email; meeting/zoom/
// huddle → transcript. Mirrored here so those source types get a real label
// instead of falling through to a raw-token default.
const SOURCE_ALIASES: Record<string, string> = {
  gmail: "email",
  outlook: "email",
  mail: "email",
  meeting: "transcript",
  meetings: "transcript",
  zoom: "transcript",
  huddle: "transcript",
  meet: "transcript",
  teams_transcript: "transcript",
  google_meet: "transcript",
};

/** Canonical facet key for a raw `source_type` — resolves provider aliases so
 *  every facet comparison/count operates on the key the coral actually
 *  displays under (a "zoom" atom shows as "Meeting" and must match a
 *  "transcript"-keyed facet, not sit outside it). */
export const canonicalSourceType = (t: string): string => SOURCE_ALIASES[t] ?? t;

export const sourceMeta = (t: string): SourceKindMeta =>
  SOURCE_META[t] ?? SOURCE_META[SOURCE_ALIASES[t] ?? ""] ?? { label: t || "Source", hue: "teal", icon: "default" };

export const hueForSource = (t: string): Hue => sourceMeta(t).hue;
export const iconKindFor = (t: string): SourceIconKind => sourceMeta(t).icon;

/** Plural collective label for a facet chip ("Slack", "Meetings", "Docs") —
 *  distinct from `sourceMeta(t).label`'s singular provenance-line form
 *  ("Slack", "Meeting"), since "a chip stands for corals from this source". */
const FACET_LABEL: Partial<Record<SourceIconKind, string>> = {
  slack: "Slack",
  meeting: "Meetings",
  doc: "Docs",
  email: "Emails",
  note: "Notes",
  code: "Coding sessions",
  // Collective label for all six github_* types at once — same shape as
  // "Slack" above (one facet chip for a whole provider's source types).
  github: "GitHub",
};
export const facetLabelFor = (t: string): string => FACET_LABEL[iconKindFor(t)] ?? sourceMeta(t).label;

/** Tiny line icon per source kind — 24-grid, 1.7 stroke, round joins. Kept as
 *  its own component (not `components/icons.tsx`, which is another agent's
 *  file) so Topic detail and every Library list can render the same glyph for
 *  the same kind of capture. */
export function SourceGlyph({ kind, size = 15, className }: { kind: SourceIconKind; size?: number; className?: string }) {
  const s = {
    width: size,
    height: size,
    viewBox: "0 0 24 24",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: 1.7,
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
    className,
  };
  switch (kind) {
    case "slack":
      return (
        <svg {...s}>
          <path d="M9 3v9M9 3a2 2 0 1 0 0 4M15 21v-9M15 21a2 2 0 1 0 0-4M3 15h9M3 15a2 2 0 1 0 4 0M21 9h-9M21 9a2 2 0 1 0-4 0" />
        </svg>
      );
    case "meeting":
      return (
        <svg {...s}>
          <rect x="3" y="5" width="13" height="14" rx="2" />
          <path d="m16 10 5-3v10l-5-3z" />
        </svg>
      );
    case "doc":
      return (
        <svg {...s}>
          <path d="M7 3h7l4 4v14H7z" />
          <path d="M14 3v4h4M10 13h6M10 17h6" />
        </svg>
      );
    case "email":
      return (
        <svg {...s}>
          <rect x="3" y="5" width="18" height="14" rx="2" />
          <path d="m3 7 9 6 9-6" />
        </svg>
      );
    case "note":
      return (
        <svg {...s}>
          <path d="M5 4h14v12l-5 5H5z" />
          <path d="M14 21v-5h5" />
        </svg>
      );
    case "code":
      return (
        <svg {...s}>
          <path d="m8 8-4 4 4 4M16 8l4 4-4 4M13.5 5l-3 14" />
        </svg>
      );
    case "github":
      // A simple branch/fork glyph — not a literal brand trace (the marks
      // this file draws are line-icon abstractions, not logos; contrast
      // `components/settings/ProviderIcon.tsx`, which DOES render the real
      // GitHub mark on the connector tile). Reads as "a code host" at 15px
      // without competing with `code`'s own glyph above, which stands for a
      // captured coding SESSION rather than a repository.
      return (
        <svg {...s}>
          <circle cx="6" cy="4" r="2.3" />
          <circle cx="6" cy="20" r="2.3" />
          <circle cx="18" cy="9" r="2.3" />
          <path d="M6 6.3v11.4M6 9c0 3.5 3 4.5 9.7 4.5M18 11.3v-.1" />
        </svg>
      );
    default:
      return (
        <svg {...s}>
          <circle cx="12" cy="12" r="9" />
        </svg>
      );
  }
}
