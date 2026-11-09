// Best-effort display mapping for `recent_activity`'s real `event_type`
// vocabulary. See widgets/README.md "Backend fields needed": the ONLY four
// `insert_activity_event` call sites in the backend today are
// `atom_ingested`, `query_run`, `supersession_created`, `promote` — the
// artboard's richer vocabulary (decision/ask-with-question-text/session/
// conflict-with-titles/meeting-transcribed) does not exist as real events
// yet, and none of the four carry an actor DISPLAY NAME (`user_id` only).

import type { ActivityEvent } from "./types";
import type { Tokens } from "./theme";
import { pluralize } from "./format";

export interface EventDisplay {
  badgeLabel: string;
  badgeColor: (t: Tokens) => string;
  body: string;
  contextTag: string;
}

function str(v: unknown): string | undefined {
  return typeof v === "string" ? v : undefined;
}
function num(v: unknown): number | undefined {
  return typeof v === "number" ? v : undefined;
}

export function describeEvent(e: ActivityEvent): EventDisplay {
  const p = e.payload ?? {};
  switch (e.event_type) {
    case "atom_ingested": {
      const count = num(p.atom_count) ?? 0;
      const sourceType = str(p.source_type);
      return {
        badgeLabel: "ingest",
        badgeColor: (t) => t.sourceAmber,
        body: `${count} new ${pluralize(count, "coral")} ingested${sourceType ? ` from ${sourceType}` : ""}.`,
        contextTag: sourceType ?? "ingest",
      };
    }
    case "query_run": {
      const citations = num(p.citation_count) ?? 0;
      const mode = str(p.mode);
      return {
        badgeLabel: "ask",
        badgeColor: (t) => t.sourceTeal,
        body:
          mode === "egress_blocked"
            ? "Query run — cloud egress was blocked for this answer."
            : `Query run · ${citations} ${pluralize(citations, "citation")}.`,
        contextTag: mode ?? "query",
      };
    }
    case "supersession_created": {
      const source = str(p.source) ?? "manual";
      return {
        badgeLabel: "conflict",
        badgeColor: (t) => t.sourceAmber,
        body: "A coral was marked as superseding another.",
        contextTag: source,
      };
    }
    case "promote": {
      const count = num(p.promoted_atom_count) ?? num(p.requested_atom_count);
      return {
        badgeLabel: "promote",
        badgeColor: (t) => t.coral,
        body: count !== undefined ? `${count} ${pluralize(count, "coral")} promoted.` : "Corals promoted.",
        contextTag: "promote",
      };
    }
    default: {
      const compact = Object.entries(p)
        .slice(0, 3)
        .map(([k, v]) => `${k}: ${String(v)}`)
        .join(", ");
      return {
        badgeLabel: e.event_type.replace(/_/g, " "),
        badgeColor: (t) => t.sourcePurple,
        body: compact || "Activity recorded.",
        contextTag: e.event_type,
      };
    }
  }
}
