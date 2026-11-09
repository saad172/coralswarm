import type { Hue } from "@/components/ui";
import type { ReefType } from "@/lib/api";

/* Kind taxonomy shared by the Workspaces (1e) and Library (1f) screens.
   Hue semantics follow DESIGN_SYSTEM.md (decision→coral, thread/source→amber,
   entity→pink, reference→plum); labels follow the redesign mock's facet
   names ("People" rather than the legacy "entity"). */

export const REEF_KIND_META: Record<ReefType, { label: string; facet: string; hue: Hue }> = {
  decision: { label: "decision", facet: "Decisions", hue: "coral" },
  thread: { label: "thread", facet: "Threads", hue: "amber" },
  entity: { label: "person", facet: "People", hue: "pink" },
  reference: { label: "reference", facet: "References", hue: "plum" },
};

export const REEF_TYPES: ReefType[] = ["decision", "thread", "entity", "reference"];

/** "3 weeks", "2h", "today" — coarse relative age, matching the mock's meta
 *  lines (no seconds/minutes precision needed for a topic card). */
export function relativeAge(iso: string): string {
  const ms = Date.now() - new Date(iso).getTime();
  const min = Math.floor(ms / 60000);
  if (min < 60) return min <= 1 ? "just now" : `${min}m`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h`;
  const day = Math.floor(hr / 24);
  if (day < 7) return `${day}d`;
  const week = Math.floor(day / 7);
  if (week < 5) return `${week}w`;
  const month = Math.floor(day / 30);
  if (month < 12) return `${month}mo`;
  return `${Math.floor(day / 365)}y`;
}
