// View-model and adapters for the shared DetailPanel (components/detail/
// DetailPanel.tsx). The panel renders ONE shape; each data source gets an
// adapter, so Home, Today and Chat pass whatever they have and the panel never
// learns about their wire formats:
//
//   - `fromActivity` / `fromActivityDetail`: the Today activity cards
//     (`GET /v1/journal/activities[/{key}]`). The list row already carries
//     title, outcome, people and topics, so the panel paints from it at once
//     and `fromActivityDetail` fills in DECIDED and CAME FROM when the detail
//     read lands.
//   - `fromSourceResult`: the chat-find / title-search tile (`SourceResult`).
//     It carries decisions and reefs but no per-source quotes.
import type { ComponentType, SVGProps } from "react";
import type { Hue } from "@/components/ui";
import { IconChats, IconLibrary, IconMeetings, IconSessions } from "@/components/icons";
import { oceanHue } from "@/lib/oceanHue";
import type { Activity, ActivityDetail, ActivityKind } from "@/lib/journalActivitiesApi";
import type { SourceResult } from "@/lib/scopedChatApi";
import { dayLabel, localDay } from "@/lib/homeView";

export type DetailKind = ActivityKind;

interface KindMeta {
  label: string;
  hue: Hue;
  Icon: ComponentType<SVGProps<SVGSVGElement>>;
}

/** README "Source-kind mapping": hue = meaning. `index` (doc, note) is the
 *  shelved-spines glyph the DS ships as IconLibrary. */
export const KIND_META: Record<DetailKind, KindMeta> = {
  meeting: { label: "meeting", hue: "teal", Icon: IconMeetings },
  slack: { label: "slack thread", hue: "amber", Icon: IconChats },
  agent: { label: "agent session", hue: "plum", Icon: IconSessions },
  doc: { label: "doc session", hue: "teal", Icon: IconLibrary },
  note: { label: "note", hue: "teal", Icon: IconLibrary },
};

export interface DetailSource {
  quote: string;
  /** Mono line, e.g. `MEETING · 14:52`. */
  meta: string;
  kind: DetailKind;
}

export interface DetailReef {
  nodeId: string;
  title: string;
}

export interface DetailItem {
  verdict?: import("@/lib/graphSurfaces").JevVerdict | null;
  usedFor?: string[];
  /** Stable key: the activity key or the source item id. */
  id: string;
  kind: DetailKind;
  /** Mono kind label in the header. */
  kindLabel: string;
  /** `thu 29 oct · 14:52–15:34`. The ocean follows it, rendered as a dot. */
  meta: string;
  graphId: string;
  oceanName: string;
  oceanHue: Hue;
  title: string;
  outcome: string | null;
  decided: string[];
  people: string[];
  /** Total people, which can exceed `people.length`. */
  peopleCount: number;
  sources: DetailSource[];
  reefs: DetailReef[];
  /** True while a richer read (DECIDED, CAME FROM) is still loading. */
  partial?: boolean;
}

const hhmm = (iso: string, tz?: string) =>
  new Intl.DateTimeFormat("en-GB", {
    timeZone: tz,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(iso));

function kindOf(raw: string): DetailKind {
  return raw in KIND_META ? (raw as DetailKind) : "note";
}

/** `thu 29 oct · 14:52–15:34` (or just the start when start == end). */
export function activityMeta(a: Pick<Activity, "start" | "end">, tz?: string): string {
  const day = dayLabel(localDay(a.start, tz));
  const s = hhmm(a.start, tz);
  const e = hhmm(a.end, tz);
  return s === e ? `${day} · ${s}` : `${day} · ${s}–${e}`;
}

export function fromActivity(
  a: Activity,
  opts: { oceanKind?: string; tz?: string } = {},
): DetailItem {
  const m = KIND_META[kindOf(a.kind)];
  return {
    id: a.key,
    kind: kindOf(a.kind),
    kindLabel: a.kind_label || m.label,
    meta: activityMeta(a, opts.tz),
    graphId: a.graph.id,
    oceanName: a.graph.name,
    oceanHue: oceanHue({ graph_id: a.graph.id, kind: opts.oceanKind }),
    title: a.title,
    outcome: a.outcome,
    decided: [],
    people: a.people,
    peopleCount: a.people_count,
    sources: [],
    reefs: a.topics.map((t) => ({ nodeId: t.node_id, title: t.title })),
    partial: true,
  };
}

export function fromActivityDetail(
  d: ActivityDetail,
  opts: { oceanKind?: string; tz?: string } = {},
): DetailItem {
  return {
    ...fromActivity(d, opts),
    decided: d.decisions,
    sources: d.sources.map((s) => ({ quote: s.quote, meta: s.meta_label, kind: kindOf(d.kind) })),
    // The detail's `reefs` is the full list; `topics` on the card is capped.
    reefs: (d.reefs.length ? d.reefs : d.topics).map((t) => ({
      nodeId: t.node_id,
      title: t.title,
    })),
    partial: false,
  };
}

export function fromSourceResult(
  r: SourceResult,
  opts: { oceanKind?: string; tz?: string } = {},
): DetailItem {
  const kind = kindOf(r.kind);
  let meta = "";
  if (r.occurred_at) {
    meta = dayLabel(localDay(r.occurred_at, opts.tz));
    if (r.duration_ms) meta += ` · ${Math.max(1, Math.round(r.duration_ms / 60_000))} min`;
    else meta += ` · ${hhmm(r.occurred_at, opts.tz)}`;
  }
  return {
    id: r.source_item_id,
    kind,
    kindLabel: r.kind_label || KIND_META[kind].label,
    meta,
    graphId: r.graph.id,
    oceanName: r.graph.name,
    oceanHue: oceanHue({ graph_id: r.graph.id, kind: opts.oceanKind }),
    title: r.title,
    outcome: r.outcome,
    decided: r.decisions,
    people: r.people,
    peopleCount: r.people_count,
    sources: r.quotes.map((q) => ({
      quote: q.quote,
      meta: `${r.kind_label.toUpperCase()}${q.occurred_at ? ` · ${hhmm(q.occurred_at, opts.tz)}` : ""}`,
      kind,
    })),
    reefs: r.reefs.map((t) => ({ nodeId: t.node_id, title: t.title })),
  };
}

/** The chat the primary "Ask about this" button opens: scoped to the item's
 *  ocean (`graph_id`) with the composer prefilled (`q`). It never auto-sends,
 *  like every `?q=` link (see ChatComposer). */
export function askAboutItemHref(item: Pick<DetailItem, "graphId" | "title">): string {
  const q = `Tell me about “${item.title}”`;
  return `/chats?${new URLSearchParams({ graph_id: item.graphId, q })}`;
}

/** Link target for a REEFS TOUCHED chip. */
export function reefHref(r: DetailReef, graphId: string): string {
  return `/topics?${new URLSearchParams({ id: r.nodeId, graph_id: graphId })}`;
}

export function fromCitation(
  source: import("../chat/evidence").EvidenceSource,
  turn: import("../chat/types").AssistantTurn,
): DetailItem {
  const c = turn.retainedCitations?.[source.n - 1];
  const kind = kindOf(source.sourceType ?? "note");
  return {
    id: source.sourceKey,
    kind,
    kindLabel: KIND_META[kind].label,
    meta: source.occurredAt || source.capturedAt || source.date,
    graphId: c?.graph_id ?? "",
    oceanName: source.ocean ?? "",
    oceanHue: source.hue,
    title: source.title,
    outcome: source.notice ?? null,
    decided: [],
    people: [],
    peopleCount: 0,
    sources: source.quote ? [{ quote: source.quote, meta: source.type, kind }] : [],
    reefs: [],
    verdict: source.verdict,
    usedFor: turn.text.split(/\n\s*\n/).filter((b) => b.includes(`[${source.n}]`)).map(answerBlockPreview),
  };
}

export function answerBlockPreview(block: string): string {
  const plain = block
    .replace(/!?(\[([^\]]*)\])\([^)]*\)/g, "$2")
    .replace(/<[^>]*>/g, "")
    .replace(/\[\d+\]/g, "")
    .replace(/^[ \t]*(?:#{1,6} |>|[-+*] |\d+\. )/gm, "")
    .replace(/[*_`~]/g, "")
    .replace(/\s+/g, " ")
    .trim();
  return Array.from(plain).slice(0, 120).join("");
}
