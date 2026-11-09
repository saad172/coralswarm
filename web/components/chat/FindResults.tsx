"use client";

import { Avatar, Badge, Dot, cn, hueForName, hueTint, hueVar } from "@/components/ui";

import { KIND_META } from "@/components/detail/detailModel";
import { dayLabel, localDay } from "@/lib/homeView";
import { openCommandPalette } from "@/lib/commandPalette";
import { oceanHue } from "@/lib/oceanHue";
import type { SourceResult } from "@/lib/scopedChatApi";

/* Chat find result tiles (README "Chat find (4b / 4c)").

   Rendered under the answer text when the answer carries `source_results`.
   A tile paints entirely from the data it arrived with: clicking it opens the
   shared DetailPanel through `fromSourceResult`, with no further request.

   Interplay with the citation UI: on a message that has tiles, the tiles ARE
   the source display. AnswerTurn drops that message's EvidenceStrip and the
   page drops the SourceRail while such a message is the latest one. The inline
   [n] chips in the prose, the header Evidence button and the context drawer
   are untouched, so the cited passages stay one click away. */

const MAX_AVATARS = 3;

/** `THU 15 OCT · 42 MIN` (uppercased by CSS). Duration is meetings only; with
 *  no date there is no line at all. */
export function tileMetaLine(r: SourceResult, tz?: string): string {
  if (!r.occurred_at) return "";
  const day = dayLabel(localDay(r.occurred_at, tz));
  return r.duration_ms ? `${day} · ${Math.max(1, Math.round(r.duration_ms / 60_000))} min` : day;
}

function Tile({
  r,
  open,
  oceanKind,
  onOpen,
}: {
  r: SourceResult;
  open: boolean;
  oceanKind?: string;
  onOpen: () => void;
}) {
  const kind = r.kind in KIND_META ? r.kind : "note";
  const meta = KIND_META[kind];
  const Icon = meta.Icon;
  const line = tileMetaLine(r);
  const shown = r.people.slice(0, MAX_AVATARS);
  const more = r.people_count - shown.length;
  return (
    <button
      data-src
      type="button"
      onClick={onOpen}
      aria-haspopup="dialog"
      aria-expanded={open}
      data-best={r.best ? "true" : undefined}
      className={cn(
        "flex min-w-0 flex-col gap-[8px] rounded-card border bg-bg px-[14px] py-[12px] text-left shadow-card transition-[border-color,box-shadow] duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
        open ? "border-ink shadow-pop" : r.best ? "border-ink-4 hover:border-ink-3" : "border-line hover:border-ink-3",
      )}
    >
      <span className="flex items-center gap-[8px]">
        <span
          className="grid h-[26px] w-[26px] flex-none place-items-center rounded-badge"
          style={{ background: hueTint(meta.hue), color: hueVar(meta.hue) }}
        >
          <Icon width={14} height={14} />
        </span>
        <span className="min-w-0 flex-1 truncate font-mono text-[10px] uppercase tracking-[0.1em] text-ink-3">
          {r.kind_label || meta.label}
        </span>
        {r.best && <Badge tone="brand">best match</Badge>}
      </span>
      <span className="line-clamp-2 text-[14px] font-medium leading-[1.35] text-ink">{r.title}</span>
      {r.outcome && <span className="line-clamp-2 text-[12.5px] leading-[1.45] text-ink-2">{r.outcome}</span>}
      {line && <span className="font-mono text-[10.5px] uppercase tracking-[0.04em] text-ink-3">{line}</span>}
      <span className="mt-auto flex items-center gap-[8px] border-t border-line-3 pt-[9px]">
        <span className="flex items-center">
          {shown.map((name, i) => (
            <Avatar
              key={`${name}-${i}`}
              name={name}
              size={20}
              hue={hueForName(name)}
              className={cn("border-2 border-bg", i > 0 && "-ml-[6px]")}
            />
          ))}
          {more > 0 && <span className="ml-[6px] font-mono text-[10px] text-ink-3">+{more}</span>}
        </span>
        <span className="flex-1" />
        <Dot hue={oceanHue({ graph_id: r.graph.id, kind: oceanKind })} size={6} />
        <span className="min-w-0 max-w-[50%] truncate font-mono text-[10.5px] text-ink-3">{r.graph.name}</span>
      </span>
    </button>
  );
}

export function FindResults({
  results,
  openId,
  onOpenChange,
  oceanKindOf,
}: {
  results: SourceResult[];
  /** The source_item_id whose DetailPanel is open, or null. */
  openId: string | null;
  onOpenChange: (id: string | null) => void;
  oceanKindOf?: (graphId: string) => string | undefined;
}) {
  if (results.length === 0) return null;
  const opened = openId ? results.find((r) => r.source_item_id === openId) : undefined;
  return (
    <section aria-label="Matching sources" className="flex flex-col gap-[10px]">
      <div className="grid grid-cols-3 gap-[10px] max-md:grid-cols-1">
        {results.map((r) => (
          <Tile
            key={r.source_item_id}
            r={r}
            open={r.source_item_id === opened?.source_item_id}
            oceanKind={oceanKindOf?.(r.graph.id)}
            onOpen={() => onOpenChange(r.source_item_id === openId ? null : r.source_item_id)}
          />
        ))}
      </div>
      <p className="text-[12.5px] leading-[1.5] text-ink-3">
        Not the one? Ask with a person or a date, or press{" "}
        <button
          type="button"
          onClick={openCommandPalette}
          aria-label="Search titles (⌘K)"
          className="rounded-badge border border-line bg-bg px-[5px] py-[1px] font-mono text-[11px] text-ink-2 transition-colors duration-150 hover:border-ink-3 hover:text-ink focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40"
        >
          ⌘K
        </button>{" "}
        to search titles directly.
      </p>

    </section>
  );
}
