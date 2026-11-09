"use client";

import { useState } from "react";
import type { JournalAggregate, JournalAggregateBlock, JournalEntry, JournalHour } from "@/lib/api";
import { Badge, Card, Chip } from "@/components/ui";
import { facetLabelFor, hueForSource, iconKindFor, SourceGlyph } from "@/components/library/sourceKind";
import { BulletList } from "@/components/today/BulletList";
import { HourDrilldown } from "@/components/today/HourDrilldown";
import { IconChevron } from "@/components/icons";
import {
  aggregateQualifier,
  apiHoursForSlot,
  filterEntriesByGraph,
  formatGapLabel,
  formatHourLabel,
  hourStartForDrilldown,
  mergeJournalHours,
  rowElementId,
  staleBadgeLabel,
  staleEntryInfo,
  type DayGapRow,
  type DayHourRow,
  type DayRow,
} from "@/lib/journal";

/* The day column — PR D's replacement for Timeline.tsx. One row per local
   hour slot (`buildDayRows`, lib/journal.ts): a real "hour" row for any slot
   with a compiled entry, a live aggregate, or the current in-progress hour;
   everything else folds into a dashed "gap" row. Ocean-switcher filtering
   (`filterGraphId`) narrows `entries[]` only — `aggregate` has no per-ocean
   breakdown at all (the endpoint is a whole-account rollup). The deterministic
   aggregate fallback (`aggregateProse` + `BlockChips`) therefore always
   renders for an hour with no matching entries, filtered or not — it's the
   only data the API gives us for that hour — and gets a small mono "all
   oceans" qualifier when a filter is active and there's more than one
   reachable ocean to disambiguate from (see `aggregateQualifier`'s doc
   comment in lib/journal.ts). */

function BlockChips({ blocks, qualifier }: { blocks: JournalAggregateBlock[]; qualifier?: string }) {
  const shown = blocks.filter((b) => b.atom_count > 0);
  if (shown.length === 0) return null;
  return (
    <div className="flex flex-wrap items-center gap-[6px]">
      {shown.map((b, i) => (
        <Chip key={`${b.source_type}-${b.title}-${i}`} hue={hueForSource(b.source_type)}>
          <SourceGlyph kind={iconKindFor(b.source_type)} size={10} />
          {b.title || facetLabelFor(b.source_type)} · {b.atom_count}
        </Chip>
      ))}
      {qualifier && <span className="font-mono text-[10.5px] text-ink-4">· {qualifier}</span>}
    </div>
  );
}

/** "N corals from <title> · M asks" — the deterministic fallback for an hour
 *  with no compiled prose yet (the compile worker hasn't reached it, or it's
 *  still in progress), built straight from the live aggregate. */
function aggregateProse(aggregate: JournalAggregate): string {
  if (aggregate.atom_count === 0 && aggregate.asks === 0) return "quiet hour — nothing captured yet.";
  const parts = aggregate.blocks
    .filter((b) => b.atom_count > 0)
    .map((b) => `${b.atom_count} item${b.atom_count === 1 ? "" : "s"} from ${b.title || facetLabelFor(b.source_type)}`);
  if (aggregate.asks > 0) parts.push(`${aggregate.asks} ask${aggregate.asks === 1 ? "" : "s"}`);
  return parts.length > 0
    ? `${parts.join(" · ")}.`
    : `${aggregate.atom_count} item${aggregate.atom_count === 1 ? "" : "s"} captured.`;
}

/** The subtle staleness marker (ticket 14) — an amber-outline quiet badge,
 *  same idiom `Badge tone="amber" outline` already uses elsewhere for a
 *  quiet-but-noticeable caveat,
 *  just sized down to sit inline with an hour card's own mono captions. Only
 *  rendered when `staleEntryInfo` finds real, known-newer activity this
 *  entry's prose predates — never merely because `compiled_at` is old. */
function StaleBadge({ entry, aggregate }: { entry: JournalEntry; aggregate: JournalAggregate }) {
  const stale = staleEntryInfo(entry, aggregate);
  if (!stale) return null;
  return (
    <Badge tone="amber" outline className="self-start rounded-badge px-[7px] py-[2px] text-[9.5px]">
      {staleBadgeLabel(stale)}
    </Badge>
  );
}

function GapRow({ row }: { row: DayGapRow }) {
  // `formatGapLabel` collapses a one-hour gap to its single boundary
  // ("· 09:00") instead of a same-looking range ("09:00–10:00") — the
  // verify ticket's copy nit. `formatGapLabel` always emits exactly one
  // " · " today, but this guards against a future copy change dropping it
  // (which would otherwise render a bare, suffix-less "·").
  const [prefix, suffix] = formatGapLabel(row).split(" · ");
  return (
    <div id={rowElementId(row)} className="grid grid-cols-[54px_minmax(0,1fr)] gap-[14px]">
      <div className="pt-[13px] text-right font-mono text-[10.5px] text-ink-4">{formatHourLabel(row.start)}</div>
      <div className="rounded-card border border-dashed border-line bg-bg px-[14px] py-[13px]">
        <p className="font-display text-[14px] italic leading-[1.4] text-ink-3">
          {prefix}
          {suffix && <span className="font-mono text-[10.5px] not-italic text-ink-4"> · {suffix}</span>}
        </p>
      </div>
    </div>
  );
}

function HourRow({
  row,
  hours,
  filterGraphId,
  reachableOceanCount,
  showOceanChips,
}: {
  row: DayHourRow;
  hours: JournalHour[];
  filterGraphId: string | null;
  reachableOceanCount: number;
  showOceanChips: boolean;
}) {
  // `filterEntriesByGraph` narrows entries per-hour BEFORE merging (leaving
  // `aggregate` untouched, per its own doc comment in lib/journal.ts) — the
  // single source of truth for the ocean-switcher predicate, so this and
  // `intensityForSlots`'s whole-account math never drift apart.
  const merged = mergeJournalHours(filterEntriesByGraph(apiHoursForSlot(hours, row), filterGraphId));
  const filteredEntries = merged.entries;
  const qualifier = aggregateQualifier(filterGraphId, reachableOceanCount);

  // Inline "everything this hour" expansion (ticket 16's drill-down
  // endpoint) — mounted only while open, so a row that's never expanded
  // never fires the request. `hourStartForDrilldown` returns `null` for the
  // rare rows that can't map to exactly one valid UTC hour boundary (see its
  // own doc comment), in which case the expand affordance is simply omitted.
  const [open, setOpen] = useState(false);
  const drillHourStart = hourStartForDrilldown(row, hours);
  const canExpand = drillHourStart !== null;
  const toggle = () => canExpand && setOpen((v) => !v);

  const drilldownId = `${rowElementId(row)}-drilldown`;

  return (
    <div id={rowElementId(row)} className="grid grid-cols-[54px_minmax(0,1fr)] gap-[14px]">
      <div className="pt-[13px] text-right font-mono text-[10.5px] text-ink-4">{formatHourLabel(row.start)}</div>
      <Card className="flex flex-col gap-[10px]">
        {filteredEntries.length > 0 ? (
          <>
            {filteredEntries.map((e) => (
              <div key={`${e.graph_id}-${e.compiled_at}`} className="flex flex-col gap-[6px]">
                {showOceanChips && <Chip className="self-start">{e.graph_name}</Chip>}
                {/* Plain text ONLY — this is LLM prose compiled over untrusted
                    captured content (JournalEntry's doc comment, lib/api.ts).
                    Never dangerouslySetInnerHTML or a markdown renderer;
                    `BulletList` splits on newlines into <li> text nodes only
                    (ticket 15's bounded bullet-list prompt contract). */}
                <BulletList text={e.summary} />
                <StaleBadge entry={e} aggregate={merged.aggregate} />
              </div>
            ))}
            {!filterGraphId && <BlockChips blocks={merged.aggregate.blocks} />}
          </>
        ) : (
          // No entries matched this hour (either genuinely none compiled yet,
          // or — under a filter — only other oceans' entries exist). The
          // deterministic aggregate is the only data the API gives us either
          // way, so it always renders here rather than a dead-end "no
          // compiled entries" message; `qualifier` is the honest disclosure
          // when it can't be broken down to just this ocean.
          <>
            <p className="text-[13px] leading-[1.6] text-ink-2">{aggregateProse(merged.aggregate)}</p>
            <BlockChips blocks={merged.aggregate.blocks} qualifier={qualifier} />
          </>
        )}
        <div className="flex items-center gap-[10px]">
          {row.current && (
            <p className="font-mono text-[10.5px] text-ink-4">in progress — compiles when the hour closes</p>
          )}
          {canExpand && (
            <button
              type="button"
              onClick={toggle}
              aria-expanded={open}
              aria-controls={drilldownId}
              className="ml-auto flex items-center gap-[4px] font-mono text-[9.5px] uppercase tracking-[0.08em] text-ink-3 hover:text-brand-2"
            >
              what it&rsquo;s made of
              <IconChevron width={9} height={9} className={open ? "rotate-180" : undefined} />
            </button>
          )}
        </div>
        {open && drillHourStart && (
          <div id={drilldownId}>
            <HourDrilldown hourStart={drillHourStart} graphId={filterGraphId} onCollapse={() => setOpen(false)} />
          </div>
        )}
      </Card>
    </div>
  );
}

export function DayJournal({
  rows,
  hours,
  filterGraphId,
  reachableOceanCount,
  showOceanChips,
}: {
  rows: DayRow[];
  /** Already day-scoped `JournalHour[]` — unfiltered; `filterGraphId` narrows
   *  entries per row, not this array. */
  hours: JournalHour[];
  filterGraphId: string | null;
  /** Count of oceans reachable on this account (`useApp().graphs.length`) —
   *  drives `aggregateQualifier`'s "all oceans" disclosure; NOT the set of
   *  oceans that happen to appear in this particular day's entries. */
  reachableOceanCount: number;
  showOceanChips: boolean;
}) {
  return (
    <div className="flex flex-col gap-[10px]">
      {rows.map((row) =>
        row.kind === "gap" ? (
          <GapRow key={rowElementId(row)} row={row} />
        ) : (
          <HourRow
            key={rowElementId(row)}
            row={row}
            hours={hours}
            filterGraphId={filterGraphId}
            reachableOceanCount={reachableOceanCount}
            showOceanChips={showOceanChips}
          />
        ),
      )}
    </div>
  );
}
