"use client";

import Link from "next/link";
import { Card, Chip } from "@/components/ui";
import { PeriodGapRow } from "@/components/today/PeriodGapRow";
import { PeriodSummaryCard } from "@/components/today/PeriodSummaryCard";
import type { JournalPeriod } from "@/lib/api";
import {
  buildTierRows,
  countLabel,
  journalHref,
  periodGapLabel,
  periodRowStats,
  rowElementId,
  weekRowLabel,
  type PeriodRow as PeriodRowT,
} from "@/lib/journal";

/* Month zoom: the month's own prose summary + one PROSE week-gist row per
   week overlapping the month (ticket 15: week tier stays prose, unlike the
   day tier), each clickable through to that week's `?z=week` view. A
   "boundary" week that starts before the 1st (or ends after the last day)
   renders with an italic title, matching the approved prototype's treatment
   for a period that straddles the epoch/month edge. Missing weeks collapse
   into a dashed gap row, same as `WeekJournal`'s day gaps. */

function WeekGistRow({
  row,
  anchorMonth,
  filterGraphId,
  showOceanChips,
}: {
  row: PeriodRowT;
  anchorMonth: number;
  filterGraphId: string | null;
  showOceanChips: boolean;
}) {
  const entries = filterGraphId ? row.entries.filter((e) => e.graph_id === filterGraphId) : row.entries;
  const { atoms, asks } = periodRowStats(entries);
  const boundary = row.start.getMonth() !== anchorMonth;
  return (
    <div id={rowElementId(row)} className="grid grid-cols-[68px_minmax(0,1fr)] gap-[14px]">
      <div className="pt-[13px] text-right font-mono text-[10.5px] text-ink-4" />
      <Link href={journalHref("week", row.start)} className="block">
        <Card className="flex flex-col gap-[6px] transition-colors hover:border-line-2">
          <div className="flex items-baseline justify-between gap-[12px]">
            <span className={boundary ? "font-display text-[16px] italic text-ink-3" : "font-display text-[16px] text-ink"}>
              {weekRowLabel(row.start)}
            </span>
            {entries.length > 0 && (
              <span className="whitespace-nowrap font-mono text-[10px] text-ink-3">
                {atoms.toLocaleString()} {countLabel(atoms, "atom")} · {asks.toLocaleString()} {countLabel(asks, "ask")}
              </span>
            )}
          </div>
          {showOceanChips && entries.length > 0 && (
            <div className="flex flex-wrap gap-[4px]">
              {entries.map((e) => (
                <Chip key={e.graph_id}>{e.graph_name}</Chip>
              ))}
            </div>
          )}
          {entries.length > 0 ? (
            entries.map((e) => (
              // Plain text ONLY — week/month tier prose, same rule as every
              // other compiled summary on this page.
              <p key={e.graph_id} className="whitespace-pre-line text-[13px] leading-[1.6] text-ink-2">
                {e.summary}
              </p>
            ))
          ) : (
            <p className="text-[12.5px] text-ink-3">{row.open ? "in progress — nothing compiled yet" : "no summary yet"}</p>
          )}
        </Card>
      </Link>
    </div>
  );
}

export function MonthJournal({
  ownPeriod,
  weekPeriods,
  anchorMonth,
  filterGraphId,
  showOceanChips,
}: {
  ownPeriod: JournalPeriod | null;
  weekPeriods: JournalPeriod[];
  /** `Date.getMonth()` of the viewed month — flags a "boundary" week (starts
   *  in the previous calendar month) for the italic treatment. */
  anchorMonth: number;
  filterGraphId: string | null;
  showOceanChips: boolean;
}) {
  const rows = buildTierRows(weekPeriods);
  return (
    <div className="flex flex-col gap-[16px]">
      <PeriodSummaryCard
        period={ownPeriod}
        filterGraphId={filterGraphId}
        showOceanChips={showOceanChips}
        compiledCaption="month summary · recompiles as weeks close"
        liveCaption="month summary · in progress"
        emptyLabel="This month hasn't compiled a summary yet."
      />
      <div className="flex flex-col gap-[10px]">
        {rows.map((row) =>
          row.kind === "gap" ? (
            <PeriodGapRow key={rowElementId(row)} row={row} tickLabel="" label={periodGapLabel(row, "week")} />
          ) : (
            <WeekGistRow
              key={rowElementId(row)}
              row={row}
              anchorMonth={anchorMonth}
              filterGraphId={filterGraphId}
              showOceanChips={showOceanChips}
            />
          ),
        )}
      </div>
    </div>
  );
}
