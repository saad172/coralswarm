"use client";

import Link from "next/link";
import { Card, Chip } from "@/components/ui";
import { BulletList } from "@/components/today/BulletList";
import { PeriodGapRow } from "@/components/today/PeriodGapRow";
import { PeriodSummaryCard } from "@/components/today/PeriodSummaryCard";
import type { JournalPeriod } from "@/lib/api";
import {
  buildTierRows,
  countLabel,
  dayRowLabel,
  journalHref,
  periodGapLabel,
  periodRowStats,
  rowElementId,
  weekdayTickLabel,
  type PeriodRow as PeriodRowT,
} from "@/lib/journal";

/* Week zoom: the week's own prose summary + one BULLET-compact day-gist row
   per day in the week (ticket 15: day tier now compiles to the same bounded
   bullet list the hour tier does), each clickable through to that day's
   `?z=day` view. Missing days (no compiled row, no live children — see
   `buildTierRows`'s doc comment) collapse into a dashed gap row exactly like
   the day view's own hour gaps. */

function DayGistRow({ row, filterGraphId, showOceanChips }: { row: PeriodRowT; filterGraphId: string | null; showOceanChips: boolean }) {
  const entries = filterGraphId ? row.entries.filter((e) => e.graph_id === filterGraphId) : row.entries;
  const { atoms, asks } = periodRowStats(entries);
  return (
    <div id={rowElementId(row)} className="grid grid-cols-[68px_minmax(0,1fr)] gap-[14px]">
      <div className="pt-[13px] text-right font-mono text-[10.5px] text-ink-4">{weekdayTickLabel(row.start)}</div>
      <Link href={journalHref("day", row.start)} className="block">
        <Card className="flex flex-col gap-[6px] transition-colors hover:border-line-2">
          <div className="flex items-baseline justify-between gap-[12px]">
            <span className="font-display text-[16px] text-ink">{dayRowLabel(row.start)}</span>
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
            entries.map((e) => <BulletList key={e.graph_id} text={e.summary} compact />)
          ) : (
            <p className="text-[12.5px] text-ink-3">{row.open ? "in progress — nothing compiled yet" : "no summary yet"}</p>
          )}
        </Card>
      </Link>
    </div>
  );
}

export function WeekJournal({
  ownPeriod,
  dayPeriods,
  filterGraphId,
  showOceanChips,
}: {
  ownPeriod: JournalPeriod | null;
  dayPeriods: JournalPeriod[];
  filterGraphId: string | null;
  showOceanChips: boolean;
}) {
  const rows = buildTierRows(dayPeriods);
  return (
    <div className="flex flex-col gap-[16px]">
      <PeriodSummaryCard
        period={ownPeriod}
        filterGraphId={filterGraphId}
        showOceanChips={showOceanChips}
        compiledCaption="week summary"
        liveCaption="week summary · in progress"
        emptyLabel="This week hasn't compiled a summary yet."
      />
      <div className="flex flex-col gap-[10px]">
        {rows.map((row) =>
          row.kind === "gap" ? (
            <PeriodGapRow
              key={rowElementId(row)}
              row={row}
              tickLabel={weekdayTickLabel(row.start)}
              label={periodGapLabel(row, "day")}
            />
          ) : (
            <DayGistRow key={rowElementId(row)} row={row} filterGraphId={filterGraphId} showOceanChips={showOceanChips} />
          ),
        )}
      </div>
    </div>
  );
}
