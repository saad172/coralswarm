"use client";

import Link from "next/link";
import { Card, Eyebrow, cn } from "@/components/ui";
import { countLabel, type Intensity, type PeriodStats } from "@/lib/journal";

/* The week/month zoom's right rail — the same anatomy as `JournalRail`
   (4 stat tiles + a glance strip + an Ask button), generalized: the strip
   buckets CHILD PERIODS (days, for a week; weeks, for a month) rather than
   hours, since there's no meaningful hour-level breakdown to show at this
   zoom without an extra fetch per row. Kept as its own component rather than
   overloading `JournalRail`'s day-specific prop shape (hourLabels/future/
   onSlotClick tied to the 24-hour grid). */

function StatTile({ value, label }: { value: number; label: string }) {
  return (
    <div className="flex flex-col gap-[3px]">
      <span className="font-mono text-[17px] text-ink">{value.toLocaleString()}</span>
      <Eyebrow>{label}</Eyebrow>
    </div>
  );
}

const INTENSITY_HEIGHT: Record<Intensity, number> = { 0: 3, 1: 8, 2: 14, 3: 22 };
const INTENSITY_OPACITY: Record<Intensity, number> = { 0: 1, 1: 0.55, 2: 0.78, 3: 1 };

export function PeriodRail({
  zoom,
  stats,
  intensity,
  labels,
  onBucketClick,
  askHref,
  askLabel,
}: {
  zoom: "week" | "month";
  stats: PeriodStats;
  /** One bucket per child period, same order as `labels`. */
  intensity: Intensity[];
  labels: string[];
  onBucketClick: (index: number) => void;
  askHref: string;
  askLabel: string;
}) {
  const periodNoun = zoom === "week" ? "day" : "week";
  return (
    <div className="flex flex-col gap-[16px] xl:sticky xl:top-[24px]">
      <Card className="grid grid-cols-2 gap-[16px]">
        <StatTile value={stats.atoms} label={countLabel(stats.atoms, "atom captured", "atoms captured")} />
        <StatTile
          value={stats.sourcesActive}
          label={countLabel(stats.sourcesActive, "source active", "sources active")}
        />
        <StatTile value={stats.asks} label={countLabel(stats.asks, "ask run", "asks run")} />
        <StatTile
          value={stats.periodsActive}
          label={countLabel(stats.periodsActive, `${periodNoun} active`, `${periodNoun}s active`)}
        />
      </Card>

      {intensity.length > 0 && (
        <Card>
          <Eyebrow className="mb-[10px]">the {zoom} at a glance</Eyebrow>
          <div className="flex h-[26px] items-end gap-[2px]">
            {intensity.map((v, i) => (
              <button
                key={i}
                type="button"
                title={labels[i]}
                aria-label={`${labels[i]} — jump to this ${periodNoun}`}
                onClick={() => onBucketClick(i)}
                className={cn(
                  "w-full min-w-[2px] rounded-[1px] transition-colors",
                  v > 0 ? "bg-brand hover:bg-brand-2" : "bg-line hover:bg-line-2",
                )}
                style={{ height: INTENSITY_HEIGHT[v], opacity: INTENSITY_OPACITY[v] }}
              />
            ))}
          </div>
        </Card>
      )}

      <Link
        href={askHref}
        className="inline-flex items-center justify-center gap-[6px] rounded-row border border-ink bg-ink px-[14px] py-[9px] text-[13px] font-medium text-white transition-colors hover:bg-ink-2 hover:border-ink-2"
      >
        {askLabel}
      </Link>
    </div>
  );
}
