"use client";

import Link from "next/link";
import { Card, Eyebrow, cn } from "@/components/ui";
import { countLabel, type DayStats, type Intensity } from "@/lib/journal";

/* The right column — 4 stat tiles + the 24-slot intensity strip + "Ask about
   this day". Everything here is a whole-account rollup (see DayJournal's
   header comment): `aggregate` has no per-ocean breakdown, so unlike the day
   column's entries, none of this narrows when the ocean switcher picks one
   workspace. */

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
/** Deliberately much fainter than any real `INTENSITY_OPACITY` value — a
 *  future hour hasn't happened yet, which needs to read as visibly distinct
 *  from a real past/current hour that was genuinely quiet (opacity 1 at
 *  height 3, the `intensity: 0` treatment). */
const FUTURE_OPACITY = 0.22;

export function JournalRail({
  stats,
  intensity,
  future,
  hourLabels,
  onSlotClick,
  askHref,
}: {
  stats: DayStats;
  /** One bucket per local hour slot, same order as `hourLabels` — the day
   *  being viewed only, never a week/month (those modes are gone). */
  intensity: Intensity[];
  /** Parallel to `intensity` — `futureSlotMask` (lib/journal.ts): true for
   *  hours that haven't happened yet (only possible while viewing today).
   *  Optional so a caller mid-migration (or a future non-today-only use of
   *  this rail) can omit it and get the old all-real-hours look. */
  future?: boolean[];
  hourLabels: string[];
  onSlotClick: (index: number) => void;
  askHref: string;
}) {
  return (
    <div className="flex flex-col gap-[16px] xl:sticky xl:top-[24px]">
      <Card className="grid grid-cols-2 gap-[16px]">
        <StatTile value={stats.atoms} label={countLabel(stats.atoms, "atom captured", "atoms captured")} />
        <StatTile
          value={stats.sourcesActive}
          label={countLabel(stats.sourcesActive, "source active", "sources active")}
        />
        <StatTile value={stats.asks} label={countLabel(stats.asks, "ask run", "asks run")} />
        <StatTile value={stats.hoursActive} label={countLabel(stats.hoursActive, "hour active", "hours active")} />
      </Card>

      <Card>
        <Eyebrow className="mb-[10px]">the day at a glance</Eyebrow>
        <div className="flex h-[26px] items-end gap-[2px]">
          {intensity.map((v, i) => {
            const isFuture = future?.[i] ?? false;
            return (
              <button
                key={i}
                type="button"
                disabled={isFuture}
                title={isFuture ? `${hourLabels[i]} — later today` : hourLabels[i]}
                aria-label={isFuture ? `${hourLabels[i]} — later today, not yet` : `${hourLabels[i]} — jump to this hour`}
                onClick={isFuture ? undefined : () => onSlotClick(i)}
                className={cn(
                  "w-full min-w-[2px] rounded-[1px] transition-colors",
                  isFuture ? "cursor-default bg-line" : v > 0 ? "bg-brand hover:bg-brand-2" : "bg-line hover:bg-line-2",
                )}
                style={{ height: INTENSITY_HEIGHT[isFuture ? 0 : v], opacity: isFuture ? FUTURE_OPACITY : INTENSITY_OPACITY[v] }}
              />
            );
          })}
        </div>
      </Card>

      <Link
        href={askHref}
        className="inline-flex items-center justify-center gap-[6px] rounded-row border border-ink bg-ink px-[14px] py-[9px] text-[13px] font-medium text-white transition-colors hover:bg-ink-2 hover:border-ink-2"
      >
        Ask about this day
      </Link>
    </div>
  );
}
