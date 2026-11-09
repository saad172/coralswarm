import { rowElementId, type PeriodGapRow as PeriodGapRowT } from "@/lib/journal";

/* The dashed "no activity" row for a period `buildTierRows` couldn't find a
   compiled row (or live composition) for — covers both a genuinely quiet
   stretch and a pre-epoch period the tier endpoint omits entirely (see
   `buildTierRows`'s doc comment). Same dashed-card idiom the day view's own
   `GapRow` (`DayJournal.tsx`) uses, generalized to any date range label. */

export function PeriodGapRow({ row, tickLabel, label }: { row: PeriodGapRowT; tickLabel: string; label: string }) {
  // `label` is `periodGapLabel`'s full "no activity · …" string — split the
  // same way `DayJournal`'s `GapRow` splits `formatGapLabel`'s, with the same
  // guard: `periodGapLabel` always emits exactly one " · " today, but a
  // future copy change dropping it would otherwise render a bare "·".
  const [prefix, suffix] = label.split(" · ");
  return (
    <div id={rowElementId(row)} className="grid grid-cols-[68px_minmax(0,1fr)] gap-[14px]">
      <div className="pt-[13px] text-right font-mono text-[10.5px] text-ink-4">{tickLabel}</div>
      <div className="rounded-card border border-dashed border-line bg-bg px-[14px] py-[13px]">
        <p className="font-display text-[14px] italic leading-[1.4] text-ink-3">
          {prefix}
          {suffix && <span className="font-mono text-[10.5px] not-italic text-ink-4"> · {suffix}</span>}
        </p>
      </div>
    </div>
  );
}
