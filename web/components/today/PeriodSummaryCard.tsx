import { Card, Chip } from "@/components/ui";
import type { JournalPeriod, JournalTierEntry } from "@/lib/api";
import { filterPeriodEntriesByGraph } from "@/lib/journal";

/* The week/month zoom's own top summary card — 2-4 sentence PROSE (ticket
   15's prompt contract: only the hour/day tiers bullet, week/month stay
   prose), plain text only, same rule as every other compiled-summary render
   on this page. `null` `period` (nothing returned yet — e.g. the sweep
   hasn't compiled this period and it isn't the open one either, a real if
   rare edge case right after a period closes) renders a quiet placeholder
   instead of crashing. */

export function PeriodSummaryCard({
  period,
  filterGraphId,
  showOceanChips,
  compiledCaption,
  liveCaption,
  emptyLabel,
}: {
  period: JournalPeriod | null;
  filterGraphId: string | null;
  showOceanChips: boolean;
  compiledCaption: string;
  liveCaption: string;
  emptyLabel: string;
}) {
  if (!period) {
    return (
      <Card className="flex flex-col gap-[6px]">
        <p className="text-[13px] leading-[1.6] text-ink-3">{emptyLabel}</p>
      </Card>
    );
  }
  const entries: JournalTierEntry[] = filterPeriodEntriesByGraph([period], filterGraphId)[0]?.entries ?? [];
  return (
    <Card className="flex flex-col gap-[10px]">
      {entries.length > 0 ? (
        entries.map((e) => (
          <div key={`${e.graph_id}-${e.compiled_at ?? "open"}`} className="flex flex-col gap-[6px]">
            {showOceanChips && <Chip className="self-start">{e.graph_name}</Chip>}
            {/* Plain text ONLY — LLM prose over untrusted captured content. */}
            <p className="whitespace-pre-line text-[14px] leading-[1.65] text-ink">{e.summary}</p>
          </div>
        ))
      ) : (
        <p className="text-[13px] leading-[1.6] text-ink-3">{emptyLabel}</p>
      )}
      <p className="font-mono text-[10px] text-ink-4">{period.open ? liveCaption : compiledCaption}</p>
    </Card>
  );
}
