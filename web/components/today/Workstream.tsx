"use client";

import { Avatar, Button, DotMandala, ErrorNote, Loading, cn, hueForName } from "@/components/ui";
import { gistRows, type Gist } from "@/lib/pageSectionTypes/todayGist";
import {
  firstName,
  formatDuration,
  kindColor,
  streamLabel,
  type ChartBar,
  type RailChip,
  type SplitItem,
  type TodayFilter,
  type TodayStats,
  type Who,
} from "@/lib/todayView";

/* The workstream panel (README "Workstream panel"): a quiet 360px container
   with the head and gist, four stat tiles, the time chart and split, and the
   People / Topics rails that filter the timeline. It renders what
   lib/todayView.ts derived; nothing here computes. */

const GIST_DOT: Record<"DONE" | "DECIDED" | "OPEN", string> = {
  DONE: "bg-ok",
  DECIDED: "bg-brand",
  OPEN: "bg-warn",
};

function Eyebrow({ children, className }: { children: string; className?: string }) {
  return (
    <div className={cn("font-mono text-[10px] uppercase tracking-[0.13em] text-ink-3", className)}>{children}</div>
  );
}

const STAT_LABELS: { key: keyof TodayStats; label: string }[] = [
  { key: "activities", label: "activities" },
  { key: "meetings", label: "meetings" },
  { key: "sessions", label: "sessions" },
  { key: "decisions", label: "decisions" },
];

export function Workstream({
  who,
  span,
  askLabel,
  onAsk,
  gist,
  gistPending = false,
  gistError = null,
  stats,
  chartLabel,
  bars,
  split,
  totalMin,
  people,
  topics,
  filter,
  onToggleFilter,
}: {
  who: Who;
  /** `09:10 – 16:40`, or empty. */
  span: string;
  askLabel: string;
  onAsk: () => void;
  /** Null hides the gist (not compiled, flag off, or the read failed). */
  gist: Gist | null;
  gistPending?: boolean;
  gistError?: Error | null;
  stats: TodayStats;
  chartLabel: string;
  bars: ChartBar[];
  split: SplitItem[];
  totalMin: number;
  people: RailChip[];
  topics: RailChip[];
  filter: TodayFilter | null;
  onToggleFilter: (f: TodayFilter) => void;
}) {
  const rows = gistRows(gist);
  return (
    <aside
      aria-label="Workstream"
      className="flex flex-col self-start overflow-hidden rounded-card border border-line-2 bg-bg-subtle-2 xl:sticky xl:top-0"
    >
      <div className="border-b border-line-2 bg-bg px-[18px] py-[14px]">
        <div className="mb-[12px] flex items-center gap-[9px]">
          <DotMandala size={15} density={0.75} />
          <span className="font-mono text-[10px] uppercase tracking-[0.13em] text-ink-3">{streamLabel(who)}</span>
          {span && <span className="font-mono text-[9.5px] text-ink-4">{span}</span>}
          <span className="ml-auto">
            <Button size="sm" onClick={onAsk}>
              {askLabel}
            </Button>
          </span>
        </div>

        {gistPending && <Loading what="reading the recap" />}
        {gistError && <ErrorNote>{gistError.message}</ErrorNote>}
        {rows.length > 0 && (
          <div className="flex flex-col gap-[10px]">
            {rows.map((r) => (
              <div key={r.label} className="grid grid-cols-[62px_minmax(0,1fr)] gap-[10px]">
                <span className="flex h-[20px] items-center gap-[6px] font-mono text-[9.5px] uppercase tracking-[0.1em] text-ink-2">
                  <span className={cn("h-[6px] w-[6px] rounded-pill", GIST_DOT[r.label])} />
                  {r.label}
                </span>
                <div className="flex flex-col gap-[2px]">
                  {r.items.map((b) => (
                    <span key={b} className="text-pretty text-[13.5px] leading-[1.45] text-ink">
                      {b}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}

        <div className={cn("grid grid-cols-4 gap-[6px]", rows.length > 0 && "mt-[14px]")}>
          {STAT_LABELS.map((s) => (
            <div
              key={s.key}
              className="flex flex-col gap-px rounded-row border border-line-2 bg-bg-subtle px-[9px] py-[7px]"
            >
              <span className="font-mono text-[15px] font-medium text-ink">{stats[s.key]}</span>
              <span className="font-mono text-[8.5px] uppercase tracking-[0.1em] text-ink-4">{s.label}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="border-b border-line-2 px-[18px] py-[14px]">
        <div className="mb-[10px] flex items-baseline gap-[8px]">
          <Eyebrow>{chartLabel}</Eyebrow>
          <span className="ml-auto font-mono text-[10px] text-ink-4">{formatDuration(totalMin)} captured</span>
        </div>
        <div role="img" aria-label={chartLabel} className="flex h-[64px] items-end gap-[3px] border-b border-line">
          {bars.map((b) => (
            <div
              key={b.id}
              title={b.tip}
              className="flex flex-1 flex-col-reverse overflow-hidden rounded-t-[2px]"
              style={{ height: `${b.heightPct}%` }}
            >
              {b.segs.map((s) => (
                <span
                  key={s.kind ?? "empty"}
                  className={cn("border-t border-bg-subtle-2", s.kind === null && "bg-line")}
                  style={{ flex: s.value, background: s.kind ? kindColor(s.kind) : undefined }}
                />
              ))}
            </div>
          ))}
        </div>
        <div className="mt-[4px] flex gap-[3px]" aria-hidden>
          {bars.map((b) => (
            <span key={b.id} className="flex-1 text-center font-mono text-[9px] text-ink-4">
              {b.label}
            </span>
          ))}
        </div>
        {split.length > 0 && (
          <>
            <div className="mt-[14px] flex h-[8px] gap-[2px] overflow-hidden rounded-[3px]" aria-hidden>
              {split.map((s) => (
                <span key={s.kind} style={{ flex: s.minutes, background: kindColor(s.kind) }} />
              ))}
            </div>
            <ul className="mt-[8px] flex flex-wrap gap-x-[14px] gap-y-[4px]">
              {split.map((s) => (
                <li key={s.kind} className="inline-flex items-center gap-[6px] text-[12px] text-ink-2">
                  <span className="h-[8px] w-[8px] rounded-[2px]" style={{ background: kindColor(s.kind) }} />
                  {s.name}
                  <span className="font-mono text-[10.5px] text-ink-3">{s.label}</span>
                </li>
              ))}
            </ul>
          </>
        )}
      </div>

      {(people.length > 0 || topics.length > 0) && (
        <div className="px-[18px] py-[14px]">
          {people.length > 0 && (
            <section aria-label="People">
              <Eyebrow className="pb-[7px]">people</Eyebrow>
              <div className="flex flex-wrap gap-[5px]">
                {people.map((p) => {
                  const on = filter?.type === "person" && filter.key === p.key;
                  return (
                    <button
                      key={p.key}
                      type="button"
                      aria-pressed={on}
                      onClick={() => onToggleFilter({ type: "person", key: p.key, label: p.label })}
                      className={cn(
                        "inline-flex items-center gap-[6px] rounded-pill border py-[2px] pl-[3px] pr-[9px] text-[12px] text-ink transition-colors",
                        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
                        on ? "border-ink bg-brand-soft" : "border-line bg-bg hover:border-ink-3",
                      )}
                    >
                      <Avatar name={p.label} size={18} hue={hueForName(p.label)} />
                      {firstName(p.label)}
                      <span className="font-mono text-[10px] text-ink-4">{p.count}</span>
                    </button>
                  );
                })}
              </div>
            </section>
          )}
          {topics.length > 0 && (
            <section aria-label="Topics">
              <Eyebrow className={cn("pb-[7px]", people.length > 0 && "pt-[14px]")}>topics</Eyebrow>
              <div className="flex flex-wrap gap-[5px]">
                {topics.map((t) => {
                  const on = filter?.type === "topic" && filter.key === t.key;
                  return (
                    <button
                      key={t.key}
                      type="button"
                      aria-pressed={on}
                      onClick={() => onToggleFilter({ type: "topic", key: t.key, label: t.label })}
                      className={cn(
                        "inline-flex items-center gap-[6px] rounded-pill border px-[9px] py-[3px] text-[12px] text-ink transition-colors",
                        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
                        on ? "border-ink bg-brand-soft" : "border-line bg-bg hover:border-ink-3",
                      )}
                    >
                      <span className="h-[5px] w-[5px] rounded-pill bg-brand" />
                      {t.label}
                      <span className="font-mono text-[10px] text-ink-4">{t.count}</span>
                    </button>
                  );
                })}
              </div>
            </section>
          )}
        </div>
      )}
    </aside>
  );
}
