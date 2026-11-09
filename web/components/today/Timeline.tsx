"use client";

import { Avatar, Chip, Seg, cn, hueForName, hueTint, hueVar } from "@/components/ui";
import { IconClose } from "@/components/icons";
import { KIND_META } from "@/components/detail/detailModel";
import type { Activity } from "@/lib/journalActivitiesApi";
import {
  durationLabel,
  eventCountLabel,
  filterPillLabel,
  firstName,
  hhmm,
  rangeLabel,
  type Density,
  type Timeline as TimelineModel,
  type TodayFilter,
} from "@/lib/todayView";
import type { Zoom } from "@/lib/journal";

/* The Today timeline (README "Timeline (left)"): toolbar, then one row per
   activity on a 54px time gutter and a 1px track with a hue dot. Cards show the
   whole story; Compact is one line per item; Month is always Compact. Quiet
   gaps (Day, Cards) are dashed rows. The rows only render what
   `buildTimeline` (lib/todayView.ts) derived. */

const DENSITY_OPTIONS: { key: Density; label: string }[] = [
  { key: "cards", label: "Cards" },
  { key: "compact", label: "Compact" },
];

const rowKey = (a: Activity) => `${a.graph.id}:${a.key}`;

function Eyebrow({ children }: { children: string }) {
  return <span className="font-mono text-[10px] uppercase tracking-[0.13em] text-ink-3">{children}</span>;
}

function Toolbar({
  count,
  filter,
  onClearFilter,
  density,
  onDensity,
  showDensity,
}: {
  count: number;
  filter: TodayFilter | null;
  onClearFilter: () => void;
  density: Density;
  onDensity: (d: Density) => void;
  showDensity: boolean;
}) {
  return (
    <div className="mb-[14px] flex items-center gap-[10px]">
      <Eyebrow>timeline</Eyebrow>
      {filter && (
        <button
          type="button"
          onClick={onClearFilter}
          title="Clear filter"
          className="inline-flex items-center gap-[6px] rounded-pill border border-line bg-bg py-[2px] pl-[10px] pr-[6px] text-[12px] text-ink transition-colors hover:border-ink-3 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40"
        >
          {filterPillLabel(filter)}
          <IconClose width={11} height={11} />
        </button>
      )}
      <span className="h-px flex-1 bg-line-2" />
      <span className="font-mono text-[10.5px] text-ink-3">{eventCountLabel(count)}</span>
      {showDensity && <Seg options={DENSITY_OPTIONS} value={density} onChange={onDensity} />}
    </div>
  );
}

function Card({
  a,
  tz,
  open,
  showOwner,
  onOpen,
}: {
  a: Activity;
  tz?: string;
  open: boolean;
  showOwner: boolean;
  onOpen: () => void;
}) {
  const meta = KIND_META[a.kind];
  const Icon = meta.Icon;
  const shown = a.people.slice(0, 4);
  const more = a.people_count - shown.length;
  return (
    <button
      type="button"
      onClick={onOpen}
      aria-pressed={open}
      className={cn(
        "flex w-full flex-col gap-[5px] rounded-card border bg-bg px-[13px] py-[11px] text-left transition-[border-color,box-shadow]",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
        open ? "border-ink shadow-pop" : "border-line-2 hover:border-ink-3",
      )}
    >
      <span className="flex min-w-0 items-center gap-[7px]">
        <span
          className="inline-flex items-center gap-[5px] rounded-badge py-[2px] pl-[5px] pr-[7px] font-mono text-[9.5px] uppercase tracking-[0.08em]"
          style={{ background: hueTint(meta.hue), color: hueVar(meta.hue) }}
        >
          <Icon width={12} height={12} />
          {a.kind_label || meta.label}
        </span>
        {showOwner && (
          <span className="inline-flex items-center gap-[5px] text-[11.5px] text-ink-2">
            <Avatar name={a.owner.name ?? "?"} size={16} hue={hueForName(a.owner.name)} />
            {firstName(a.owner.name)}
          </span>
        )}
        <span className="ml-auto whitespace-nowrap font-mono text-[10px] text-ink-4">
          {rangeLabel(a, tz)} {"·"} {durationLabel(a)}
        </span>
      </span>
      <span className="block truncate text-[14px] font-medium text-ink">{a.title}</span>
      {a.outcome && <span className="block text-[13px] leading-[1.5] text-ink-2">{a.outcome}</span>}
      <span className="mt-[3px] flex items-center gap-[6px]">
        <span className="flex gap-[2px]">
          {shown.map((p) => (
            <Avatar key={p} name={p} size={20} hue={hueForName(p)} />
          ))}
        </span>
        {more > 0 && <span className="font-mono text-[10px] text-ink-4">+{more}</span>}
        <span className="flex-1" />
        {a.decision_count > 0 && (
          <Chip hue="coral">
            {a.decision_count} {a.decision_count === 1 ? "decision" : "decisions"}
          </Chip>
        )}
        <Chip hue={meta.hue}>
          {a.source_count} {a.source_count === 1 ? "source" : "sources"}
        </Chip>
      </span>
    </button>
  );
}

function CompactRow({ a, open, onOpen }: { a: Activity; open: boolean; onOpen: () => void }) {
  return (
    <div className="-ml-[8px]">
      <button
        type="button"
        onClick={onOpen}
        aria-pressed={open}
        className={cn(
          "flex w-full min-w-0 items-center gap-[8px] rounded-row border px-[8px] py-[5px] text-left transition-colors",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
          open ? "border-line bg-bg" : "border-transparent hover:bg-bg-hover",
        )}
      >
        <span className="min-w-0 flex-1 truncate text-[13px] text-ink-2">
          <b className="font-medium text-ink">{a.title}</b>
          {a.outcome ? ` — ${a.outcome}` : ""}
        </span>
        <span className="whitespace-nowrap font-mono text-[10px] text-ink-4">{durationLabel(a)}</span>
      </button>
    </div>
  );
}

export function Timeline({
  model,
  zoom,
  tz,
  density,
  onDensity,
  filter,
  onClearFilter,
  openKey,
  onOpen,
  isTeam,
  internalUserId,
  truncated,
}: {
  model: TimelineModel;
  zoom: Zoom;
  tz?: string;
  density: Density;
  onDensity: (d: Density) => void;
  filter: TodayFilter | null;
  onClearFilter: () => void;
  /** `graph:key` of the activity whose panel is open. */
  openKey: string | null;
  onOpen: (index: number) => void;
  isTeam: boolean;
  internalUserId: string | null;
  truncated: boolean;
}) {
  const compact = density === "compact" || zoom === "month";
  const empty = model.all.length === 0;
  const noun = zoom === "day" ? "day" : `${zoom}`;
  return (
    <div className="min-w-0">
      <Toolbar
        count={model.visible.length}
        filter={filter}
        onClearFilter={onClearFilter}
        density={density}
        onDensity={onDensity}
        showDensity={zoom !== "month"}
      />

      {model.rows.map((row) => {
        if (row.type === "head") {
          return (
            <div
              key={row.id}
              className="mb-[10px] ml-[68px] mt-[6px] font-mono text-[10px] uppercase tracking-[0.13em] text-ink-2"
            >
              {row.label}
            </div>
          );
        }
        if (row.type === "gap") {
          return (
            <div key={row.id} className="grid grid-cols-[54px_minmax(0,1fr)] gap-x-[14px]">
              <div className="pt-[2px] text-right font-mono text-[10px] text-ink-4">{row.start}</div>
              <div className="border-l border-dashed border-line pb-[14px] pl-[18px] font-mono text-[10px] text-ink-4">
                quiet {"·"} {row.label}
              </div>
            </div>
          );
        }
        const a = row.activity;
        const open = openKey === rowKey(a);
        const meta = KIND_META[a.kind];
        return (
          <div key={row.id} className="grid grid-cols-[54px_minmax(0,1fr)] gap-x-[14px]">
            <div
              className={cn(
                "text-right font-mono text-[10.5px] text-ink-3",
                compact ? "pt-[6px]" : "pt-[11px]",
              )}
            >
              {hhmm(a.start, tz)}
            </div>
            <div className={cn("relative border-l border-line-2 pl-[18px]", compact ? "pb-[4px]" : "pb-[14px]")}>
              <span
                aria-hidden
                className={cn(
                  "absolute -left-[4.5px] h-[8px] w-[8px] rounded-pill ring-[3px] ring-bg",
                  compact ? "top-[10px]" : "top-[14px]",
                )}
                style={{ background: hueVar(meta.hue) }}
              />
              {compact ? (
                <CompactRow a={a} open={open} onOpen={() => onOpen(row.index)} />
              ) : (
                <Card
                  a={a}
                  tz={tz}
                  open={open}
                  showOwner={isTeam && !!a.owner.user_id && a.owner.user_id !== internalUserId}
                  onOpen={() => onOpen(row.index)}
                />
              )}
            </div>
          </div>
        );
      })}

      {empty && (
        <div className="py-[40px] text-center">
          <div className="mb-[6px] font-display text-[22px] text-ink-2">Nothing captured this {noun}</div>
          <div className="text-[13px] text-ink-3">
            {isTeam
              ? "No meetings, sessions or threads in this workspace."
              : "No meetings, sessions or threads you were part of."}
          </div>
        </div>
      )}
      {!empty && model.visible.length === 0 && (
        <div className="py-[40px] text-center text-[13px] text-ink-3">Nothing matches this filter.</div>
      )}
      {truncated && (
        <div className="mt-[8px] font-mono text-[10px] text-ink-4">Showing the earliest 1,000 activities of this period.</div>
      )}
    </div>
  );
}
