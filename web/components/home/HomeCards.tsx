"use client";

import Link from "next/link";
import { useState, type ReactNode } from "react";
import { Dot, hueTint, hueVar, cn } from "@/components/ui";
import { IconAsk, IconChevron, IconSend } from "@/components/icons";
import { KIND_META } from "@/components/detail/detailModel";
import { workspaceHeadlineText, type WorkspaceActivityRow } from "@/lib/pageSectionTypes/workspaceActivity";
import type { HomeKpi } from "@/lib/pageSectionTypes/home";
import type { ConnectorStatusSummary } from "@/lib/pageSectionTypes/connectorStatus";
import {
  HEAT_WEEKDAYS,
  formatKpiDelta,
  sparkBars,
  type FeedGroup,
  type FeedItem,
  type Heatmap,
  type IngestChart,
  type OceanRankRow,
  type SourceSplit,
} from "@/lib/homeView";
import { oceanHue } from "@/lib/oceanHue";

/* The cards on Home (README 2b / 4a). Presentational: the page owns the data,
   these own the markup. Every colour is a token (globals.css / tailwind config);
   the only inline colours are `var(--...)` references that come from the data
   (an ocean's hue, a source type's colour). Motion is the house 0.15s
   colour/border/shadow transition and nothing else. */

const CARD = "rounded-card border border-line bg-bg shadow-card";
const MONO_LABEL = "font-mono text-[9.5px] uppercase tracking-[0.1em] text-ink-3";

export function HomeCard({ className, children, ...rest }: { className?: string; children: ReactNode } & React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn(CARD, "px-[16px] py-[14px]", className)} {...rest}>
      {children}
    </div>
  );
}

// ── Ask bar ──────────────────────────────────────────────────────────────

export function AskBar({ onAsk }: { onAsk: (query: string) => void }) {
  const [value, setValue] = useState("");
  const submit = () => {
    const q = value.trim();
    if (q) onAsk(q);
  };
  return (
    <div className="flex h-[42px] items-center gap-[10px] rounded-pill border border-line bg-bg pl-[16px] pr-[8px] shadow-card">
      <IconAsk width={15} height={15} className="flex-none text-ink-3" aria-hidden />
      <input
        aria-label="Ask your ocean"
        placeholder="Ask your ocean…"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" && !e.nativeEvent.isComposing) {
            e.preventDefault();
            submit();
          }
        }}
        className="min-w-0 flex-1 border-0 bg-transparent text-[14px] text-ink outline-none placeholder:text-ink-4"
      />
      <span className="hidden font-mono text-[10px] text-ink-4 sm:inline">opens a new chat</span>
      <button
        type="button"
        aria-label="Send to a new chat"
        onClick={submit}
        className="grid h-[28px] w-[28px] flex-none place-items-center rounded-pill bg-ink text-bg transition-colors hover:bg-ink-2 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40"
      >
        <IconSend width={14} height={14} />
      </button>
    </div>
  );
}

// ── KPI row ──────────────────────────────────────────────────────────────

export function KpiCard({ label, kpi }: { label: string; kpi: HomeKpi }) {
  const delta = formatKpiDelta(kpi);
  const bars = sparkBars(kpi.daily);
  return (
    <div className={cn(CARD, "flex min-w-0 flex-col gap-[6px] px-[14px] py-[12px]")}>
      <span className={cn(MONO_LABEL, "truncate whitespace-nowrap")}>{label}</span>
      <span className="flex items-baseline gap-[7px]">
        <span className="font-mono text-[22px] font-medium tracking-[-0.02em] text-ink">{kpi.total.toLocaleString("en-US")}</span>
        {delta && (
          <span className={cn("font-mono text-[10.5px]", delta.tone === "ok" ? "text-ok" : "text-ink-3")} aria-label={`${delta.text} against the prior 30 days`}>
            {delta.text}
          </span>
        )}
      </span>
      <div className="flex h-[24px] items-end gap-px" aria-hidden>
        {bars.map((b, i) => (
          <span key={i} className={cn("flex-1 rounded-[1px]", b.today ? "bg-brand" : "bg-line")} style={{ height: b.h }} />
        ))}
      </div>
    </div>
  );
}

/** The sixth card. A button that opens the connector panel; when the status
 *  endpoint is unavailable it is a plain link to /connectors instead. */
export function ConnectorCard({
  summary,
  open,
  onOpen,
}: {
  summary: ConnectorStatusSummary | null;
  open: boolean;
  onOpen: () => void;
}) {
  const cls = cn(
    CARD,
    "flex min-w-0 flex-col gap-[6px] px-[14px] py-[12px] text-left text-ink transition-colors hover:border-ink-3 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
    open && "border-ink hover:border-ink",
  );
  if (!summary) {
    return (
      <Link href="/connectors" className={cls}>
        <span className="flex w-full items-center gap-[6px]">
          <span className={MONO_LABEL}>connectors</span>
          <IconChevron width={13} height={13} className="ml-auto -rotate-90 text-ink-3" />
        </span>
        <span className="font-mono text-[22px] font-medium tracking-[-0.02em]">{"—"}</span>
        <span className="font-mono text-[10px] text-ink-3">manage connectors</span>
      </Link>
    );
  }
  const bar = connectorBar(summary);
  const line = [summary.failing ? `${summary.failing} failing` : "", summary.stale ? `${summary.stale} stale` : ""].filter(Boolean).join(" · ");
  return (
    <button type="button" onClick={onOpen} aria-expanded={open} aria-label={`Connectors: ${summary.healthy} of ${summary.total} healthy. Open details`} className={cls}>
      <span className="flex w-full items-center gap-[6px]">
        <span className={MONO_LABEL}>connectors</span>
        <IconChevron width={13} height={13} className="ml-auto -rotate-90 text-ink-3" />
      </span>
      <span className="flex items-baseline gap-[6px]">
        <span className="font-mono text-[22px] font-medium tracking-[-0.02em]">
          {summary.healthy}/{summary.total}
        </span>
        <span className="font-mono text-[10.5px] text-ink-3">healthy</span>
      </span>
      <ConnectorBar parts={bar} className="mt-[4px] h-[6px] w-full gap-[2px] rounded-[2px]" />
      <span className="w-full truncate font-mono text-[10px] text-bad">{line || " "}</span>
    </button>
  );
}

export interface ConnectorBarPart {
  label: string;
  n: number;
  color: string;
}

export function connectorBar(s: ConnectorStatusSummary): ConnectorBarPart[] {
  return [
    { label: "healthy", n: s.healthy, color: "var(--ok)" },
    { label: "stale", n: s.stale, color: "var(--warn)" },
    { label: "failing", n: s.failing, color: "var(--bad)" },
  ];
}

/** Tri-segment bar with flex weights by count. */
export function ConnectorBar({ parts, className }: { parts: ConnectorBarPart[]; className?: string }) {
  return (
    <div className={cn("flex overflow-hidden", className)} aria-hidden>
      {parts
        .filter((p) => p.n > 0)
        .map((p) => (
          <span key={p.label} className="rounded-[inherit]" style={{ flex: p.n, background: p.color }} />
        ))}
    </div>
  );
}

// ── Charts row ───────────────────────────────────────────────────────────

export function IngestedPerDayCard({ chart, split }: { chart: IngestChart; split: SourceSplit | null }) {
  return (
    <HomeCard className="flex flex-col">
      <div className="mb-[12px] flex items-baseline gap-[10px]">
        <span className="text-[13.5px] font-medium text-ink">Ingested per day</span>
        <span className="font-mono text-[10.5px] text-ink-3">{chart.stacked ? "by ocean" : "all oceans"}</span>
        <span className="ml-auto flex flex-wrap justify-end gap-x-[12px] gap-y-[4px]">
          {chart.legend.map((l) => (
            <span key={l.name} className="inline-flex items-center gap-[5px] whitespace-nowrap text-[11.5px] text-ink-2">
              <span className="h-[8px] w-[8px] rounded-[2px]" style={{ background: l.color }} />
              {l.name}
            </span>
          ))}
        </span>
      </div>
      <div className="grid flex-1 grid-cols-[30px_minmax(0,1fr)] grid-rows-[minmax(168px,1fr)_auto] gap-[6px]">
        <div className="relative" aria-hidden>
          {chart.yTicks.map((y) => (
            <span
              key={y.pct}
              className="absolute right-0 translate-y-1/2 font-mono text-[9.5px] text-ink-4"
              style={{ bottom: `${y.pct}%` }}
            >
              {y.value}
            </span>
          ))}
        </div>
        <div className="relative" role="img" aria-label={`Corals ingested per day, ${chart.days.length} days`}>
          {chart.yTicks.map((y) => (
            <span key={y.pct} className="absolute inset-x-0 border-t border-line-3" style={{ bottom: `${y.pct}%` }} />
          ))}
          <div className="absolute inset-0 flex items-end gap-[3px]">
            {chart.days.map((d) => (
              <div
                key={d.day}
                title={d.tip}
                className="flex flex-1 flex-col-reverse overflow-hidden rounded-t-[2px]"
                style={{ height: `${d.heightPct}%` }}
              >
                {d.segments.map((s) => (
                  <span key={s.graphId || "all"} className="border-t border-bg" style={{ flex: s.value, background: s.color }} />
                ))}
              </div>
            ))}
          </div>
        </div>
        <span />
        <div className="flex gap-[3px]" aria-hidden>
          {chart.days.map((d) => (
            <span key={d.day} className="flex-1 whitespace-nowrap font-mono text-[9.5px] text-ink-4">
              {d.xLabel}
            </span>
          ))}
        </div>
      </div>
      {split && split.items.length > 0 && (
        <div className="mt-[14px] border-t border-line-3 pt-[12px]">
          <div className="mb-[7px] flex items-baseline gap-[8px]">
            <span className={MONO_LABEL}>by source type</span>
            <span className="ml-auto font-mono text-[10.5px] text-ink-3">{split.totalLabel}</span>
          </div>
          <div className="flex h-[10px] gap-[2px] overflow-hidden rounded-[3px]">
            {split.items.map((s) => (
              <span key={s.id} title={`${s.name} · ${s.pct}`} style={{ flex: s.value, background: s.color }} />
            ))}
          </div>
          <div className="mt-[8px] flex flex-wrap gap-x-[16px] gap-y-[4px]">
            {split.items.map((s) => (
              <span key={s.id} className="inline-flex items-center gap-[6px] text-[12px] text-ink-2">
                <span className="h-[8px] w-[8px] rounded-[2px]" style={{ background: s.color }} />
                {s.name}
                <span className="font-mono text-[10.5px] text-ink-3">{s.pct}</span>
              </span>
            ))}
          </div>
        </div>
      )}
    </HomeCard>
  );
}

export function OceansByVolumeCard({ rows }: { rows: OceanRankRow[] }) {
  return (
    <HomeCard className="flex flex-col">
      <div className="mb-[10px] flex items-baseline gap-[8px]">
        <span className="text-[13.5px] font-medium text-ink">Oceans by volume</span>
        <span className="ml-auto font-mono text-[10.5px] text-ink-3">corals {"·"} 30d</span>
      </div>
      <div className="flex flex-col gap-[9px]">
        {rows.map((r) => (
          <div key={r.graphId} className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-[10px] gap-y-[3px]">
            <span className="truncate whitespace-nowrap text-[12.5px] text-ink">
              {r.name}
              {r.sub && <span className="ml-[6px] font-mono text-[9.5px] uppercase tracking-[0.06em] text-ink-4">{r.sub}</span>}
            </span>
            <span className="font-mono text-[11px] text-ink-2">{r.valueLabel}</span>
            <div className="col-span-2 flex h-[6px] overflow-hidden rounded-[2px] bg-bg-2">
              <div className="flex gap-px" style={{ width: `${r.widthPct}%` }}>
                {r.segments.map((s) => (
                  <span key={s.id} title={s.name || undefined} style={{ flex: s.value, background: s.color }} />
                ))}
              </div>
            </div>
          </div>
        ))}
      </div>
    </HomeCard>
  );
}

const HEAT_BG = ["bg-bg-2", "bg-heat-1", "bg-heat-2", "bg-heat-3", "bg-heat-4"] as const;

export function ActivityHeatmapCard({ heat }: { heat: Heatmap }) {
  return (
    <HomeCard>
      <div className="mb-[10px] flex items-baseline gap-[8px]">
        <span className="text-[13.5px] font-medium text-ink">Activity</span>
        {heat.busiest && <span className="ml-auto font-mono text-[10.5px] text-ink-3">{heat.busiest}</span>}
      </div>
      <div className="grid grid-cols-7 gap-[4px]">
        {HEAT_WEEKDAYS.map((w) => (
          <span key={w} className="text-center font-mono text-[9px] uppercase tracking-[0.06em] text-ink-4">
            {w}
          </span>
        ))}
        {heat.cells.map((c, i) => (
          <span
            key={i}
            title={c.tip || undefined}
            className={cn(
              "grid aspect-square place-items-center rounded-[4px] border font-mono text-[9px]",
              c.blank ? "border-transparent" : HEAT_BG[c.level],
              !c.blank && (c.today ? "border-ink" : "border-transparent"),
              c.level >= 3 ? "text-bg" : "text-ink-3",
            )}
          >
            {c.label}
          </span>
        ))}
      </div>
      <div className="mt-[10px] flex items-center gap-[4px] font-mono text-[9.5px] text-ink-4" aria-hidden>
        less
        {HEAT_BG.map((b) => (
          <span key={b} className={cn("h-[10px] w-[10px] rounded-[3px]", b)} />
        ))}
        more
      </div>
    </HomeCard>
  );
}

// ── Bottom row ───────────────────────────────────────────────────────────

export function KindTile({ kind, size = 26 }: { kind: keyof typeof KIND_META; size?: number }) {
  const m = KIND_META[kind];
  const Icon = m.Icon;
  return (
    <span
      className="grid flex-none place-items-center rounded-row"
      style={{ width: size, height: size, background: hueTint(m.hue), color: hueVar(m.hue) }}
    >
      <Icon width={13} height={13} />
    </span>
  );
}

export function RecentlyIngestedCard({
  groups,
  countLabel,
  selectedKey,
  onOpen,
}: {
  groups: FeedGroup[];
  countLabel: string;
  selectedKey: string | null;
  onOpen: (item: FeedItem) => void;
}) {
  return (
    <HomeCard className="pb-[8px]">
      <div className="flex items-baseline gap-[10px]">
        <span className="text-[13.5px] font-medium text-ink">Recently ingested</span>
        <span className="ml-auto font-mono text-[10.5px] text-ink-3">{countLabel}</span>
      </div>
      {groups.map((g) => (
        <div key={g.label} className="mt-[12px]">
          <div className="border-b border-line-2 pb-[6px] font-mono text-[10px] uppercase tracking-[0.13em] text-ink-3">{g.label}</div>
          {g.items.map((it) => {
            const a = it.activity;
            return (
              <button
                key={a.key}
                type="button"
                onClick={() => onOpen(it)}
                className={cn(
                  "-mx-[8px] grid w-[calc(100%+16px)] grid-cols-[26px_minmax(0,1fr)_auto_40px] items-center gap-[12px] rounded-row px-[8px] py-[7px] text-left transition-colors hover:bg-bg-hover focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
                  selectedKey === a.key && "bg-bg-hover",
                )}
              >
                <KindTile kind={a.kind} />
                <span className="min-w-0 truncate whitespace-nowrap text-[13.5px] text-ink">{a.title}</span>
                <span className="hidden items-center gap-[5px] whitespace-nowrap font-mono text-[10px] uppercase tracking-[0.06em] text-ink-3 sm:flex">
                  <Dot hue={it.oceanHue} size={5} />
                  {a.graph.name}
                </span>
                <span className="text-right font-mono text-[10.5px] text-ink-4">{it.time}</span>
              </button>
            );
          })}
        </div>
      ))}
    </HomeCard>
  );
}

export function WorkspacesCard({ rows, now }: { rows: WorkspaceActivityRow[]; now: Date }) {
  return (
    <HomeCard className="pb-[8px]">
      <div className="pb-[4px] text-[13.5px] font-medium text-ink">From your workspaces</div>
      {rows.map((w) => (
        <Link
          key={w.graph_id}
          href={`/workspaces?${new URLSearchParams({ id: w.graph_id })}`}
          className="-mx-[8px] grid grid-cols-[minmax(0,1fr)_auto] gap-x-[10px] gap-y-[2px] rounded-row border-t border-line-3 px-[8px] py-[8px] transition-colors hover:bg-bg-hover"
        >
          <span className="flex items-center gap-[6px] text-[13px] font-medium text-ink">
            <Dot hue={oceanHue({ graph_id: w.graph_id, kind: w.kind })} size={6} />
            <span className="truncate">{w.name}</span>
          </span>
          <span className="font-mono text-[10.5px] text-ink-4">{agoLabel(w.last_activity_at, now)}</span>
          <span className="col-span-2 truncate pl-[12px] text-[12.5px] text-ink-2">{workspaceHeadlineText(w.headline)}</span>
        </Link>
      ))}
    </HomeCard>
  );
}

function agoLabel(iso: string, now: Date): string {
  const mins = Math.max(0, Math.floor((now.getTime() - Date.parse(iso)) / 60_000));
  if (mins < 60) return `${Math.max(1, mins)}m`;
  if (mins < 60 * 24) return `${Math.floor(mins / 60)}h`;
  return `${Math.floor(mins / 1440)}d`;
}

export function EmptyNote() {
  return (
    <div className="rounded-card border border-line-2 bg-bg-subtle px-[14px] py-[12px] text-[12.5px] leading-[1.5] text-ink-2">
      Workspaces you join are already searchable from chat. Your private ocean fills from what you connect.
    </div>
  );
}
