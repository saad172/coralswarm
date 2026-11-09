"use client";

import { useRef, type ReactNode } from "react";
import { IconButton, Seg, cn } from "@/components/ui";
import { IconToday } from "@/components/icons";
import { dayLabel } from "@/lib/homeView";
import { formatDayParam, type Zoom } from "@/lib/journal";
import type { StripCell } from "@/lib/todayView";

/* The Today shell (README "Top row" and "Title line"): the 14-day date strip,
   the calendar button for older dates, the Day / Week / Month switch, and the
   serif title with its mono meta. Both the activities view and the hour-row
   fallback render inside it. There is no breadcrumb and no prev / next
   cluster; the strip, the calendar and the switch are the whole navigation. */

const ZOOM_OPTIONS: { key: Zoom; label: string }[] = [
  { key: "day", label: "Day" },
  { key: "week", label: "Week" },
  { key: "month", label: "Month" },
];

const DOT_BG = { brand: "bg-brand", muted: "bg-ink-4", none: "bg-transparent" } as const;

function Strip({ cells, today, onPick, pending = false, unavailable = false }: { cells: StripCell[]; today: string; onPick: (day: string) => void; pending?: boolean; unavailable?: boolean }) {
  return (
    <div
      role="group"
      aria-label="Last 14 days"
      aria-busy={pending}
      className="flex min-w-0 flex-1 gap-[2px] rounded-control border border-line-2 bg-bg-2 p-[3px]"
    >
      {pending && <span role="status" className="sr-only">Loading day activity…</span>}
      {cells.map((c) => (
        <button
          key={c.day}
          type="button"
          onClick={() => onPick(c.day)}
          aria-pressed={c.selected}
          aria-current={c.day === today ? "date" : undefined}
          aria-label={`${dayLabel(c.day)}${!pending && !unavailable && c.count ? `, ${c.count} ${c.count === 1 ? "activity" : "activities"}` : ""}`}
          className={cn(
            "flex min-w-0 flex-1 flex-col items-center gap-[2px] rounded-row border pb-[6px] pt-[5px] transition-[background-color,border-color,box-shadow]",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40",
            c.selected ? "border-line bg-bg" : "border-transparent hover:bg-bg-hover",
            c.shadow && "shadow-row",
          )}
        >
          <span
            className={cn(
              "font-mono text-[9px] uppercase tracking-[0.08em]",
              c.day === today ? "text-brand-2" : "text-ink-3",
            )}
          >
            {c.weekday}
          </span>
          <span
            className={cn(
              "text-[13px]",
              c.selected ? "font-semibold text-ink" : pending || unavailable || c.count ? "font-normal text-ink" : "font-normal text-ink-4",
            )}
          >
            {c.date}
          </span>
          <span
            aria-hidden
            className={cn("rounded-pill", pending || unavailable ? DOT_BG.none : DOT_BG[c.dotTone])}
            style={{ width: c.dotPx, height: c.dotPx }}
          />
        </button>
      ))}
    </div>
  );
}

/** The calendar button. A native date input does the picking; it sits visually
 *  hidden beside the button, which asks the browser to open it. */
function DatePick({ value, max, onPick }: { value: Date; max: Date; onPick: (iso: string) => void }) {
  const ref = useRef<HTMLInputElement>(null);
  const open = () => {
    const el = ref.current;
    if (!el) return;
    if (typeof el.showPicker === "function") {
      try {
        el.showPicker();
        return;
      } catch {
        /* not allowed here: fall through to a click */
      }
    }
    el.focus();
    el.click();
  };
  return (
    <div className="relative flex-none">
      <IconButton label="Pick an older date" size={34} onClick={open}>
        <IconToday width={15} height={15} />
      </IconButton>
      <input
        ref={ref}
        type="date"
        aria-label="Date"
        tabIndex={-1}
        value={formatDayParam(value)}
        max={formatDayParam(max)}
        onChange={(e) => e.target.value && onPick(e.target.value)}
        className="pointer-events-none absolute left-0 top-0 h-px w-px opacity-0"
      />
    </div>
  );
}

export function TodayShell({
  zoom,
  anchor,
  now,
  strip,
  stripPending = false,
  stripUnavailable = false,
  today,
  onPickDay,
  onPickDate,
  onZoom,
  title,
  meta,
  children,
}: {
  zoom: Zoom;
  anchor: Date;
  now: Date;
  strip: StripCell[];
  stripPending?: boolean;
  stripUnavailable?: boolean;
  /** Local `YYYY-MM-DD` of today. */
  today: string;
  onPickDay: (day: string) => void;
  onPickDate: (iso: string) => void;
  onZoom: (z: Zoom) => void;
  title: string;
  meta: string;
  children: ReactNode;
}) {
  return (
    <div>
      <div className="flex items-center gap-[10px]">
        <Strip cells={strip} today={today} onPick={onPickDay} pending={stripPending} unavailable={stripUnavailable} />
        <DatePick value={anchor} max={now} onPick={onPickDate} />
        <Seg options={ZOOM_OPTIONS} value={zoom} onChange={onZoom} />
      </div>

      <div className="mb-[16px] mt-[22px] flex items-baseline gap-[12px]">
        <h1 className="font-display text-[30px] font-normal leading-[1.15] text-ink">{title}</h1>
        {meta && <span className="font-mono text-[10.5px] text-ink-3">{meta}</span>}
      </div>

      {children}
    </div>
  );
}
