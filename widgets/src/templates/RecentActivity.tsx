import { useLayoutEffect, useState } from "preact/hooks";
import { tokensFor, FONT_MONO } from "../theme";
import { useToolOutput, useTheme } from "../openai";
import { gutter, useHostLayout, type SizeTier } from "../layout";
import type { RecentActivityOutput, ActivityEvent } from "../types";
import { Card } from "../components/Card";
import { isToolFailure, ToolFailure } from "../components/ToolFailure";
import { Empty } from "../components/Empty";
import { Working } from "../components/Working";
import { describeEvent } from "../eventDisplay";
import { dayBucket, timeOfDay, pluralize } from "../format";
import { todayHref } from "../links";

/** Tier default when the host gives no `maxHeight` to budget against. */
const TIER_PREVIEW_COUNT: Record<SizeTier, number> = { compact: 3, regular: 5, wide: 8 };
/** Rough chrome estimate: Card's own header + one day-group header row +
 *  the "N of M shown" footer bar — the fixed vertical space every layout
 *  pays regardless of how many events are visible. */
const CHROME_PX = 120;
/** Rough per-event row height: two content lines (badge row + body) plus
 *  the row's own border/padding and bottom margin. Both estimates are
 *  deliberately generous (better to under-fill the budget than clip the
 *  last row against `maxHeight`). */
const EVENT_ROW_PX = 72;

/** The `maxHeight` for the scrolling event body under a host cap, or `null`
 *  when the widget should size itself freely. `previewCountFor` clamps the
 *  preview to at least three events, so a cap shorter than three rows (and
 *  every EXPANDED render, which deliberately exceeds the preview) would
 *  otherwise overflow `Card`'s `overflow: hidden` and be silently clipped —
 *  the footer and its toggle included. Bounding the body instead keeps the
 *  card inside the cap and leaves the events reachable by scrolling, which
 *  is what ask_ocean's inline wrapper already does with the same signal. */
function scrollBodyHeight(fullscreen: boolean, maxHeight: number | null): number | null {
  if (fullscreen || maxHeight == null) return null;
  // No lower floor: a floor that exceeds the cap pushes the card past it and
  // the host clips the FOOTER — the toggle and the "n of m" count, the one
  // part that must survive a cap too short for the events. A few px of
  // scroller is ugly; an unreachable toggle is a dead end. `CHROME_PX`
  // over-estimates the real header+footer, so the card lands inside the cap.
  return Math.max(0, maxHeight - CHROME_PX);
}

/** How many events to show before the "Show all N events" toggle, given
 *  the host's reported layout and the total event count:
 *   - `fullscreen` shows every event (nothing to save space for).
 *   - otherwise, with a known `maxHeight`, back-solve a row budget from it,
 *     clamped to [3, 10] so a very short or very tall cap can't produce an
 *     unreasonable preview.
 *   - otherwise (no height signal at all) fall back to a per-size-tier
 *     default. */
function previewCountFor(fullscreen: boolean, maxHeight: number | null, size: SizeTier, total: number): number {
  if (fullscreen) return total;
  if (maxHeight != null) {
    const budget = Math.floor((maxHeight - CHROME_PX) / EVENT_ROW_PX);
    return Math.min(total, Math.max(3, Math.min(10, budget)));
  }
  return Math.min(total, TIER_PREVIEW_COUNT[size]);
}

function groupByDay(events: ActivityEvent[]): Array<{ bucket: string; events: ActivityEvent[] }> {
  const groups = new Map<string, ActivityEvent[]>();
  for (const e of events) {
    const b = dayBucket(e.created_at);
    const group = groups.get(b);
    if (group) group.push(e);
    else groups.set(b, [e]);
  }
  // `Map` iteration preserves insertion (first-seen) order, so this needs no
  // separate `order` array to get the same result without a non-null
  // assertion on every lookup.
  return Array.from(groups, ([bucket, groupedEvents]) => ({ bucket, events: groupedEvents }));
}

export function RecentActivityTemplate() {
  const output = useToolOutput<RecentActivityOutput>();
  const theme = useTheme();
  const t = tokensFor(theme);
  const [expanded, setExpanded] = useState(false);
  const { size, maxHeight, displayMode } = useHostLayout();
  const compact = size === "compact";
  useLayoutEffect(() => setExpanded(false), [output]);
  if (isToolFailure(output)) return <ToolFailure t={t} output={output} tool="recent_activity" />;

  if (!output) {
    return <Working t={t} label="Loading activity…" />;
  }

  const events = output.events ?? [];
  const fullscreen = displayMode === "fullscreen";
  const previewCount = previewCountFor(fullscreen, maxHeight, size, events.length);
  const scrollBody = scrollBodyHeight(fullscreen, maxHeight);
  const visibleEvents = expanded ? events : events.slice(0, previewCount);
  const bodyGutter = gutter(size, 18);
  const status = events.length > 0 ? `recent activity · ${events.length} event${events.length === 1 ? "" : "s"}` : "recent activity";

  return (
    <Card t={t} status={status} linkLabel="open today" linkHref={todayHref()}>
      {events.length === 0 ? (
        <Empty t={t} title="Nothing has happened yet." body="Activity shows up here once corals start capturing." />
      ) : (
        <div
          data-scroll-body={scrollBody === null ? "unbounded" : "bounded"}
          style={scrollBody === null ? undefined : { maxHeight: scrollBody, overflowY: "auto" }}
        >
        {groupByDay(visibleEvents).map(({ bucket, events: dayEvents }) => {
          const people = new Set(dayEvents.map((e) => e.user_id)).size;
          return (
            <div key={bucket} style={{ padding: `14px ${bodyGutter}px 6px` }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "4px 0 10px" }}>
                <span style={{ fontFamily: FONT_MONO, fontSize: 11, letterSpacing: "0.08em", textTransform: "uppercase", color: t.secondary }}>
                  {bucket}
                </span>
                <span style={{ flex: 1, height: 1, background: t.lineSoft }} />
                <span style={{ fontFamily: FONT_MONO, fontSize: 11, color: t.secondary }}>
                  {dayEvents.length} {pluralize(dayEvents.length, "event")} · {people} {pluralize(people, "person", "people")}
                </span>
              </div>
              {dayEvents.map((e) => {
                const d = describeEvent(e);
                return (
                  <div key={e.id} data-time-column={compact ? "inline" : "gutter"} style={{ display: "grid", gridTemplateColumns: compact ? "minmax(0, 1fr)" : "40px minmax(0, 1fr)", gap: compact ? 0 : 12 }}>
                    {!compact && (
                      <div style={{ textAlign: "right", paddingTop: 11, fontFamily: FONT_MONO, fontSize: 11, color: t.secondary }}>
                        {timeOfDay(e.created_at)}
                      </div>
                    )}
                    <div style={{ position: "relative", borderLeft: `1px solid ${t.lineSoft}`, padding: "0 0 12px 18px" }}>
                      <span
                        style={{
                          position: "absolute",
                          left: -4.5,
                          top: 13,
                          width: 8,
                          height: 8,
                          borderRadius: 999,
                          background: d.badgeColor(t),
                          boxShadow: `0 0 0 3px ${t.bg}`,
                        }}
                      />
                      <div style={{ display: "flex", flexDirection: "column", gap: 5, border: `1px solid ${t.lineSoft}`, borderRadius: 11, padding: "11px 13px", background: t.bg }}>
                        <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 8 }}>
                          <span
                            style={{
                              display: "inline-flex",
                              alignItems: "center",
                              gap: 4,
                              borderRadius: 999,
                              padding: "2px 8px",
                              background: `${d.badgeColor(t)}30`,
                              color: d.badgeColor(t),
                              fontFamily: FONT_MONO,
                              fontSize: 10,
                              letterSpacing: "0.06em",
                              textTransform: "uppercase",
                              whiteSpace: "nowrap",
                            }}
                          >
                            {d.badgeLabel}
                          </span>
                          {compact && (
                            <span style={{ fontFamily: FONT_MONO, fontSize: 11, color: t.secondary }}>
                              {timeOfDay(e.created_at)}
                            </span>
                          )}
                          <span style={{ flex: 1 }} />
                          <span style={{ fontFamily: FONT_MONO, fontSize: 11, color: t.secondary, overflowWrap: "anywhere" }}>{d.contextTag}</span>
                        </div>
                        <div style={{ fontSize: 13.5, lineHeight: 1.5, color: t.ink, overflowWrap: "anywhere" }}>{d.body}</div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          );
        })}
        </div>
      )}
      {(events.length > previewCount || output.has_more) && (
        <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "4px 12px", padding: `8px ${gutter(size, 16)}px`, fontFamily: FONT_MONO, fontSize: 11, color: t.secondary, background: t.headerBg, borderTop: `1px solid ${t.lineSoft}` }}>
          <span>{visibleEvents.length} of {events.length} events shown</span>
          {events.length > previewCount && (
            <button
              type="button"
              aria-expanded={expanded}
              onClick={() => setExpanded((value) => !value)}
              style={{ minHeight: 44, padding: "8px 0", border: 0, background: "transparent", color: t.secondary, fontFamily: FONT_MONO, fontSize: 11, cursor: "pointer", textDecoration: "underline", textUnderlineOffset: 3 }}
            >
              {expanded ? "Show fewer events" : `Show all ${events.length} events`}
            </button>
          )}
          {output.has_more && <span>ask for the next page</span>}
        </div>
      )}
    </Card>
  );
}
