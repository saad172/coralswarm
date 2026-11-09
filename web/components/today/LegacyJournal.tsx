"use client";

import { useQueryClient } from "@tanstack/react-query";
import { useReturnRefetch } from "@/lib/useReturnRefetch";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useApp } from "@/lib/providers";
import { ApiError } from "@/lib/api";
import { journalDayQueryOptions, journalRollupsQueryOptions } from "@/lib/appQueries";
import { ErrorNote, Loading } from "@/components/ui";
import { usePageSection } from "@/lib/pageSections";
import { SectionLiveness, sectionLiveness } from "@/components/PageSection";
import type { JournalResponse, JournalPeriod } from "@/lib/api";
import { DayJournal } from "@/components/today/DayJournal";
import { JournalRail } from "@/components/today/JournalRail";
import { WeekJournal } from "@/components/today/WeekJournal";
import { MonthJournal } from "@/components/today/MonthJournal";
import { PeriodRail } from "@/components/today/PeriodRail";
import {
  askAboutDayHref,
  askAboutMonthHref,
  askAboutWeekHref,
  buildDayRows,
  dayRowLabel,
  formatDayParam,
  formatHourLabel,
  futureSlotMask,
  hoursOfLocalDay,
  intensityForPeriods,
  intensityForSlots,
  isSameLocalDay,
  localDayToUtcRange,
  zoomRequestRange,
  localMonthBounds,
  localWeekBounds,
  parseDayParam,
  rowContainingInstant,
  rowElementId,
  statsForDay,
  statsForPeriods,
  weekRowLabel,
} from "@/lib/journal";

// Bootstrap directly selects the legacy rollups when activities are disabled.

const GRID = "grid grid-cols-1 gap-[24px] xl:grid-cols-[minmax(0,1fr)_300px]";

function errMessage(e: unknown): string | null {
  if (!e) return null;
  return e instanceof ApiError ? e.message : "Could not load the journal.";
}

export function LegacyDay({ day }: { day: Date }) {
  const { getAuthToken, queryScope, currentGraph, graphs } = useApp();
  const dayKey = formatDayParam(day);
  const [now, setNow] = useState(() => new Date());
  const isToday = isSameLocalDay(day, now);

  // The bearer is read inside the query, never depended on (see `token` in
  // lib/providers.tsx). A day visited before paints from cache.
  const qc = useQueryClient();
  const dayQueryKey = journalDayQueryOptions({ getAuthToken, scope: queryScope }, dayKey).queryKey;
  const range = localDayToUtcRange(parseDayParam(dayKey));
  const query = usePageSection<JournalResponse>("journal_rollups", { granularity: "hour", from: range.from.toISOString(), to: range.to.toISOString() }, true,
    dayQueryKey);
  const hours = useMemo(() => query.data?.hours ?? [], [query.data]);
  const loading = !query.error && query.data === undefined;
  const err = errMessage(query.error);

  // Only while viewing today: the 60s tick's job is to catch the midnight
  // rollover.
  useEffect(() => {
    if (!isToday) return;
    const id = setInterval(() => setNow(new Date()), 60_000);
    return () => clearInterval(id);
  }, [isToday]);
  const returnKey = JSON.stringify(dayQueryKey);
  const refreshOnReturn = useCallback(() => {
    setNow(new Date());
    void qc.invalidateQueries({ queryKey: JSON.parse(returnKey), exact: true }, { cancelRefetch: false });
  }, [qc, returnKey]);
  useReturnRefetch(refreshOnReturn, { enabled: isToday && !query.isRefreshing, inFlight: () => query.isRefreshing });

  const slots = useMemo(() => hoursOfLocalDay(parseDayParam(dayKey)), [dayKey]);
  const rows = useMemo(
    () => buildDayRows(slots, hours.map((h) => new Date(h.hour_start)), isToday ? now : null),
    [slots, hours, isToday, now],
  );

  const filterGraphId = currentGraph?.id ?? null;
  const reachableOceanCount = graphs.length;
  const distinctOceans = useMemo(() => new Set(hours.flatMap((h) => h.entries.map((e) => e.graph_id))), [hours]);
  const showOceanChips = !filterGraphId && distinctOceans.size > 1;

  const stats = useMemo(() => statsForDay(hours), [hours]);
  const intensity = useMemo(() => intensityForSlots(slots, hours), [slots, hours]);
  const hourLabels = useMemo(() => slots.map((s) => formatHourLabel(s.start)), [slots]);
  const futureSlots = useMemo(() => futureSlotMask(slots, isToday ? now : null), [slots, isToday, now]);

  const scrollToSlot = useCallback(
    (i: number) => {
      const row = rowContainingInstant(rows, slots[i].start);
      if (!row) return;
      document.getElementById(rowElementId(row))?.scrollIntoView({ behavior: "smooth", block: "center" });
    },
    [rows, slots],
  );

  return (
    <>
      <div className="mb-3 flex justify-end"><SectionLiveness title="Today" state={query} /></div>
      {err && (
        <div className="mb-[16px]">
          <ErrorNote>{err}</ErrorNote>
        </div>
      )}
      {loading ? (
        <Loading what="reading the journal" />
      ) : query.data !== undefined ? (
        <div className={GRID}>
          <div className="min-w-0">
            <DayJournal
              rows={rows}
              hours={hours}
              filterGraphId={filterGraphId}
              reachableOceanCount={reachableOceanCount}
              showOceanChips={showOceanChips}
            />
          </div>
          <JournalRail
            stats={stats}
            intensity={intensity}
            future={futureSlots}
            hourLabels={hourLabels}
            onSlotClick={scrollToSlot}
            askHref={askAboutDayHref(day)}
          />
        </div>
      ) : null}
    </>
  );
}

export function LegacyPeriod({ zoom, anchor }: { zoom: "week" | "month"; anchor: Date }) {
  const { getAuthToken, queryScope, currentGraph } = useApp();
  const anchorKey = formatDayParam(anchor);

  const ctx = { getAuthToken, scope: queryScope };
  const range = zoomRequestRange(zoom, parseDayParam(anchorKey));
  const rangeParams = { from: range.from.toISOString(), to: range.to.toISOString() };
  const parent = usePageSection<{ periods: JournalPeriod[] }>("journal_rollups", { ...rangeParams, granularity: zoom }, true,
    journalRollupsQueryOptions(ctx, zoom, zoom, anchorKey).queryKey);
  const childZoom = zoom === "week" ? "day" : "week";
  const children = usePageSection<{ periods: JournalPeriod[] }>("journal_rollups", { ...rangeParams, granularity: childZoom }, true,
    journalRollupsQueryOptions(ctx, childZoom, zoom, anchorKey).queryKey);
  const live = sectionLiveness([parent, children]);
  const ownPeriod = parent.data?.periods[0] ?? null;
  const childPeriods = useMemo(() => children.data?.periods ?? [], [children.data]);
  const loading = (!parent.error && parent.data === undefined) || (!children.error && children.data === undefined);
  const err = errMessage(parent.error ?? children.error);

  const filterGraphId = currentGraph?.id ?? null;
  const distinctOceans = useMemo(
    () => new Set(childPeriods.flatMap((p) => p.entries.map((e) => e.graph_id))),
    [childPeriods],
  );
  const showOceanChips = !filterGraphId && distinctOceans.size > 1;

  const anchorDay = parseDayParam(anchorKey);
  // The server's own `period_start` is authoritative for the period it
  // returned; the client's local bounds only label the ask link when nothing
  // has compiled yet.
  const clientBounds = zoom === "week" ? localWeekBounds(anchorDay) : localMonthBounds(anchorDay);
  const periodStart = ownPeriod ? new Date(ownPeriod.period_start) : clientBounds.start;

  const stats = useMemo(() => statsForPeriods(childPeriods), [childPeriods]);
  const intensity = useMemo(() => intensityForPeriods(zoom, childPeriods), [zoom, childPeriods]);
  const labels = useMemo(
    () => childPeriods.map((p) => (zoom === "week" ? dayRowLabel(new Date(p.period_start)) : weekRowLabel(new Date(p.period_start)))),
    [zoom, childPeriods],
  );
  const scrollToBucket = useCallback(
    (i: number) => {
      const p = childPeriods[i];
      if (!p) return;
      document.getElementById(rowElementId({ start: new Date(p.period_start) }))?.scrollIntoView({ behavior: "smooth", block: "center" });
    },
    [childPeriods],
  );

  const askHref = zoom === "week" ? askAboutWeekHref(periodStart) : askAboutMonthHref(periodStart);
  const askLabel = zoom === "week" ? "Ask about this week" : "Ask about this month";

  return (
    <>
      <div className="mb-3 flex justify-end"><SectionLiveness title="Today" state={live} /></div>
      {err && (
        <div className="mb-[16px]">
          <ErrorNote>{err}</ErrorNote>
        </div>
      )}
      {loading ? (
        <Loading what="reading the journal" />
      ) : parent.data !== undefined && children.data !== undefined ? (
        <div className={GRID}>
          <div className="min-w-0">
            {zoom === "week" ? (
              <WeekJournal ownPeriod={ownPeriod} dayPeriods={childPeriods} filterGraphId={filterGraphId} showOceanChips={showOceanChips} />
            ) : (
              <MonthJournal
                ownPeriod={ownPeriod}
                weekPeriods={childPeriods}
                anchorMonth={anchorDay.getMonth()}
                filterGraphId={filterGraphId}
                showOceanChips={showOceanChips}
              />
            )}
          </div>
          <PeriodRail
            zoom={zoom}
            stats={stats}
            intensity={intensity}
            labels={labels}
            onBucketClick={scrollToBucket}
            askHref={askHref}
            askLabel={askLabel}
          />
        </div>
      ) : null}
    </>
  );
}
