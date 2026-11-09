"use client";

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { useApp } from "@/lib/providers";
import { DetailPanel } from "@/components/detail/DetailPanel";
import { fromActivity, fromActivityDetail } from "@/components/detail/detailModel";
import { Timeline } from "@/components/today/Timeline";
import { Workstream } from "@/components/today/Workstream";
import { journalActivityQueryOptions, type Activity } from "@/lib/journalActivitiesApi";
import type { Gist } from "@/lib/pageSectionTypes/todayGist";
import type { Zoom } from "@/lib/journal";
import {
  askHref,
  askLabel,
  buildTimeline,
  dayBars,
  hourBars,
  monthChartDays,
  peopleRail,
  periodSpan,
  periodStats,
  selfNames,
  stepIndex,
  streamSpan,
  timeSplit,
  toggleFilter,
  topicRail,
  weekChartDays,
  type Density,
  type TodayFilter,
  type Who,
} from "@/lib/todayView";

/* The activities body of Today: the timeline on the left and the sticky
   workstream panel on the right, plus the detail panel over the page. The
   parent keys this component by (zoom, day, who), so a new period starts with
   no filter and no open panel (the prototype resets both on every navigation).

   The open panel is remembered by activity key, not by position: filtering or a
   refetch that drops the open item closes the panel instead of silently
   showing a different one. */

const rowKey = (a: Activity) => `${a.graph.id}:${a.key}`;

export function TodayActivities({
  zoom,
  anchor,
  now,
  who,
  tz,
  activities,
  truncated,
  gist,
  gistPending = false,
  gistError = null,
  density,
  onDensity,
}: {
  zoom: Zoom;
  anchor: Date;
  now: Date;
  who: Who;
  tz?: string;
  activities: Activity[];
  truncated: boolean;
  /** Null hides the gist rows; stats and charts stay. */
  gist: Gist | null;
  gistPending?: boolean;
  gistError?: Error | null;
  density: Density;
  onDensity: (d: Density) => void;
}) {
  const router = useRouter();
  const { authReady, getAuthToken, queryScope, internalUserId, graphs } = useApp();
  const ctx = { getAuthToken, scope: queryScope };

  const [filter, setFilter] = useState<TodayFilter | null>(null);
  const [openKey, setOpenKey] = useState<string | null>(null);

  const model = useMemo(
    () => buildTimeline({ activities, zoom, tz, filter, compact: density === "compact" }),
    [activities, zoom, tz, filter, density],
  );
  const { all, visible } = model;

  const openIndex = openKey ? visible.findIndex((a) => rowKey(a) === openKey) : -1;
  const open = openIndex >= 0 ? visible[openIndex] : null;

  const detail = useQuery({
    ...journalActivityQueryOptions(ctx, open?.key ?? "", open?.graph.id ?? "", tz),
    enabled: authReady && !!open,
    retry: false,
  });

  const stats = useMemo(() => periodStats(all), [all]);
  const span = useMemo(() => periodSpan(all, tz), [all, tz]);
  const bars = useMemo(
    () =>
      zoom === "day"
        ? hourBars(all, tz)
        : zoom === "week"
          ? dayBars(all, weekChartDays(anchor), "week", tz)
          : dayBars(all, monthChartDays(anchor, now), "month", tz),
    [all, zoom, anchor, now, tz],
  );
  const split = useMemo(() => timeSplit(bars), [bars]);
  const people = useMemo(() => peopleRail(all, { who, self: selfNames(all, internalUserId) }), [all, who, internalUserId]);
  const topics = useMemo(() => topicRail(all), [all]);
  const oceanKinds = useMemo(() => new Map(graphs.map((g) => [g.id, g.kind as string])), [graphs]);

  const panelItem = open
    ? detail.data && detail.data.key === open.key
      ? fromActivityDetail(detail.data, { oceanKind: oceanKinds.get(open.graph.id), tz })
      : fromActivity(open, { oceanKind: oceanKinds.get(open.graph.id), tz })
    : null;

  const step = (dir: 1 | -1) => {
    if (!visible.length) return;
    setOpenKey(rowKey(visible[stepIndex(openIndex, visible.length, dir)]));
  };

  return (
    <>
      <div className="grid grid-cols-1 items-start gap-[24px] xl:grid-cols-[minmax(0,1fr)_360px]">
        <Timeline
          model={model}
          zoom={zoom}
          tz={tz}
          density={density}
          onDensity={onDensity}
          filter={filter}
          onClearFilter={() => {
            setFilter(null);
            setOpenKey(null);
          }}
          openKey={open ? rowKey(open) : null}
          onOpen={(i) => setOpenKey(rowKey(visible[i]))}
          isTeam={who === "team"}
          internalUserId={internalUserId}
          truncated={truncated}
        />
        <Workstream
          who={who}
          span={streamSpan(zoom, span)}
          askLabel={askLabel(zoom, anchor, now)}
          onAsk={() => router.push(askHref(zoom, anchor))}
          gist={gist}
          gistPending={gistPending}
          gistError={gistError}
          stats={stats}
          chartLabel={zoom === "day" ? "time by hour" : zoom === "month" ? "time by day · last 14 days" : "time by day"}
          bars={bars}
          split={split.items}
          totalMin={split.totalMin}
          people={people}
          topics={topics}
          filter={filter}
          onToggleFilter={(f) => {
            setFilter((cur) => toggleFilter(cur, f));
            setOpenKey(null);
          }}
        />
      </div>

      {panelItem && (
        <DetailPanel
          item={panelItem}
          loading={detail.isPending}
          position={{ index: openIndex, total: visible.length }}
          onPrev={() => step(-1)}
          onNext={() => step(1)}
          onClose={() => setOpenKey(null)}
        />
      )}
    </>
  );
}
