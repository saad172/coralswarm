"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { usePageSection } from "@/lib/pageSections";
import type { ActivitiesResponse } from "@/lib/journalActivitiesApi";
import { useApp } from "@/lib/providers";
import { ApiError } from "@/lib/api";
import { Button, ErrorNote, Loading, Seg } from "@/components/ui";
import { SectionLiveness, sectionLiveness } from "@/components/PageSection";
import { HeaderMeta } from "@/components/HeaderMeta";
import { TodayShell } from "@/components/today/TodayShell";
import { TodayActivities } from "@/components/today/TodayActivities";
import { LegacyDay, LegacyPeriod } from "@/components/today/LegacyJournal";
import { useDensity } from "@/components/today/useDensity";
import { browserTimeZone } from "@/lib/pageSectionTypes/home";
import { todayActivitiesQueryOptions } from "@/lib/todayQueries";
import { journalGistQueryOptions } from "@/lib/todayQueries";
import { formatDayParam, isSameLocalDay, journalHref, parseDayParam, parseZoomParam, type Zoom } from "@/lib/journal";
import {
  buildStrip,
  dedupeActivities,
  emptyStrip,
  periodMeta,
  periodSpan,
  periodTitle,
  periodWindow,
  selectionDays,
  teamGraph,
  type Who,
} from "@/lib/todayView";

// Bootstrap selects all period reads in parallel, including the feature-off rollups.

const WHO_OPTIONS: { key: Who; label: string }[] = [
  { key: "mine", label: "Mine" },
  { key: "team", label: "Team" },
];

function TodayInner() {
  const router = useRouter();
  const params = useSearchParams();
  const { getAuthToken, queryScope, userId, graphs, currentGraph, activitiesEnabled, gistEnabled } = useApp();
  const ctx = { getAuthToken, scope: queryScope };
  const tz = useMemo(() => browserTimeZone(), []);

  // Only the local day matters, so `now` changes at midnight, not every tick.
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    const id = setInterval(() => {
      const n = new Date();
      setNow((prev) => (isSameLocalDay(prev, n) ? prev : n));
    }, 60_000);
    return () => clearInterval(id);
  }, []);

  const dParam = params.get("d");
  const zoom: Zoom = parseZoomParam(params.get("z"));
  const wantTeam = params.get("who") === "team";
  const day = useMemo(() => parseDayParam(dParam, now), [dParam, now]);
  const [density, setDensity] = useDensity(userId);

  // ── Who: Team needs a workspace and a readable answer ───────────────────
  const teamG = useMemo(() => teamGraph(graphs, currentGraph), [graphs, currentGraph]);
  const teamWanted = wantTeam && !!teamG;
  const win = useMemo(() => periodWindow(zoom, day), [zoom, day]);

  const who: Who = teamWanted ? "team" : "mine";
  const graphId = who === "team" ? (teamG?.id ?? null) : null;
  const activityParams = { ...win, who, graph_id: graphId, tz };
  const activityKey = todayActivitiesQueryOptions(ctx, { ...win, who, graphId, tz }).queryKey;
  const acts = usePageSection<ActivitiesResponse>("journal_activities", activityParams, activitiesEnabled, activityKey);
  const legacy = !activitiesEnabled;
  const stripQ = usePageSection<ActivitiesResponse>("journal_strip", { who, graph_id: graphId, tz }, activitiesEnabled,
    ["journal", queryScope, "strip", { who, graphId, tz, day: formatDayParam(now) }]);
  const gistParams = { granularity: zoom, anchor: formatDayParam(day), who, graph_id: graphId, tz };
  const gistQ = usePageSection<import("@/lib/pageSectionTypes/todayGist").GistResponse>("journal_gist", gistParams,
    activitiesEnabled && gistEnabled, journalGistQueryOptions(ctx, { granularity: zoom, anchor: formatDayParam(day), who, graphId, tz }).queryKey);
  const live = sectionLiveness([acts, stripQ, ...(gistEnabled ? [gistQ] : [])]);
  const showToggle = !!teamG && !legacy;

  // ── Shell ───────────────────────────────────────────────────────────────
  const data = acts.data ?? null;
  const activities = useMemo(() => dedupeActivities(data?.activities ?? []), [data]);
  const span = useMemo(() => periodSpan(activities, tz), [activities, tz]);
  const sel = selectionDays(zoom, day);
  const cells = useMemo(
    () => buildStrip(stripQ.data?.day_activity ?? emptyStrip(now), { zoom, ...sel }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [stripQ.data, now, zoom, sel.from, sel.to],
  );

  const go = (z: Zoom, d: Date) => router.push(journalHref(z, d, now, who));

  let body;
  if (legacy) {
    body = zoom === "day" ? <LegacyDay day={day} /> : <LegacyPeriod zoom={zoom} anchor={day} />;
  } else if ((acts.isPending || (!acts.error && acts.data === undefined))) {
    body = <Loading what="reading the journal" />;
  } else if (acts.error) {
    body = <ErrorNote>{acts.error instanceof ApiError ? acts.error.message : "Could not load the journal."}</ErrorNote>;
  } else {
    body = (
      <TodayActivities
        key={`${zoom}|${formatDayParam(day)}|${who}`}
        zoom={zoom}
        anchor={day}
        now={now}
        who={who}
        tz={tz}
        activities={activities}
        truncated={!!data?.truncated}
        gist={gistQ.data?.gist ?? null}
        gistPending={gistEnabled && gistQ.isPending}
        gistError={gistEnabled ? gistQ.error : null}
        density={density}
        onDensity={setDensity}
      />
    );
  }

  return (
    <>
      {showToggle && (
        <HeaderMeta>
          <Seg options={WHO_OPTIONS} value={who} onChange={(w) => router.push(journalHref(zoom, day, now, w))} />
        </HeaderMeta>
      )}
      <TodayShell
        zoom={zoom}
        anchor={day}
        now={now}
        strip={cells}
        stripPending={!legacy && stripQ.isPending}
        stripUnavailable={legacy || !!stripQ.error}
        today={formatDayParam(now)}
        onPickDay={(d) => go("day", parseDayParam(d))}
        onPickDate={(iso) => go(zoom, parseDayParam(iso))}
        onZoom={(z) => go(z, day)}
        title={periodTitle(zoom, day)}
        meta={legacy || (acts.isPending || (!acts.error && acts.data === undefined)) ? "" : periodMeta({ zoom, who, count: activities.length, span })}
      >
        {!legacy && <div className="mb-3 flex justify-end"><SectionLiveness title="Today" state={live} /></div>}
        {!legacy && (stripQ.error || gistQ.error) && <ErrorNote action={<Button variant="ghost" size="sm" onClick={() => void live.refresh()}>Retry Today</Button>}>{stripQ.error?.message ?? gistQ.error?.message}</ErrorNote>}
        {body}
      </TodayShell>
    </>
  );
}

export default function TodayPage() {
  return (
    <Suspense fallback={<Loading what="loading the journal" />}>
      <TodayInner />
    </Suspense>
  );
}
