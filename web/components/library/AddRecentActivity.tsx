"use client";

import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { activityQueryOptions } from "@/lib/appQueries";
import { Eyebrow, Chip, Loading } from "@/components/ui";
import { SourceGlyph, hueForSource, iconKindFor, sourceMeta } from "./sourceKind";

const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

/** "Added recently" rail — the last 7 days of `atom_ingested` activity,
 *  reusing the same `api.activity.list` feed Today/Currents already read
 *  (see archive/web-legacy-ui/screens/currents.page.tsx's `describe()`),
 *  filtered to ingest events only since this rail is about what just
 *  arrived, not every kind of activity. */
export function AddRecentActivity() {
  const { authReady, getAuthToken, queryScope, currentGraph } = useApp();
  // The bearer is read inside the query, never depended on (see `token` in
  // lib/providers.tsx). A failed read renders as "nothing ingested".
  const query = useQuery({
    ...activityQueryOptions({ getAuthToken, scope: queryScope }, currentGraph?.id ?? null),
    enabled: authReady,
  });
  const events = query.data ?? [];
  const loading = query.isPending;

  const cutoff = Date.now() - SEVEN_DAYS_MS;
  const recent = events
    .filter((e) => e.event_type === "atom_ingested" && Date.parse(e.created_at) >= cutoff)
    .slice(0, 12);

  return (
    <div>
      <div className="mb-[12px] flex items-center gap-[8px]">
        <Eyebrow>added recently</Eyebrow>
        <span className="ml-auto text-[12px] text-ink-3">last 7 days</span>
      </div>
      {loading ? (
        <Loading what="loading activity" />
      ) : recent.length === 0 ? (
        <p className="text-[12.5px] text-ink-3">Nothing ingested in the last 7 days.</p>
      ) : (
        <div className="flex flex-col gap-[6px]">
          {recent.map((e) => {
            const type = typeof e.payload?.source_type === "string" ? e.payload.source_type : "note";
            const hue = hueForSource(type);
            return (
              <div key={e.id} className="flex items-center gap-[10px] rounded-control border border-line bg-bg px-[12px] py-[10px]">
                <span className="flex-none" style={{ color: `var(--hue-${hue})` }}>
                  <SourceGlyph kind={iconKindFor(type)} size={14} />
                </span>
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-[12.5px] text-ink">{sourceMeta(type).label} item spawned</span>
                  <span className="mt-[2px] block font-mono text-[9px] text-ink-3">{new Date(e.created_at).toLocaleString()}</span>
                </span>
                <Chip hue={hue}>ingested</Chip>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
