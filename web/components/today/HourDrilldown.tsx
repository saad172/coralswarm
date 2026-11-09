"use client";

import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { journalHourItemsQueryOptions } from "@/lib/appQueries";
import { api, ApiError, type JournalHourItemActivity, type JournalHourItemAsk, type JournalHourItemAtom } from "@/lib/api";
import { Eyebrow } from "@/components/ui";
import { facetLabelFor } from "@/components/library/sourceKind";

/* The hour card's inline "everything this hour" expansion (ticket 16's
   uncapped drill-down endpoint) — grouped Sessions / Captured / Asks /
   Events per the approved prototype, "N of M shown" from the endpoint's
   ALWAYS-full `totals` (never affected by pagination), load-more via the
   opaque keyset cursor. Mounted only while its parent `HourRow` is expanded
   (`DayJournal.tsx`), so a row that's never opened never fires the request.
   Text-only rendering throughout: `excerpt`/`label`/`question` are stored,
   already-excerpted strings (never LLM-compiled prose over untrusted
   content the way `summary` is), but still rendered as plain React text
   children, never `dangerouslySetInnerHTML`. */

/** "Load more" pages for one (hour, ocean) scope, appended after the cached
 *  first page. Tagged with the scope so a page fetched for a previous hour
 *  or ocean can never be shown under this one. */
interface MorePages {
  scope: string;
  atoms: JournalHourItemAtom[];
  activity: JournalHourItemActivity[];
  asks: JournalHourItemAsk[];
  hasMore: boolean;
  nextCursor: string | null;
}

function ItemRow({ time, children }: { time: string; children: ReactNode }) {
  return (
    <div className="flex items-baseline gap-[10px] border-b border-dotted border-line-2 py-[5px] text-[12.5px] last:border-b-0">
      <time className="w-[42px] flex-none font-mono text-[10px] text-ink-4">{time}</time>
      <span className="min-w-0 flex-1 text-ink-2">{children}</span>
    </div>
  );
}

function fmtTime(iso: string): string {
  return new Date(iso).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", hour12: false });
}

function Section({ label, count, children }: { label: string; count: number; children: ReactNode }) {
  if (count === 0) return null;
  return (
    <div className="flex flex-col gap-[2px]">
      <Eyebrow className="flex items-center gap-[6px]">
        {label}
        <span className="text-brand-2">{count}</span>
      </Eyebrow>
      {children}
    </div>
  );
}

export function HourDrilldown({
  hourStart,
  graphId,
  onCollapse,
}: {
  hourStart: string;
  graphId: string | null;
  onCollapse: () => void;
}) {
  const { authReady, getAuthToken, queryScope } = useApp();
  // The first page is a cached query: re-opening an hour paints at once. The
  // bearer is read inside it, never depended on (it rotates about once a
  // minute and used to blank the open hour each time).
  const query = useQuery({
    ...journalHourItemsQueryOptions({ getAuthToken, scope: queryScope }, hourStart, graphId),
    enabled: authReady,
  });
  const scope = `${hourStart}|${graphId ?? ""}`;
  const [more, setMore] = useState<MorePages | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [moreError, setMoreError] = useState<string | null>(null);
  // Guards against out-of-order "load more" responses: a slow page for the
  // PREVIOUS hour/ocean scope landing after a change must not paint another
  // ocean's atoms into this list or corrupt the next cursor.
  const requestIdRef = useRef(0);
  useEffect(() => {
    requestIdRef.current += 1;
    setLoadingMore(false);
    setMoreError(null);
  }, [scope]);

  const first = query.data;
  const extra = more?.scope === scope && first ? more : null;
  const state = {
    loading: query.isPending,
    loadingMore,
    error: query.isError && !first
      ? (query.error instanceof ApiError ? query.error.message : "Could not load this hour's items.")
      : moreError,
    atoms: [...(first?.atoms ?? []), ...(extra?.atoms ?? [])],
    activity: [...(first?.activity ?? []), ...(extra?.activity ?? [])],
    asks: [...(first?.asks ?? []), ...(extra?.asks ?? [])],
    totals: first?.totals ?? { atoms: 0, activity: 0, asks: 0 },
    hasMore: extra ? extra.hasMore : (first?.has_more ?? false),
    nextCursor: extra ? extra.nextCursor : (first?.next_cursor ?? null),
  };

  const loadMore = useCallback(
    async (cursor: string | null) => {
      const token = authReady ? getAuthToken() : null;
      if (!token || !cursor) return;
      const requestId = ++requestIdRef.current;
      const pageScope = scope;
      setLoadingMore(true);
      setMoreError(null);
      try {
        const res = await api.journal.hourItems(token, hourStart, { graphId: graphId ?? undefined, cursor });
        if (requestIdRef.current !== requestId) return;
        setMore((m) => {
          const prev = m?.scope === pageScope ? m : null;
          return {
            scope: pageScope,
            atoms: [...(prev?.atoms ?? []), ...res.atoms],
            activity: [...(prev?.activity ?? []), ...res.activity],
            asks: [...(prev?.asks ?? []), ...res.asks],
            hasMore: res.has_more,
            nextCursor: res.next_cursor,
          };
        });
      } catch (e) {
        if (requestIdRef.current !== requestId) return;
        setMoreError(e instanceof ApiError ? e.message : "Could not load this hour's items.");
      } finally {
        if (requestIdRef.current === requestId) setLoadingMore(false);
      }
    },
    [authReady, getAuthToken, hourStart, graphId, scope],
  );

  const sessions = state.atoms.filter((a) => a.source_type === "coding_session");
  const captured = state.atoms.filter((a) => a.source_type !== "coding_session");
  const shown = state.atoms.length + state.activity.length + state.asks.length;
  const totalShown = state.totals.atoms + state.totals.activity + state.totals.asks;

  return (
    <div className="flex flex-col gap-[12px] border-t border-line-2 pt-[12px]">
      {state.loading ? (
        <p className="font-mono text-[10.5px] uppercase tracking-[0.1em] text-ink-3">loading everything this hour…</p>
      ) : state.error ? (
        <p className="text-[12.5px] text-bad">{state.error}</p>
      ) : shown === 0 ? (
        <p className="text-[12.5px] text-ink-3">Nothing captured this hour yet.</p>
      ) : (
        <>
          <Section label="Coding sessions" count={sessions.length}>
            {sessions.map((a) => (
              <ItemRow key={a.id} time={fmtTime(a.occurred_at)}>
                {a.group ? <b className="font-medium text-ink">{a.group}</b> : "Coding session"}
                {a.excerpt ? ` — ${a.excerpt}` : ""}
              </ItemRow>
            ))}
          </Section>
          <Section label="Captured" count={captured.length}>
            {captured.map((a) => (
              <ItemRow key={a.id} time={fmtTime(a.occurred_at)}>
                <span className="text-ink-3">{facetLabelFor(a.source_type)}</span>
                {a.excerpt ? ` — ${a.excerpt}` : a.group ? ` — ${a.group}` : ""}
              </ItemRow>
            ))}
          </Section>
          <Section label="Asks" count={state.asks.length}>
            {state.asks.map((a) => (
              <ItemRow key={a.id} time={fmtTime(a.created_at)}>
                &ldquo;{a.question}&rdquo;
                <span className="text-ink-4"> · {a.citation_count} citation{a.citation_count === 1 ? "" : "s"}</span>
              </ItemRow>
            ))}
          </Section>
          <Section label="Events" count={state.activity.length}>
            {state.activity.map((a) => (
              <ItemRow key={a.id} time={fmtTime(a.created_at)}>
                {a.label}
              </ItemRow>
            ))}
          </Section>

          <p className="font-mono text-[10px] text-ink-4">
            everything this hour · {shown} of {totalShown} shown
            {state.hasMore && (
              <>
                {" — "}
                <button
                  type="button"
                  onClick={() => void loadMore(state.nextCursor)}
                  disabled={state.loadingMore}
                  className="text-brand-2 underline decoration-dotted underline-offset-2 disabled:opacity-50"
                >
                  {state.loadingMore ? "loading…" : `load ${totalShown - shown} more`}
                </button>
              </>
            )}
            {" · caps affect prose, never this list"}
          </p>
        </>
      )}
      <button
        type="button"
        onClick={onCollapse}
        className="self-start font-mono text-[9.5px] uppercase tracking-[0.08em] text-ink-3 hover:text-brand-2"
      >
        collapse ↑
      </button>
    </div>
  );
}
