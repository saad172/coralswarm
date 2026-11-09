"use client";

import { useState } from "react";
import Link from "next/link";
import { useQueries } from "@tanstack/react-query";
import { Eyebrow, Button, hueVar } from "@/components/ui";
import type { SourceGroupDetail } from "@/lib/api";
import { evidenceHref } from "@/lib/provenance";
import { fmtDate, sourceCoralLine, visibleSlice } from "@/lib/coral";
import { SourceGlyph, sourceMeta } from "./sourceKind";
import { channelName } from "./sourceGroups";
import { TOPIC_META } from "@/components/topic/topicMeta";
import { sourceGroupQueryOptions } from "@/lib/libraryQueries";

/* Right-hand detail pane for a selected source group (perf/library-
   source-groups): stats, the topics it settled into (`detail.reefs` — the
   backend already titled/typed these, no node↔atom edge lookup here), a
   promote-the-source action, and its corals, newest first. Ported from
   archive's RawSourcesMode preview pane; the caller (`library/sources/
   page.tsx`) fetches page 1 via `sourceGroupQueryOptions` and passes the
   resolved `detail` down — this component only reaches for MORE pages
   itself, once the user expands past page 1's corals. */
export function SourceDetail({
  detail,
  getAuthToken,
  graphId,
  canPromote,
  onPromote,
}: {
  detail: SourceGroupDetail;
  getAuthToken: () => string | null;
  graphId: string;
  canPromote: boolean;
  onPromote: () => void;
}) {
  const m = sourceMeta(detail.source_type);
  const label = channelName(detail);
  const stats: [string, number | string][] = [
    ["items", detail.coral_count],
    ["topics", detail.reef_count],
    ["ingests", detail.instance_count],
    ["last", fmtDate(detail.last_ts) || "—"],
  ];
  const [expanded, setExpanded] = useState(false);

  // Pages beyond the first (already in `detail.corals`, supplied by the
  // caller's own `sourceGroupQueryOptions({ page: 1 })`) — "Load more" fetches
  // one more via the SAME query options, page 2, 3, … stacked with
  // `useQueries` rather than a second fetch mechanism, so a page already
  // loaded for this group stays cached across a collapse/re-expand or a
  // group re-select within the 60s staleness window.
  const [morePages, setMorePages] = useState(0);
  const extra = useQueries({
    queries: Array.from({ length: morePages }, (_, i) =>
      sourceGroupQueryOptions({
        getAuthToken,
        enabled: true,
        graphId,
        channelKey: detail.channel_key,
        page: i + 2,
      }),
    ),
  });
  const loadingMore = extra.length > 0 && extra[extra.length - 1].isPending;
  const hasMore = extra.length > 0 ? (extra[extra.length - 1].data?.has_more ?? true) : detail.has_more;

  // Every loaded page's corals, in order — each page is already
  // newest-first server-side, and page 2 continues page 1's ordering, so no
  // client re-sort is needed (unlike the old client-side channel fold this
  // replaces, which DID need one to merge several source instances' corals).
  const allCorals = [...detail.corals, ...extra.flatMap((q) => q.data?.corals ?? [])];
  const latest = visibleSlice(allCorals, expanded);
  const remaining = allCorals.length - latest.length;

  return (
    <div className="overflow-hidden rounded-card border border-line bg-bg shadow-card">
      <div
        className="flex items-center gap-3 border-b border-line-2 px-[18px] py-[16px]"
        style={{ borderTop: `3px solid ${hueVar(m.hue)}` }}
      >
        <span
          className="grid h-[38px] w-[38px] flex-none place-items-center rounded-card"
          style={{ background: `color-mix(in srgb, ${hueVar(m.hue)} 16%, transparent)`, color: hueVar(m.hue) }}
        >
          <SourceGlyph kind={m.icon} size={19} />
        </span>
        <div className="min-w-0 flex-1">
          <div className="truncate font-display text-[19px] leading-[1.15] text-ink">{label}</div>
          <div className="mt-[3px] font-mono text-[10.5px] uppercase tracking-[0.08em] text-ink-3">{m.label}</div>
        </div>
      </div>

      <div className="px-[18px] py-[16px]">
        <div className="mb-[16px] grid grid-cols-4 gap-[10px]">
          {stats.map(([k, v]) => (
            <div key={k} className="rounded-control bg-bg-2 px-[10px] py-[9px]">
              <div className="font-display text-[19px] leading-none text-ink">{v}</div>
              <div className="mt-[5px] font-mono text-[9px] uppercase tracking-[0.08em] text-ink-3">{k}</div>
            </div>
          ))}
        </div>

        <Eyebrow className="mb-[8px]">settled into · {detail.reefs.length}</Eyebrow>
        <div className="mb-[16px] flex flex-wrap gap-[6px]">
          {detail.reefs.length === 0 ? (
            <span className="text-[12.5px] text-ink-3">Not settled into a topic yet.</span>
          ) : (
            detail.reefs.map((t) => {
              const tm = TOPIC_META[t.type];
              return (
                <Link
                  key={t.node_id}
                  href={`/topics?id=${t.node_id}`}
                  className="inline-flex items-center gap-[5px] rounded-pill px-[9px] py-[3px] font-mono text-[10px] transition-opacity hover:opacity-80"
                  style={{ color: hueVar(tm.hue), background: `color-mix(in srgb, ${hueVar(tm.hue)} 16%, transparent)` }}
                >
                  {tm.glyph} {t.title}
                </Link>
              );
            })
          )}
        </div>

        {canPromote && (
          <div className="mb-[16px] rounded-control border border-line px-[14px] py-[13px]">
            <p className="mb-[10px] text-[13px] leading-[1.5] text-ink-2">
              Publish every topic grown from this source into a shared workspace.{" "}
              <span className="font-mono text-[11px] text-ink-3">copy-on-promote · widening only</span>
            </p>
            <Button size="sm" onClick={onPromote}>
              promote →
            </Button>
          </div>
        )}

        <Eyebrow className="mb-[8px]">items · {detail.coral_count}</Eyebrow>
        <div className="flex flex-col gap-[6px]">
          {latest.length === 0 && <div className="text-[12.5px] text-ink-3">No items under this source yet.</div>}
          {latest.map((c) => {
            const { title, body } = sourceCoralLine(c);
            return (
              <Link key={c.atom_id} href={evidenceHref(c.atom_id, graphId)} className="flex items-start gap-[9px] rounded-control border border-line bg-bg px-[11px] py-[9px] transition-colors hover:bg-bg-hover focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40">
                <span className="mt-[2px] font-mono text-[10px]" style={{ color: hueVar(m.hue) }}>
                  ▸
                </span>
                <div className="min-w-0 flex-1">
                  <div className="truncate text-[12.5px] leading-[1.4] text-ink">{title}</div>
                  {body && (
                    <div className="mt-[2px] line-clamp-3 text-[12px] leading-[1.4] text-ink-2">{body}</div>
                  )}
                  <div className="mt-[3px] font-mono text-[10px] text-ink-3">{fmtDate(c.timestamp)}</div>
                </div>
              </Link>
            );
          })}
        </div>
        {remaining > 0 && (
          <button
            type="button"
            onClick={() => setExpanded(true)}
            className="mt-[10px] inline-flex rounded-pill border border-line bg-bg px-[14px] py-[6px] font-mono text-[11px] text-ink-2 transition-colors hover:border-ink-3"
          >
            Show all {detail.coral_count} ↓
          </button>
        )}
        {expanded && hasMore && (
          <button
            type="button"
            disabled={loadingMore}
            onClick={() => setMorePages((n) => n + 1)}
            className="mt-[10px] inline-flex rounded-pill border border-line bg-bg px-[14px] py-[6px] font-mono text-[11px] text-ink-2 transition-colors hover:border-ink-3 disabled:opacity-60"
          >
            {loadingMore ? "Loading…" : "Load more ↓"}
          </button>
        )}
      </div>
    </div>
  );
}
