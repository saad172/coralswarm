"use client";

import { useState } from "react";
import { Eyebrow } from "@/components/ui";
import type { PromoteTarget } from "@/components/PromoteModal";

/* Source-level promotion (ADR-0006/0007 D8): promote every topic grown from
   ONE source instance — the whole meeting or thread — into a shared workspace
   at once, instead of topic-by-topic. Same one-way, copy-on-promote, widen-
   only boundary as the topic-level promote button in the header. Ported from
   archive's SourcePromoteControl. */
/** Same collapsed count as the candidates list, so the two sections below
 *  the fold read as one rhythm rather than two arbitrary limits. */
const COLLAPSED = 4;

export function TopicPromoteSources({
  graphId,
  sources,
  hasTargets,
  onPromote,
}: {
  graphId: string | undefined;
  sources: { id: string; label: string; count: number }[];
  hasTargets: boolean;
  onPromote: (target: PromoteTarget) => void;
}) {
  const [expanded, setExpanded] = useState(false);

  if (!hasTargets || sources.length === 0 || !graphId) return null;

  // Capped like the candidates list above it. A topic grown from a couple of
  // dozen notes listed a couple of dozen rows here, each reading "note ·
  // Jul 18, 2026" — a wall of near-identical lines that made the rail far
  // taller than the timeline it sits beside.
  const visible = expanded ? sources : sources.slice(0, COLLAPSED);
  const more = sources.length - visible.length;

  return (
    <div>
      <div className="mb-[3px] flex items-center gap-[8px]">
        <Eyebrow>promote a whole source</Eyebrow>
        <span className="h-px flex-1 bg-line-2" />
        <span className="font-mono text-[9.5px] text-ink-4">{sources.length}</span>
      </div>
      <p className="mb-[9px] text-[11.5px] leading-[1.5] text-ink-3">
        Publish every topic grown from one source — a whole meeting or thread — into a shared
        workspace at once. Copies, and only ever widens who can see it.
      </p>

      <div className="flex flex-col gap-[1px]">
        {visible.map((s) => (
          <div
            key={s.id}
            className="flex items-center gap-[8px] rounded-row px-[8px] py-[5px] transition-colors hover:bg-bg-hover"
          >
            <span className="min-w-0 flex-1 truncate font-mono text-[10.5px] text-ink-2" title={s.label}>
              {s.label}
            </span>
            <span className="flex-none font-mono text-[9.5px] text-ink-4">
              {s.count} item{s.count === 1 ? "" : "s"}
            </span>
            <button
              type="button"
              onClick={() => onPromote({ kind: "sources", sources: [s], graphId })}
              className="flex-none rounded-badge px-[6px] py-[1px] text-[11px] text-ink-3 transition-colors hover:bg-ink hover:text-white"
            >
              promote
            </button>
          </div>
        ))}
      </div>

      {more > 0 && (
        <button
          type="button"
          onClick={() => setExpanded(true)}
          className="mt-[6px] font-mono text-[10px] text-ink-3 transition-colors hover:text-ink"
        >
          show {more} more ↓
        </button>
      )}
    </div>
  );
}
