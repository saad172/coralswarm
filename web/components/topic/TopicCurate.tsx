"use client";

import { useState, type ReactNode } from "react";
import { Eyebrow } from "@/components/ui";

/* Curation tools, collapsed by default.
 *
 * A topic page is read far more often than it is curated. The mock (2b) shows
 * only how the topic was reached — timeline, facts, source mix, citations —
 * but four curation surfaces were ported alongside it from the archive
 * (supersession, suggested corals, promote-a-source, merge-a-duplicate) and
 * stacked into the right rail. Together they made the rail about twice the
 * height of the timeline, so the page always scrolled and always left a tall
 * empty gutter beside the rail.
 *
 * They are real features and stay one click away, but they are not what the
 * page is for. Behind this disclosure the default view fits a viewport.
 *
 * Open state is per-visit and deliberately not persisted: someone who curates
 * one topic is usually not curating the next one they open.
 *
 * `onOpen` (added alongside the topics-page ocean-derivation fix): the two
 * always-collapsed consumers below (`TopicSupersession`, `TopicMerge`) need
 * the FULL reef roster for their target picker, which is exactly the drain
 * this disclosure exists to defer — closed by default means it was never
 * needed on load. Firing a callback the first time this opens lets the
 * caller fetch that roster lazily, on the click, instead of eagerly on every
 * page load whether or not anyone ever opens this section. */

export function TopicCurate({ children, onOpen }: { children: ReactNode; onOpen?: () => void }) {
  const [open, setOpen] = useState(false);

  return (
    <div className="mt-[24px] border-t border-line-2 pt-[16px]">
      <button
        type="button"
        onClick={() => {
          const next = !open;
          setOpen(next);
          if (next) onOpen?.();
        }}
        aria-expanded={open}
        className="group flex w-full items-center gap-[8px] text-left"
      >
        <Eyebrow className="transition-colors group-hover:text-ink">curate this topic</Eyebrow>
        <span className="h-px flex-1 bg-line-2" />
        <span className="flex-none font-mono text-[10px] text-ink-3 transition-colors group-hover:text-ink">
          {open ? "hide ↑" : "supersession · suggestions · promote · merge ↓"}
        </span>
      </button>

      {open && (
        <div className="mt-[16px] grid grid-cols-1 items-start gap-x-[28px] gap-y-[22px] lg:grid-cols-2">
          {children}
        </div>
      )}
    </div>
  );
}
