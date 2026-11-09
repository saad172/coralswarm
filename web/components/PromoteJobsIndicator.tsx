"use client";

/* The persistent "promoting in the background" indicator — the passive
 * signal that didn't exist before this: closing `PromoteModal` (or
 * navigating off the page it's on) used to leave literally no trace
 * anywhere in the UI that a promote was still running, or any sign at all
 * once it finished. Mounted once in `AppShell`'s header (every page under
 * `(app)/layout.tsx`), reading the SAME shared state
 * (`lib/promoteJobsProvider.tsx`) every `PromoteModal` mount does — so this
 * is never a stale snapshot, it's the live, currently-polled batch.
 *
 * The collapsed pill is always an expand control — one batch or several.
 * Expanding opens a popover of the same per-ocean progress cards the
 * Promote modal draws (`PromoteProgressList`), so closing the modal never
 * loses the live count, meter, or processing stages. Clicking a finished
 * row still navigates to the DESTINATION topic when there is exactly one
 * (see `batchLinkTargetNodeId`); a still-`promoting` or `error` row, or
 * one whose batch promoted to more than one ocean, stays unlinked.
 *
 * Per this project's convention (no component-render harness — see
 * PromoteModal.tsx's file comment and AccountIndicator.tsx's): every
 * decision (label text, phase, progress, dismiss timing) is a tested pure
 * function in lib/promoteJob.ts; this file is only how those answers are
 * drawn, covered by typecheck + the production build.
 */

import { useRef, useState } from "react";
import Link from "next/link";
import { Meter, Popover } from "@/components/ui";
import { cn } from "@/components/ui/cn";
import { IconChevron } from "@/components/icons";
import { PromoteProgressList } from "./PromoteProgressList";
import {
  batchLinkTargetNodeId,
  batchPhase,
  batchProgressPct,
  indicatorExpandAriaLabel,
  indicatorSummaryPhase,
  indicatorTriggerLabel,
  promoteCardItemTitle,
  visibleBatches,
  type BatchPhase,
  type PromoteBatch,
} from "@/lib/promoteJob";
import { usePromoteJobs } from "@/lib/promoteJobsProvider";

/** Status dot colour per phase — the design system's status tokens
 *  (`--ok`/`--bad`), not one of the five taxonomy hues: this is reporting a
 *  STATE, not labelling a kind of content, which is what the hue set is
 *  for. "promoting" uses `--brand` (the one hue the skill reserves for
 *  actions/emphasis) and pulses, since it's the only phase still active. */
function PhaseDot({ phase }: { phase: BatchPhase }) {
  const color = phase === "done" ? "var(--ok)" : phase === "error" ? "var(--bad)" : "var(--brand)";
  return (
    <span
      aria-hidden
      className={cn("h-[6px] w-[6px] flex-none rounded-pill", phase === "promoting" && "animate-pulse")}
      style={{ background: color }}
    />
  );
}

/** One dismiss ("×") control — only ever shown for a terminal (done/error)
 *  batch; a "promoting" one has nothing to dismiss (there's no acknowledged
 *  outcome yet). Stops propagation so it never also triggers a parent
 *  `<Link>` or the pill's expand toggle. */
function DismissButton({ onDismiss, label }: { onDismiss: () => void; label: string }) {
  return (
    <button
      type="button"
      aria-label={label}
      onClick={(e) => {
        e.preventDefault();
        e.stopPropagation();
        onDismiss();
      }}
      className="flex-none rounded-badge px-[4px] text-[11px] text-ink-3 transition-colors hover:text-ink"
    >
      ×
    </button>
  );
}

/** Expanded body for one batch — topic title, optional destination link,
 *  and the live per-ocean progress cards. */
function ExpandedBatch({ batch, onDismiss }: { batch: PromoteBatch; onDismiss: () => void }) {
  const phase = batchPhase(batch);
  const targetNodeId = batchLinkTargetNodeId(batch);
  return (
    <div className="flex flex-col gap-[8px]">
      <div className="flex items-start justify-between gap-[8px] px-[2px]">
        <div className="min-w-0">
          <p className="truncate font-display text-[14px] italic leading-[1.3] text-ink">{batch.reefTitle}</p>
          {targetNodeId ? (
            <Link
              href={`/topics?id=${encodeURIComponent(targetNodeId)}`}
              aria-label={`Open topic for ${batch.reefTitle}`}
              className="mt-[2px] inline-block font-mono text-[10.5px] text-brand-2 transition-colors hover:text-brand"
            >
              Open topic
            </Link>
          ) : phase === "promoting" ? (
            <p className="mt-[2px] font-mono text-[10.5px] text-ink-3">copying into the chosen workspace</p>
          ) : null}
        </div>
        {phase !== "promoting" && (
          <DismissButton
            onDismiss={onDismiss}
            label={`Dismiss ${phase === "done" ? "promoted" : "promote failed"} notice for ${batch.reefTitle}`}
          />
        )}
      </div>
      <PromoteProgressList
        rows={batch.legs.map((leg) => ({
          oceanId: leg.oceanId,
          oceanName: leg.oceanName,
          itemTitle: promoteCardItemTitle(batch.reefTitle, leg.itemTitle),
          leg,
        }))}
      />
    </div>
  );
}

const PILL =
  "inline-flex min-w-0 items-center gap-[7px] rounded-pill border border-line bg-bg py-[3px] pl-[8px] pr-[7px] text-[11px]";

export default function PromoteJobsIndicator({ className }: { className?: string }) {
  const { batches, dismissBatch } = usePromoteJobs();
  const visible = visibleBatches(batches);
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);

  if (visible.length === 0) return null;

  const single = visible.length === 1 ? visible[0] : null;
  const singlePhase = single ? batchPhase(single) : null;
  const pct = single && singlePhase === "promoting" ? batchProgressPct(single) : null;

  return (
    <div className={cn("relative min-w-0", className)}>
      <span className={cn(PILL, "max-w-[300px] transition-colors hover:border-ink-3")}>
        <button
          ref={triggerRef}
          type="button"
          aria-expanded={open}
          aria-label={indicatorExpandAriaLabel(visible)}
          onClick={() => setOpen((v) => !v)}
          className="flex min-w-0 flex-1 items-center gap-[7px] rounded-pill text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40"
        >
          <PhaseDot phase={indicatorSummaryPhase(visible)} />
          <span className="min-w-0 flex-1 truncate font-mono text-[11px] text-ink-2">{indicatorTriggerLabel(visible)}</span>
          {pct !== null && <Meter value={pct} max={100} className="w-[46px] flex-none" />}
          <IconChevron
            width={10}
            height={10}
            aria-hidden
            className={cn("flex-none text-ink-3 transition-transform", open ? "rotate-0" : "-rotate-90")}
          />
        </button>
        {single && singlePhase !== "promoting" && !open && (
          <DismissButton
            onDismiss={() => dismissBatch(single.reefNodeId)}
            label={`Dismiss ${singlePhase === "done" ? "promoted" : "promote failed"} notice for ${single.reefTitle}`}
          />
        )}
      </span>
      <Popover
        open={open}
        onClose={() => setOpen(false)}
        triggerRef={triggerRef}
        align="right"
        className="max-h-[min(420px,70vh)] w-[360px] p-[12px]"
      >
        <div className="flex flex-col gap-[14px]">
          {visible.map((b) => (
            <ExpandedBatch key={b.reefNodeId} batch={b} onDismiss={() => dismissBatch(b.reefNodeId)} />
          ))}
        </div>
      </Popover>
    </div>
  );
}
