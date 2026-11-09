"use client";

/* Shared per-ocean progress cards — the promoting panel inside
 * `PromoteModal` and the expanded header pill (`PromoteJobsIndicator`)
 * draw the same card so a job you closed out of the modal and a job you
 * left open never disagree about counts, stages, or status words.
 *
 * Every label here is a tested function in lib/promoteJob.ts; this file
 * is only how those answers are drawn. */

import { Meter } from "@/components/ui";
import {
  PROCESSING_STAGE_ORDER,
  legPhaseWord,
  legProgressPct,
  legStatusLabel,
  processingStageLine,
  promoteLegHeading,
  succeededCoralLabel,
  type PromoteLeg,
} from "@/lib/promoteJob";

export function PromoteLegCard({
  oceanName,
  itemTitle,
  leg,
}: {
  oceanName: string;
  itemTitle?: string;
  leg: PromoteLeg | null;
}) {
  const pct = leg?.phase === "job" ? legProgressPct(leg) : null;
  return (
    <div className="rounded-card border border-line-2 bg-bg px-[13px] py-[10px]">
      <div className="mb-[6px] flex items-center justify-between gap-[8px]">
        <span className="min-w-0 truncate text-[12.5px] font-medium text-ink">
          {promoteLegHeading(oceanName, itemTitle)}
        </span>
        <span className="flex-none font-mono text-[10.5px] text-ink-3">{legPhaseWord(leg)}</span>
      </div>
      {(leg === null || leg.phase === "job") && (
        <>
          {pct !== null ? (
            <Meter value={pct} max={100} />
          ) : (
            <div className="h-[6px] w-full overflow-hidden rounded-pill bg-bg-hover" aria-hidden>
              <div className="h-full w-2/5 animate-pulse rounded-pill bg-brand" />
            </div>
          )}
          <div className="mt-[5px] font-mono text-[10px] text-ink-3">
            {leg?.phase === "job" ? legStatusLabel(leg) : "starting…"}
          </div>
          {leg?.phase === "job" && leg.processing && (
            <ol className="mt-2 space-y-1 text-[11px] text-ink-3">
              {PROCESSING_STAGE_ORDER.map((name) => {
                const stage = leg.processing?.stages.find((s) => s.stage === name);
                return stage ? <li key={name}>{processingStageLine(stage)}</li> : null;
              })}
            </ol>
          )}
        </>
      )}
      {leg?.phase === "succeeded" && (
        <p className="font-mono text-[10px] text-ink-3">{succeededCoralLabel(leg.coralsRequested)}</p>
      )}
      {leg?.phase === "failed" && leg.error && <p className="font-mono text-[10px] text-bad">{leg.error}</p>}
    </div>
  );
}

export function PromoteProgressList({
  rows,
}: {
  rows: { oceanId: string; oceanName: string; itemTitle?: string; leg: PromoteLeg | null }[];
}) {
  return (
    <div className="flex flex-col gap-[8px]">
      {rows.map(({ oceanId, oceanName, itemTitle, leg }) => (
        <PromoteLegCard
          key={`${oceanId}:${leg?.itemId ?? (leg?.phase === "job" ? leg.jobId : "")}`}
          oceanName={oceanName}
          itemTitle={itemTitle}
          leg={leg}
        />
      ))}
    </div>
  );
}
