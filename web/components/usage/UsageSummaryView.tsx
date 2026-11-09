"use client";

import type { ReactNode } from "react";
import { Avatar, Dot, Empty, Meter, Mono, workspaceHue } from "@/components/ui";
import type { UsageSummary } from "@/lib/api";
import {
  axisLabel,
  byKindEmptyCopy,
  fmtCredits,
  fmtUsd,
  hasUnbilledUsage,
  isHonestlyEmpty,
  periodLabel,
  spendNote,
} from "@/lib/usageSummary";

/* Usage-metering PR12: the shared content behind the org Usage page, the org
   Billing page's spend section, and the personal Usage page — one renderer
   so the three surfaces can never quietly disagree about what a number
   means (the exact bug this PR exists to fix: the org Usage and Billing
   pages used to hardcode the SAME fictional "$186" independently).

   Every number rendered here comes straight off `UsageSummary` — this
   component does the only presentation math the server deliberately leaves
   to the client (percentages via `Meter`, currency/count formatting, hue
   assignment via `workspaceHue`), never re-derives a number the server already
   computed. All of that math now lives in `@/lib/usageSummary` so it can be
   tested without a DOM; this file is rendering only. Read that module's
   header for why — two production bugs survived a passing suite precisely
   because the decisions were inline here. */

export function UsageSummaryView({
  summary,
  showAskers = true,
}: {
  summary: UsageSummary;
  /** Hide the "busiest askers" section — always true on the org page
   *  (multi-user), false by default on the personal page (a single-user
   *  breakdown of yourself adds nothing). */
  showAskers?: boolean;
}) {
  if (isHonestlyEmpty(summary)) {
    return (
      <Empty
        title="Nothing metered here yet"
        hint="Synthesis and background extraction are the metered things — retrieval and embeddings are included. Once they run, spend shows up here for real, not as a placeholder."
      />
    );
  }

  const maxAxisCredits = Math.max(1, ...summary.spend.by_axis.map((a) => a.credits));
  const maxWorkspaceCost = Math.max(1, ...summary.by_workspace.map((w) => w.cost_usd));
  const maxAsks = Math.max(1, ...summary.top_askers.map((a) => a.asks));
  // Drives every disclosure below. One predicate, so the spend tile, the
  // by-kind panel and the workspace caveat can never tell three different
  // stories about the same screen.
  const unbilled = hasUnbilledUsage(summary);

  return (
    <>
      <div className="mb-[10px] grid grid-cols-1 gap-[12px] sm:grid-cols-3">
        <StatCard
          label="balance"
          value={fmtCredits(summary.balance_credits)}
          unit="credits"
          note={
            summary.credit_conversion_usd_per_credit != null
              ? `≈ ${fmtUsd(summary.balance_credits * summary.credit_conversion_usd_per_credit)}`
              : undefined
          }
        />
        <StatCard
          label="spend"
          value={fmtCredits(summary.spend.total_credits)}
          unit={`credits · ${periodLabel(summary.period_start)}`}
          note={spendNote(summary)}
        />
        <StatCard
          label="plan"
          value={summary.plan?.name ?? "—"}
          unit={summary.plan ? `${fmtCredits(summary.plan.credits)} credits · ${summary.plan.refresh_kind}` : "no plan assigned yet"}
          // Slot-priced plans only. `credits` above is already the total, so
          // this line explains where that total came from rather than
          // restating it — and it renders nothing at all for a flat plan
          // instead of a placeholder "1 slot" that nobody bought.
          note={
            summary.plan?.seats != null && summary.plan.credits_per_seat != null
              ? `${summary.plan.seats} ${summary.plan.seats === 1 ? "slot" : "slots"} × ${fmtCredits(summary.plan.credits_per_seat)} credits`
              : undefined
          }
          // No self-serve checkout exists yet (hand-sales only) — this is a
          // contact link, not a purchase button. Shown only when the account
          // isn't already on a paid or comped plan, so a Pro/Team/Founder
          // account is never told to upgrade past what it already has.
          action={
            summary.plan == null || summary.plan.code === "free" ? (
              <a
                href="mailto:person@example.com?subject=Upgrade%20my%20CoralSwarm%20plan"
                className="mt-[9px] inline-flex w-fit items-center justify-center gap-[5px] rounded-row border border-ink bg-ink px-[10px] py-[5px] text-[12.5px] font-medium text-white transition-colors hover:border-ink-2 hover:bg-ink-2"
              >
                Upgrade
              </a>
            ) : undefined
          }
        />
      </div>

      <div className="mb-[16px] grid grid-cols-1 gap-[12px] sm:grid-cols-2">
        <StatCard
          label="asks this period"
          value={summary.top_askers.reduce((n, a) => n + a.asks, 0).toLocaleString()}
          unit="questions"
        />
        <StatCard
          label="ingested this period"
          value={summary.atoms_ingested.toLocaleString()}
          unit="atoms captured"
        />
      </div>

      <div className="mb-[14px] grid grid-cols-1 gap-[14px] sm:grid-cols-2">
        <div>
          <Mono className="mb-[9px] block tracking-[0.13em]">spend by kind</Mono>
          <div className="flex flex-col gap-[10px] rounded-card border border-line p-[14px]">
            {summary.spend.by_axis.length === 0 ? (
              <span className="text-[12.5px] text-ink-3">{byKindEmptyCopy(summary)}</span>
            ) : (
              summary.spend.by_axis.map((a) => (
                <div key={a.axis} className="flex items-center gap-[10px]">
                  <span className="w-[130px] flex-none truncate text-[12.5px] font-medium text-ink">
                    {axisLabel(a.axis)}
                  </span>
                  <Meter value={a.credits} max={maxAxisCredits} className="flex-1" />
                  <span className="w-[110px] flex-none text-right font-mono text-[11px] text-ink-2">
                    {fmtCredits(a.credits)} cr · {a.count.toLocaleString()}×
                  </span>
                </div>
              ))
            )}
          </div>
        </div>
        <div>
          {/* The provenance tag is mono per the design system's voice rule —
              it says where this column's dollars come from, and they are NOT
              the same kind of number as the ledger-derived tiles above:
              `by_workspace` prices raw `llm_usage` tokens, because ledger rows
              on the job/query axes carry no graph_id to group by. */}
          <div className="mb-[9px] flex items-baseline gap-[9px]">
            <Mono className="tracking-[0.13em]">by workspace</Mono>
            <Mono className="ml-auto text-ink-4">estimated model cost</Mono>
          </div>
          <div className="flex flex-col gap-[10px] rounded-card border border-line p-[14px]">
            {summary.by_workspace.length === 0 ? (
              <span className="text-[12.5px] text-ink-3">No metered activity this period.</span>
            ) : (
              <>
                {summary.by_workspace.map((w) => (
                  <div key={w.graph_id ?? "unattributed"} className="flex items-center gap-[10px]">
                    {w.unattributed ? (
                      <span className="h-[7px] w-[7px] flex-none rounded-pill bg-ink-4" />
                    ) : (
                      <Dot hue={workspaceHue(w.graph_id)} size={7} />
                    )}
                    <span className="min-w-0 flex-1 truncate text-[12.5px] text-ink">
                      {w.unattributed ? "unattributed" : (w.name ?? "deleted workspace")}
                    </span>
                    <Meter value={w.cost_usd} max={maxWorkspaceCost} className="w-[80px] flex-none" />
                    <span className="w-[64px] flex-none text-right font-mono text-[11px] text-ink-2">
                      {fmtUsd(w.cost_usd)}
                    </span>
                  </div>
                ))}
                {unbilled && (
                  <p className="border-t border-line-3 pt-[9px] text-[11px] text-ink-4">
                    Estimated from raw usage over the whole month — not what you were billed.
                  </p>
                )}
              </>
            )}
          </div>
        </div>
      </div>

      {showAskers && (
        <>
          <div className="mb-[9px] flex items-center gap-[9px]">
            <Mono className="tracking-[0.13em]">busiest askers</Mono>
            <Mono className="ml-auto text-ink-4">{periodLabel(summary.period_start)}</Mono>
          </div>
          {summary.top_askers.length === 0 ? (
            <div className="rounded-card border border-line px-[15px] py-[12px] text-[12.5px] text-ink-3">
              No questions asked this period.
            </div>
          ) : (
            <div className="overflow-hidden rounded-card border border-line">
              {summary.top_askers.map((a) => (
                <div
                  key={a.user_id ?? "unattributed"}
                  className="flex items-center gap-[12px] border-b border-line-3 px-[15px] py-[10px] last:border-b-0"
                >
                  <Avatar name={a.email ?? "system"} size={24} />
                  <span className="min-w-0 flex-1 truncate text-[13px] text-ink">
                    {a.email ?? "unattributed (no acting user)"}
                  </span>
                  <Meter value={a.asks} max={maxAsks} className="w-[150px] flex-none" />
                  <span className="w-[66px] flex-none text-right font-mono text-[11px] text-ink-2">
                    {a.asks.toLocaleString()} asks
                  </span>
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </>
  );
}

function StatCard({
  label,
  value,
  unit,
  note,
  action,
}: {
  label: string;
  value: string;
  unit: string;
  note?: string;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-col rounded-card border border-line px-[14px] py-[13px]">
      <Mono className="mb-[6px] tracking-[0.1em]">{label}</Mono>
      <div className="mb-[4px] flex items-baseline gap-[6px]">
        <span className="truncate font-mono text-[21px] font-medium text-ink">{value}</span>
      </div>
      <div className="text-[12px] text-ink-3">{unit}</div>
      {note && <div className="mt-[2px] text-[11px] text-ink-4">{note}</div>}
      {action}
    </div>
  );
}
