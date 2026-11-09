import type { UsageSummary } from "./api";

/* Presentation logic for `UsageSummaryView` — the org Usage page, the org
   Billing page's spend section, and the personal Usage page.

   It lives here rather than in the component for the reason the two bugs this
   module fixes were found by looking at production instead of by running the
   suite: every individual number on that screen was correct and the screen was
   still wrong. What was untested was the RELATIONSHIP between the numbers, and
   a relationship is only testable if the decision that expresses it is a pure
   function. `usageSummary.test.ts` asserts those relationships directly; there
   is no DOM harness in this app (every test under `web/lib` is pure logic), so
   anything left inline in the .tsx is, structurally, untestable.

   The two bugs, both live on production 2026-08-13:

   1. The period label read "July 2026" in August, because `toLocaleDateString`
      renders in the viewer's zone and `2026-08-01T00:00:00Z` is 31 July for
      everyone west of Greenwich. See `periodLabel`.
   2. The page contradicted itself: `spend $0.00` and "No metered activity this
      period" beside `by workspace: Primary ocean $155.79`. See
      `ledgerCoverage`. */

/** The reporting window's own timezone. `backend/src/routes/usage.rs`'s
 *  `month_start` truncates to a UTC calendar month and the org Usage page's
 *  lead promises "Month to date, UTC" — so every label derived from
 *  `period_start` has to be rendered in UTC to keep that promise. Omitting
 *  this is the whole of bug 1: the instant is right, the label was localised
 *  to a zone the window was never defined in. */
const PERIOD_TZ = "UTC";

export function fmtCredits(n: number): string {
  return n.toLocaleString(undefined, { maximumFractionDigits: n !== 0 && Math.abs(n) < 10 ? 2 : 0 });
}

export function fmtUsd(n: number): string {
  return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

const AXIS_LABEL: Record<string, string> = {
  usage_job: "background extraction",
  usage_query: "questions asked",
  usage_journal: "journal compiles",
};

export function axisLabel(axis: string): string {
  return AXIS_LABEL[axis] ?? axis.replace(/^usage_/, "").replace(/_/g, " ");
}

/** "August 2026" — the month `period_start` falls in, in UTC. */
export function periodLabel(startIso: string): string {
  const d = new Date(startIso);
  if (Number.isNaN(d.getTime())) return "this period";
  return d.toLocaleDateString(undefined, { month: "long", year: "numeric", timeZone: PERIOD_TZ });
}

/** "13 Aug" — a day marker for provenance lines, same UTC rule as above. */
export function shortUtcDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "an earlier date";
  return d.toLocaleDateString(undefined, { day: "numeric", month: "short", timeZone: PERIOD_TZ });
}

/** How much of the reporting window `credit_ledger` actually accounts for.
 *
 *  The tiles read the ledger; `by_workspace`/`top_askers` read `llm_usage`/
 *  `query_history`, which reach back to the beginning of the content plane.
 *  Both are windowed to the same `period_start`, but the ledger began at
 *  deploy with no backfill, so in the first month it covers only the tail of
 *  the window — which is why a true `spend: 0` could sit beside a true
 *  `$155.79`. This is the fact that reconciles them. */
export type LedgerCoverage =
  /** The ledger reaches back to `period_start` (or earlier): every number on
   *  the screen describes the same window, no disclosure needed. From the
   *  second month of metering onward this is the only case. */
  | { kind: "full" }
  /** The ledger starts partway through the window — spend before `startsAt`
   *  was never billed. */
  | { kind: "partial"; startsAt: string }
  /** This account has never had a ledger row at all. */
  | { kind: "none" };

export function ledgerCoverage(
  summary: Pick<UsageSummary, "period_start" | "ledger_coverage_start">,
): LedgerCoverage {
  const startsAt = summary.ledger_coverage_start;
  if (!startsAt) return { kind: "none" };
  const floor = new Date(startsAt).getTime();
  const windowStart = new Date(summary.period_start).getTime();
  // An unparseable timestamp is treated as "no coverage claim we can stand
  // behind" rather than silently as full coverage — defaulting to `full` would
  // suppress the disclosure, i.e. reproduce the bug.
  if (Number.isNaN(floor) || Number.isNaN(windowStart)) return { kind: "none" };
  return floor <= windowStart ? { kind: "full" } : { kind: "partial", startsAt };
}

/** Estimated model cost this period, summed across workspaces — the
 *  `llm_usage`-derived figure, NOT a ledger total. */
export function rawUsageUsd(summary: Pick<UsageSummary, "by_workspace">): number {
  return summary.by_workspace.reduce((n, w) => n + w.cost_usd, 0);
}

/** True when this period contains real model spend the ledger does not
 *  account for — the exact condition under which the screen would otherwise
 *  contradict itself, and therefore the condition that MUST produce a
 *  disclosure rather than a bare "$0.00". Every copy function below keys off
 *  this one predicate so they cannot drift apart from each other. */
export function hasUnbilledUsage(
  summary: Pick<UsageSummary, "period_start" | "ledger_coverage_start" | "by_workspace">,
): boolean {
  return rawUsageUsd(summary) > 0 && ledgerCoverage(summary).kind !== "full";
}

/** The note under the `spend` tile. Always states the billed dollar figure;
 *  adds where the ledger starts when it doesn't cover the whole window, so a
 *  zero is never left to read as "nothing happened". */
export function spendNote(
  summary: Pick<UsageSummary, "period_start" | "ledger_coverage_start" | "by_workspace" | "spend">,
): string {
  const usd = fmtUsd(summary.spend.total_cost_usd);
  if (!hasUnbilledUsage(summary)) return usd;
  const coverage = ledgerCoverage(summary);
  return coverage.kind === "partial"
    ? `${usd} billed · ledger from ${shortUtcDate(coverage.startsAt)}`
    : `${usd} billed · ledger not started`;
}

/** What the "spend by kind" panel says when it has no rows. Unchanged when
 *  there is genuinely nothing to report; when raw usage exists, it has to
 *  explain the gap instead of asserting that nothing happened. */
export function byKindEmptyCopy(
  summary: Pick<UsageSummary, "period_start" | "ledger_coverage_start" | "by_workspace">,
): string {
  if (!hasUnbilledUsage(summary)) return "No metered activity this period.";
  const coverage = ledgerCoverage(summary);
  return coverage.kind === "partial"
    ? `No billed activity this period. The ledger begins ${shortUtcDate(coverage.startsAt)}; the workspace figures cover the whole month.`
    : "Nothing billed yet. The workspace figures are estimated model cost, not charges.";
}

/** Whether to render the honest empty state instead of a dashboard of zeroes.
 *
 *  Note that raw usage keeps this false: an account with `by_workspace` rows
 *  has something real to show, so it gets the dashboard PLUS the disclosure
 *  above — hiding it behind an empty state would drop real spend from view,
 *  which is the same under-reporting the unattributed bucket exists to
 *  prevent. */
export function isHonestlyEmpty(summary: UsageSummary): boolean {
  return (
    !summary.has_ledger_history &&
    summary.spend.total_credits === 0 &&
    summary.by_workspace.length === 0 &&
    summary.balance_credits === 0
  );
}
