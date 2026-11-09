import { afterAll, describe, expect, it } from "vitest";
import type { UsageSummary } from "./api";
import {
  byKindEmptyCopy,
  hasUnbilledUsage,
  isHonestlyEmpty,
  ledgerCoverage,
  periodLabel,
  rawUsageUsd,
  shortUtcDate,
  spendNote,
} from "./usageSummary";

/* Both bugs these tests pin were live on production while the PR12 suite was
   green, because every assertion in it was about ONE number at a time. These
   are deliberately about the relationships instead: the label vs. the window
   it labels, and the tiles vs. the breakdown beside them. */

const NO_METERED_ACTIVITY = "No metered activity this period.";

/** August 2026, the month both production bugs were observed in. */
function summary(over: Partial<UsageSummary> = {}): UsageSummary {
  return {
    period_start: "2026-08-01T00:00:00Z",
    period_end: "2026-08-13T21:00:00Z",
    balance_credits: 0,
    plan: null,
    credit_conversion_usd_per_credit: 0.022,
    spend: { total_credits: 0, total_cost_usd: 0, by_axis: [] },
    by_workspace: [],
    top_askers: [],
    atoms_ingested: 0,
    has_ledger_history: false,
    ledger_coverage_start: null,
    ...over,
  };
}

/** The production shape: real spend on `llm_usage`, nothing on the ledger. */
function withRawUsage(over: Partial<UsageSummary> = {}): UsageSummary {
  return summary({
    by_workspace: [
      { graph_id: "11111111-1111-4111-8111-111111111111", name: "Primary ocean", cost_usd: 155.79, unattributed: false },
      { graph_id: null, name: null, cost_usd: 0.16, unattributed: true },
    ],
    ...over,
  });
}

describe("periodLabel — bug 2: the window is UTC, so the label must be too", () => {
  const ORIGINAL_TZ = process.env.TZ;
  afterAll(() => {
    // `process.env.TZ = undefined` assigns the STRING "undefined", which is
    // not a valid zone — delete instead when it was never set, so this block
    // can't leave a later test file running in a broken zone.
    if (ORIGINAL_TZ === undefined) delete process.env.TZ;
    else process.env.TZ = ORIGINAL_TZ;
  });

  /** Without this the suite proves nothing on a UTC CI runner: the bug is
   *  invisible there, so an assertion that only runs in the runner's own zone
   *  would pass with or without the fix. Failing loudly beats passing
   *  vacuously — the whole lesson of this train. */
  function forceTz(tz: string) {
    process.env.TZ = tz;
    const observed = new Date("2026-08-01T00:00:00Z").getHours();
    return { observed };
  }

  it("renders the month of period_start, not the viewer's local month", () => {
    // America/Chicago is where this was found: 2026-08-01T00:00:00Z is
    // 2026-07-31 19:00 there, and the page read "credits · July 2026" on the
    // 13th of August.
    const { observed } = forceTz("America/Chicago");
    expect(observed, "TZ override had no effect — this test cannot detect the bug it exists for").toBe(19);
    expect(periodLabel("2026-08-01T00:00:00Z")).toBe("August 2026");
  });

  it("renders the same month in a zone east of Greenwich", () => {
    // The mirror case: Asia/Tokyo is +9, so a naive local render lands in the
    // right month here and the wrong one in Chicago. Only a UTC render is
    // correct in both.
    const { observed } = forceTz("Asia/Tokyo");
    expect(observed, "TZ override had no effect").toBe(9);
    expect(periodLabel("2026-08-01T00:00:00Z")).toBe("August 2026");
  });

  it("is stable across zones for a December window, where a wrong zone also breaks the year", () => {
    forceTz("America/Chicago");
    expect(periodLabel("2026-12-01T00:00:00Z")).toBe("December 2026");
    forceTz("UTC");
    expect(periodLabel("2026-12-01T00:00:00Z")).toBe("December 2026");
  });

  it("degrades to a neutral phrase rather than 'Invalid Date' on unparseable input", () => {
    expect(periodLabel("not-a-timestamp")).toBe("this period");
  });

  it("applies the same UTC rule to the short day marker", () => {
    forceTz("America/Chicago");
    // 13 Aug 00:30 UTC is still 12 Aug locally — the ledger start date must
    // not drift a day for the same reason the month must not.
    expect(shortUtcDate("2026-08-13T00:30:00Z")).toBe("Aug 13");
    expect(shortUtcDate("nonsense")).toBe("an earlier date");
  });
});

describe("ledgerCoverage", () => {
  it("is 'none' when the account has never had a ledger row", () => {
    expect(ledgerCoverage(summary())).toEqual({ kind: "none" });
  });

  it("is 'partial' when the ledger starts inside the reporting window", () => {
    // The production state on 2026-08-13: metering shipped mid-month with no
    // backfill, so the ledger covers the 13th onward and the window opens on
    // the 1st.
    expect(ledgerCoverage(summary({ ledger_coverage_start: "2026-08-13T04:12:00Z" }))).toEqual({
      kind: "partial",
      startsAt: "2026-08-13T04:12:00Z",
    });
  });

  it("is 'full' once the ledger predates the window — the second month onward", () => {
    expect(ledgerCoverage(summary({ ledger_coverage_start: "2026-07-02T00:00:00Z" })).kind).toBe("full");
  });

  it("treats a ledger starting exactly at the window boundary as full coverage", () => {
    expect(ledgerCoverage(summary({ ledger_coverage_start: "2026-08-01T00:00:00Z" })).kind).toBe("full");
  });

  it("falls back to 'none', never 'full', on an unparseable timestamp", () => {
    // Defaulting to full coverage would suppress the disclosure, i.e. silently
    // reproduce the bug on bad data.
    expect(ledgerCoverage(summary({ ledger_coverage_start: "garbage" })).kind).toBe("none");
  });
});

describe("the screen cannot contradict itself — bug 1", () => {
  it("detects raw spend the ledger does not account for", () => {
    expect(rawUsageUsd(withRawUsage())).toBeCloseTo(155.95, 10);
    expect(hasUnbilledUsage(withRawUsage())).toBe(true);
  });

  it("does not cry unbilled once the ledger covers the whole window", () => {
    expect(hasUnbilledUsage(withRawUsage({ ledger_coverage_start: "2026-07-02T00:00:00Z" }))).toBe(false);
  });

  it("does not cry unbilled when there is no raw usage either", () => {
    expect(hasUnbilledUsage(summary())).toBe(false);
  });

  /** The exact production contradiction: `spend $0.00` and "No metered
   *  activity this period" rendered beside `Primary ocean $155.79`. Whatever
   *  the copy says, it may not be the flat denial. */
  it("never claims 'no metered activity' while showing workspace spend", () => {
    const copy = byKindEmptyCopy(withRawUsage());
    expect(copy).not.toBe(NO_METERED_ACTIVITY);
    expect(copy).toMatch(/billed/i);
  });

  it("names the date the ledger begins when coverage is partial", () => {
    const s = withRawUsage({ ledger_coverage_start: "2026-08-13T04:12:00Z" });
    expect(byKindEmptyCopy(s)).toContain("Aug 13");
    expect(spendNote(s)).toBe("$0.00 billed · ledger from Aug 13");
  });

  it("says the ledger has not started when there is no coverage at all", () => {
    expect(spendNote(withRawUsage())).toBe("$0.00 billed · ledger not started");
  });

  it("leaves the copy untouched when the numbers genuinely agree", () => {
    // No disclosure noise on an account whose ledger covers the window: a
    // caveat shown when nothing is inconsistent is its own kind of dishonesty.
    const s = summary({
      ledger_coverage_start: "2026-07-02T00:00:00Z",
      has_ledger_history: true,
      spend: { total_credits: 12, total_cost_usd: 0.26, by_axis: [] },
    });
    expect(byKindEmptyCopy(s)).toBe(NO_METERED_ACTIVITY);
    expect(spendNote(s)).toBe("$0.26");
  });

  /** The invariant, stated once over the whole matrix: the disclosure appears
   *  in exactly the cases the predicate fires, so the two panels can never
   *  drift into telling different stories. */
  it("ties the disclosure to the predicate across every coverage state", () => {
    const cases: UsageSummary[] = [
      summary(),
      summary({ ledger_coverage_start: "2026-08-13T04:12:00Z", has_ledger_history: true }),
      summary({ ledger_coverage_start: "2026-07-02T00:00:00Z", has_ledger_history: true }),
      withRawUsage(),
      withRawUsage({ ledger_coverage_start: "2026-08-13T04:12:00Z", has_ledger_history: true }),
      withRawUsage({ ledger_coverage_start: "2026-07-02T00:00:00Z", has_ledger_history: true }),
    ];
    for (const s of cases) {
      const discloses = byKindEmptyCopy(s) !== NO_METERED_ACTIVITY;
      expect(discloses, `disclosure disagreed with hasUnbilledUsage for ${JSON.stringify(s.ledger_coverage_start)}`).toBe(
        hasUnbilledUsage(s),
      );
      // And the spend note carries the same signal, never one without the other.
      expect(spendNote(s).includes("ledger")).toBe(hasUnbilledUsage(s));
    }
  });
});

describe("isHonestlyEmpty", () => {
  it("is true for a genuinely never-metered account (PR12's negative control)", () => {
    expect(isHonestlyEmpty(summary())).toBe(true);
  });

  it("is false when raw usage exists, so real spend is shown with its caveat rather than hidden", () => {
    expect(isHonestlyEmpty(withRawUsage())).toBe(false);
  });

  it("is false once the account has ledger history, even in a quiet month", () => {
    expect(isHonestlyEmpty(summary({ has_ledger_history: true, ledger_coverage_start: "2026-07-02T00:00:00Z" }))).toBe(
      false,
    );
  });
});
