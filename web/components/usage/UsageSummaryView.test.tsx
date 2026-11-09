// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import type { UsageSummary } from "@/lib/api";
import { UsageSummaryView } from "./UsageSummaryView";

afterEach(() => {
  cleanup();
});

function summary(overrides: Partial<UsageSummary> = {}): UsageSummary {
  return {
    period_start: "2026-09-01T00:00:00Z",
    period_end: "2026-09-18T00:00:00Z",
    balance_credits: 640,
    plan: {
      code: "team_standard",
      name: "Team",
      credits: 12000,
      seats: 4,
      credits_per_seat: 3000,
      refresh_kind: "monthly",
      granted_at: "2026-01-01T00:00:00Z",
    },
    credit_conversion_usd_per_credit: 0.0044,
    spend: { total_credits: 360, total_cost_usd: 1.58, by_axis: [] },
    by_workspace: [],
    top_askers: [],
    atoms_ingested: 128,
    has_ledger_history: true,
    ledger_coverage_start: "2026-07-15T00:00:00Z",
    ...overrides,
  };
}

describe("UsageSummaryView — seat-priced plan copy (CS-433)", () => {
  /// The billing concept is a **slot** (D15, founder 2026-09-21) — "seat"
  /// is reserved for team-ocean access and must never appear in this
  /// plan-breakdown line. The underlying API field (`plan.seats`) is left
  /// untouched by that rename; only the rendered word changes.
  it("renders the per-plan breakdown as slots, never seats", () => {
    render(<UsageSummaryView summary={summary()} />);
    expect(screen.getByText("4 slots × 3,000 credits")).toBeInTheDocument();
    expect(screen.queryByText(/\bseats?\b/i)).not.toBeInTheDocument();
  });

  it("renders nothing for a flat (non seat-priced) plan's breakdown line", () => {
    render(
      <UsageSummaryView
        summary={summary({
          plan: {
            code: "free",
            name: "Free",
            credits: 1000,
            seats: null,
            credits_per_seat: null,
            refresh_kind: "lifetime",
            granted_at: "2026-01-01T00:00:00Z",
          },
        })}
      />,
    );
    expect(screen.queryByText(/slot/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/seat/i)).not.toBeInTheDocument();
  });
});
