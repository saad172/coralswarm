"use client";

/* DEV-ONLY PREVIEW — delete before merge.
 *
 * Renders the REAL production components (SettingsShell, SettingsHeading,
 * UsageSummaryView) with a fixture UsageSummary, skipping the network fetch
 * entirely — same shape as `app/dev/slack-onboarding/page.tsx` and
 * `app/dev/out-of-credits/page.tsx`. This is the actual shipped code, not a
 * design-tool recreation of it. */

import { SettingsShell } from "@/components/settings/SettingsShell";
import { SettingsHeading } from "@/components/settings/SettingsHeading";
import { UsageSummaryView } from "@/components/usage/UsageSummaryView";
import type { UsageSummary } from "@/lib/api";

const FIXTURE: UsageSummary = {
  period_start: "2026-08-01T00:00:00Z",
  period_end: "2026-08-18T04:00:00Z",
  balance_credits: 640,
  plan: {
    code: "free",
    name: "Free",
    credits: 1000,
    seats: null,
    credits_per_seat: null,
    refresh_kind: "lifetime",
    granted_at: "2026-01-01T00:00:00Z",
  },
  credit_conversion_usd_per_credit: 0.0044,
  spend: {
    total_credits: 360,
    total_cost_usd: 1.58,
    by_axis: [
      { axis: "usage_query", credits: 210, cost_usd: 0.92, count: 38 },
      { axis: "usage_job", credits: 130, cost_usd: 0.57, count: 12 },
      { axis: "usage_journal", credits: 20, cost_usd: 0.09, count: 3 },
    ],
  },
  by_workspace: [
    { graph_id: "11111111-1111-1111-1111-111111111111", name: "Product Ocean", cost_usd: 1.02, unattributed: false },
    { graph_id: "22222222-2222-2222-2222-222222222222", name: "Sales Ocean", cost_usd: 0.41, unattributed: false },
    { graph_id: null, name: null, cost_usd: 0.15, unattributed: true },
  ],
  top_askers: [{ user_id: "33333333-3333-3333-3333-333333333333", email: "person@example.com", asks: 47 }],
  atoms_ingested: 128,
  has_ledger_history: true,
  ledger_coverage_start: "2026-07-15T00:00:00Z",
};

export default function UsagePagePreview() {
  return (
    <SettingsShell>
      <SettingsHeading title="Where the month went">
        Month to date, UTC. Synthesis and background extraction are the metered things — retrieval and
        embeddings are included.
      </SettingsHeading>
      <UsageSummaryView summary={FIXTURE} showAskers={false} />
    </SettingsShell>
  );
}
