"use client";

import { useQuery } from "@tanstack/react-query";
import { usageSummaryQueryOptions } from "@/lib/appQueries";
import { ApiError } from "@/lib/api";
import { useApp } from "@/lib/providers";
import { ErrorNote, Loading } from "@/components/ui";
import { OrgSettingsShell } from "@/components/org/OrgSettingsShell";
import { useOrgAdmin } from "@/components/org/useOrgAdmin";
import { UsageSummaryView } from "@/components/usage/UsageSummaryView";

/* Usage-metering PR12: un-mocked. Every number here now comes from
   `GET /v1/usage/summary` — `credit_ledger`/`credit_balances` for the
   headline balance/spend, `llm_usage`/`query_history` for the by-workspace
   and by-asker breakdowns. Previously this page hardcoded the same
   fictional "$186" the Billing page did; both are real now, in the same
   pass, so they can no longer visibly disagree. */
export default function SettingsOrganizationUsagePage() {
  const { authReady, getAuthToken, queryScope } = useApp();
  // The org branch of `/v1/usage/summary` is admin-only on the server
  // (settings launch 2.6) — don't fire a request OrgGate is about to hide
  // the answer to anyway, and that a member would only get a 403 for.
  const { isAdmin } = useOrgAdmin();
  const query = useQuery({ ...usageSummaryQueryOptions({ getAuthToken, scope: queryScope }), enabled: authReady && isAdmin });
  const summary = query.data ?? null;
  const err = query.isError && !query.data
    ? (query.error instanceof ApiError ? query.error.message : "Couldn't load usage.")
    : null;

  return (
    <OrgSettingsShell
      title="Where the month went"
      lead="Month to date, UTC. Synthesis and background extraction are the metered things — retrieval and embeddings are included."
    >
      {err ? (
        <ErrorNote>{err}</ErrorNote>
      ) : summary === null ? (
        <Loading what="usage" />
      ) : (
        <UsageSummaryView summary={summary} showAskers />
      )}
    </OrgSettingsShell>
  );
}
