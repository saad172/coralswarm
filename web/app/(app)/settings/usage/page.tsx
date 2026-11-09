"use client";

import { useQuery } from "@tanstack/react-query";
import { usageSummaryQueryOptions } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";
import { ApiError } from "@/lib/api";
import { ErrorNote, Loading } from "@/components/ui";
import { SettingsShell } from "@/components/settings/SettingsShell";
import { SettingsHeading } from "@/components/settings/SettingsHeading";
import { useSettingsRole } from "@/components/settings/useSettingsRole";
import { UsageSummaryView } from "@/components/usage/UsageSummaryView";

/* Promoted out of the Account page's buried "usage & credits" section into
   its own nav destination (enforcement-rollout follow-up). Personal-only —
   an org member/admin's own personal-account spend is a narrower question
   than "Organization → Usage" (the whole org's spend, admin-gated) already
   answers, so this page renders nothing for org context rather than
   fetching and showing a second, differently-scoped number. The nav row
   itself is ALSO hidden for org context (SettingsShell) — this guard is
   defense in depth for a direct link, not the primary gate. */
export default function SettingsUsagePage() {
  const { authReady, getAuthToken, queryScope } = useApp();
  const settingsRole = useSettingsRole();
  const query = useQuery({ ...usageSummaryQueryOptions({ getAuthToken, scope: queryScope }), enabled: authReady && settingsRole.role === "personal" });
  const summary = query.data ?? null;
  const err = query.isError && !query.data
    ? (query.error instanceof ApiError ? query.error.message : "Couldn't load usage.")
    : null;

  return (
    <SettingsShell>
      {settingsRole.loaded && settingsRole.role === "personal" && (
        <>
          <SettingsHeading title="Where the month went">
            Month to date, UTC. Synthesis and background extraction are the metered things — retrieval
            and embeddings are included.
          </SettingsHeading>
          {err ? (
            <ErrorNote>{err}</ErrorNote>
          ) : summary === null ? (
            <Loading what="usage" />
          ) : (
            <UsageSummaryView summary={summary} showAskers={false} />
          )}
        </>
      )}
    </SettingsShell>
  );
}
