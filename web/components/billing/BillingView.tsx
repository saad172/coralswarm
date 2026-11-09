"use client";

import { useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ErrorNote, Loading, Meter, Notice } from "@/components/ui";
import { Mono } from "@/components/ui";
import { ApiError } from "@/lib/api";
import { invalidateBillingQueries, billingSummaryQueryOptions } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";
import { ExtraUsageSection } from "./ExtraUsageSection";
import { PlanSection } from "./PlanSection";
import { SeatsSection } from "./SeatsSection";
import { formatCredits, formatDate, paymentMethodLine, trialBillingView } from "./billingModel";

/* The billing page body, shared by Organization -> Billing (admins) and the
   personal Billing page. The caller decides who may see it. */

function Heading({ children }: { children: string }) {
  return (
    <div className="mb-[9px] mt-[22px] flex items-center gap-[9px]">
      <Mono className="tracking-[0.13em]">{children}</Mono>
    </div>
  );
}

export function BillingView({ path }: { path: string }) {
  const { authReady, getAuthToken, queryScope } = useApp();
  const qc = useQueryClient();
  const query = useQuery({ ...billingSummaryQueryOptions({ getAuthToken, scope: queryScope }), enabled: authReady });
  const [returned, setReturned] = useState<"checkout" | "topup" | null>(null);

  // Stripe sends people back with ?checkout=success or ?topup=success. The
  // webhook may land a moment later, so refetch now and say so.
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const which = params.get("checkout") === "success" ? "checkout" : params.get("topup") === "success" ? "topup" : null;
    if (which) {
      setReturned(which);
      void invalidateBillingQueries(qc);
    }
  }, [qc]);

  const summary = query.data ?? null;
  if (query.isError && !summary) {
    return (
      <ErrorNote>
        {query.error instanceof ApiError && query.error.status === 403
          ? "Only an admin can see billing."
          : "Couldn't load billing. Reload the page to try again."}
      </ErrorNote>
    );
  }
  if (!summary) return <Loading what="billing" />;

  const isOrg = summary.account_kind === "org";
  const credits = summary.credits;
  const resets = formatDate(credits.resets_at);
  const line = paymentMethodLine(summary.payment_method);
  const trial = trialBillingView(summary);

  return (
    <>
      {returned && (
        <div className="mb-[14px]">
          <Notice>
            {returned === "checkout"
              ? "Thanks. Your subscription is being set up and this page updates in a moment."
              : "Thanks. Your funds are being added and the balance updates in a moment."}
          </Notice>
        </div>
      )}

      <Heading>plan</Heading>
      <PlanSection summary={summary} path={path} />

      {isOrg && (
        <>
          <Heading>seats</Heading>
          <SeatsSection summary={summary} />
        </>
      )}

      <Heading>credits</Heading>
      <div className="rounded-card border border-line px-[16px] py-[16px]">
        {trial ? (
          <>
            <div className="mb-[8px] flex items-baseline gap-[8px]">
              <span className="font-mono text-[22px] font-medium text-ink">{formatCredits(trial.used)}</span>
              <span className="text-[12.5px] text-ink-3">{`of ${formatCredits(trial.total)} trial credits used`}</span>
            </div>
            {trial.total > 0 && <Meter value={trial.used} max={trial.total} />}
            <p className="mt-[8px] text-[12.5px] text-ink-3">{trial.creditsText}</p>
          </>
        ) : (
          <>
            <div className="mb-[8px] flex items-baseline gap-[8px]">
              <span className="font-mono text-[22px] font-medium text-ink">{formatCredits(credits.allowance_used)}</span>
              <span className="text-[12.5px] text-ink-3">{`of ${formatCredits(credits.allowance_total)} plan credits used`}</span>
            </div>
            {credits.allowance_total > 0 && <Meter value={credits.allowance_used} max={credits.allowance_total} />}
            <p className="mt-[8px] text-[12.5px] text-ink-3">
              {resets ? `Resets on ${resets}.` : "Plan credits reset every billing period."}
            </p>
          </>
        )}
      </div>

      <Heading>extra usage</Heading>
      <ExtraUsageSection summary={summary} path={path} />

      <Heading>payment method</Heading>
      <div className="rounded-card border border-line px-[16px] py-[14px] text-[13.5px] text-ink">
        {line ? (
          <>
            {line.label}
            {line.detail && <span className="text-ink-3">{` · ${line.detail}`}</span>}
            <span className="mt-[3px] block text-[12.5px] text-ink-3">Change it from Manage billing.</span>
          </>
        ) : (
          <span className="text-ink-3">No payment method on file.</span>
        )}
      </div>
    </>
  );
}
