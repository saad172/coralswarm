"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { Card, CardHead, ErrorNote, Meter, Mono, Notice, Skeleton } from "@/components/ui";
import type { BillingMe } from "@/lib/api";
import { billingMeQueryOptions } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";

/* Settings → Account "Your usage" (settings launch 2.7). Read-only, for every
   signed-in user, from `GET /v1/billing/me`. Credits only — the endpoint never
   carries dollars and nothing here formats a currency. The view is split from
   the fetching container so each response shape is testable without a query
   client. */

type BlockedReason = NonNullable<BillingMe["blocked"]["reason"]>;

const BLOCKED_WHAT: Record<BlockedReason, string> = {
  free_plan: "Your free plan doesn't include this. Upgrade to keep going.",
  credits_frozen: "Your credits are on hold right now, so new work is paused.",
  extra_usage_off: "You've used all your plan credits, and extra usage is turned off.",
  member_cap_reached: "You've reached your extra usage cap, so new work is paused.",
  monthly_limit_reached: "Your organization has reached its monthly extra usage limit, so new work is paused.",
  extra_balance_exhausted: "You've used all your plan credits and your extra usage balance.",
};

const BLOCKED_ADMIN_FIX: Record<BlockedReason, string> = {
  free_plan: "Ask your organization admin to move you to a paid plan.",
  credits_frozen: "Ask your organization admin to look into it.",
  extra_usage_off: "Ask your organization admin to turn on extra usage.",
  member_cap_reached: "Ask your organization admin to raise your extra usage cap.",
  monthly_limit_reached: "Ask your organization admin to raise the monthly limit.",
  extra_balance_exhausted: "Ask your organization admin to add more extra usage.",
};

const BLOCKED_PERSONAL_FIX: Record<BlockedReason, string> = {
  free_plan: "Choose a plan in Billing to continue.",
  credits_frozen: "Open Billing to see what's going on.",
  extra_usage_off: "Open Billing to turn on extra usage or change your plan.",
  member_cap_reached: "Open Billing to raise your extra usage cap.",
  monthly_limit_reached: "Open Billing to raise your monthly limit.",
  extra_balance_exhausted: "Open Billing to add more extra usage.",
};

const fmt = (n: number) => Math.round(n).toLocaleString("en-US");

function fmtDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric", timeZone: "UTC" });
}

export function BlockedBanner({ data }: { data: BillingMe }) {
  const reason = data.blocked.reason;
  if (!data.blocked.blocked) return null;
  const team = data.account_kind === "team_org";
  const what = reason ? BLOCKED_WHAT[reason] : "New work is paused right now.";
  const fix = reason ? (team ? BLOCKED_ADMIN_FIX[reason] : BLOCKED_PERSONAL_FIX[reason]) : team ? "Ask your organization admin." : "Open Billing for details.";
  return (
    <Notice
      tone="warn"
      className="mb-[12px]"
      action={
        team ? undefined : (
          <Link href="/settings/billing" className="font-medium text-ink underline underline-offset-2">
            Go to Billing
          </Link>
        )
      }
    >
      <span className="font-medium text-ink">New work is paused.</span> {what} {fix}
    </Notice>
  );
}

export function YourUsageView({ data }: { data: BillingMe }) {
  const credits = data.plan_credits;
  const extra = data.extra_usage;
  const extraLabel = extra.scope === "monthly_limit" ? "Monthly extra usage limit" : "Your extra usage cap";
  const reset = credits ? fmtDate(credits.resets_at) : "";
  return (
    <div className="px-[14px] py-[13px]">
      <BlockedBanner data={data} />

      {credits ? (
        <Meter
          value={credits.used}
          max={credits.total}
          ariaLabel="Plan credits used"
          label={
            <>
              <span className="text-ink">
                {fmt(credits.used)} of {fmt(credits.total)} credits used
              </span>
              {reset && <span className="text-ink-3">Resets {reset}</span>}
            </>
          }
        />
      ) : (
        <p className="text-[12.5px] leading-[1.55] text-ink-2">
          Your allowance starts with your first billing period.
        </p>
      )}

      {extra.status === "off" && (
        <p className="mt-[12px] text-[12.5px] leading-[1.55] text-ink-2">
          Extra usage is off. Once your plan credits run out, new work pauses until they reset.
        </p>
      )}

      {extra.status === "on" && (
        <div className="mt-[12px]">
          {extra.limit_credits != null ? (
            <Meter
              value={extra.used_credits}
              max={extra.limit_credits}
              ariaLabel={extraLabel}
              label={
                <>
                  <span className="text-ink">
                    {extraLabel}: {fmt(extra.used_credits)} of {fmt(extra.limit_credits)} credits used
                  </span>
                  {extra.percent_used != null && <span className="text-ink-3">{Math.round(extra.percent_used)}%</span>}
                </>
              }
            />
          ) : (
            <p className="text-[12.5px] leading-[1.55] text-ink-2">
              Extra usage is on with no limit. You&apos;ve used {fmt(extra.used_credits)} extra credits.
            </p>
          )}
        </div>
      )}
    </div>
  );
}

export function YourUsageCard() {
  const { authReady, getAuthToken, queryScope } = useApp();
  const q = useQuery({ ...billingMeQueryOptions({ getAuthToken, scope: queryScope }), enabled: authReady });
  return (
    <Card flush className="mb-[14px]">
      <CardHead title={<Mono className="tracking-[0.13em]">your usage</Mono>} />
      {q.isLoading || !authReady ? (
        <div className="px-[14px] py-[13px]">
          <Skeleton h={6} />
        </div>
      ) : q.isError || !q.data ? (
        <div className="px-[14px] py-[13px]">
          <ErrorNote>Couldn&apos;t load your usage. Try again in a moment.</ErrorNote>
        </div>
      ) : (
        <YourUsageView data={q.data} />
      )}
    </Card>
  );
}
