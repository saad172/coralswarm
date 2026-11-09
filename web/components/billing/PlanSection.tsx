"use client";

import { useState } from "react";
import { Badge, Button, ErrorNote, Notice } from "@/components/ui";
import { api, type BillingSummary } from "@/lib/api";
import { useApp } from "@/lib/providers";
import { billingErrorMessage } from "./billingErrors";
import { formatDate, isSubscribed, subscribeTier, trialBillingView } from "./billingModel";
import { goToStripe, returnUrl } from "./navigate";

/* Plan, renewal, the cancel-at-period-end notice, Subscribe (Stripe Checkout)
   when nothing is running, and Manage billing (Stripe portal). */
export function PlanSection({ summary, path }: { summary: BillingSummary; path: string }) {
  const { getAuthToken } = useApp();
  const [busy, setBusy] = useState<"subscribe" | "portal" | null>(null);
  const [error, setError] = useState<string | null>(null);

  const sub = summary.subscription;
  const subscribed = isSubscribed(summary);
  const planName = summary.plan?.name ?? "Free";
  const renews = formatDate(sub.renews_at);
  const cancelsOn = formatDate(sub.cancel_at ?? sub.renews_at);
  const isOrg = summary.account_kind === "org";
  const trial = trialBillingView(summary);

  async function subscribe() {
    const token = getAuthToken();
    if (!token) return;
    setBusy("subscribe");
    setError(null);
    try {
      const session = await api.billing.subscribe(token, {
        tier: subscribeTier(summary),
        successUrl: returnUrl(path, "checkout=success"),
        cancelUrl: returnUrl(path),
      });
      await goToStripe(session.checkout_url);
    } catch (e) {
      setError(billingErrorMessage(e, "Couldn't start checkout. Try again."));
    } finally {
      setBusy(null);
    }
  }

  async function openPortal() {
    const token = getAuthToken();
    if (!token) return;
    setBusy("portal");
    setError(null);
    try {
      const { url } = await api.billing.portal(token, returnUrl(path));
      await goToStripe(url);
    } catch (e) {
      setError(billingErrorMessage(e, "Couldn't open billing. Try again."));
    } finally {
      setBusy(null);
    }
  }

  return (
    <div className="rounded-card border border-line px-[16px] py-[16px]">
      <div className="flex items-center gap-[22px]">
        <div className="min-w-0 flex-1">
          <div className="mb-[5px] flex items-center gap-[9px]">
            <span className="text-[17px] font-semibold text-ink">{planName}</span>
            {trial && !sub.cancel_at_period_end && !sub.cancel_at && <Badge tone="ok">free trial</Badge>}
            {!trial && subscribed && !sub.cancel_at_period_end && sub.status !== "past_due" && <Badge tone="ok">active</Badge>}
            {subscribed && sub.cancel_at_period_end && <Badge tone="warn">ending</Badge>}
            {sub.status === "past_due" && <Badge tone="bad">payment past due</Badge>}
          </div>
          <span className="block text-[12.5px] text-ink-3">
            {trial && !sub.cancel_at_period_end && !sub.cancel_at
              ? trial.planText
              : !subscribed
              ? isOrg
                ? "No subscription yet. Subscribe to add seats and credits for your team."
                : "You're on the Free plan. Subscribe to Pro for more credits and extra usage."
              : sub.cancel_at_period_end
                ? cancelsOn
                  ? `Ends on ${cancelsOn}.`
                  : "Ends at the close of this billing period."
                : renews
                  ? `Renews on ${renews}.`
                  : "Renewal date not available yet."}
          </span>
          {trial && !sub.cancel_at_period_end && !sub.cancel_at && trial.priceText && (
            <span className="mt-[3px] block text-[12.5px] text-ink-3">{trial.priceText}</span>
          )}
        </div>
        <div className="flex flex-none items-center gap-[8px]">
          {!subscribed && (
            <Button variant="primary" size="sm" disabled={busy !== null} onClick={() => void subscribe()}>
              {busy === "subscribe" ? "Opening checkout…" : isOrg ? "Subscribe" : "Subscribe to Pro"}
            </Button>
          )}
          {sub.has_billing_account && (
            <Button variant="secondary" size="sm" disabled={busy !== null} onClick={() => void openPortal()}>
              {busy === "portal" ? "Opening…" : "Manage billing"}
            </Button>
          )}
        </div>
      </div>
      {subscribed && sub.cancel_at_period_end && (
        <Notice tone="warn" className="mt-[12px]">
          {cancelsOn ? `This plan is set to end on ${cancelsOn}.` : "This plan is set to end at the close of this billing period."}{" "}
          You keep everything until then. After it ends, extra-usage credits you bought are held, and they come back if
          you resubscribe. Use Manage billing to keep the plan.
        </Notice>
      )}
      {error && (
        <div className="mt-[12px]">
          <ErrorNote>{error}</ErrorNote>
        </div>
      )}
    </div>
  );
}
