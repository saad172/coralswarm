"use client";

import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Button, ErrorNote, Input, Notice, Toggle } from "@/components/ui";
import { api, ApiError, type BillingSummary } from "@/lib/api";
import { invalidateBillingQueries } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";
import { billingErrorMessage } from "./billingErrors";
import { TRIAL_EXTRA_USAGE_COPY, trialRefusesExtraUsage } from "@/lib/orgSelfserve";
import { EXTRA_USAGE_PRICE_PER_CREDIT, canUseExtraUsage, hasFrozenCredits } from "./billingModel";
import {
  buildExtraUsageUpdate,
  fieldForErrorCode,
  formFromExtraUsage,
  validateTopupAmount,
  type ExtraUsageFieldErrors,
  type ExtraUsageFormState,
} from "./extraUsageModel";
import { formatMicroUsd } from "./money";
import { goToStripe, returnUrl } from "./navigate";

/* Extra usage: a prepaid balance spent after the plan's credits run out.
   Off by default. The monthly limit (and, for organizations, each member's
   share) resets on the 1st of every month, UTC. */

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="font-mono text-[18px] font-medium text-ink">{value}</div>
      <div className="text-[12px] text-ink-3">{label}</div>
    </div>
  );
}

function ExtraUsageForm({ summary }: { summary: BillingSummary }) {
  const { getAuthToken } = useApp();
  const qc = useQueryClient();
  const extra = summary.extra_usage;
  const isOrg = summary.account_kind === "org";
  const [form, setForm] = useState<ExtraUsageFormState>(() => formFromExtraUsage(extra));
  const [fieldErrors, setFieldErrors] = useState<ExtraUsageFieldErrors>({});
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);
  const [saving, setSaving] = useState(false);
  const set = (patch: Partial<ExtraUsageFormState>) => {
    setForm((f) => ({ ...f, ...patch }));
    setSaved(false);
  };

  async function save() {
    setError(null);
    setSaved(false);
    const built = buildExtraUsageUpdate(form, { isOrg });
    if (!built.ok) {
      setFieldErrors(built.errors);
      return;
    }
    setFieldErrors({});
    const token = getAuthToken();
    if (!token) return;
    setSaving(true);
    try {
      await api.billing.extraUsage.update(token, built.body);
      setSaved(true);
      void invalidateBillingQueries(qc);
    } catch (e) {
      const message = billingErrorMessage(e, "Couldn't save extra usage. Try again.");
      const field = e instanceof ApiError ? fieldForErrorCode(e.code) : null;
      if (field && !(field === "memberCap" && !isOrg)) setFieldErrors({ [field]: message });
      else setError(message);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="flex flex-col gap-[14px]">
      <Toggle
        checked={form.enabled}
        onChange={(enabled) => set({ enabled })}
        ariaLabel="Extra usage"
        label={<span className="text-[13.5px] font-medium text-ink">Let usage continue past plan credits</span>}
        hint={`When this is on, work keeps going after the plan's credits run out and is paid from your balance at ${EXTRA_USAGE_PRICE_PER_CREDIT} per credit. When it is off, work stops at the plan's credits.`}
      />

      {form.enabled && (
        <>
          <div className="grid grid-cols-1 gap-[12px] sm:grid-cols-2">
            <Input
              label="Monthly limit ($)"
              inputMode="decimal"
              placeholder={isOrg ? "100 (default)" : "50 (default)"}
              value={form.limit}
              onChange={(e) => set({ limit: e.target.value })}
              error={fieldErrors.limit}
              hint="The most extra usage can cost in a month. Resets on the 1st (UTC)."
            />
            {isOrg && (
              <Input
                label="Per-member limit (× a seat's credits)"
                inputMode="decimal"
                placeholder="1 (default)"
                value={form.memberCap}
                onChange={(e) => set({ memberCap: e.target.value })}
                error={fieldErrors.memberCap}
                hint="Each member can use up to this many times one seat's monthly credits as extra usage. Resets on the 1st (UTC)."
              />
            )}
          </div>

          <div className="rounded-control border border-line-3 px-[12px] py-[12px]">
            <Toggle
              checked={form.reloadEnabled}
              onChange={(reloadEnabled) => set({ reloadEnabled })}
              ariaLabel="Auto-reload"
              label={<span className="text-[13.5px] font-medium text-ink">Auto-reload</span>}
              hint="Add funds by card automatically when the balance gets low. Off by default. It never goes past the monthly limit."
            />
            {form.reloadEnabled && (
              <div className="mt-[12px] grid grid-cols-1 gap-[12px] sm:grid-cols-2">
                <Input
                  label="When the balance drops to ($)"
                  inputMode="decimal"
                  value={form.reloadThreshold}
                  onChange={(e) => set({ reloadThreshold: e.target.value })}
                  error={fieldErrors.reloadThreshold}
                />
                <Input
                  label="Add ($)"
                  inputMode="decimal"
                  value={form.reloadAmount}
                  onChange={(e) => set({ reloadAmount: e.target.value })}
                  error={fieldErrors.reloadAmount}
                />
              </div>
            )}
          </div>
        </>
      )}

      {error && <ErrorNote>{error}</ErrorNote>}
      {saved && <Notice>Saved.</Notice>}
      <div>
        <Button variant="primary" size="sm" disabled={saving} onClick={() => void save()}>
          {saving ? "Saving…" : "Save extra usage"}
        </Button>
      </div>
    </div>
  );
}

function TopUp({ summary, path }: { summary: BillingSummary; path: string }) {
  const { getAuthToken } = useApp();
  const [amount, setAmount] = useState("25");
  const [fieldError, setFieldError] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const off = !summary.extra_usage.enabled;

  async function topUp() {
    setError(null);
    const v = validateTopupAmount(amount);
    if (!v.ok) {
      setFieldError(v.error);
      return;
    }
    setFieldError(null);
    const token = getAuthToken();
    if (!token) return;
    setBusy(true);
    try {
      const session = await api.billing.extraUsage.topUp(token, {
        amountMicroUsd: v.micro,
        successUrl: returnUrl(path, "topup=success"),
        cancelUrl: returnUrl(path),
      });
      await goToStripe(session.checkout_url);
    } catch (e) {
      setError(billingErrorMessage(e, "Couldn't start the payment. Try again."));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="border-t border-line-3 pt-[14px]">
      <div className="flex flex-wrap items-end gap-[8px]">
        <Input
          label="Add funds ($)"
          inputMode="decimal"
          value={amount}
          onChange={(e) => setAmount(e.target.value)}
          wrapperClassName="w-[140px]"
          error={fieldError ?? undefined}
          disabled={off}
        />
        <Button variant="secondary" size="sm" disabled={busy || off} onClick={() => void topUp()}>
          {busy ? "Opening checkout…" : "Add funds"}
        </Button>
      </div>
      <p className="mt-[6px] text-[12px] text-ink-3">
        {off ? "Turn on extra usage to add funds. " : ""}Between $5 and $1,000 at a time. You pay by card on a secure Stripe page.
      </p>
      {error && (
        <div className="mt-[8px]">
          <ErrorNote>{error}</ErrorNote>
        </div>
      )}
    </div>
  );
}

export function ExtraUsageSection({ summary, path }: { summary: BillingSummary; path: string }) {
  const extra = summary.extra_usage;
  const frozen = hasFrozenCredits(summary);
  const usable = canUseExtraUsage(summary);
  const inTrial = trialRefusesExtraUsage(summary.trial);

  return (
    <div className="rounded-card border border-line px-[16px] py-[16px]">
      {frozen && (
        <Notice tone="warn" className="mb-[14px]">
          Your {formatMicroUsd(extra.balance_micro_usd)} of extra-usage credits are on hold because the subscription
          ended. They come back when you resubscribe.
        </Notice>
      )}

      {inTrial ? (
        // #1521: extra usage, top-ups and auto-reload are refused (409
        // `trial_active`) until the trial's first charge. Say so up front
        // rather than offering controls that will be refused.
        <p className="text-[13px] leading-[1.55] text-ink-2">{TRIAL_EXTRA_USAGE_COPY}</p>
      ) : !usable ? (
        <p className="text-[13px] leading-[1.55] text-ink-2">
          {frozen
            ? "Extra usage is paused until you resubscribe."
            : "Extra usage is available on paid plans. Subscribe to keep working past your plan's credits."}
        </p>
      ) : (
        <>
          <div className="mb-[16px] flex flex-wrap gap-x-[32px] gap-y-[10px]">
            <Stat label="Balance" value={formatMicroUsd(extra.balance_micro_usd)} />
            <Stat
              label={
                extra.monthly_limit_micro_usd != null
                  ? `Spent this month, of ${formatMicroUsd(extra.monthly_limit_micro_usd)}`
                  : "Spent this month"
              }
              value={formatMicroUsd(extra.spent_this_period_micro_usd)}
            />
          </div>
          {/* Re-keyed on the stored values so a refetch after saving resets the fields. */}
          <ExtraUsageForm key={JSON.stringify(extra)} summary={summary} />
          <div className="mt-[16px]">
            <TopUp summary={summary} path={path} />
          </div>
        </>
      )}
    </div>
  );
}
