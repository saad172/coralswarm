"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { api, type SeatQuote } from "@/lib/api";
import { appInvalidation, invalidateBillingQueries } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";
import { billingErrorMessage } from "./billingErrors";
import { formatDate } from "./billingModel";
import { formatCents } from "./money";

/* The seat quote → confirm step, shared by Billing's Seats section and every
   place that invites people (Members, the onboarding invite step). The quote
   shows what changes on the next invoice; nothing is charged until the admin
   confirms, and even then the prorated amount lands on the next invoice. */

export const SEAT_TIER = "standard";

export function seatWord(n: number): string {
  return n === 1 ? "seat" : "seats";
}

export function SeatQuoteDialog({
  quote,
  description,
  onCancel,
  onDone,
}: {
  /** Open while non-null. */
  quote: SeatQuote | null;
  /** Overrides the default "review the price" line. */
  description?: ReactNode;
  onCancel: () => void;
  /** The change went through; the quote it applied is passed back. */
  onDone: (quote: SeatQuote) => void;
}) {
  const { getAuthToken } = useApp();
  const qc = useQueryClient();
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // One key per quote, reused if the same confirm is retried after a dropped response.
  const idempotencyKey = useRef<string | null>(null);
  useEffect(() => {
    idempotencyKey.current = quote ? crypto.randomUUID() : null;
    setError(null);
  }, [quote]);

  async function confirm() {
    if (!quote) return;
    const token = getAuthToken();
    if (!token) return;
    setConfirming(true);
    setError(null);
    try {
      const body = {
        tier: quote.tier,
        quantity: quote.quantity,
        idempotencyKey: idempotencyKey.current ?? crypto.randomUUID(),
      };
      if (quote.action === "add") await api.orgs.slots.purchase(token, body);
      else await api.orgs.slots.reduce(token, body);
      void invalidateBillingQueries(qc);
      void qc.invalidateQueries({ queryKey: appInvalidation.orgSlots() });
      onDone(quote);
    } catch (e) {
      setError(billingErrorMessage(e, "The seat change didn't go through. Try again."));
    } finally {
      setConfirming(false);
    }
  }

  const adding = quote?.action === "add";
  const nextInvoice = quote ? formatDate(quote.next_invoice_at) : null;

  return (
    <ConfirmDialog
      open={quote !== null}
      title={quote ? `${adding ? "Add" : "Remove"} ${quote.quantity} ${seatWord(quote.quantity)}?` : ""}
      description={
        description ??
        (adding
          ? "Review the price. Nothing is charged right now: the prorated amount is added to your next invoice."
          : "Review the change. Nothing is refunded right now: the prorated credit is applied to your next invoice.")
      }
      confirmLabel={quote ? `${adding ? "Add" : "Remove"} ${quote.quantity} ${seatWord(quote.quantity)}` : "Confirm"}
      busyLabel={adding ? "Adding…" : "Removing…"}
      busy={confirming}
      error={error}
      onConfirm={() => void confirm()}
      onCancel={onCancel}
    >
      {quote && (
        <dl className="grid grid-cols-[1fr_auto] gap-x-[16px] gap-y-[6px] text-[13px] text-ink-2">
          <dt>Seats</dt>
          <dd className="text-right font-mono text-ink">{`${quote.current_quantity} → ${quote.new_quantity}`}</dd>
          <dt>{quote.proration_cents < 0 ? "Prorated credit" : "Prorated charge"}</dt>
          <dd className="text-right font-mono text-ink">{formatCents(Math.abs(quote.proration_cents))}</dd>
          <dt>{nextInvoice ? `Next invoice, ${nextInvoice}` : "Next invoice"}</dt>
          <dd className="text-right font-mono text-ink">{formatCents(quote.next_invoice_total_cents)}</dd>
          <dt>Each invoice after that</dt>
          <dd className="text-right font-mono text-ink">{formatCents(quote.recurring_cents_after)}</dd>
        </dl>
      )}
    </ConfirmDialog>
  );
}
