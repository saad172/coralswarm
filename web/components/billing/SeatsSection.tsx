"use client";

import { useState } from "react";
import { Button, ErrorNote, Input, Meter, Notice } from "@/components/ui";
import { api, type BillingSummary, type SeatQuote } from "@/lib/api";
import { useApp } from "@/lib/providers";
import { billingErrorMessage } from "./billingErrors";
import { isSubscribed } from "./billingModel";
import { SEAT_TIER, SeatQuoteDialog, seatWord } from "./SeatQuoteDialog";

/* Organization seats: add or give back, always through a quote first. The
   quote shows what changes on the next invoice; nothing is charged until the
   admin confirms, and even then the prorated amount lands on the next invoice
   instead of charging a card right away. */

export function SeatsSection({ summary }: { summary: BillingSummary }) {
  const { getAuthToken } = useApp();
  const [quantity, setQuantity] = useState("1");
  const [quoting, setQuoting] = useState<"add" | "reduce" | null>(null);
  const [quote, setQuote] = useState<SeatQuote | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState<string | null>(null);

  const { used, total } = summary.seats;
  const subscribed = isSubscribed(summary);

  async function requestQuote(action: "add" | "reduce") {
    const n = Number(quantity);
    if (!Number.isInteger(n) || n < 1 || n > 500 || !/^\d+$/.test(quantity.trim())) {
      setError("Pick a number of seats between 1 and 500.");
      return;
    }
    const token = getAuthToken();
    if (!token) return;
    setQuoting(action);
    setError(null);
    setDone(null);
    try {
      const q = await api.billing.seatQuote(token, { quantity: n, action, tier: SEAT_TIER });
      setQuote(q);
    } catch (e) {
      setError(billingErrorMessage(e, "Couldn't price that change. Try again."));
    } finally {
      setQuoting(null);
    }
  }

  function applied(q: SeatQuote) {
    setDone(
      q.action === "add"
        ? `Added ${q.quantity} ${seatWord(q.quantity)}. You now have ${q.new_quantity}.`
        : `Removed ${q.quantity} ${seatWord(q.quantity)}. You now have ${q.new_quantity}.`,
    );
    setQuote(null);
    setQuantity("1");
  }

  return (
    <div className="rounded-card border border-line px-[16px] py-[16px]">
      <div className="mb-[8px] flex items-baseline gap-[8px]">
        <span className="font-mono text-[22px] font-medium text-ink">{used}</span>
        <span className="text-[12.5px] text-ink-3">{`of ${total} ${seatWord(total)} in use`}</span>
      </div>
      {total > 0 && <Meter value={used} max={total} tone="var(--hue-plum)" className="mb-[12px]" />}

      {subscribed ? (
        <div className="flex flex-wrap items-end gap-[8px] border-t border-line-3 pt-[12px]">
          <Input
            type="number"
            min={1}
            max={500}
            value={quantity}
            onChange={(e) => setQuantity(e.target.value)}
            wrapperClassName="w-[90px]"
            aria-label="Number of seats"
          />
          <Button variant="secondary" size="sm" disabled={quoting !== null} onClick={() => void requestQuote("add")}>
            {quoting === "add" ? "Pricing…" : "Add seats"}
          </Button>
          <Button variant="secondary" size="sm" disabled={quoting !== null} onClick={() => void requestQuote("reduce")}>
            {quoting === "reduce" ? "Pricing…" : "Reduce seats"}
          </Button>
        </div>
      ) : (
        <p className="border-t border-line-3 pt-[12px] text-[12.5px] text-ink-3">
          Subscribe to add seats. You can give seats back any time they aren&rsquo;t in use.
        </p>
      )}
      {error && (
        <div className="mt-[10px]">
          <ErrorNote>{error}</ErrorNote>
        </div>
      )}
      {done && (
        <div className="mt-[10px]">
          <Notice>{done}</Notice>
        </div>
      )}

      <SeatQuoteDialog quote={quote} onCancel={() => setQuote(null)} onDone={applied} />
    </div>
  );
}
