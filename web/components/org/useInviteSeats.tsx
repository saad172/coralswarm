"use client";

import { useRef, useState, type ReactElement } from "react";
import { SEAT_TIER, SeatQuoteDialog, seatWord } from "@/components/billing/SeatQuoteDialog";
import { billingErrorMessage } from "@/components/billing/billingErrors";
import { api, type SeatQuote } from "@/lib/api";
import { useApp } from "@/lib/providers";

/* Inviting past the seat count goes through the same seat quote → confirm
   step as Billing, before any invite is sent. A team org already has its
   seats from Checkout (two at least), so going past them is a deliberate
   purchase the owner should see priced first. The server enforces it too:
   the web sends every invite with `allow_purchase: false`, and an invite that
   finds no free seat comes back 409 `no_free_seat` instead of buying one. The
   pages answer that 409 by refetching the pool and calling `requestSeats`
   for the invites still unsent.

   `requestSeats(toBuy, free, count, send)` prices the missing seats and opens
   the dialog; confirming buys them and then runs `send`. Cancelling sends
   nothing. */

export interface InviteSeats {
  requestSeats: (toBuy: number, free: number, count: number, send: () => void) => Promise<void>;
  /** A quote request is in flight. */
  pricing: boolean;
  /** Plain copy for a quote that could not be priced (e.g. no subscription). */
  error: string | null;
  clearError: () => void;
  /** Seats this hook bought (confirmed in the dialog) since the last
   *  `clearAdded`. A page shows `seatsAddedNote(added)` when an invite
   *  sent after the purchase then failed or was skipped, so the admin knows
   *  the seats stayed and a re-send uses them. */
  added: number;
  clearAdded: () => void;
  /** Render once; it is closed until `requestSeats` opens it. */
  dialog: ReactElement;
}

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

export function useInviteSeats(): InviteSeats {
  const { getAuthToken } = useApp();
  const [quote, setQuote] = useState<SeatQuote | null>(null);
  const [context, setContext] = useState<{ free: number; count: number } | null>(null);
  const [pricing, setPricing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [added, setAdded] = useState(0);
  const after = useRef<(() => void) | null>(null);

  async function requestSeats(toBuy: number, free: number, count: number, send: () => void) {
    const token = getAuthToken();
    if (!token || toBuy < 1) return;
    setPricing(true);
    setError(null);
    try {
      const q = await api.billing.seatQuote(token, { quantity: toBuy, action: "add", tier: SEAT_TIER });
      after.current = send;
      setContext({ free, count });
      setQuote(q);
    } catch (e) {
      setError(billingErrorMessage(e, "Couldn't price the extra seats. Try again."));
    } finally {
      setPricing(false);
    }
  }

  const description = quote && context && (
    <>
      {context.free === 0
        ? `Every seat is taken, so ${context.count === 1 ? "this invite needs" : `these ${context.count} invites need`} ${plural(quote.quantity, "more seat")}.`
        : `You have ${plural(context.free, "free seat")}. ${context.count === 1 ? "This invite needs" : `These ${context.count} invites need`} ${quote.quantity} more.`}{" "}
      Nothing is charged right now: the prorated amount is added to your next invoice. The{" "}
      {context.count === 1 ? "invite sends" : "invites send"} once the {seatWord(quote.quantity)}{" "}
      {quote.quantity === 1 ? "is" : "are"} added.
    </>
  );

  const dialog = (
    <SeatQuoteDialog
      quote={quote}
      description={description ?? undefined}
      onCancel={() => {
        after.current = null;
        setQuote(null);
      }}
      onDone={(done) => {
        setAdded((n) => n + done.quantity);
        const send = after.current;
        after.current = null;
        setQuote(null);
        send?.();
      }}
    />
  );

  return {
    requestSeats,
    pricing,
    error,
    clearError: () => setError(null),
    added,
    clearAdded: () => setAdded(0),
    dialog,
  };
}
