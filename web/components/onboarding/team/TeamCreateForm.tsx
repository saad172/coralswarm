"use client";

// "Create a team": name + seats, then `POST /v1/orgs` and off to Stripe
// Checkout. The server creates the team and its Checkout in one call; the
// team's database is built only after Checkout completes (founder decision,
// 2026-10-27), which the return page follows.

import { useEffect, useRef, useState, type FormEvent } from "react";
import { Button, ErrorNote, Input, Notice } from "@/components/ui";
import { WizardActions } from "@/components/onboarding/StepCard";
import type { OrgCreateRequest, OrgCreateResult, OrgTrialCheckout } from "@/lib/api";
import {
  CANCELED_PATH,
  CREATE_IN_PROGRESS_MAX_RETRIES,
  DEFAULT_SEATS,
  MAX_SEATS,
  MIN_SEATS,
  RETURN_PATH,
  attemptFor,
  attemptQuery,
  createErrorView,
  dropAttempt,
  formatSeatPrice,
  formatWait,
  readAttempt,
  readAttempts,
  recordAttemptOrg,
  repeatTrialCopy,
  resumeErrorView,
  statusQuery,
  trialCopy,
  validateCreateForm,
  type AttemptStore,
  type CreateFormErrors,
  type ErrorView,
  type SignupAttempt,
} from "@/lib/orgSelfserve";

export interface TeamCreateFormProps {
  /** `api.orgSelfserve.create` with the bearer already bound. */
  create: (body: OrgCreateRequest, key: string) => Promise<OrgCreateResult>;
  /** `api.orgSelfserve.trialCheckout` with the bearer already bound. */
  trialCheckout: (orgId: string, body: { seats: number; success_url: string; cancel_url: string }) => Promise<OrgTrialCheckout>;
  /** Leave for a Stripe Checkout URL (`goToStripe`). */
  goToCheckout: (url: string) => void | Promise<void>;
  /** Go to one of our own pages (a hard load), by path. */
  goTo: (url: string) => void;
  /** Absolute URL on this app for Stripe to send people back to. */
  returnUrl: (path: string, query: string) => string;
  /** The standard seat's monthly price, when known. */
  priceCents: number | null;
  /** This person's stored attempts (`signupStore(userId)`). */
  store: AttemptStore | null;
  mintKey?: () => string;
}

/** Overridable in tests (`sleepForTests`). */
let sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
export function setSleepForTests(fn: (ms: number) => Promise<void>): void {
  sleep = fn;
}

interface RepeatTrial {
  checkoutUrl: string;
  seats: number;
  orgId: string;
  attempt: SignupAttempt;
}

export function TeamCreateForm({
  create,
  trialCheckout,
  goToCheckout,
  goTo,
  returnUrl,
  priceCents,
  store,
  mintKey,
}: TeamCreateFormProps) {
  // A half-finished sign-up in this browser comes back with its values, so a
  // retry reuses its key and gets the same team back.
  const [name, setName] = useState(() => readAttempt(store)?.name ?? "");
  const [seats, setSeats] = useState(() => String(readAttempt(store)?.seats ?? DEFAULT_SEATS));
  const [fieldErrors, setFieldErrors] = useState<CreateFormErrors>({});
  const [error, setError] = useState<ErrorView | null>(null);
  const [busy, setBusy] = useState(false);
  const [repeat, setRepeat] = useState<RepeatTrial | null>(null);
  // The attempt the last error was about (for "See its setup").
  const [errorAttempt, setErrorAttempt] = useState<SignupAttempt | null>(null);
  const [waitUntil, setWaitUntil] = useState<number | null>(null);
  const [now, setNow] = useState(() => Date.now());
  // A ref, not state: two clicks in one frame both see `busy === false`.
  const inFlight = useRef(false);

  useEffect(() => {
    if (waitUntil === null) return;
    const id = setInterval(() => {
      const t = Date.now();
      setNow(t);
      if (t >= waitUntil) {
        setWaitUntil(null);
        // The message named the wait; once it's over it no longer applies.
        setError(null);
      }
    }, 1000);
    return () => clearInterval(id);
  }, [waitUntil]);

  const waiting = waitUntil !== null && now < waitUntil;
  const seatCount = /^\d+$/.test(seats.trim()) ? Number(seats.trim()) : DEFAULT_SEATS;
  const shownSeats = Math.min(MAX_SEATS, Math.max(MIN_SEATS, seatCount));
  const price = formatSeatPrice(priceCents);

  async function submit(e?: FormEvent) {
    e?.preventDefault();
    if (inFlight.current || waiting) return;
    const valid = validateCreateForm({ name, seats });
    if (!valid.ok) {
      setFieldErrors(valid.errors);
      return;
    }
    setFieldErrors({});
    setError(null);
    inFlight.current = true;
    setBusy(true);
    const attempt = attemptFor(store, valid.name, valid.seats, mintKey ?? (() => crypto.randomUUID()));
    setErrorAttempt(attempt);
    const query = attemptQuery(attempt);
    const urls = { success_url: returnUrl(RETURN_PATH, query), cancel_url: returnUrl(CANCELED_PATH, query) };
    let leaving = false;
    try {
      const body = { name: valid.name, seats: valid.seats, ...urls };
      // 409 `create_in_progress`: another submit of this same attempt is
      // still running. Wait and resend the SAME key; the answer is then
      // that submit's replay. Bounded, then the message shows.
      let res: OrgCreateResult | null = null;
      for (let tries = 0; res === null; tries++) {
        try {
          res = await create(body, attempt.key);
        } catch (err) {
          const v = createErrorView(err);
          if (!v.autoRetryMs || tries >= CREATE_IN_PROGRESS_MAX_RETRIES) throw err;
          await sleep(v.autoRetryMs);
        }
      }
      if (!res) return;
      recordAttemptOrg(store, attempt.key, res.org_id);
      const orgId = res.org_id;
      if (res.checkout_url) {
        if (res.trial === false) {
          setRepeat({ checkoutUrl: res.checkout_url, seats: res.seats, orgId, attempt });
          return;
        }
        leaving = true;
        await goToCheckout(res.checkout_url);
        return;
      }
      if (res.resume_payment) {
        // A newer "resume payment" Checkout replaced the first one.
        try {
          const tc = await trialCheckout(res.org_id, { seats: valid.seats, ...urls });
          if (!tc.trial) {
            setRepeat({ checkoutUrl: tc.checkout_url, seats: tc.seats, orgId, attempt });
            return;
          }
          leaving = true;
          await goToCheckout(tc.checkout_url);
        } catch (err) {
          const v = resumeErrorView(err);
          if (v.startOver) dropAttempt(store, attempt.key);
          if (v.alreadyPaid) {
            // Already paid: its setup page says the rest.
            leaving = true;
            goTo(`${RETURN_PATH}?${statusQuery(orgId, attempt)}`);
            return;
          }
          setError(v);
        }
        return;
      }
      // The Checkout is already done: the return page says what happened.
      leaving = true;
      goTo(`${RETURN_PATH}?${statusQuery(orgId, attempt)}`);
    } catch (err) {
      const v = createErrorView(err);
      if (v.statusOrgId) {
        // 409 `signup_complete`: this sign-up is already paid for. Follow
        // its setup instead of showing an error.
        recordAttemptOrg(store, attempt.key, v.statusOrgId);
        leaving = true;
        goTo(`${RETURN_PATH}?${statusQuery(v.statusOrgId, attempt)}`);
        return;
      }
      if (!v.keepAttempt) dropAttempt(store, attempt.key);
      if (v.field) setFieldErrors({ [v.field]: v.message });
      if (v.retryAfterS) {
        setNow(Date.now());
        setWaitUntil(Date.now() + v.retryAfterS * 1000);
      }
      setError(v.field ? null : v);
    } finally {
      inFlight.current = false;
      // Leaving for Stripe: keep the button busy until the page unloads.
      if (!leaving) setBusy(false);
    }
  }

  if (repeat) {
    return (
      <div>
        <Notice tone="warn" className="mb-[16px]">
          {repeatTrialCopy(repeat.seats, priceCents)}
        </Notice>
        <p className="mb-[18px] text-[13px] leading-[1.6] text-ink-2">
          Your team is set up as soon as the payment goes through. You can cancel any time in Settings &rarr; Billing.
        </p>
        <WizardActions>
          <Button
            variant="primary"
            disabled={busy}
            onClick={() => {
              setBusy(true);
              void (async () => {
                await goToCheckout(repeat.checkoutUrl);
              })();
            }}
          >
            {busy ? "Opening payment…" : "Continue to payment"}
          </Button>
          <Button variant="ghost" disabled={busy} onClick={() => setRepeat(null)}>
            Back
          </Button>
        </WizardActions>
      </div>
    );
  }

  const remaining = waiting && waitUntil !== null ? (waitUntil - now) / 1000 : 0;

  return (
    <form onSubmit={submit} noValidate>
      <Input
        label="team name"
        value={name}
        onChange={(e) => setName(e.target.value)}
        placeholder="e.g. Northwind Traders"
        autoComplete="organization"
        maxLength={120}
        wrapperClassName="mb-[14px]"
        disabled={busy}
        error={fieldErrors.name}
        hint={fieldErrors.name ? undefined : "Your team sees this name. You can change it later."}
      />
      <Input
        label="seats"
        type="number"
        inputMode="numeric"
        min={MIN_SEATS}
        max={MAX_SEATS}
        step={1}
        value={seats}
        onChange={(e) => setSeats(e.target.value)}
        wrapperClassName="mb-[16px] max-w-[340px]"
        disabled={busy}
        error={fieldErrors.seats}
        hint={
          fieldErrors.seats
            ? undefined
            : price
              ? `${price} per seat per month after the trial. At least ${MIN_SEATS}.`
              : `At least ${MIN_SEATS}. You can add more later.`
        }
      />

      <div className="mb-[18px] rounded-card border border-line bg-bg-2 px-[16px] py-[14px]" aria-label="How the free trial works">
        <ul className="flex flex-col gap-[7px] text-[12.5px] leading-[1.55] text-ink-2">
          {trialCopy(shownSeats, priceCents).map((line, i) => (
            <li key={line} className={i === 0 ? "font-medium text-ink" : undefined}>
              {line}
            </li>
          ))}
        </ul>
      </div>

      {error && (
        <ErrorNote className="mb-[14px]">
          {error.message}
          {error.verifyEmail && (
            <a href="/settings" className="mt-[4px] block underline">
              Open Settings &rsaquo; Account
            </a>
          )}
          {error.statusLink && errorAttempt && (
            <a
              href={`${RETURN_PATH}?${(() => {
                const orgId = readAttempts(store).find((a) => a.key === errorAttempt.key)?.orgId;
                return orgId ? statusQuery(orgId, errorAttempt) : attemptQuery(errorAttempt);
              })()}`}
              className="mt-[4px] block underline"
            >
              See your team&rsquo;s setup
            </a>
          )}
          {error.startOver && (
            <button
              type="button"
              className="mt-[4px] block underline"
              onClick={() => {
                // The old attempt is already dropped; a submit now mints a
                // new key, i.e. a new sign-up.
                if (errorAttempt) dropAttempt(store, errorAttempt.key);
                setError(null);
              }}
            >
              Start again
            </button>
          )}
        </ErrorNote>
      )}

      <WizardActions trailing={waiting ? `ready again in ${formatWait(remaining)}` : "you'll be its admin"}>
        <Button type="submit" variant="primary" disabled={busy || waiting}>
          {busy ? "Creating…" : "Continue to payment"}
        </Button>
      </WizardActions>
    </form>
  );
}
