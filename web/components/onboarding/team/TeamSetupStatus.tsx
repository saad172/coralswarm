"use client";

// Where Stripe sends the creator back to (success_url and cancel_url): polls
// the new team's setup with backoff and walks payment → setting up → ready,
// then switches into the team and lands on its /chats. Everything it decides
// comes from `setupView` (lib/orgSelfserve.ts); this file only renders it and
// runs the buttons.
//
// Every request here uses the creator's CURRENT session, never a token for
// the new team: the binding gate refuses that team until it is ready, so the
// switch (`setActive`) happens only once the status says `ready`.
//
// `owned` (this browser started the sign-up) gates the two things a link
// must never trigger on its own: paying, and switching accounts. A team
// opened by `?org=` alone gets neither; it gets an "Open <team>" button.

import { useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, ErrorNote, Notice } from "@/components/ui";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";
import { StepRail } from "@/components/onboarding/OnboardingChrome";
import { ApiError, type OrgSetupStatus, type OrgTrialCheckout } from "@/lib/api";
import { orgSetupStatusQueryOptions, type QueryCtx } from "@/lib/appQueries";
import {
  CANCELED_PATH,
  CREATE_PATH,
  RETURN_PATH,
  SETUP_STAGES,
  SUPPORT_EMAIL,
  formatWait,
  nextPollDelay,
  repeatTrialCopy,
  resumeErrorView,
  resumeSeats,
  retryErrorView,
  setupView,
  statusQuery,
  type ReturnPage,
  type SetupView,
} from "@/lib/orgSelfserve";

export interface TeamSetupStatusProps {
  ctx: QueryCtx;
  orgId: string;
  /** The attempt this team came from, when this browser (or the URL) has
   *  it: its name heads the card and it goes back into the new return URLs.
   *  `null` when the page was opened for a known team (`?org=`). The seats
   *  "Resume payment" buys come from the status body (`resumeSeats`). */
  attempt: { key: string; name: string; seats: number } | null;
  /** This browser started this team's sign-up. Default `true`. */
  owned?: boolean;
  page: ReturnPage;
  /** The standard seat price, for the repeat-trial copy. */
  priceCents: number | null;
  retry: (orgId: string) => Promise<OrgSetupStatus>;
  trialCheckout: (orgId: string, body: { seats: number; success_url: string; cancel_url: string }) => Promise<OrgTrialCheckout>;
  /** Switch into the ready team and open it (setActive, then a hard load). */
  openTeam: (orgId: string) => Promise<void>;
  goToCheckout: (url: string) => void | Promise<void>;
  goTo: (url: string) => void;
  returnUrl: (path: string, query: string) => string;
  /** Called once, before `openTeam` (drop the stored attempt, flag the banner). */
  onReady?: () => void;
  onStartOver?: () => void;
}

function supportHref(orgId: string): string {
  const subject = encodeURIComponent("Team setup didn't finish");
  const body = encodeURIComponent(`Team: ${orgId}`);
  return `mailto:${SUPPORT_EMAIL}?subject=${subject}&body=${body}`;
}

export function TeamSetupStatus({
  ctx,
  orgId,
  attempt,
  owned = true,
  page,
  priceCents,
  retry,
  trialCheckout,
  openTeam,
  goToCheckout,
  goTo,
  returnUrl,
  onReady,
  onStartOver,
}: TeamSetupStatusProps) {
  const queryClient = useQueryClient();
  const [mountedAt] = useState(() => Date.now());
  const [now, setNow] = useState(() => Date.now());
  const [busy, setBusy] = useState<null | "resume" | "retry" | "open">(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [retryWaitUntil, setRetryWaitUntil] = useState<number | null>(null);
  const [repeatCheckout, setRepeatCheckout] = useState<OrgTrialCheckout | null>(null);
  const viewRef = useRef<SetupView | null>(null);
  const opened = useRef(false);
  // The backoff counts polls since `pollBase`: reset on a retry and whenever
  // the screen changes, so a new phase starts at the quick end again.
  const pollBase = useRef(0);
  const pollKind = useRef<SetupView["kind"] | null>(null);
  const statusKey = orgSetupStatusQueryOptions(ctx, orgId).queryKey;

  const query = useQuery({
    ...orgSetupStatusQueryOptions(ctx, orgId),
    refetchInterval: (q) => {
      // Signed out: asking again can't help until they sign back in.
      if (q.state.error instanceof ApiError && q.state.error.status === 401) return false;
      const v = viewRef.current;
      if (!v) return nextPollDelay(0, { poll: true, kind: "checking" });
      const count = q.state.dataUpdateCount + q.state.errorUpdateCount;
      if (v.kind !== pollKind.current) {
        pollKind.current = v.kind;
        pollBase.current = count;
      }
      return nextPollDelay(count - pollBase.current, v);
    },
    refetchIntervalInBackground: true,
  });

  // A one-second clock while a time-based edge is ahead: the post-Stripe
  // grace, and a retry's `retry_after_s`.
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const err = query.error;
  const notFound = err instanceof ApiError && err.status === 404;
  const signedOut = err instanceof ApiError && err.status === 401;
  const view = setupView({ status: query.data ?? null, notFound, page, now, elapsedMs: now - mountedAt, owned });
  viewRef.current = view;
  // A failed read with an answer already on screen: keep it, say we're retrying.
  const stale = !!err && !notFound && !signedOut;

  useEffect(() => {
    // Only a team this browser started switches by itself; any other gets
    // the "Open" button (a link must never switch someone's account).
    if (view.kind !== "ready" || !owned || opened.current) return;
    opened.current = true;
    setBusy("open");
    onReady?.();
    openTeam(orgId).catch(() => {
      setBusy(null);
      setActionError("Your team is ready, but we couldn't switch you into it. Try again, or pick it from the account switcher.");
    });
  }, [view.kind, owned, orgId, openTeam, onReady]);

  function openNow(failure: string) {
    opened.current = true;
    setActionError(null);
    setBusy("open");
    openTeam(orgId).catch(() => {
      setBusy(null);
      setActionError(failure);
    });
  }

  const urls = () => {
    const q = statusQuery(orgId, attempt);
    return { success_url: returnUrl(RETURN_PATH, q), cancel_url: returnUrl(CANCELED_PATH, q) };
  };

  async function resume() {
    if (!owned) return;
    const seats = resumeSeats(query.data, attempt);
    if (seats === null) {
      setActionError(`We couldn't tell how many seats this team is buying. Start again, or email ${SUPPORT_EMAIL}.`);
      return;
    }
    setBusy("resume");
    setActionError(null);
    try {
      const tc = await trialCheckout(orgId, { seats, ...urls() });
      if (!tc.trial) {
        setRepeatCheckout(tc);
        setBusy(null);
        return;
      }
      await goToCheckout(tc.checkout_url);
    } catch (e) {
      const v = resumeErrorView(e);
      setActionError(v.message);
      setBusy(null);
      void queryClient.invalidateQueries({ queryKey: statusKey });
    }
  }

  async function restart() {
    setBusy("retry");
    setActionError(null);
    try {
      const data = await retry(orgId);
      queryClient.setQueryData(statusKey, data);
      // Restart the backoff from the top: count polls from here.
      const qs = queryClient.getQueryState(statusKey);
      pollBase.current = (qs?.dataUpdateCount ?? 0) + (qs?.errorUpdateCount ?? 0);
      void queryClient.invalidateQueries({ queryKey: statusKey });
    } catch (e) {
      const v = retryErrorView(e);
      setActionError(v.message);
      if (v.retryAfterS) setRetryWaitUntil(Date.now() + v.retryAfterS * 1000);
      if (v.refresh) void queryClient.invalidateQueries({ queryKey: statusKey });
    } finally {
      setBusy(null);
    }
  }

  function startOver() {
    onStartOver?.();
    goTo(CREATE_PATH);
  }

  const retryWaiting = retryWaitUntil !== null && now < retryWaitUntil;
  const teamName = attempt?.name ?? null;
  const title = view.title;
  const stage = Math.min(view.stage, SETUP_STAGES.length);

  if (repeatCheckout) {
    return (
      <StepCard eyebrow="new team" title="Your subscription starts today" width={560}>
        <Notice tone="warn" className="mb-[16px]">
          {repeatTrialCopy(repeatCheckout.seats, priceCents)}
        </Notice>
        <WizardActions>
          <Button
            variant="primary"
            disabled={busy !== null}
            onClick={() => {
              setBusy("resume");
              void goToCheckout(repeatCheckout.checkout_url);
            }}
          >
            {busy === "resume" ? "Opening payment…" : "Continue to payment"}
          </Button>
          <Button variant="ghost" onClick={() => setRepeatCheckout(null)}>
            Back
          </Button>
        </WizardActions>
      </StepCard>
    );
  }

  return (
    <StepCard eyebrow={teamName ?? "new team"} title={title} width={560}>
      <StepRail steps={[...SETUP_STAGES]} currentIndex={stage} className="mb-[18px]" />
      <p role="status" aria-live="polite" className="mb-[18px] text-[13.5px] leading-[1.6] text-ink-2">
        {signedOut ? "You're signed out. Sign in with the account that created this team to see its setup." : view.body}
      </p>

      {actionError && <ErrorNote className="mb-[14px]">{actionError}</ErrorNote>}

      <WizardActions
        trailing={
          view.poll && !signedOut ? (
            <span aria-live="polite">{stale ? "can't reach CoralSwarm, still trying" : "this page updates by itself"}</span>
          ) : undefined
        }
      >
        {view.canResume && (
          <Button variant="primary" onClick={() => void resume()} disabled={busy !== null}>
            {busy === "resume" ? "Opening payment…" : "Resume payment"}
          </Button>
        )}
        {view.canRetry && (
          <Button variant="primary" onClick={() => void restart()} disabled={busy !== null || retryWaiting}>
            {busy === "retry" ? "Retrying…" : retryWaiting && retryWaitUntil ? `Retry in ${formatWait((retryWaitUntil - now) / 1000)}` : "Retry setup"}
          </Button>
        )}
        {((view.kind === "ready" && owned && actionError) || view.canOpen) && (
          <Button
            variant={view.kind === "ready" ? "primary" : "ghost"}
            disabled={busy === "open"}
            onClick={() =>
              // `opened` stays true: only this click switches now, never the
              // effect as well.
              openNow("We couldn't switch you into this team. Pick it from the account switcher.")
            }
          >
            {busy === "open"
              ? "Opening…"
              : teamName
                ? `Open \u201c${teamName}\u201d`
                : owned
                  ? "Open your team"
                  : "Open this team"}
          </Button>
        )}
        {view.canStartOver && (
          <Button variant="primary" onClick={startOver}>
            Start again
          </Button>
        )}
        {(view.kind === "failed" || view.kind === "failed_retryable") && (
          <a href={supportHref(orgId)} className="text-[13px] text-ink-2 underline">
            Contact support
          </a>
        )}
        {(view.kind === "not_found" ||
          view.kind === "not_ours" ||
          view.kind === "abandoned" ||
          view.kind === "canceled" ||
          page === "canceled") &&
          view.kind !== "ready" && (
            <Button variant="ghost" onClick={() => goTo("/chats")}>
              Back to CoralSwarm
            </Button>
          )}
      </WizardActions>
    </StepCard>
  );
}
