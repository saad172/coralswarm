"use client";

// Stripe's success_url and cancel_url both land here. First work out WHICH
// team this is, and whether this browser started it:
//
//   * an attempt this browser stored for the `?org=` team, or for the URL's
//     key: ours. Follow it, and switch into the team once it is ready;
//   * an attempt only in the URL (another browser, or a link someone sent): ASK first ("Continue setting up
//     '<name>'?"). Only a click replays `POST /v1/orgs` with its key. A
//     replay of the person's own key never creates a second team, but a
//     crafted link carries a fresh key, and opening a link must never create
//     a team in the reader's account;
//   * just `?org=` (the switcher, or a link): follow that team's status, but
//     never pay for it, never switch into it without a click, and never
//     touch this browser's stored attempts, which belong to other teams.

import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import { Button, ErrorNote } from "@/components/ui";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";
import {
  CANCELED_PATH,
  CREATE_IN_PROGRESS_MAX_RETRIES,
  CREATE_PATH,
  JUST_CREATED_KEY,
  RETURN_PATH,
  attemptByKey,
  attemptByOrg,
  attemptFromQuery,
  attemptQuery,
  createErrorView,
  dropAttempt,
  dropAttemptForOrg,
  orgFromQuery,
  recordAttemptOrg,
  rememberAttempt,
  type ErrorView,
  type ReturnPage,
  type SignupAttempt,
} from "@/lib/orgSelfserve";
import type { TeamSignupDeps } from "./TeamSignupShell";
import { TeamSetupStatus } from "./TeamSetupStatus";

type Attempt = Omit<SignupAttempt, "orgId">;

type Resolved =
  | { kind: "resolving" }
  /** `owned`: this browser started (or the person just confirmed) it. */
  | { kind: "ok"; orgId: string; attempt: Attempt | null; owned: boolean }
  /** An attempt only the URL knows about: wait for a click. */
  | { kind: "confirm"; attempt: Attempt; busy: boolean }
  | { kind: "none" }
  | { kind: "error"; view: ErrorView; attempt: Attempt | null };

/** Where the page starts, from the URL and this browser's store. Pure. */
export function resolveReturn(
  params: Pick<URLSearchParams, "get">,
  store: TeamSignupDeps["store"],
): { kind: "ok"; orgId: string; attempt: Attempt | null; owned: boolean } | { kind: "replay"; attempt: Attempt } | { kind: "confirm"; attempt: Attempt } | { kind: "none" } {
  const org = orgFromQuery(params);
  const fromUrl = attemptFromQuery(params);
  // Ours: a stored attempt for this exact team.
  if (org) {
    const mine = attemptByOrg(store, org);
    if (mine) return { kind: "ok", orgId: org, attempt: mine, owned: true };
  }
  if (fromUrl) {
    const mine = attemptByKey(store, fromUrl.key);
    if (mine && mine.name === fromUrl.name && mine.seats === fromUrl.seats) {
      if (mine.orgId && (!org || mine.orgId === org)) return { kind: "ok", orgId: mine.orgId, attempt: mine, owned: true };
      // Our own attempt, its org not recorded yet: replaying our own key is
      // safe without asking.
      if (!mine.orgId) return { kind: "replay", attempt: mine };
    }
    return { kind: "confirm", attempt: fromUrl };
  }
  if (org) return { kind: "ok", orgId: org, attempt: null, owned: false };
  return { kind: "none" };
}

export function TeamReturn({ deps, page }: { deps: TeamSignupDeps; page: ReturnPage }) {
  const params = useSearchParams();
  const [state, setState] = useState<Resolved>({ kind: "resolving" });
  const live = useRef(true);

  async function replay(attempt: Attempt, confirmed: boolean) {
    const q = attemptQuery(attempt);
    const body = {
      name: attempt.name,
      seats: attempt.seats,
      success_url: deps.returnUrl(RETURN_PATH, q),
      cancel_url: deps.returnUrl(CANCELED_PATH, q),
    };
    // Confirmed by a click: this browser now follows the team (so the
    // switcher knows it, and the person can resume its payment here).
    const remember = (orgId: string) => {
      if (confirmed) rememberAttempt(deps.store, { ...attempt, orgId });
      else recordAttemptOrg(deps.store, attempt.key, orgId);
    };
    for (let tries = 0; ; tries++) {
      try {
        const res = await deps.create(body, attempt.key);
        remember(res.org_id);
        if (live.current) setState({ kind: "ok", orgId: res.org_id, attempt, owned: true });
        return;
      } catch (err) {
        const view = createErrorView(err);
        // 409 `signup_complete`: already paid; that is the team.
        if (view.statusOrgId) {
          remember(view.statusOrgId);
          if (live.current) setState({ kind: "ok", orgId: view.statusOrgId, attempt, owned: true });
          return;
        }
        if (view.autoRetryMs && tries < CREATE_IN_PROGRESS_MAX_RETRIES) {
          await new Promise((r) => setTimeout(r, view.autoRetryMs));
          continue;
        }
        if (!view.keepAttempt) dropAttempt(deps.store, attempt.key);
        if (live.current) setState({ kind: "error", view, attempt });
        return;
      }
    }
  }

  useEffect(() => {
    live.current = true;
    const start = resolveReturn(params, deps.store);
    if (start.kind === "replay") void replay(start.attempt, false);
    else if (start.kind === "confirm") setState({ kind: "confirm", attempt: start.attempt, busy: false });
    else setState(start);
    return () => {
      live.current = false;
    };
    // `params` is stable for this page load; `deps` changes with the bearer.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (state.kind === "resolving") {
    return (
      <p role="status" className="font-mono text-sm text-ink-3">
        checking&hellip;
      </p>
    );
  }

  if (state.kind === "confirm") {
    return (
      <StepCard eyebrow="new team" title={`Continue setting up “${state.attempt.name}”?`} width={520}>
        <p className="mb-[18px] text-[13.5px] leading-[1.6] text-ink-2">
          This link is for a team sign-up started somewhere else, like another browser.
          Continue only if you started it. If you didn&rsquo;t, close this page: nothing happens until you continue.
        </p>
        <WizardActions>
          <Button
            variant="primary"
            disabled={state.busy}
            onClick={() => {
              setState({ ...state, busy: true });
              void replay(state.attempt, true);
            }}
          >
            {state.busy ? "Checking…" : "Continue"}
          </Button>
          <Button variant="ghost" disabled={state.busy} onClick={() => deps.goTo("/chats")}>
            Back to CoralSwarm
          </Button>
        </WizardActions>
      </StepCard>
    );
  }

  if (state.kind === "none" || state.kind === "error") {
    const failed = state.kind === "error" ? state : null;
    return (
      <StepCard title="We couldn't find this sign-up" width={520}>
        {failed ? (
          <ErrorNote className="mb-[16px]">{failed.view.message}</ErrorNote>
        ) : (
          <p className="mb-[18px] text-[13.5px] leading-[1.6] text-ink-2">
            This page didn&rsquo;t say which team it was for. If you paid, your team appears in the account switcher as
            soon as it&rsquo;s set up.
          </p>
        )}
        <WizardActions>
          <Button
            variant="primary"
            onClick={() => {
              // Only the attempt this page was about; any other team this
              // browser is setting up stays remembered.
              if (failed?.attempt) dropAttempt(deps.store, failed.attempt.key);
              deps.goTo(CREATE_PATH);
            }}
          >
            Start again
          </Button>
          <Button variant="ghost" onClick={() => deps.goTo("/chats")}>
            Back to CoralSwarm
          </Button>
        </WizardActions>
      </StepCard>
    );
  }

  const { orgId, owned } = state;
  return (
    <TeamSetupStatus
      ctx={deps.ctx}
      orgId={orgId}
      attempt={state.attempt}
      owned={owned}
      page={page}
      priceCents={deps.priceCents}
      retry={deps.retry}
      trialCheckout={deps.trialCheckout}
      openTeam={deps.openTeam}
      goToCheckout={deps.goToCheckout}
      goTo={deps.goTo}
      returnUrl={deps.returnUrl}
      onReady={() => {
        // Only this team's attempt, and only when it is ours.
        if (!owned) return;
        dropAttemptForOrg(deps.store, orgId);
        try {
          window.sessionStorage.setItem(JUST_CREATED_KEY, orgId);
        } catch {
          /* the banner just won't explain a repeat-card conversion */
        }
      }}
      onStartOver={() => {
        if (owned) dropAttemptForOrg(deps.store, orgId);
      }}
    />
  );
}
