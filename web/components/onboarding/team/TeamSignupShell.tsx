"use client";

// The frame every team sign-up page shares: the onboarding chrome, the
// signed-out redirect (these pages live outside the `(app)` layout, which
// does that for app pages), the boot-error state, and the feature-flag gate.
//
// The flag gate has three answers, not two. Off: the "isn't open yet" card.
// Unknown (the bootstrap read failed): the create form says so and offers a
// retry, but the return pages carry on, because the status endpoint is the
// real authority over a team that already exists, and a flaky read must
// never lock a creator out of a team they paid for.
// It hands the page its wired dependencies, so the form and status
// components stay plain props and render in tests without Clerk.

import { useEffect, useMemo, type ReactNode } from "react";
import { useRouter } from "next/navigation";
import { useClerk } from "@clerk/nextjs";
import { useQuery } from "@tanstack/react-query";
import { OnboardingChrome, HeaderPill } from "@/components/onboarding/OnboardingChrome";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";
import { Button } from "@/components/ui";
import { api } from "@/lib/api";
import { orgSelfserveEnabledQueryOptions, seatTiersQueryOptions, type QueryCtx } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";
import { setPostSignInRedirect } from "@/lib/postSignInRedirect";
import { goToStripe, returnUrl } from "@/components/billing/navigate";
import type { OrgCreateRequest } from "@/lib/api";
import { signupStore, type AttemptStore } from "@/lib/orgSelfserve";

export interface TeamSignupDeps {
  ctx: QueryCtx;
  /** This signed-in person's stored sign-up attempts in this browser. */
  store: AttemptStore | null;
  priceCents: number | null;
  create: (body: OrgCreateRequest, key: string) => ReturnType<typeof api.orgSelfserve.create>;
  retry: (orgId: string) => ReturnType<typeof api.orgSelfserve.retry>;
  trialCheckout: (
    orgId: string,
    body: { seats: number; success_url: string; cancel_url: string },
  ) => ReturnType<typeof api.orgSelfserve.trialCheckout>;
  openTeam: (orgId: string) => Promise<void>;
  goToCheckout: (url: string) => Promise<void>;
  goTo: (url: string) => void;
  returnUrl: (path: string, query: string) => string;
}

function bearerFrom(getAuthToken: () => string | null): string {
  const t = getAuthToken();
  if (!t) throw new Error("Not authenticated");
  return t;
}

export function TeamSignupShell({
  children,
  continueWhenFlagUnknown = false,
}: {
  children: (deps: TeamSignupDeps) => ReactNode;
  /** The return pages: render on a failed flag read (see the header). */
  continueWhenFlagUnknown?: boolean;
}) {
  const { ready, authReady, bootError, retryBoot, getAuthToken, queryScope, userId } = useApp();
  const router = useRouter();
  const clerk = useClerk();

  useEffect(() => {
    if (ready && !authReady) {
      setPostSignInRedirect(window.location.pathname + window.location.search);
      router.replace("/login");
    }
  }, [ready, authReady, router]);

  const ctx = useMemo<QueryCtx>(() => ({ getAuthToken, scope: queryScope }), [getAuthToken, queryScope]);
  const flag = useQuery({ ...orgSelfserveEnabledQueryOptions(ctx), enabled: ready && authReady });
  const tiers = useQuery({ ...seatTiersQueryOptions(ctx), enabled: ready && authReady });
  const priceCents = tiers.data?.find((t) => t.code === "standard")?.price_usd_cents ?? null;

  const store = useMemo(() => signupStore(userId), [userId]);

  const deps = useMemo<TeamSignupDeps>(
    () => ({
      ctx,
      store,
      priceCents,
      create: (body, key) => api.orgSelfserve.create(bearerFrom(getAuthToken), body, key),
      retry: (orgId) => api.orgSelfserve.retry(bearerFrom(getAuthToken), orgId),
      trialCheckout: (orgId, body) => api.orgSelfserve.trialCheckout(bearerFrom(getAuthToken), orgId, body),
      // Same order as components/instanceSwitch.ts: let Clerk persist the
      // switch, THEN a hard load, so the next page mints a token for the new
      // team (a soft navigation would keep the old org claim).
      openTeam: async (orgId) => {
        await clerk.setActive({ organization: orgId });
        window.location.assign("/chats");
      },
      goToCheckout: (url) => goToStripe(url),
      goTo: (url) => window.location.assign(url),
      returnUrl: (path, query) => returnUrl(path, query),
    }),
    [ctx, store, priceCents, getAuthToken, clerk],
  );

  let body: ReactNode;
  if (!ready) {
    body = bootError ? (
      <StepCard title="Couldn't open CoralSwarm" width={520}>
        <p className="mb-[18px] text-[13.5px] leading-[1.6] text-ink-2">{bootError}</p>
        <WizardActions>
          <Button variant="primary" onClick={retryBoot}>
            Try again
          </Button>
        </WizardActions>
      </StepCard>
    ) : (
      <p role="status" className="font-mono text-sm text-ink-3">
        surfacing&hellip;
      </p>
    );
  } else if (!authReady) {
    body = null;
  } else if (flag.isPending) {
    body = (
      <p role="status" className="font-mono text-sm text-ink-3">
        checking&hellip;
      </p>
    );
  } else if (flag.isError && !continueWhenFlagUnknown) {
    body = (
      <StepCard title="We couldn't check whether team sign-up is open" width={520}>
        <p className="mb-[18px] text-[13.5px] leading-[1.6] text-ink-2">
          CoralSwarm didn&rsquo;t answer. Check your connection and try again.
        </p>
        <WizardActions>
          <Button variant="primary" onClick={() => void flag.refetch()}>
            Try again
          </Button>
          <Button variant="ghost" onClick={() => window.location.assign("/chats")}>
            Back to CoralSwarm
          </Button>
        </WizardActions>
      </StepCard>
    );
  } else if (flag.data === false) {
    body = (
      <StepCard title="Creating a team isn't open yet" width={520}>
        <p className="mb-[18px] text-[13.5px] leading-[1.6] text-ink-2">
          Teams are being set up by hand for now. Email person@example.com if you&rsquo;d like one.
        </p>
        <WizardActions>
          <Button variant="primary" onClick={() => window.location.assign("/chats")}>
            Back to CoralSwarm
          </Button>
        </WizardActions>
      </StepCard>
    );
  } else {
    body = children(deps);
  }

  return <OnboardingChrome right={<HeaderPill>new team</HeaderPill>}>{body}</OnboardingChrome>;
}
