"use client";

import { Suspense, useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { SignUp, useAuth } from "@clerk/nextjs";
import AuthShell from "@/components/auth/AuthShell";
import { Loading } from "@/components/ui";
import { useQuery } from "@tanstack/react-query";
import { emailPrefillQueryOptions } from "@/lib/appQueries";

// Sign-up goes through Clerk's <SignUp> (ADR-0007 D4).
function WebSignup() {
  const { isLoaded, isSignedIn } = useAuth();
  const router = useRouter();
  // `?pt=` prefill (CodeRabbit round 1, #1005). The connector nudge email
  // (`services::unmatched_recipient_email`) sends unmatched Slack members
  // here with an OPAQUE token, never the address itself — an earlier
  // version carried the raw email as `?email=`, which browser history,
  // proxy access logs, analytics, and `Referer` headers can all retain.
  // `routes::email_prefill` resolves the token to the address it was minted
  // for; this is a fetch, not a redirect, so the address never appears in
  // any URL at all. `undefined` on any failure (no token, unknown/expired
  // token, network error) — the form just renders with an empty email box
  // instead, never a wrong one, matching the query-param version's own
  // "prefill only" contract below.
  const prefillToken = useSearchParams().get("pt");
  const emailParam = useQuery({ ...emailPrefillQueryOptions(prefillToken ?? ""), enabled: isLoaded && !isSignedIn && !!prefillToken }).data ?? undefined;

  useEffect(() => {
    if (isLoaded && isSignedIn) router.replace("/home");
  }, [isLoaded, isSignedIn, router]);

  if (!isLoaded || isSignedIn) return <Loading what="surfacing" className="py-[24px]" />;

  // This page used to bounce to /login on `<slug>.<apex>`, where self-serve
  // sign-up didn't exist. Claim-based tenancy (PR 2.1) removed org hosts, so
  // that guard could never fire again and is gone with them — an org is still
  // joined by invitation, through /onboarding/join, which never routes here.
  //
  // Social-provider logos are restyled in globals.css — clerk-js renders them as
  // CSS masks pointing at img.clerk.com/static/<provider>.svg, which 415s (the
  // CDN doesn't serve SVG), so we re-point them at the working .png assets there.
  // Appearance is set once on <ClerkProvider> (app/layout.tsx →
  // lib/clerkAppearance.ts) so every Clerk surface stays identical.
  //
  // .cs-signup-cta: 4c's primary CTA is coral, not the ink used everywhere
  // else Clerk renders formButtonPrimary (4a/4b sign-in, and every Clerk
  // surface in Settings). Since the appearance object is shared across all
  // of them (see the long comment in app/globals.css on why this page never
  // gets its own `appearance` prop), the colour swap is a scoped CSS rule
  // rather than a second appearance object.
  return (
    <div className="cs-signup-cta">
      <SignUp
        routing="hash"
        signInUrl="/login"
        fallbackRedirectUrl="/home"
        initialValues={emailParam ? { emailAddress: emailParam } : undefined}
      />
    </div>
  );
}

export default function SignupPage() {
  // --bg-2 page, --bg card — see app/login/page.tsx for why they differ.
  //
  // <Suspense> is REQUIRED, not decoration: `WebSignup` reads
  // `useSearchParams()` for the `?pt=` prefill token, and Next 15 fails the
  // build outright ("useSearchParams() should be wrapped in a suspense
  // boundary") for a page that reads it outside one — which would break the
  // whole build, not just this route.
  // `app/onboarding/join/page.tsx` wraps its own params consumer the same way
  // for the same reason.
  return (
    <AuthShell>
      <Suspense fallback={<Loading what="surfacing" className="py-[24px]" />}>
        <WebSignup />
      </Suspense>
    </AuthShell>
  );
}
