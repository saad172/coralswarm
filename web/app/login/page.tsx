"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { SignIn, useAuth } from "@clerk/nextjs";
import AuthShell from "@/components/auth/AuthShell";
import { consumePostSignInRedirect } from "@/lib/postSignInRedirect";

// Sign-in goes through Clerk's <SignIn> (ADR-0007 D4).
function WebLogin() {
  const { isLoaded, isSignedIn } = useAuth();
  const router = useRouter();

  useEffect(() => {
    // Stateful deep links return to their saved path/query after sign-in:
    // Gmail add-on pairing codes and signed Teams onboarding tickets both
    // need the original URL. Ordinary sign-ins fall back to /home.
    if (isLoaded && isSignedIn) router.replace(consumePostSignInRedirect() ?? "/home");
  }, [isLoaded, isSignedIn, router]);

  // Social-provider logos are restyled in globals.css — clerk-js renders them as
  // CSS masks pointing at img.clerk.com/static/<provider>.svg, which 415s (the
  // CDN doesn't serve SVG), so we re-point them at the working .png assets there.
  //
  // Sign-up is always offered here. It used to be suppressed on `<slug>.<apex>`,
  // where self-serve sign-up was meaningless because orgs are joined by
  // invitation — but that condition was read off the hostname, and claim-based
  // tenancy (PR 2.1) leaves one apex serving every tenant, so the gate was
  // constantly true and could only ever mislead. Invited members still arrive
  // through /join, never here.
  //
  // Appearance is set once on <ClerkProvider> (app/layout.tsx →
  // lib/clerkAppearance.ts) so every Clerk surface stays identical.
  return <SignIn routing="hash" fallbackRedirectUrl="/home" signUpUrl="/signup" />;
}

export default function LoginPage() {
  // AuthShell sits a step down the surface scale (--bg-2) so the Clerk card,
  // which stays on --bg, actually reads as a raised surface — same
  // background on both made the card vanish into the page. It also supplies
  // the wordmark and the floating host chip; see components/auth/AuthShell.tsx.
  return <AuthShell><WebLogin /></AuthShell>;
}
