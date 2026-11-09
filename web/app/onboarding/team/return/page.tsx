"use client";

// Stripe Checkout's success_url for a new team: follows the team from
// payment through setup to ready, then opens it. <Suspense> because
// TeamReturn reads useSearchParams, which the build needs bounded.

import { Suspense } from "react";
import { TeamReturn } from "@/components/onboarding/team/TeamReturn";
import { TeamSignupShell } from "@/components/onboarding/team/TeamSignupShell";

export default function TeamReturnPage() {
  return (
    <TeamSignupShell continueWhenFlagUnknown>
      {(deps) => (
        <Suspense fallback={null}>
          <TeamReturn deps={deps} page="return" />
        </Suspense>
      )}
    </TeamSignupShell>
  );
}
