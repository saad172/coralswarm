"use client";

// Stripe Checkout's cancel_url for a new team: the team is saved but not
// paid for, so this offers "Resume payment" while the 24-hour window is open.
// It still follows the status, in case the payment went through after all.

import { Suspense } from "react";
import { TeamReturn } from "@/components/onboarding/team/TeamReturn";
import { TeamSignupShell } from "@/components/onboarding/team/TeamSignupShell";

export default function TeamCanceledPage() {
  return (
    <TeamSignupShell continueWhenFlagUnknown>
      {(deps) => (
        <Suspense fallback={null}>
          <TeamReturn deps={deps} page="canceled" />
        </Suspense>
      )}
    </TeamSignupShell>
  );
}
