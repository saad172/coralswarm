"use client";

// "Create a team": the self-serve team sign-up form (PR B, on A2 #1533 and
// C′ #1521). Name and seats, then Stripe Checkout: card up front, $0 today,
// a free-credit trial. Hidden (an "isn't open yet" card) while the backend's
// sign-up flag is off.

import { StepCard } from "@/components/onboarding/StepCard";
import { TeamCreateForm } from "@/components/onboarding/team/TeamCreateForm";
import { TeamSignupShell } from "@/components/onboarding/team/TeamSignupShell";

export default function CreateTeamPage() {
  return (
    <TeamSignupShell>
      {(deps) => (
        <StepCard
          eyebrow="new team"
          title="Create a team"
          description="A team has its own members and its own private database, separate from your personal account. Your personal account stays exactly as it is."
          width={580}
        >
          <TeamCreateForm
            create={deps.create}
            trialCheckout={deps.trialCheckout}
            goToCheckout={deps.goToCheckout}
            goTo={deps.goTo}
            returnUrl={deps.returnUrl}
            priceCents={deps.priceCents}
            store={deps.store}
          />
        </StepCard>
      )}
    </TeamSignupShell>
  );
}
