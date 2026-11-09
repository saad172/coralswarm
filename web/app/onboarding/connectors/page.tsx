"use client";

// Step 2 keeps its progression while the flagged flow shares the catalog and wizard.

import { Suspense } from "react";
import { useRouter } from "next/navigation";
import { ConnectorGrid } from "@/components/connectors/ConnectorsPageV2";
import { Button, Loading } from "@/components/ui";
import { IconBrowser } from "@/components/icons";
import { StepRail, OnboardingChrome, HeaderPill } from "@/components/onboarding/OnboardingChrome";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";

const STEPS = ["Secure it", "Connectors", "First answer"];

export default function ConnectorsPage() {
  const router = useRouter();
  const next = () => router.push("/onboarding/answer");

  return (
    <OnboardingChrome
      right={
        <>
          <StepRail steps={STEPS} currentIndex={1} />
          <HeaderPill icon={<IconBrowser width={12} height={12} />}>in your browser</HeaderPill>
        </>
      }
    >
      <StepCard
        eyebrow="step 2 of 3"
        title="Connect where your work already happens"
        description="Sign in to your tools and choose what CoralSwarm captures. You can add more sources later."
        width={900}
      >
        <div className="mb-4"><Suspense fallback={<Loading what="loading connectors" />}><ConnectorGrid onboarding /></Suspense></div>

        <WizardActions>
          <Button variant="primary" onClick={next}>
            Ask your first question
          </Button>
          <Button variant="secondary" onClick={next}>
            Paste something instead
          </Button>
        </WizardActions>
      </StepCard>
    </OnboardingChrome>
  );
}
