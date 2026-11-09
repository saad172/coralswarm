"use client";

// 5b — Step 1 · About your private space.
//
// This screen used to generate a "recovery passphrase" and claim it was
// wired to the on-device engine that encrypts your private space — it
// wasn't. Filed as a security-review blocker 2026-07-27 (two independent
// bugs: `eng.setup()` is unreachable from /onboarding because
// `EngineProvider` only mounts inside the (app) route group, and the
// generated passphrase was a build-time constant baked into the static
// HTML — ~23 bits, identical for every visitor). The on-device engine
// itself is also off in every environment today (`NEXT_PUBLIC_ENGINE` is
// unset), so there was nothing for this screen to truthfully wire up to.
//
// Fixed 2026-08-03 by no longer claiming a protection that isn't active:
// this step now states what's actually true today (TLS in transit,
// provider-managed disk encryption at rest) and moves on. If the on-device
// private-ocean engine ships, first-time setup is EngineGate's job
// (components/EngineGate.tsx) — it already prompts for a real, user-chosen
// passphrase and calls `eng.setup()` correctly, so this step doesn't need
// to duplicate it.

import { useRouter } from "next/navigation";
import { Button } from "@/components/ui";
import { IconLock } from "@/components/icons";
import { StepRail, OnboardingChrome } from "@/components/onboarding/OnboardingChrome";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";

const STEPS = ["Secure it", "Connectors", "First answer"];

export default function SecurePage() {
  const router = useRouter();

  return (
    <OnboardingChrome right={<StepRail steps={STEPS} currentIndex={0} />}>
      <StepCard
        eyebrow="step 1 of 3"
        title="How your space is protected"
        description="Everything you send to CoralSwarm travels over an encrypted connection. The database and storage that hold it encrypt data at rest as standard infrastructure from their providers."
        width={520}
      >
        <div className="mb-[18px] flex items-center gap-[13px] rounded-card border border-line bg-bg-subtle p-[13px_15px]">
          <span className="grid h-[30px] w-[30px] flex-none place-items-center rounded-field bg-ok/[13%] text-ok">
            <IconLock width={15} height={15} />
          </span>
          <p className="flex-1 text-[12.5px] leading-[1.6] text-ink-2">
            That protects your data in transit and at rest, but CoralSwarm can still read it —
            that&rsquo;s what makes search and answers possible. It isn&rsquo;t end-to-end encrypted.
          </p>
        </div>

        <WizardActions>
          <Button variant="primary" onClick={() => router.push("/onboarding/connectors")}>
            Continue
          </Button>
        </WizardActions>
      </StepCard>
    </OnboardingChrome>
  );
}
