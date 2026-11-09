"use client";

import { useRouter } from "next/navigation";
import { Button } from "@/components/ui";
import { StepRail, OnboardingChrome, HeaderPill } from "@/components/onboarding/OnboardingChrome";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";
import { ORG_STEPS, ORG_ROUTES } from "@/components/onboarding/orgSteps";

const ROWS = [
  { name: "How answers are produced", desc: "Cloud synthesis runs under no-training, zero-data-retention terms.", value: "Cloud" },
  { name: "Model tiers members may use", desc: "Turning a tier off removes it from every composer in the org.", value: "Fast + Smart" },
  { name: "Meeting capture", desc: "Members choose which meetings to save, one at a time, through their connectors.", value: "Allowed" },
];

export default function OrgPolicyPage() {
  const router = useRouter();

  return (
    <OnboardingChrome
      right={
        <>
          <StepRail steps={ORG_STEPS} currentIndex={2} />
          <HeaderPill tone="ok">acme.{process.env.NEXT_PUBLIC_WEB_APEX || "coralswarm.example"}</HeaderPill>
        </>
      }
    >
      <StepCard
        eyebrow="step 3 of 5"
        title="Decide the rules once, for everyone"
        description="These become the default for every workspace. A member may choose something stricter than you set here — never looser."
        width={560}
      >
        <div className="mb-[14px] rounded-card border border-line bg-bg-subtle p-[12px_14px]">
          <div className="mb-[6px] font-mono text-[9.5px] uppercase tracking-[0.13em] text-ink-3">
            how content is processed
          </div>
          <div className="text-[12.5px] leading-[1.5] text-ink-2">
            Cloud AI providers extract topics under zero-data-retention terms. None of them may
            train on your data. Embeddings also use cloud providers, except for accounts with the
            retired sources-only setting.
          </div>
          <div className="mt-[7px] font-mono text-[9.5px] leading-[1.5] text-ink-4">
            the same for every workspace · not a setting
          </div>
        </div>

        <div className="mb-[14px] overflow-hidden rounded-card border border-line">
          {ROWS.map((r, i, arr) => (
            <div
              key={r.name}
              className={`flex items-center gap-[14px] p-[9px_14px] ${i < arr.length - 1 ? "border-b border-line-3" : ""}`}
            >
              <span className="min-w-0 flex-1">
                <span className="text-[13.5px] font-medium text-ink">{r.name}</span>
                <span className="mt-[3px] block text-[12.5px] leading-[1.5] text-ink-3">{r.desc}</span>
              </span>
              <span className="flex-none rounded-row border border-line px-[12px] py-[6px] text-[12.5px] font-medium text-ink">
                {r.value} <span className="text-ink-4">▾</span>
              </span>
            </div>
          ))}
        </div>

        <WizardActions trailing="changeable any time in org settings">
          <Button variant="brand" onClick={() => router.push(ORG_ROUTES[3])}>
            Save policy
          </Button>
          <Button variant="secondary" onClick={() => router.push(ORG_ROUTES[3])}>
            Use the defaults
          </Button>
        </WizardActions>
      </StepCard>
    </OnboardingChrome>
  );
}
