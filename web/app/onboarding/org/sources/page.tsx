"use client";

// 6b — Step 2 · Sources. Slack uses the same personal setup on Connectors.
// The other source choices keep their existing prototype behavior.

import { useState } from "react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui";
import { StepRail, OnboardingChrome, HeaderPill } from "@/components/onboarding/OnboardingChrome";
import { ProviderMark } from "@/components/settings/ProviderIcon";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";
import { ORG_STEPS, ORG_ROUTES } from "@/components/onboarding/orgSteps";

type Source = {
  key: string;
  name: string;
  letter: string;
  hue: "amber" | "plum" | "ink";
  desc: string;
  tag?: string;
  action: string;
};

const SOURCES: Source[] = [
  {
    key: "slack",
    name: "Slack",
    letter: "S",
    hue: "amber",
    desc: "Each person connects their own account for public channel history.",
    action: "Connect",
  },
  {
    key: "microsoft",
    name: "Microsoft",
    letter: "M",
    hue: "plum",
    desc: "Calendar syncs automatically; email is opt-in per thread, never a whole inbox.",
    action: "Connect",
  },
  {
    key: "other",
    name: "Gmail, Zoom",
    letter: "+",
    hue: "ink",
    desc: "Per-thread email ingestion and Zoom transcripts. Not available yet.",
    tag: "soon",
    action: "Notify me",
  },
];

export default function OrgSourcesPage() {
  const router = useRouter();
  const [connected, setConnected] = useState<Set<string>>(new Set());

  return (
    <OnboardingChrome
      right={
        <>
          <StepRail steps={ORG_STEPS} currentIndex={1} />
          <HeaderPill tone="ok">acme.{process.env.NEXT_PUBLIC_WEB_APEX || "coralswarm.example"}</HeaderPill>
        </>
      }
    >
      <StepCard
        eyebrow="step 2 of 5"
        title="Connect your sources"
        description="Each person connects their own Slack account from Connectors. Their authorization and disconnect apply only to them."
        width={560}
      >
        <div className="mb-4 flex flex-col gap-[8px]">
          {SOURCES.map((s) => {
            const on = connected.has(s.key);
            return (
              <div
                key={s.key}
                className="flex items-center gap-[12px] rounded-card p-[13px_15px]"
                style={{
                  border: `1px solid ${on ? "var(--ink)" : "var(--line)"}`,
                  background: on ? "var(--bg-subtle)" : "var(--bg)",
                }}
              >
                <ProviderMark provider={s.key} letter={s.letter} hue={s.hue} box={32} />
                <span className="min-w-0 flex-1">
                  <span className="flex items-center gap-[8px]">
                    <span className="text-[14px] font-semibold text-ink">{s.name}</span>
                    {s.tag && (
                      <span
                        className={`inline-flex items-center rounded-pill px-[7px] py-[1px] font-mono text-[8.5px] uppercase tracking-[0.06em] ${
                          s.tag === "connected" ? "bg-ok/[13%] text-ok" : "bg-bg-2 text-ink-3"
                        }`}
                      >
                        {s.tag}
                      </span>
                    )}
                  </span>
                  <span className="mt-[3px] block text-[12.5px] leading-[1.5] text-ink-3">{s.desc}</span>
                </span>
                <button
                  type="button"
                  onClick={() => {
                    if (s.key === "slack") {
                      router.push("/connectors");
                      return;
                    }
                    setConnected((prev) => {
                      const next = new Set(prev);
                      if (next.has(s.key)) next.delete(s.key);
                      else next.add(s.key);
                      return next;
                    });
                  }}
                  className={
                    on
                      ? "flex-none rounded-field bg-ink px-[14px] py-[8px] text-[13px] font-medium text-white hover:bg-ink-2"
                      : "flex-none rounded-field border border-line bg-bg px-[14px] py-[8px] text-[13px] font-medium text-ink-2 hover:bg-bg-hover"
                  }
                >
                  {s.action}
                </button>
              </div>
            );
          })}
        </div>

        <WizardActions trailing="first sync usually takes a few minutes">
          <Button variant="brand" onClick={() => router.push(ORG_ROUTES[2])}>
            Sync and continue
          </Button>
          <Button variant="secondary" onClick={() => router.push(ORG_ROUTES[2])}>
            Skip for now
          </Button>
        </WizardActions>
      </StepCard>
    </OnboardingChrome>
  );
}
