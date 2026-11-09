"use client";

// 5f — Step 3 · First answer. The wizard's closing screen: a real cited
// answer (CiteChip/AnswerMeta from the kit, per the task brief) built from
// what the previous step just "synced" — illustrative copy, since there's
// no live ask pipeline wired into onboarding, but rendered with the same
// citation primitives the real ask surface uses (lib/citePlugin.ts /
// AnswerMarkdown.tsx), not a page-local look-alike.

import { useRouter } from "next/navigation";
import { Button, CiteChip, DotMandala, Mono } from "@/components/ui";
import { IconBrowser, IconConnectors, IconPeople, IconPlug } from "@/components/icons";
import { StepRail, OnboardingChrome, HeaderPill } from "@/components/onboarding/OnboardingChrome";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";

const STEPS = ["Secure it", "Connectors", "First answer"];

const QUESTIONS = [
  { text: "What did we settle on for auth?", meta: "3 sources", on: true },
  { text: "Why did we drop the custom JWT layer?", meta: "2 sources", on: false },
  { text: "Summarise everything I added today", meta: "6 sources", on: false },
];

const NEXT = [
  {
    title: "Connect Slack",
    desc: "Connect your own account to capture public channels you belong to.",
    icon: IconConnectors,
    hue: "amber" as const,
  },
  {
    title: "Let your AI tools write in",
    desc: "Claude Code and Cursor can add context over MCP. Read-only until you allow writes.",
    icon: IconPlug,
    hue: "teal" as const,
  },
  {
    title: "Join your team later",
    desc: "If your company uses CoralSwarm, an admin can invite you. Nothing you have moves.",
    icon: IconPeople,
    hue: "pink" as const,
  },
];

export default function FirstAnswerPage() {
  const router = useRouter();

  return (
    <OnboardingChrome
      right={
        <>
          <StepRail steps={STEPS} currentIndex={2} />
          <HeaderPill icon={<IconBrowser width={12} height={12} />}>in your browser</HeaderPill>
        </>
      }
    >
      <div className="w-full" style={{ maxWidth: 560 }}>
        <StepCard
          eyebrow="step 3 of 3"
          title="Ask it something you'd otherwise go digging for"
          description="These come from what just synced, so the answer will cite it."
          width={560}
          className="mb-[14px]"
        >
          <div className="mb-[18px] flex flex-col gap-[7px]">
            {QUESTIONS.map((q) => (
              <button
                type="button"
                key={q.text}
                className="flex items-center gap-[11px] rounded-control p-[11px_14px] text-left"
                style={{
                  border: `1px solid ${q.on ? "var(--ink)" : "var(--line)"}`,
                  background: q.on ? "var(--bg-subtle)" : "var(--bg)",
                }}
              >
                <span className="min-w-0 flex-1 text-[13.5px] text-ink">{q.text}</span>
                <span className="flex-none font-mono text-[10px] text-ink-4">{q.meta}</span>
              </button>
            ))}
          </div>

          <div className="rounded-card border border-line bg-bg-subtle p-[14px_15px]">
            <div className="mb-[9px] flex items-center gap-[8px]">
              <DotMandala size={14} density={0.75} />
              <span className="font-mono text-[9.5px] uppercase tracking-[0.1em] text-ink-3">
                answered from 4 sources · 2.1s
              </span>
            </div>
            <p className="text-[13.5px] leading-[1.65] text-ink">
              You settled on Clerk with org-scoped sessions, dropping the custom JWT layer — it
              kept losing its session on hard navigation
              <CiteChip n={1} />. Every client now signs in through the same flow
              <CiteChip n={3} />.
            </p>
          </div>
        </StepCard>

        <div className="rounded-frame border border-line bg-bg p-[18px_20px] shadow-pop">
          <div className="mb-3 flex items-center gap-[9px]">
            <Mono className="uppercase tracking-[0.13em] text-ink-3">when you have a minute</Mono>
            <span className="ml-auto font-mono text-[9.5px] text-ink-4">none of this is required</span>
          </div>
          <div className="grid grid-cols-1 gap-[10px] sm:grid-cols-2">
            {NEXT.map((n) => (
              <div key={n.title} className="flex items-start gap-[11px] rounded-control border border-line-2 p-[12px_13px]">
                <span className="mt-[1px] flex-none" style={{ color: `var(--hue-${n.hue})` }}>
                  <n.icon width={15} height={15} />
                </span>
                <span className="min-w-0 flex-1">
                  <span className="block text-[13px] font-medium text-ink">{n.title}</span>
                  <span className="mt-[3px] block text-[12px] leading-[1.5] text-ink-3">{n.desc}</span>
                </span>
              </div>
            ))}
          </div>
        </div>

        <WizardActions className="mt-5 justify-center">
          <Button variant="brand" size="lg" onClick={() => router.push("/home")}>
            Go to your workspace
          </Button>
        </WizardActions>
      </div>
    </OnboardingChrome>
  );
}
