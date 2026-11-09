"use client";

// 6d — Step 4 · Workspaces. A workspace is a boundary, not a folder — people
// see only the ones they're in. Setting up the first one or two here means
// invited members (6e/6g) land somewhere real on day one.

import { useRouter } from "next/navigation";
import { Button } from "@/components/ui";
import { StepRail, OnboardingChrome, HeaderPill } from "@/components/onboarding/OnboardingChrome";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";
import { ORG_STEPS, ORG_ROUTES } from "@/components/onboarding/orgSteps";

const WORKSPACES = [
  { name: "Platform", desc: "Engineering decisions, architecture, incidents", kind: "team", hue: "teal", joining: "@acme.com", on: true },
  { name: "Company", desc: "Everything the whole org may read", kind: "org", hue: "coral", joining: "all members", on: false },
  { name: "Security", desc: "Baselines, findings, compliance", kind: "team", hue: "plum", joining: "invite only", on: false },
] as const;

const ROWS = [
  { name: "New members land in", desc: "Their first shared workspace. They always also get a workspace of their own.", value: "Platform" },
  { name: "Who may create a workspace", desc: "Members can be allowed to spin one up without asking an admin.", value: "Admins only" },
];

export default function OrgWorkspacesPage() {
  const router = useRouter();

  return (
    <OnboardingChrome
      right={
        <>
          <StepRail steps={ORG_STEPS} currentIndex={3} />
          <HeaderPill tone="ok">acme.{process.env.NEXT_PUBLIC_WEB_APEX || "coralswarm.example"}</HeaderPill>
        </>
      }
    >
      <StepCard
        eyebrow="step 4 of 5"
        title="Set up the shared spaces"
        description="A workspace is a boundary, not a folder — people see only the ones they are in. Start with one or two; you can split them later without moving anything."
        width={560}
      >
        <div className="mb-[14px] flex flex-col gap-[8px]">
          {WORKSPACES.map((w) => (
            <div
              key={w.name}
              className="flex items-center gap-[12px] rounded-card p-[13px_15px]"
              style={{
                border: `1px solid ${w.on ? "var(--ink)" : "var(--line)"}`,
                background: w.on ? "var(--bg-subtle)" : "var(--bg)",
              }}
            >
              <span className="h-[9px] w-[9px] flex-none rounded-pill" style={{ background: `var(--hue-${w.hue})` }} />
              <span className="min-w-0 flex-1">
                <span className="flex items-center gap-[8px]">
                  <span className="text-[14px] font-semibold text-ink">{w.name}</span>
                  <span
                    className="inline-flex items-center rounded-pill px-[7px] py-[1px] font-mono text-[8.5px] uppercase tracking-[0.06em]"
                    style={{
                      color: w.kind === "org" ? "var(--brand-2)" : "var(--hue-teal)",
                      background: `color-mix(in srgb, ${w.kind === "org" ? "var(--brand)" : "var(--hue-teal)"} 16%, transparent)`,
                    }}
                  >
                    {w.kind}
                  </span>
                </span>
                <span className="mt-[3px] block text-[12.5px] text-ink-3">{w.desc}</span>
              </span>
              <span className="flex-none font-mono text-[10.5px] text-ink-4">{w.joining}</span>
            </div>
          ))}
          <button type="button" className="flex items-center gap-[9px] rounded-card border-[1.5px] border-dashed border-line p-[12px_15px] text-[13px] text-ink-3 hover:border-ink-4 hover:text-ink-2">
            <span className="text-[15px] leading-none">+</span>Add another workspace
          </button>
        </div>

        <div className="mb-4 overflow-hidden rounded-card border border-line">
          {ROWS.map((r, i, arr) => (
            <div
              key={r.name}
              className={`flex items-center gap-[14px] p-[12px_14px] ${i < arr.length - 1 ? "border-b border-line-3" : ""}`}
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

        <WizardActions trailing="members see only what they are added to">
          <Button variant="brand" onClick={() => router.push(ORG_ROUTES[4])}>
            Continue to invites
          </Button>
          <Button variant="secondary" onClick={() => router.push(ORG_ROUTES[4])}>
            One workspace is fine
          </Button>
        </WizardActions>
      </StepCard>
    </OnboardingChrome>
  );
}
