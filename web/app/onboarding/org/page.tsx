"use client";

// 6a — Step 1 · The organization. Name + identifier. There is no
// org-creation endpoint in this codebase yet (Clerk organizations exist,
// but nothing here calls createOrganization or a backend provisioning
// route), so "Create and continue" advances the wizard rather than
// performing a real create — see the task report. The slug format itself
// IS real: isValidSlug is the same check the backend gate and the
// `tenant_databases.slug` CHECK constraints apply.
//
// The identifier used to be an ADDRESS: `<slug>.coralswarm.example`, the host the
// org signed in on. Claim-based tenancy (PR 2.1) removed org hostnames — the
// tenant is named by the Clerk token's `org_id` claim and everyone signs in at
// the same apex — so the field lost its hostname suffix and the copy below
// lost the sentence claiming a session "works only there". That sentence
// described the host-based binding gate, which no longer exists.

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { Button, Input } from "@/components/ui";
import { StepRail, OnboardingChrome, HeaderPill } from "@/components/onboarding/OnboardingChrome";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";
import { ORG_STEPS, ORG_ROUTES } from "@/components/onboarding/orgSteps";
import { isValidSlug } from "@/lib/apiBase";

export default function OrgCreatePage() {
  const router = useRouter();
  const [name, setName] = useState("Acme Inc");
  const [slug, setSlug] = useState("acme");

  const slugValid = useMemo(() => isValidSlug(slug), [slug]);

  return (
    <OnboardingChrome
      right={
        <>
          <StepRail steps={ORG_STEPS} currentIndex={0} />
          <HeaderPill>creating from your personal account</HeaderPill>
        </>
      }
    >
      <StepCard eyebrow="step 1 of 5" title="Create the organization" width={560}>
        <p className="mb-5 -mt-3 text-[13.5px] leading-[1.6] text-ink-2">
          An organization is a separate place, not a folder inside your personal account. Yours stays
          exactly as it is, and nothing moves between them unless you promote it.
        </p>

        <Input
          label="name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          wrapperClassName="mb-4"
        />

        <div className="mb-[6px] font-mono text-[9.5px] uppercase tracking-[0.13em] text-ink-3">
          identifier
        </div>
        <div
          className="mb-[8px] flex items-center rounded-field border-[1.5px] px-[12px] py-[10px]"
          style={{
            borderColor: slugValid ? "var(--ink)" : "var(--bad)",
            boxShadow: `0 0 0 3px color-mix(in srgb, ${slugValid ? "var(--ink)" : "var(--bad)"} 8%, transparent)`,
          }}
        >
          <input
            value={slug}
            onChange={(e) => setSlug(e.target.value.toLowerCase())}
            className="min-w-0 flex-1 bg-transparent font-mono text-[13.5px] text-ink outline-none"
          />
          <span
            className={`ml-auto flex-none font-mono text-[10.5px] ${slugValid ? "text-ok" : "text-bad"}`}
          >
            {slugValid ? "available" : "invalid"}
          </span>
        </div>
        <p className="mb-[18px] font-mono text-[10.5px] leading-[1.6] text-ink-4">
          A short, permanent name for this organization. Your team signs in where everyone else does; what
          keeps one org&rsquo;s data out of another&rsquo;s is that each one gets its own database, chosen
          from your session, never from a link.
        </p>

        <div className="mb-[18px] overflow-hidden rounded-card border border-line">
          {[
            { name: "Who may join", desc: "Anyone with a matching email address joins automatically, as a Member.", value: "@acme.com" },
            { name: "First shared workspace", desc: "Everyone starts here. You can add more, and scope them, later.", value: "Platform" },
          ].map((r, i, arr) => (
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

        <WizardActions trailing="you'll be the owner · 25 slots on the Team plan">
          <Button variant="brand" disabled={!slugValid || !name.trim()} onClick={() => router.push(ORG_ROUTES[1])}>
            Create and continue
          </Button>
        </WizardActions>
      </StepCard>
    </OnboardingChrome>
  );
}
