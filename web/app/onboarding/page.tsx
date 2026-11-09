"use client";

// 5a — How it works. Opens the personal first-run by explaining the model
// before asking for anything: no input required on this screen. Unlike
// every other 5×/6× step this isn't part of a numbered rail — it's the
// screen before step 1 — so the header just shows the signed-in email.

import { useRouter } from "next/navigation";
import { useUser } from "@clerk/nextjs";
import { Button, Mono } from "@/components/ui";
import { IconChats, IconInbox, IconLibrary, IconLock } from "@/components/icons";
import { OnboardingChrome } from "@/components/onboarding/OnboardingChrome";

const STEPS = [
  {
    step: "you",
    title: "You hand it things",
    desc: "Paste a note, drop a transcript, connect a channel. It never crawls your accounts looking for material.",
    note: "nothing arrives on its own",
    icon: IconInbox,
    hue: "coral" as const,
  },
  {
    step: "it",
    title: "It forms topics",
    desc: "Everything is split, indexed, and grouped into decisions, threads, people and references.",
    note: "proposed for your review — you confirm",
    icon: IconLibrary,
    hue: "teal" as const,
  },
  {
    step: "you",
    title: "You ask, it cites",
    desc: "Answers quote the source and link back to it. When two sources disagree, it says so instead of picking one.",
    note: "no answer without a citation",
    icon: IconChats,
    hue: "plum" as const,
  },
];

export default function HowItWorksPage() {
  const router = useRouter();
  const { user } = useUser();
  const email = user?.primaryEmailAddress?.emailAddress ?? user?.emailAddresses?.[0]?.emailAddress;

  return (
    <OnboardingChrome right={email && <Mono className="text-ink-4">{email}</Mono>}>
      <div className="w-full max-w-[900px]">
        <div className="mb-6 text-center">
          <h1 className="mb-[9px] font-display text-[38px] leading-[1.3] tracking-[-0.01em] text-ink">
            Here&rsquo;s how it actually works
          </h1>
          <p className="mx-auto max-w-[62ch] text-[15px] leading-[1.65] text-ink-2">
            Three things happen, in this order. Nothing happens that you didn&rsquo;t start.
          </p>
        </div>

        <div className="mb-5 grid grid-cols-1 gap-[14px] sm:grid-cols-3">
          {STEPS.map((s) => (
            <div
              key={s.title}
              className="flex flex-col gap-[9px] rounded-card border border-line bg-bg p-[17px] shadow-card"
            >
              <div className="flex items-center gap-[9px]">
                <span
                  className="grid h-[26px] w-[26px] flex-none place-items-center rounded-row"
                  style={{
                    background: `color-mix(in srgb, var(--hue-${s.hue}) 16%, transparent)`,
                    color: `var(--hue-${s.hue})`,
                  }}
                >
                  <s.icon width={14} height={14} />
                </span>
                <span className="font-mono text-[9px] uppercase tracking-[0.1em] text-ink-4">{s.step}</span>
              </div>
              <div className="text-[15px] font-semibold tracking-[-0.01em] text-ink">{s.title}</div>
              <p className="text-[12.5px] leading-[1.6] text-ink-2">{s.desc}</p>
              <div className="mt-auto border-t border-line-3 pt-[9px] font-mono text-[10px] leading-[1.5] text-ink-4">
                {s.note}
              </div>
            </div>
          ))}
        </div>

        <div className="mb-5 flex items-center gap-[14px] rounded-card border border-line bg-bg p-[15px_17px]">
          <span className="grid h-[30px] w-[30px] flex-none place-items-center rounded-field bg-ok/[13%] text-ok">
            <IconLock width={15} height={15} />
          </span>
          <p className="flex-1 text-[13px] leading-[1.6] text-ink-2">
            CoralSwarm keeps your content on our servers so it can index and answer questions over it
            — it isn&rsquo;t private or on-device. Data is encrypted in transit, and the databases and
            storage that hold it encrypt data at rest. If you&rsquo;re joining an organization, that org
            gets its own separate database, isolated from every other organization&rsquo;s.
          </p>
        </div>

        <div className="flex items-center justify-center gap-[10px]">
          <Button variant="brand" size="lg" onClick={() => router.push("/onboarding/secure")}>
            Got it — set it up
          </Button>
          <Button variant="secondary" size="lg" onClick={() => router.push("/home")}>
            Look around first
          </Button>
        </div>
      </div>
    </OnboardingChrome>
  );
}
