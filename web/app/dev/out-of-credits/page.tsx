"use client";

/* DEV-ONLY PREVIEW — delete before merge.
 *
 * Renders the real `ThreadTurn` internals against a fixture turn so the new
 * "out of credits" (402) blocked-state can be reviewed without signing in,
 * without a reachable backend, and without touching any real account's
 * billing state. Same shape as `app/dev/slack-onboarding/page.tsx`: lives
 * OUTSIDE the `(app)` route group deliberately, since that group's layout
 * gates on Clerk.
 *
 * Shows `outOfCredits` next to `rateLimited` and a plain `error` turn so the
 * three states are visually distinguishable, not just individually correct. */

import { PageHead } from "@/components/ui";
import { ThreadTurn } from "@/components/chat/AnswerTurn";
import { emptyAssistant, type Turn, type UserTurn } from "@/components/chat/types";

function userTurn(key: string, text: string): UserTurn {
  return { key, role: "user", text };
}

const noop = () => {};

export default function OutOfCreditsPreview() {
  const outOfCredits: Turn = { ...emptyAssistant("a1", "How many credits do I have left?"), pending: false, outOfCredits: true };
  const rateLimited: Turn = { ...emptyAssistant("a2", "What did we ship last week?"), pending: false, rateLimited: true };
  const genericError: Turn = { ...emptyAssistant("a3", "Summarize the Q3 roadmap"), pending: false, error: "That question didn't go through." };

  return (
    <div className="mx-auto max-w-[720px] px-6 py-10">
      <PageHead title="Out-of-credits preview (dev only)" />

      <div className="mb-10">
        <div className="mb-2 font-mono text-[11px] uppercase tracking-[0.13em] text-ink-4">
          outOfCredits (402 — new)
        </div>
        <ThreadTurn turn={userTurn("u1", "How many credits do I have left?")} onCite={noop} onRegenerate={noop} onAsk={noop} />
        <ThreadTurn turn={outOfCredits} onCite={noop} onRegenerate={noop} onAsk={noop} />
      </div>

      <div className="mb-10">
        <div className="mb-2 font-mono text-[11px] uppercase tracking-[0.13em] text-ink-4">
          rateLimited (429 — existing, for comparison)
        </div>
        <ThreadTurn turn={userTurn("u2", "What did we ship last week?")} onCite={noop} onRegenerate={noop} onAsk={noop} />
        <ThreadTurn turn={rateLimited} onCite={noop} onRegenerate={noop} onAsk={noop} />
      </div>

      <div>
        <div className="mb-2 font-mono text-[11px] uppercase tracking-[0.13em] text-ink-4">
          generic error (existing, for comparison)
        </div>
        <ThreadTurn turn={userTurn("u3", "Summarize the Q3 roadmap")} onCite={noop} onRegenerate={noop} onAsk={noop} />
        <ThreadTurn turn={genericError} onCite={noop} onRegenerate={noop} onAsk={noop} />
      </div>
    </div>
  );
}
