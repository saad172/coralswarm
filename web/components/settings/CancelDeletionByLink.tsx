"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { api, ApiError } from "@/lib/api";
import { Button } from "@/components/ui";
import { cancelErrorCopy, RESTORED_COPY } from "./deleteAccountModel";

/* `/account/delete/cancel#token=...` — the link in the deletion email.
   No sign-in needed. Security properties:
   - The token rides in the URL FRAGMENT, which browsers never send to any
     server (and never put in a Referer). It is read once from `location.hash`,
     then the hash is scrubbed from the address bar and history.
   - Nothing happens on page load. Email link scanners pre-fetch links, so the
     cancel needs a click. The token is single use.
   - The page sets `referrer: no-referrer` (see its `page.tsx`). The web loads
     no third-party analytics; its own client metrics record route templates
     only, never page URLs. */

type Phase =
  | { kind: "ready" }
  | { kind: "working" }
  | { kind: "done" }
  | { kind: "error"; message: string };

export function CancelDeletionByLink() {
  const [phase, setPhase] = useState<Phase | null>(null);
  const token = useRef<string | null>(null);

  useEffect(() => {
    const t = new URLSearchParams(window.location.hash.replace(/^#/, "")).get("token");
    token.current = t;
    if (window.location.hash) {
      window.history.replaceState(null, "", window.location.pathname + window.location.search);
    }
    setPhase(
      t
        ? { kind: "ready" }
        : { kind: "error", message: cancelErrorCopy(new ApiError("invalid_token", 400, "invalid_token")) },
    );
  }, []);

  async function cancel() {
    const t = token.current;
    if (!t) return;
    setPhase({ kind: "working" });
    try {
      await api.accountDeletion.cancelWithToken(t);
      token.current = null;
      setPhase({ kind: "done" });
    } catch (e) {
      setPhase({ kind: "error", message: cancelErrorCopy(e) });
    }
  }

  const title =
    phase?.kind === "done"
      ? "Deletion cancelled"
      : phase?.kind === "error"
        ? "Couldn't cancel"
        : "Cancel account deletion?";

  return (
    <main className="mx-auto flex min-h-screen max-w-[460px] flex-col justify-center px-6 text-center">
      <h1 className="text-[22px] font-semibold tracking-[-0.01em] text-ink">{title}</h1>
      {(phase?.kind === "ready" || phase?.kind === "working") && (
        <>
          <p className="mt-3 text-[14px] leading-[1.6] text-ink-3">
            Your account is scheduled for deletion. Cancel it to keep your account and everything in it.
          </p>
          <Button className="mt-4 self-center" disabled={phase.kind === "working"} onClick={() => void cancel()}>
            {phase.kind === "working" ? "Cancelling…" : "Cancel deletion"}
          </Button>
        </>
      )}
      {(phase?.kind === "done" || phase?.kind === "error") && (
        <>
          <p className="mt-3 text-[14px] leading-[1.6] text-ink-3" role={phase.kind === "error" ? "alert" : undefined}>
            {phase.kind === "done" ? RESTORED_COPY : phase.message}
          </p>
          <Link className="mt-4 text-[14px] underline" href="/">Continue to Coralswarm</Link>
        </>
      )}
    </main>
  );
}
