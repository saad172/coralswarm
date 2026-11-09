"use client";

import { useEffect, useState } from "react";

/* The small "you are here" pill floated over the auth canvas (4a/4b/4c) —
 * a green dot plus the browser's own host.
 *
 * KEPT through claim-based tenancy PR 2.1, which deleted every other
 * host-derived thing on these pages, because this one derives nothing: it
 * echoes `window.location.host` verbatim, a string already sitting in the
 * visitor's URL bar. It classifies no plane and names no tenant, so unlike the
 * "Signing in to …" note it removed (see components/auth/AuthShell.tsx) it
 * cannot become a false claim when one apex serves every tenant — and with
 * that note gone it is the only "which deployment am I on" cue left, which is
 * worth more, not less. Resolved post-mount so SSR/first paint renders
 * nothing and there is no hydration mismatch. */
export default function HostChip({ className }: { className?: string }) {
  const [host, setHost] = useState<string | null>(null);

  useEffect(() => {
    if (typeof window !== "undefined") setHost(window.location.host);
  }, []);

  if (!host) return null;

  return (
    <span
      className={
        "inline-flex items-center gap-[8px] rounded-pill border border-line bg-bg px-[12px] py-[5px] font-mono text-[11px] text-ink-3 " +
        (className ?? "")
      }
    >
      <span className="h-[6px] w-[6px] flex-none rounded-pill bg-ok" />
      {host}
    </span>
  );
}
