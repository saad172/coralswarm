"use client";

import { Fragment, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useApp } from "@/lib/providers";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import AppShell from "@/components/AppShell";
import { ENGINE_ENABLED, EngineProvider } from "@/lib/engine/provider";
import EngineGate from "@/components/EngineGate";
import { PromoteJobsProvider } from "@/lib/promoteJobsProvider";
import { setPostSignInRedirect } from "@/lib/postSignInRedirect";
import { useCriticalPageReadLease } from "@/lib/pageReadRegistration";

export default function AppLayout({ children }: { children: React.ReactNode }) {
  const { ready, token, bootError, retryBoot } = useApp();
  const router = useRouter();
  const admission = useEvidenceAdmission();
  const hasToken = token !== null && token !== "";
  useCriticalPageReadLease("app account verification", ready && !!token && !admission.ready && !bootError);

  useEffect(() => {
    if (ready && !hasToken) {
      // Read straight off `window`, not `usePathname`/`useSearchParams`:
      // those need a Suspense boundary this top-level layout doesn't have,
      // and every page under it would inherit that requirement. `children`
      // (the page being redirected away from) never mounts while signed
      // out, so nothing under this layout gets a chance to stash its own
      // return path before this redirect fires. Preserve Gmail add-on pairing
      // codes and signed Teams onboarding tickets through login; see
      // `lib/postSignInRedirect.ts`.
      setPostSignInRedirect(window.location.pathname + window.location.search);
      router.replace("/login");
    }
  }, [ready, hasToken, router]);

  if (!ready)
    return (
      <div className="grid h-screen place-items-center px-6 text-center font-mono text-sm text-ink-3">
        {bootError ? (
          <div className="flex flex-col items-center gap-3">
            <span>{bootError}</span>
            <button
              type="button"
              onClick={retryBoot}
              className="rounded-row border border-line bg-bg px-3 py-[6px] font-mono text-[11px] text-ink-2 transition-colors hover:border-brand hover:text-brand-2"
            >
              retry
            </button>
          </div>
        ) : (
          "surfacing…"
        )}
      </div>
    );
  if (!token) return null;
  if (!admission.ready) return <div role="status" className="grid h-screen place-items-center text-sm text-ink-2">Verifying the current account…</div>;
  // PromoteJobsProvider is mounted here — ONE instance for the whole signed-in
  // session — so an in-flight "reef" promote (PromoteModal.tsx) survives that
  // modal closing and any page navigation under this layout: the persistent
  // indicator in AppShell's header and every PromoteModal mount both read the
  // SAME shared state, rather than each page re-deriving it locally. See
  // lib/promoteJobsProvider.tsx's file-level comment.
  const shell = (
    <PromoteJobsProvider token={token}>
      <AppShell>{children}</AppShell>
    </PromoteJobsProvider>
  );
  // On-device private engine (ADR-0007), behind NEXT_PUBLIC_ENGINE. When off,
  // the provider/gate aren't mounted and the app is the server-backed cloud app.
  const content = ENGINE_ENABLED ? (
    <EngineProvider token={token}>
      <EngineGate>{shell}</EngineGate>
    </EngineProvider>
  ) : (
    shell
  );
  return <Fragment key={admission.key}>{content}</Fragment>;
}
