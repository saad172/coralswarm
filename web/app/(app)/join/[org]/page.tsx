// CS-439 (org-onboarding plan §3 item 39, D19) — the requester's landing
// page for a "join {org}" link (an invite click, or a typed request). All
// the actual logic lives in components/join/JoinLanding.tsx, a client
// component this thin server wrapper renders — this file exists ONLY to own
// the Next 15 async `params`, which a "use client" module cannot.
import { Suspense } from "react";
import { Loading } from "@/components/ui";
import { JoinLanding } from "@/components/join/JoinLanding";

export default async function JoinOrgPage({ params }: { params: Promise<{ org: string }> }) {
  const { org } = await params;
  return (
    // `useSearchParams()` (read inside JoinLanding, for the `?t=` invite
    // token CS-440 will consume) needs a Suspense boundary — same convention
    // app/(app)/addin/approve/page.tsx already uses for its own query param.
    <Suspense fallback={<Loading what="loading" />}>
      <JoinLanding org={org} />
    </Suspense>
  );
}
