"use client";

// CS-439 (org-onboarding plan §3 item 39, D19) — the /join/[org] landing
// page. D19: someone who already has a CoralSwarm account (their own
// personal ocean) and is matched-but-not-a-member of an ordinary org gets
// emailed a "join {org}" link; it "lands them in the app, already signed
// in, at the join request" — no signup step, which is why this page lives
// under app/(app) (behind the standard auth gate) rather than beside the
// unauthenticated /onboarding/join Clerk-invite flow.
//
// `org` is a Clerk org id (`org_...`), not a display name — see
// app/(app)/join/[org]/page.tsx. This page does NOT trust it for display:
// there is no endpoint today that resolves an org's name from a
// non-member's request (grepped `clerk_org_id` across backend/src/routes —
// every route that returns a `name` alongside it requires either admin
// membership or an already-open join-request row). Until CS-440 threads an
// org name through the invite token, this renders the honest fallback,
// "this organization" — see the PR description for the follow-up.

import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { Button, Card, ErrorNote, Loading, PageTitle } from "@/components/ui";
import { useApp } from "@/lib/providers";
import { useInstanceSwitcher } from "@/components/useInstanceSwitcher";
import { useMyJoinRequests } from "@/lib/useMyJoinRequests";
import { api, ApiError } from "@/lib/api";

// `added` (CS-447, item 9): auto-add's three gates all passed and this
// person is a member already — a different sentence from `sent`, because
// "your request has been sent to your admin" would be false and nothing
// would ever come along to correct it.
type Status = "idle" | "sending" | "sent" | "added" | "error";
type ErrorKind = "not_found" | "generic" | null;

/** CS-440's join-notice JWT carries `org_name` in its (unverified) payload
 *  segment — display-only, per the backend's own doc comment
 *  (`services::unmatched_recipient_email::JoinNoticeClaims.org_name`): the
 *  server verifies the signature and re-derives every fact that matters
 *  (`account_id`, `user_id`, `email`, …) from the SIGNED claims
 *  independently, so a tampered payload here changes nothing except which
 *  greeting this page shows before that verification ever runs — never
 *  read for any authorization decision. Same base64url-JSON decode shape
 *  as `lib/engine/engineSession.ts`'s `decodeSegment` (a `TextDecoder` pass
 *  so a non-ASCII org name like "Acme & Co" decodes correctly), not shared
 *  with it because that module's contract is "safe to use only to REFUSE"
 *  and this reads a different claim for a different, purely cosmetic
 *  purpose. `null` for anything not shaped like a JWT, not valid base64url
 *  JSON, or missing/blank `org_name` — the caller falls back to the
 *  existing "this organization" copy either way. */
function orgNameFromInviteToken(token: string): string | null {
  try {
    const segment = token.split(".")[1];
    if (!segment) return null;
    const b64 = segment.replace(/-/g, "+").replace(/_/g, "/");
    const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
    const binary = atob(padded);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (typeof parsed !== "object" || parsed === null) return null;
    const name = (parsed as { org_name?: unknown }).org_name;
    return typeof name === "string" && name.trim() ? name : null;
  } catch {
    // Malformed base64, malformed UTF-8, malformed JSON — all the same
    // answer: no display name, fall back.
    return null;
  }
}

export function JoinLanding({ org }: { org: string }) {
  const router = useRouter();
  const searchParams = useSearchParams();
  // CS-440 — the opaque `?t=` from the join-this-org email. Forwarded to
  // `POST /v1/join-requests` so the request records `source=matched_not_member`
  // rather than `self_request`. Absent (`null`) becomes `undefined` and the
  // client omits `token`, leaving an ordinary self-request.
  const inviteToken = searchParams.get("t");
  // CS-441 (item 4) — display-only org name recovered from the token's own
  // payload, before any auth or org lookup exists for this caller. `null`
  // when there's no token, or it isn't shaped the way `mint_join_notice_
  // token` shapes one — the title below falls back unchanged either way.
  const orgNameFromToken = inviteToken ? orgNameFromInviteToken(inviteToken) : null;

  const { authReady, getAuthToken } = useApp();
  const { loaded: instancesLoaded, instances, active, switchTo } = useInstanceSwitcher();
  // `loading` — while the caller's existing open requests are still
  // resolving, `myRequests` reads as [] regardless of whether one exists for
  // THIS org, so an early click here can't yet be caught by the "already
  // asked" effect below. The backend dedupes a redundant POST (D9/D10: a
  // second `create` for an already-open request just returns the same
  // `sent_to_admin`), so this isn't a correctness bug — CodeRabbit flagged
  // it (PR #1023) as an avoidable redundant request, and disabling the
  // button for this one extra beat is the cheap fix.
  const { requests: myRequests, loading: requestsLoading, refresh } = useMyJoinRequests();

  const [status, setStatus] = useState<Status>("idle");
  const [errorKind, setErrorKind] = useState<ErrorKind>(null);
  const [switching, setSwitching] = useState(false);

  // Already asked, from a prior visit — jump straight to the sent state
  // instead of re-showing "Ask to join" for a request that's already open.
  // Never clobbers an in-flight click's own state.
  useEffect(() => {
    if (status === "idle" && myRequests.some((r) => r.org.clerk_org_id === org)) {
      setStatus("sent");
    }
  }, [myRequests, org, status]);

  if (!instancesLoaded) {
    return (
      <Card className="max-w-[460px]">
        <Loading what="checking your access" />
      </Card>
    );
  }

  const membership = instances.find((i) => i.kind === "org" && i.id === org);
  if (membership) {
    const isActive = active?.id === org;
    return (
      <Card className="max-w-[460px]">
        <PageTitle italicWord={membership.name}>{`You’re already a member of ${membership.name}.`}</PageTitle>
        <div className="mt-[16px]">
          {isActive ? (
            <Button variant="brand" onClick={() => router.push("/")}>
              Go to {membership.name}
            </Button>
          ) : (
            <Button
              variant="brand"
              disabled={switching}
              onClick={async () => {
                setSwitching(true);
                try {
                  await switchTo(membership);
                } catch {
                  setSwitching(false);
                }
              }}
            >
              {switching ? "Switching…" : `Go to ${membership.name}`}
            </Button>
          )}
        </div>
      </Card>
    );
  }

  async function ask() {
    if (requestsLoading) return; // see the destructure above
    const token = authReady ? getAuthToken() : null;
    if (!token) {
      setStatus("error");
      setErrorKind("generic");
      return;
    }
    setStatus("sending");
    setErrorKind(null);
    try {
      const result = await api.joinRequests.create(token, org, inviteToken ?? undefined);
      setStatus(result.status === "added" ? "added" : "sent");
      refresh();
    } catch (e) {
      setStatus("error");
      setErrorKind(e instanceof ApiError && e.status === 404 ? "not_found" : "generic");
    }
  }

  if (status === "added") {
    return (
      <Card className="max-w-[460px]">
        <p className="text-[13.5px] leading-[1.6] text-ink">
          You&rsquo;re in{orgNameFromToken ? ` — ${orgNameFromToken} is yours to open` : ""}.
        </p>
        <Link href="/" className="mt-[16px] inline-block text-[13px] text-brand-2 hover:underline">
          Open your workspace
        </Link>
      </Card>
    );
  }

  if (status === "sent") {
    return (
      <Card className="max-w-[460px]">
        <p className="text-[13.5px] leading-[1.6] text-ink">Your request has been sent to your admin.</p>
        <Link href="/" className="mt-[16px] inline-block text-[13px] text-brand-2 hover:underline">
          Back to your workspace
        </Link>
      </Card>
    );
  }

  if (status === "error" && errorKind === "not_found") {
    return (
      <Card className="max-w-[460px]">
        <p className="text-[13.5px] leading-[1.6] text-ink">That organization isn&rsquo;t available.</p>
        <Link href="/" className="mt-[16px] inline-block text-[13px] text-brand-2 hover:underline">
          Back to your workspace
        </Link>
      </Card>
    );
  }

  return (
    <Card className="max-w-[460px]">
      <PageTitle italicWord={orgNameFromToken ?? "CoralSwarm"}>
        {orgNameFromToken ? `Join ${orgNameFromToken} on CoralSwarm?` : "Join this organization on CoralSwarm?"}
      </PageTitle>
      {status === "error" && errorKind === "generic" && (
        <ErrorNote className="mt-[12px]">Something went wrong — try again.</ErrorNote>
      )}
      <div className="mt-[16px] flex items-center gap-[8px]">
        <Button variant="brand" disabled={status === "sending" || requestsLoading} onClick={() => void ask()}>
          {status === "sending" ? "Sending…" : "Ask to join"}
        </Button>
        <Button variant="secondary" onClick={() => router.push("/")}>
          Not now
        </Button>
      </div>
    </Card>
  );
}
