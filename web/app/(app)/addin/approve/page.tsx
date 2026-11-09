"use client";

// Gmail Add-on pairing approval (backend/src/routes/addin.rs, migrations
// 0209_addin_tokens/0210_addin_pairing_codes). Reached via a link the Add-on
// itself opens (`https://app.<apex>/addin/approve?code=<code>`) — never
// linked to from inside this app.
//
// Auth gating is the standard `(app)/` shell (`AppLayout` redirects to
// /login when signed out, and stashes this exact URL via
// `lib/postSignInRedirect.ts` so a first-time visitor lands back here —
// not on /ask — once they've signed in). By the time this component
// renders, the caller is authenticated.
//
// One click: show the code, approve it, tell the user to go back to Gmail.
// The backend refuses (400) an org-context approval outright (add-on tokens
// are personal-plane only) — that response's own message is what renders
// below on that path, no special-casing needed here.

import { Suspense, useState } from "react";
import { useSearchParams } from "next/navigation";
import { useUser } from "@clerk/nextjs";
import { useApp } from "@/lib/providers";
import { api, ApiError } from "@/lib/api";
import { Button, Card, ErrorNote, Loading, Mono, PageHead } from "@/components/ui";
import { approvalKind, approvedLine, approvingAsLine, primaryEmailOf } from "@/lib/addinApprove";
import { MicrosoftApprove } from "@/components/addin/MicrosoftApprove";

type Status = "idle" | "approving" | "approved" | "error";

function ApproveContent() {
  const searchParams = useSearchParams();
  const code = (searchParams.get("code") ?? "").trim();
  const { authReady, getAuthToken } = useApp();
  // WHICH ACCOUNT this code is about to be bound to. Approval binds a
  // long-lived bearer to the session in THIS browser, which is routinely a
  // different account from the one Gmail is open in — see lib/addinApprove.ts
  // for why naming it before (and after) the button is the whole point.
  const { user } = useUser();
  const email = primaryEmailOf(user);
  const [status, setStatus] = useState<Status>("idle");
  const [error, setError] = useState<string | null>(null);

  const onApprove = async () => {
    // Minted fresh here, not held in state — same reasoning every other
    // caller in this app follows (`connectors/page.tsx`'s own comment):
    // Clerk session tokens are short-lived, so the only one that's certainly
    // valid is the one fetched immediately before the request.
    const token = authReady ? getAuthToken() : null;
    if (!token) {
      setStatus("error");
      setError("Your session isn't ready yet — reload and try again.");
      return;
    }
    setStatus("approving");
    setError(null);
    try {
      await api.addin.approve(token, code);
      setStatus("approved");
    } catch (e) {
      setStatus("error");
      setError(e instanceof ApiError ? e.message : "Couldn't approve this code.");
    }
  };

  if (!code) {
    return (
      <Card className="max-w-[460px]">
        <ErrorNote>
          No pairing code was given. Open this page from the link the Gmail Add-on
          showed you, not directly.
        </ErrorNote>
      </Card>
    );
  }

  return (
    <Card className="max-w-[460px]">
      <p className="mb-[14px] text-[13.5px] leading-[1.6] text-ink-2">
        Approving this pairs the Gmail Add-on with{" "}
        <span className="text-ink">your</span> personal workspace. Only approve a code
        you just saw inside Gmail.
      </p>
      {/* The code is LABELLED, not just displayed: this page is reachable by
          URL, so "the code in front of me" and "the code this page is about
          to approve" are two different things, and only one of them is on
          screen. Naming it is what lets someone check them against each
          other. */}
      <Mono className="mb-[6px] block text-ink-3">pairing code</Mono>
      <div className="mb-[14px] flex items-center justify-center rounded-control border border-line-2 bg-bg-2 py-[16px]">
        <span className="font-mono text-[22px] tracking-[0.18em] text-ink">{code}</span>
      </div>
      {status === "approved" ? (
        <div
          className="rounded-control border px-[14px] py-[11px] text-[13px] leading-[1.55] text-ink"
          style={{ borderColor: "var(--ok)", background: "color-mix(in srgb, var(--ok) 8%, var(--bg))" }}
        >
          {/* Names the account a SECOND time, and quotes the add-on's own
              button label byte-for-byte — see lib/addinApprove.ts. */}
          ✓ {approvedLine(email)}
        </div>
      ) : (
        <>
          {/* Directly above the button, deliberately: this is the last thing
              read before the click that binds the credential. */}
          <div className="mb-[14px] rounded-control border border-line-2 bg-bg-2 px-[12px] py-[10px]">
            <span className="block break-all text-[13px] leading-[1.5] text-ink">
              {approvingAsLine(email)}
            </span>
            <span className="mt-[5px] block text-[12px] leading-[1.5] text-ink-3">
              Not the right account? Sign out and sign back in as the one whose workspace these emails
              belong in.
            </span>
          </div>
          {error && <ErrorNote className="mb-[12px]">{error}</ErrorNote>}
          <Button variant="brand" full disabled={status === "approving"} onClick={onApprove}>
            {status === "approving" ? "Approving…" : "Approve"}
          </Button>
        </>
      )}
    </Card>
  );
}

/** `?kind=microsoft` selects the Teams/Outlook flow (components/addin/
 *  MicrosoftApprove.tsx); anything else is the Gmail flow below, unchanged. */
function ApproveRouter() {
  const searchParams = useSearchParams();
  if (approvalKind(searchParams.get("kind")) === "microsoft")
    return <MicrosoftApprove code={(searchParams.get("code") ?? "").trim()} />;
  return <GmailApprove />;
}

export default function AddinApprovePage() {
  return (
    <Suspense fallback={<Loading what="loading" />}>
      <ApproveRouter />
    </Suspense>
  );
}

function GmailApprove() {
  return (
    <>
      <PageHead title="Connect the Gmail Add-on">
        One click pairs the Gmail Add-on with your workspace — no tokens to copy,
        nothing to type.
      </PageHead>
      {/* useSearchParams() needs a Suspense boundary — same convention
          connectors/page.tsx already uses for its own OAuth-redirect param. */}
      <Suspense fallback={<Loading what="loading" />}>
        <ApproveContent />
      </Suspense>
      <Mono className="mt-[14px] block">
        Didn&rsquo;t open this from Gmail? You can close this tab.
      </Mono>
    </>
  );
}
