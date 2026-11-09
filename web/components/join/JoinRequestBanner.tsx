"use client";

// CS-439 (org-onboarding plan §3 item 39, D3/D9/D10/D13) — the requester's
// waiting-state banner. Mounted once in AppShell.tsx, beside TrialBanner,
// so it shows on every page of the personal ocean for as long as ≥1 open
// join request exists — the person is a real CoralSwarm user with their own
// personal ocean the entire time they wait (D3); this is the only thing that
// tells them a request even exists, since there is no email/push notice to
// the requester (that would be D9/D10's silent-rejection guarantee leaking
// through the back door).
//
// WHAT THIS DELIBERATELY DOES NOT SAY, per D9/D10/D13: no count, no ETA, no
// "position in queue", no "we'll email you", and never the word "pending" —
// `pending` already means an invited member who hasn't signed in (D3's
// `derive_pending`), the opposite of this person (signed in, no membership,
// no seat). The org's own admin-side pool state (D9: seats_used/purchased,
// "the pool was full") is never disclosed here either way — the backend
// already refuses to return it to a non-member; this component just doesn't
// invent a substitute.

import { useEffect, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { IconButton, Notice } from "@/components/ui";
import { IconClose } from "@/components/icons";
import { useMyJoinRequests } from "@/lib/useMyJoinRequests";

const DISMISS_PREFIX = "cs_join_request_dismissed:";

/** Keyed by BOTH the signed-in user and the org — `sessionStorage` outlives
 *  a logout/login in the same tab (CodeRabbit, PR #1023), and an org-only
 *  key would let one account's dismissal hide the next account's banner for
 *  the same org. No explicit reset-on-logout is needed alongside this: the
 *  in-memory `dismissed` state below is destroyed and recreated fresh with
 *  the rest of this component whenever `(app)/layout.tsx` unmounts the
 *  shell for a signed-out session. */
function dismissKey(userId: string, orgId: string): string {
  return `${DISMISS_PREFIX}${userId}:${orgId}`;
}

/** `sessionStorage` throws in a private window / with storage disabled —
 *  every read and write here is wrapped so that failure degrades to "not
 *  dismissed this render" rather than crashing the shell that hosts it.
 *  `userId: null` (identity not resolved yet) reads/writes as "not
 *  dismissed" rather than guessing a key — failing open to SHOWING the
 *  banner is the safe direction here. */
function readDismissed(userId: string | null | undefined, orgId: string): boolean {
  if (typeof window === "undefined" || !userId) return false;
  try {
    return window.sessionStorage.getItem(dismissKey(userId, orgId)) === "1";
  } catch {
    return false;
  }
}

function writeDismissed(userId: string | null | undefined, orgId: string): void {
  if (!userId) return;
  try {
    window.sessionStorage.setItem(dismissKey(userId, orgId), "1");
  } catch {
    // Dismissal just won't be remembered across a reload this session —
    // never let a storage failure block the click that hides the banner.
  }
}

export function JoinRequestBanner() {
  const { userId } = useAuth();
  const { requests } = useMyJoinRequests(true);
  const [dismissed, setDismissed] = useState<Set<string>>(new Set());

  // sessionStorage is only meaningful post-mount (this component also
  // renders under the build-time prerender, which has no `window` — see
  // lib/apiBase.ts's comment on the same constraint). Re-syncs whenever the
  // request list changes so a newly-arrived org's prior-session dismissal is
  // picked up too; never REMOVES an id already recorded here, so an
  // in-session dismissal (via the click handler below) can't be undone by
  // this effect re-running.
  useEffect(() => {
    if (requests.length === 0) return;
    setDismissed((prev) => {
      let next: Set<string> | null = null;
      for (const r of requests) {
        if (!prev.has(r.org.clerk_org_id) && readDismissed(userId, r.org.clerk_org_id)) {
          if (!next) next = new Set(prev);
          next.add(r.org.clerk_org_id);
        }
      }
      return next ?? prev;
    });
  }, [requests, userId]);

  const visible = requests.filter((r) => !dismissed.has(r.org.clerk_org_id));
  if (visible.length === 0) return null;

  return (
    <div className="flex flex-none flex-col gap-[8px] px-[20px] pt-[10px]">
      {visible.map((r) => {
        const name = r.org.name ?? "this organization";
        return (
          <Notice
            key={r.id}
            tone="ok"
            action={
              <IconButton
                label={`Dismiss join request notice for ${name}`}
                size={22}
                className="border-transparent"
                onClick={() => {
                  writeDismissed(userId, r.org.clerk_org_id);
                  setDismissed((prev) => new Set(prev).add(r.org.clerk_org_id));
                }}
              >
                <IconClose width={13} height={13} />
              </IconButton>
            }
          >
            <span className="block">Your request to join {name} has been sent to your admin.</span>
            <span className="mt-[2px] block text-ink-3">You can keep using your own workspace meanwhile.</span>
          </Notice>
        );
      })}
    </div>
  );
}
