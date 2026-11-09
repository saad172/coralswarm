"use client";

// The free-trial strip for a self-serve team (#1521): credits left and what
// happens when they run out. Mounted once in AppShell beside JoinRequestBanner.
// Admin-only. Boot seeds a distinct DB-only trial cache; full Billing keeps
// its complete summary and payment lookup. Unknown boot reads stay silent
// without a duplicate mount request. Explicit billing changes refresh both.
// Copy is decided by `trialBannerView` (lib/orgSelfserve.ts).

import { useEffect, useState } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { IconButton, Notice } from "@/components/ui";
import { IconClose } from "@/components/icons";
import { trialBannerQueryOptions } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";
import { useOrgAdmin } from "@/components/org/useOrgAdmin";
import { JUST_CREATED_KEY, trialBannerView, type TrialBannerView } from "@/lib/orgSelfserve";

const DISMISS_PREFIX = "cs_trial_banner_dismissed:";

function readSession(key: string): string | null {
  try {
    return window.sessionStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeSession(key: string, value: string): void {
  try {
    window.sessionStorage.setItem(key, value);
  } catch {
    /* not remembered across a reload; fine */
  }
}

export function TrialBanner() {
  const { authReady, getAuthToken, queryScope, bootstrapFacts } = useApp();
  const { loaded, hasOrg, isAdmin, orgId } = useOrgAdmin();
  const eligible = authReady && loaded && hasOrg && isAdmin;
  const query = useQuery({
    ...trialBannerQueryOptions({ getAuthToken, scope: queryScope }, bootstrapFacts?.trialBannerMode ?? "legacy"),
    enabled: eligible,
    // Boot seeds success OR unavailable(null). No duplicate mount request;
    // active invalidation can still replace either with a fresh DB-only read.
    refetchOnMount: bootstrapFacts == null,
  });
  const [dismissed, setDismissed] = useState(false);
  const [justCreated, setJustCreated] = useState(false);

  useEffect(() => {
    if (!orgId) return;
    setDismissed(readSession(DISMISS_PREFIX + orgId) === "1");
    setJustCreated(readSession(JUST_CREATED_KEY) === orgId);
  }, [orgId]);

  if (!eligible || !orgId) return null;
  const v = trialBannerView(query.data?.trial, justCreated);
  if (!v || (v.dismissible && dismissed)) return null;

  return (
    <TrialBannerNotice
      view={v}
      onDismiss={() => {
        writeSession(DISMISS_PREFIX + orgId, "1");
        setDismissed(true);
      }}
    />
  );
}

/** The strip itself, given what it says. */
export function TrialBannerNotice({ view: v, onDismiss }: { view: TrialBannerView; onDismiss: () => void }) {
  return (
    <div className="flex-none px-[20px] pt-[10px]">
      <Notice
        tone={v.tone}
        action={
          <span className="flex flex-none items-center gap-[10px]">
            {v.billingLink && (
              <Link href="/settings/organization/billing" className="text-[12.5px] font-medium text-ink underline">
                Billing
              </Link>
            )}
            {v.dismissible && (
              <IconButton
                label="Dismiss"
                size={22}
                className="border-transparent"
                onClick={onDismiss}
              >
                <IconClose width={13} height={13} />
              </IconButton>
            )}
          </span>
        }
      >
        {v.text}
      </Notice>
    </div>
  );
}
