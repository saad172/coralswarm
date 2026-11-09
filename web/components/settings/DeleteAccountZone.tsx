"use client";

import Link from "next/link";
import { useState } from "react";
import { ORG_DELETION_PATH } from "@/components/org/orgDeletionModel";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useReverification } from "@clerk/nextjs";
import { api, ApiError } from "@/lib/api";
import { deleteAccountQueryOptions } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";
import { Button, Input } from "@/components/ui";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { useSettingsRole } from "./useSettingsRole";
import { emailMatches, memberLabel, PENDING_PATH, startErrorCopy } from "./deleteAccountModel";

/* Settings → Account danger zone: "Delete account" (settings launch plan
   decisions 14, 17, 19). Backend: PR #1476.

   Shown only while the active context is the personal account. Team-org
   members get "Leave organization" (on the page itself) instead. A
   personal-context user who still belongs to a team org, or who shares the
   account with other people, sees WHY they cannot delete yet.

   Deleting is SCHEDULED, not immediate: the account locks now and is purged
   after a 7-day grace period unless the user cancels. So after the 202 we do
   NOT sign out; we go to the "Deletion scheduled" page, which is also where
   the central API error handling (`lib/api.ts`) sends a locked account. */

/** The shape Clerk's `useReverification` looks for in a fetcher's result. */
const REVERIFICATION_HINT = {
  clerk_error: { type: "forbidden", reason: "reverification-error", metadata: { reverification: "strict" } },
} as const;

export function DeleteAccountZone() {
  const { role, loaded } = useSettingsRole();
  const { authReady, getAuthToken, queryScope } = useApp();
  const qc = useQueryClient();
  const options = deleteAccountQueryOptions({ getAuthToken, scope: queryScope });
  const personal = loaded && role === "personal";
  const query = useQuery({ ...options, enabled: authReady && personal });

  // Clerk's re-verification: when the fetcher hands back a reverification hint
  // the hook opens Clerk's verify modal, then calls the fetcher once more.
  const start = useReverification(async (token: string, typedEmail: string) => {
    try {
      return await api.meDeleteAccount.post(token, typedEmail);
    } catch (e) {
      if (e instanceof ApiError && e.code === "reverification_required") return REVERIFICATION_HINT;
      throw e;
    }
  });

  const [open, setOpen] = useState(false);
  const [typed, setTyped] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  if (!personal || !query.data) return null;
  const { eligible, email, blocking_organizations: blocking, shared_account_members: sharedMembers } = query.data;

  function close() {
    setOpen(false);
    setTyped("");
    setErr(null);
  }

  async function confirm() {
    const token = getAuthToken();
    if (!token || busy || !emailMatches(typed, email)) return;
    setBusy(true);
    setErr(null);
    try {
      const res = await start(token, typed.trim());
      // A second hint means verification did not take.
      if (!res || "clerk_error" in res) throw new ApiError("reverification_required", 403, "reverification_required");
    } catch (e) {
      setErr(startErrorCopy(e));
      // The blockers may have changed under us; show the truth.
      if (e instanceof ApiError && (e.code === "team_org_membership" || e.code === "shared_personal_account")) {
        void qc.invalidateQueries({ queryKey: ["deleteAccount"] });
      }
      setBusy(false);
      return;
    }
    window.location.assign(PENDING_PATH);
  }

  return (
    <section
      aria-label="Delete account"
      className="mt-[18px] rounded-card border border-bad/35 px-[15px] py-[13px]"
    >
      <div className="text-[13.5px] font-medium text-ink">Delete account</div>
      {eligible ? (
        <div className="mt-[3px] flex items-center gap-[14px]">
          <p className="min-w-0 flex-1 text-[12.5px] leading-[1.5] text-ink-3">
            Permanently delete your account and everything in it. This can&apos;t be undone.
          </p>
          <Button variant="danger" size="sm" className="flex-none" onClick={() => setOpen(true)}>
            Delete account
          </Button>
        </div>
      ) : (
        <div className="mt-[3px] space-y-[8px] text-[12.5px] leading-[1.5] text-ink-3">
          {blocking.length > 0 && (
            <div>
              <p>
                You can&apos;t delete your account while you belong to an organization. Leave each organization
                below first. If you own one, transfer ownership or delete the organization before you leave.
              </p>
              <ul className="mt-[6px] list-disc pl-[18px]">
                {blocking.map((o) => (
                  <li key={o.account_id}>
                    <span className="font-mono text-ink-2">{o.slug}</span> ({o.owner ? "owner" : o.role})
                    {o.can_delete_org && o.clerk_org_id && (
                      <>
                        {" "}
                        <Link
                          className="underline"
                          href={`${ORG_DELETION_PATH}?org=${encodeURIComponent(o.clerk_org_id)}`}
                        >
                          Delete this organization
                        </Link>
                      </>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {sharedMembers.length > 0 && (
            <div>
              <p>Other people use this account: {sharedMembers.map(memberLabel).join(", ")}. Remove them first.</p>
            </div>
          )}
        </div>
      )}

      <ConfirmDialog
        open={open}
        title="Delete your account?"
        description="Deletion is scheduled, and you have 7 days to change your mind."
        confirmLabel="Schedule deletion"
        busyLabel="Scheduling…"
        busy={busy}
        error={err}
        onConfirm={() => void confirm()}
        onCancel={close}
        confirmDisabled={!emailMatches(typed, email)}
      >
        <ul className="mb-3 list-disc pl-[18px] text-[12.5px] leading-[1.6] text-ink-2">
          <li>Your account is locked now, and connected apps and devices are signed out.</li>
          <li>Your subscription is cancelled now. The current billing period is not refunded.</li>
          <li>
            In 7 days, all your workspaces, items, connections, credentials and keys are permanently deleted,
            unless you cancel from the email we send you or by signing back in. This can&apos;t be undone.
          </li>
          <li>Any extra usage you bought is forfeited when the deletion completes.</li>
          <li>Billing records are kept, as the law and our financial obligations require.</li>
        </ul>
        <Input
          label={
            <>
              Type <span className="font-mono">{email}</span> to confirm
            </>
          }
          value={typed}
          onChange={(e) => setTyped(e.target.value)}
          autoComplete="off"
          spellCheck={false}
          disabled={busy}
        />
      </ConfirmDialog>
    </section>
  );
}
