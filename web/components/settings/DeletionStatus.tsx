"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { useQuery } from "@tanstack/react-query";
import { plainPageSectionQuery, PLAIN_LOAD_PATHS } from "@/lib/pageShell";
import { api, type DeleteAccountEligibility } from "@/lib/api";
import { Button, ErrorNote } from "@/components/ui";
import { cancelErrorCopy, formatPurgeDate, RESTORED_COPY } from "./deleteAccountModel";

/* The "Deletion scheduled" page body (`/account/delete/pending`). It is where
   Delete account lands after the 202 and where `lib/api.ts` sends any locked
   account. It reads its own token from Clerk rather than the app boot, because
   a locked account's boot calls are exactly what is being refused. */

export function DeletionStatus() {
  const { isLoaded, isSignedIn, getToken, userId, orgId } = useAuth();
  const query = useQuery({ ...plainPageSectionQuery<DeleteAccountEligibility>(PLAIN_LOAD_PATHS.deleteAccount, `${userId}:${orgId ?? "personal"}`, {}, getToken), enabled: isLoaded && !!isSignedIn });
  const deletion = query.data?.deletion;
  const loadErr = query.isError;
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [restored, setRestored] = useState(false);

  useEffect(() => {
    if (!isLoaded) return;
    if (!isSignedIn) {
      window.location.assign("/login");
      return;
    }
  }, [isLoaded, isSignedIn, getToken]);

  const cancel = useCallback(async () => {
    setBusy(true);
    setErr(null);
    try {
      const token = await getToken();
      if (!token) throw new Error("no token");
      await api.meDeleteAccount.cancel(token);
      setRestored(true);
    } catch (e) {
      setErr(cancelErrorCopy(e));
    } finally {
      setBusy(false);
    }
  }, [getToken]);

  let title = "Loading…";
  let body: React.ReactNode = null;
  if (restored) {
    title = "Deletion cancelled";
    body = (
      <>
        <p>{RESTORED_COPY}</p>
        <Link className="mt-4 inline-block underline" href="/">Continue to Coralswarm</Link>
      </>
    );
  } else if (loadErr) {
    title = "Couldn't load your account status";
    body = <p>Refresh the page to try again.</p>;
  } else if (deletion !== undefined) {
    if (deletion?.state === "grace") {
      title = "Deletion scheduled";
      body = (
        <>
          <p>
            Your account is locked and will be permanently deleted on{" "}
            <strong>{formatPurgeDate(deletion.purge_after)}</strong>. Your subscription is already cancelled.
          </p>
          {deletion.can_cancel && (
            <Button className="mt-4" variant="danger" disabled={busy} onClick={() => void cancel()}>
              {busy ? "Cancelling…" : "Cancel deletion"}
            </Button>
          )}
        </>
      );
    } else if (deletion?.state === "running" || deletion?.state === "completed") {
      title = "Your account is being deleted";
      body = <p>The deletion has started and can&apos;t be cancelled. You don&apos;t need to do anything.</p>;
    } else {
      title = "No deletion pending";
      body = <Link className="underline" href="/">Continue to Coralswarm</Link>;
    }
  }

  return (
    <main className="mx-auto flex min-h-screen max-w-[460px] flex-col justify-center px-6 text-center">
      <h1 className="text-[22px] font-semibold tracking-[-0.01em] text-ink">{title}</h1>
      <div className="mt-3 text-[14px] leading-[1.6] text-ink-3">{body}</div>
      {err && <ErrorNote>{err}</ErrorNote>}
    </main>
  );
}
