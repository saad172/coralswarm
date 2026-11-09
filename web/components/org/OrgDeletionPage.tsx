"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { useAuth, useClerk, useReverification } from "@clerk/nextjs";
import { skipToken, useQuery } from "@tanstack/react-query";
import { plainPageSectionQuery, PLAIN_LOAD_PATHS } from "@/lib/pageShell";
import { api, ApiError, type OrgDeletionStatus } from "@/lib/api";
import { Button, ErrorNote, Input } from "@/components/ui";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import {
  DELETE_ORG_FACTS,
  formatDeletionDate,
  nameMatches,
  orgDeletionErrorCopy,
  UNDONE_COPY,
} from "./orgDeletionModel";

/* `/org/deletion`: in-app org deletion (backend control 0387,
   routes/org_deletion.rs). One page for the whole flow:

   - before a deletion: the owner reads what happens and confirms by typing
     the org's name (Clerk re-verification first); anyone else is told only
     the owner can;
   - during the 7-day grace: everyone sees the date; the owner can download
     the export and undo;
   - after grace: "being deleted".

   The org comes from `?org=` (the account-deletion page links here for an org
   the user owns) or the active Clerk org. Like the account-deletion page it
   reads with its own Clerk token instead of the app boot, because while an
   org is being deleted every other route refuses that org's token
   (`lib/api.ts` sends a refused screen here). */

const REVERIFICATION_HINT = {
  clerk_error: { type: "forbidden", reason: "reverification-error", metadata: { reverification: "strict" } },
} as const;

export function OrgDeletionPage({ orgParam }: { orgParam: string | null }) {
  const { isLoaded, isSignedIn, orgId: activeOrg, getToken, userId } = useAuth();
  const clerk = useClerk();
  const orgId = orgParam || activeOrg || null;
  const query = useQuery<OrgDeletionStatus>({ ...(orgId
    ? plainPageSectionQuery<OrgDeletionStatus>(PLAIN_LOAD_PATHS.orgDeletion,
      `${userId}:${activeOrg ?? "personal"}`, { org_id: orgId }, getToken)
    : { queryKey: ["org-deletion-unselected", userId, activeOrg], queryFn: skipToken }),
    enabled: isLoaded && !!isSignedIn && !!orgId });
  const status = query.data ?? (query.isError ? null : undefined);
  const unavailable = query.error instanceof ApiError && query.error.status === 404;
  const [open, setOpen] = useState(false);
  const [typed, setTyped] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [undone, setUndone] = useState(false);

  const { refetch } = query;
  const load = useCallback(async () => { await refetch(); }, [refetch]);
  useEffect(() => {
    if (isLoaded && !isSignedIn) window.location.assign("/login");
  }, [isLoaded, isSignedIn]);

  const request = useReverification(async (token: string, org: string, name: string) => {
    try {
      return await api.orgDeletion.request(token, org, name);
    } catch (e) {
      if (e instanceof ApiError && e.code === "reverification_required") return REVERIFICATION_HINT;
      throw e;
    }
  });

  // The export is the org's data leaving: it needs the same fresh
  // re-verification as the request itself (the server answers 403
  // `reverification_required` otherwise).
  const exportOwned = useReverification(async (token: string, org: string) => {
    try {
      return await api.orgDeletion.exportFile(token, org);
    } catch (e) {
      if (e instanceof ApiError && e.code === "reverification_required") return REVERIFICATION_HINT;
      throw e;
    }
  });

  async function confirm() {
    if (!orgId || busy || !nameMatches(typed, status?.org_name)) return;
    setBusy(true);
    setErr(null);
    try {
      const token = await getToken();
      if (!token) throw new Error("no token");
      const res = await request(token, orgId, typed.trim());
      if (!res || "clerk_error" in res) throw new ApiError("reverification_required", 403, "reverification_required");
      setOpen(false);
      setTyped("");
      await load();
    } catch (e) {
      setErr(orgDeletionErrorCopy(e));
    } finally {
      setBusy(false);
    }
  }

  async function undo() {
    if (!orgId) return;
    setBusy(true);
    setErr(null);
    try {
      const token = await getToken();
      if (!token) throw new Error("no token");
      await api.orgDeletion.cancel(token, orgId);
      setUndone(true);
    } catch (e) {
      setErr(orgDeletionErrorCopy(e));
    } finally {
      setBusy(false);
    }
  }

  async function download() {
    if (!orgId) return;
    setBusy(true);
    setErr(null);
    try {
      const token = await getToken();
      if (!token) throw new Error("no token");
      const res = await exportOwned(token, orgId);
      if (!res || "clerk_error" in res) throw new ApiError("reverification_required", 403, "reverification_required");
      const blob = res;
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "coralswarm-organization-export.ndjson";
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setErr(orgDeletionErrorCopy(e));
    } finally {
      setBusy(false);
    }
  }

  const toPersonal = () =>
    void clerk.setActive({ organization: null }).then(() => window.location.assign("/"));

  let title = "Loading…";
  let body: React.ReactNode = null;
  const d = status?.deletion ?? null;

  if (!orgId) {
    title = "No organization selected";
    body = <p>Choose the organization in the menu first.</p>;
  } else if (unavailable) {
    title = "Not available";
    body = <p>Deleting an organization isn&apos;t available yet. Contact support.</p>;
  } else if (undone) {
    title = "Deletion undone";
    body = (
      <>
        <p>{UNDONE_COPY}</p>
        <Link className="mt-4 inline-block underline" href="/settings/organization/billing">
          Go to Billing
        </Link>
      </>
    );
  } else if (status === null) {
    title = "Couldn't load the organization";
    body = <p>Refresh the page to try again.</p>;
  } else if (status && d && (d.state === "requested" || d.state === "grace")) {
    title = "Organization scheduled for deletion";
    body = (
      <>
        <p>
          Access has ended for everyone in this organization. It will be permanently deleted on{" "}
          <strong>{formatDeletionDate(d.grace_ends_at)}</strong>.
        </p>
        {status.is_owner ? (
          <div className="mt-4 flex flex-col items-center gap-[10px]">
            {d.export_available && (
              <Button variant="secondary" disabled={busy} onClick={() => void download()}>
                Download an export
              </Button>
            )}
            {(d.can_cancel || d.state === "requested") && (
              <Button variant="danger" disabled={busy} onClick={() => void undo()}>
                {busy ? "Working…" : "Undo the deletion"}
              </Button>
            )}
            <p className="text-[12.5px]">
              Undoing brings access back but not the subscription: subscribe again from Billing.
            </p>
          </div>
        ) : (
          <>
            <p className="mt-2">Only the organization&apos;s owner can download its data or undo the deletion.</p>
            <Button className="mt-4" variant="secondary" onClick={toPersonal}>
              Go to your personal workspace
            </Button>
          </>
        )}
      </>
    );
  } else if (status && d && (d.state === "awaiting_approval" || d.state === "destroying" || d.state === "done")) {
    title = "This organization is being deleted";
    body = (
      <>
        <p>The deletion has started and can&apos;t be undone.</p>
        <Button className="mt-4" variant="secondary" onClick={toPersonal}>
          Go to your personal workspace
        </Button>
      </>
    );
  } else if (status) {
    title = "Delete organization";
    if (!status.is_owner) {
      body = <p>Only the organization&apos;s owner can delete it.</p>;
    } else if (status.blocked_reason === "not_self_serve") {
      body = <p>This organization was set up by CoralSwarm. Contact support to delete it.</p>;
    } else {
      body = (
        <>
          <ul className="mx-auto mt-1 max-w-[420px] list-disc pl-[18px] text-left text-[13px] leading-[1.6]">
            {DELETE_ORG_FACTS.map((f) => (
              <li key={f}>{f}</li>
            ))}
          </ul>
          <Button className="mt-4" variant="danger" onClick={() => setOpen(true)}>
            Delete organization
          </Button>
        </>
      );
    }
  }

  return (
    <main className="mx-auto flex min-h-screen max-w-[520px] flex-col justify-center px-6 text-center">
      <h1 className="text-[22px] font-semibold tracking-[-0.01em] text-ink">{title}</h1>
      <div className="mt-3 text-[14px] leading-[1.6] text-ink-3">{body}</div>
      {err && !open && <ErrorNote>{err}</ErrorNote>}
      <ConfirmDialog
        open={open}
        title="Delete this organization?"
        description="Access ends now for everyone, with no refund. Every member's private notes here will be permanently deleted too, with no copy for them. You have 7 days to export or undo."
        confirmLabel="Delete organization"
        busyLabel="Deleting…"
        busy={busy}
        error={open ? err : null}
        onConfirm={() => void confirm()}
        onCancel={() => {
          setOpen(false);
          setTyped("");
          setErr(null);
        }}
        confirmDisabled={!nameMatches(typed, status?.org_name)}
      >
        <Input
          label={
            <>
              Type <span className="font-mono">{status?.org_name ?? ""}</span> to confirm
            </>
          }
          value={typed}
          onChange={(e) => setTyped(e.target.value)}
          autoComplete="off"
          spellCheck={false}
          disabled={busy}
        />
      </ConfirmDialog>
    </main>
  );
}
