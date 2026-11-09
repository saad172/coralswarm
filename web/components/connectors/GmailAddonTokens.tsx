"use client";

/* The "Gmail Add-on" section on Connectors — the one place a paired Gmail
 * Add-on can be seen and revoked from the web app.
 *
 * WHY IT HAS TO EXIST AT ALL: the add-on's credential is a long-lived PAT
 * (`csat_…`, `backend/src/routes/addin.rs`), not a Clerk session. Apps Script
 * keeps it in `PropertiesService` on a Google account this app can't reach,
 * so "Disconnect" inside Gmail is the ONLY revocation path without this
 * screen — and that path is unavailable in exactly the case that matters
 * ("I lost the phone/laptop that Gmail is signed in on"). Hence the list, the
 * per-token Revoke, and Revoke all.
 *
 * NO `confirm()` ANYWHERE. Revoke is two-step inline (`Revoke` → `Confirm` /
 * `Cancel`), the same shape `settings/account/page.tsx`'s "Leave org"
 * already uses. A native
 * `confirm()` would be the only modal dialog in the app and is suppressible
 * per-origin by the browser, which would turn a destructive act into a
 * one-click one for anyone who ever ticked that box.
 *
 * Every DECISION here (who sees it, how a row is labelled, what a failure
 * says, whether a failed revoke still drops its row) lives in
 * `addinTokensModel.ts` so it can be tested — this file is wiring, state and
 * markup only. */

import { useEffect, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { addinTokensQueryOptions } from "@/lib/appQueries";
import { api, ApiError, type AddinToken } from "@/lib/api";
import { Button, Card, ErrorNote, Loading, Mono } from "@/components/ui";
import {
  addinHeaderStatus,
  addinListErrorCopy,
  addinRevokeErrorCopy,
  addinSectionState,
  addinTokensAudience,
  revokeFailureClearsRow,
  toTokenRows,
  withoutToken,
} from "./addinTokensModel";

const NO_TOKENS: AddinToken[] = [];

function statusOf(e: unknown): number | null {
  return e instanceof ApiError ? e.status : null;
}

export function GmailAddonTokens({ initialTokens, computedAt }: { initialTokens?: AddinToken[]; computedAt?: string } = {}) {
  const { getAuthToken, authReady, queryScope, isPersonalOrg } = useApp();
  const { isLoaded: authLoaded, orgId, orgRole } = useAuth();
  // Derived in the model, not here — a personal org must land on "show" (see
  // that module's point 1), and `orgId != null` would get that wrong.
  const audience = addinTokensAudience({ loaded: authLoaded, orgId, orgRole, isPersonalOrg });

  const qc = useQueryClient();
  /* Identity-scoped: these rows are the account's live credentials, so
     rendering them under a different identity would be CWE-200. After an
     account/org switch the outgoing account's rows live under another key. */
  const options = { ...addinTokensQueryOptions({ getAuthToken, scope: queryScope }), ...(initialTokens ? { queryKey: ["addinTokens", queryScope, computedAt], initialData: initialTokens, staleTime: Infinity } : {}) };
  // Never call the endpoint outside the personal plane — it 400s there by
  // design, and "wait" means Clerk hasn't said which plane this is yet.
  const query = useQuery({ ...options, enabled: authReady && audience === "show" && !initialTokens });
  const visibleTokens = query.data ?? NO_TOKENS;
  const setTokens = (update: (t: AddinToken[]) => AddinToken[]) =>
    qc.setQueryData<AddinToken[]>(options.queryKey, (t) => (t ? update(t) : t));
  // True on EVERY attempt with nothing to show, a Retry included: otherwise a
  // retry after a failure would put the section back in the `empty` state —
  // re-announcing "no paired add-ons" while it is still in flight. See
  // `addinSectionState`'s own note.
  const loading = query.data === undefined && (query.isPending || query.isFetching);
  // Degrades, never throws: until the sibling backend PR deploys this
  // endpoint 404s, and a section that crashed the page on that would take
  // the whole Connectors screen down with it.
  const loadErr = !loading && query.error ? addinListErrorCopy(statusOf(query.error)) : null;
  const load = () => query.refetch();
  const [actionErr, setActionErr] = useState<string | null>(null);
  const [confirming, setConfirming] = useState<string | null>(null);
  const [revoking, setRevoking] = useState<string | null>(null);
  const [confirmAll, setConfirmAll] = useState(false);
  const [revokingAll, setRevokingAll] = useState(false);

  // A real account/org switch while this page stays mounted: no pending
  // confirmation or action error from the previous account carries over.
  useEffect(() => {
    setActionErr(null);
    setConfirming(null);
    setConfirmAll(false);
  }, [queryScope]);

  const rows = toTokenRows(visibleTokens);

  async function revokeOne(id: string): Promise<boolean> {
    const authToken = authReady ? getAuthToken() : null;
    if (!authToken) {
      setActionErr("Your session isn't ready yet — reload and try again.");
      return false;
    }
    try {
      await api.addin.revokeToken(authToken, id);
      setTokens((t) => withoutToken(t, id));
      return true;
    } catch (e) {
      const status = statusOf(e);
      setActionErr(addinRevokeErrorCopy(status));
      // A 404 means the row is provably gone already; anything else means it
      // may still be live and must stay revocable.
      if (revokeFailureClearsRow(status)) setTokens((t) => withoutToken(t, id));
      return revokeFailureClearsRow(status);
    }
  }

  async function handleRevoke(id: string) {
    setActionErr(null);
    setRevoking(id);
    try {
      await revokeOne(id);
    } finally {
      setRevoking(null);
      setConfirming(null);
    }
  }

  async function handleRevokeAll() {
    setActionErr(null);
    setRevokingAll(true);
    try {
      /* Sequential, not `Promise.all`: this is the "I lost my phone" path, so
         partial failure has to be legible — one bad DELETE leaves exactly its
         own row on screen, still revocable, instead of an unordered pile of
         rejections whose surviving rows nobody can map back. There are at
         most a handful of tokens; the latency is not the cost that matters
         here. `rows` is snapshotted first because `revokeOne` mutates
         `tokens` as it goes. */
      const ids = visibleTokens.map((t) => t.id);
      for (const id of ids) {
        await revokeOne(id);
      }
    } finally {
      setRevokingAll(false);
      setConfirmAll(false);
      setConfirming(null);
    }
  }

  /* ONE derived state for the header line, the body and the Revoke-all
     button. `rows.length === 0` is true while loading, after a failed load,
     AND when there genuinely are no tokens; reading the count directly is
     what made the header claim "no paired add-ons" above its own error line
     on the expected-404 path (CodeRabbit round 1 on #906). */
  const sectionState = addinSectionState({ loading, loadError: loadErr, rowCount: rows.length });
  const headerStatus = addinHeaderStatus(sectionState, rows.length);

  // Renders NOTHING in an organization workspace, and nothing before Clerk
  // has resolved which workspace this is.
  if (audience !== "show") return null;

  return (
    <>
      <Mono className="mb-[10px] tracking-[0.13em]">gmail add-on</Mono>
      <Card className="mb-[22px] flex flex-col gap-[11px]">
        <div className="flex items-start justify-between gap-[14px]">
          <div className="min-w-0">
            <span className="block text-[15px] font-semibold text-ink">Gmail Add-on</span>
            {/* `null` on the error state: the ErrorNote below already says
                what went wrong, and any count here would contradict it. */}
            {headerStatus !== null && (
              <Mono className="mt-[2px] block text-ink-3">{headerStatus}</Mono>
            )}
          </div>
          {sectionState === "loaded" &&
            (confirmAll ? (
              <span className="flex flex-none items-center gap-[8px]">
                <Button
                  variant="danger"
                  size="sm"
                  disabled={revokingAll}
                  onClick={() => void handleRevokeAll()}
                >
                  {revokingAll ? "revoking…" : "Confirm"}
                </Button>
                <Button size="sm" disabled={revokingAll} onClick={() => setConfirmAll(false)}>
                  Cancel
                </Button>
              </span>
            ) : (
              <Button
                variant="danger"
                size="sm"
                className="flex-none"
                onClick={() => {
                  setConfirming(null);
                  setConfirmAll(true);
                }}
              >
                Revoke all
              </Button>
            ))}
        </div>

        <p className="text-[13px] leading-[1.5] text-ink-2">
          One click on an open email saves its whole thread to your workspace. Each pairing gets its own
          long-lived key — revoking one signs that copy of the add-on out immediately.
        </p>

        {actionErr && <ErrorNote>{actionErr}</ErrorNote>}

        {/* Same `sectionState` the header used, so the two can never disagree
            about whether anything is paired. */}
        {sectionState === "loading" ? (
          <Loading what="loading add-on keys" className="py-[20px]" />
        ) : sectionState === "error" ? (
          <ErrorNote action={<Button size="sm" onClick={() => void load()}>Retry</Button>}>
            {loadErr}
          </ErrorNote>
        ) : sectionState === "empty" ? (
          <div className="rounded-control border border-dashed border-line bg-bg-2 px-[14px] py-[13px] text-[12.5px] leading-[1.55] text-ink-2">
            {/* Deliberately does NOT say "install it from the Google Workspace
                Marketplace": the add-on is not listed there (see
                extensions/gmail-addin/README.md — it installs today via Apps
                Script's own "Test deployments"), and copy that names a store
                page nobody can reach is worse than no copy. What IS true, and
                is the one thing someone needs to find, is where the card
                lives once the add-on is installed. */}
            No add-on is paired yet — with the CoralSwarm add-on installed, open any email in Gmail
            and tap <span className="text-ink">Connect CoralSwarm</span> on the CoralSwarm card in
            the right-hand sidebar.
          </div>
        ) : (
          <div className="flex flex-col rounded-control border border-line-2">
            {rows.map((row) => (
              <div
                key={row.id}
                className="flex items-center gap-[10px] border-b border-line-3 px-[12px] py-[9px] last:border-b-0"
              >
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-[12.5px] text-ink">{row.name}</span>
                  <span className="mt-[3px] block font-mono text-[10px] text-ink-3">
                    paired {row.createdLabel} · last used {row.lastUsedLabel}
                  </span>
                </span>
                {confirming === row.id ? (
                  <span className="flex flex-none items-center gap-[6px]">
                    <Button
                      variant="danger"
                      size="sm"
                      disabled={revoking === row.id}
                      onClick={() => void handleRevoke(row.id)}
                    >
                      {revoking === row.id ? "revoking…" : "Confirm"}
                    </Button>
                    <Button size="sm" disabled={revoking === row.id} onClick={() => setConfirming(null)}>
                      Cancel
                    </Button>
                  </span>
                ) : (
                  <Button
                    variant="danger"
                    size="sm"
                    className="flex-none"
                    disabled={revokingAll}
                    onClick={() => {
                      setConfirmAll(false);
                      setActionErr(null);
                      setConfirming(row.id);
                    }}
                  >
                    Revoke
                  </Button>
                )}
              </div>
            ))}
          </div>
        )}
      </Card>
    </>
  );
}
