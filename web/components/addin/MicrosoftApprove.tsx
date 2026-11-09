"use client";

// The Microsoft (Teams + Outlook) half of /addin/approve. The Gmail half stays in
// app/(app)/addin/approve/page.tsx exactly as it was; this renders only for
// `?kind=microsoft`. Flow: GET pair/info → pick a workspace → POST pair/approve
// {code, account_id}. The panes poll on their own and connect.

import { useQuery } from "@tanstack/react-query";
import { plainPageSectionQuery, PLAIN_LOAD_PATHS } from "@/lib/pageShell";
import { useEffect, useState } from "react";
import { useUser } from "@clerk/nextjs";
import { useApp } from "@/lib/providers";
import { api, ApiError, type AddinPairInfo } from "@/lib/api";
import { Button, Card, ErrorNote, Loading, Mono, PageHead } from "@/components/ui";
import { microsoftApprovedLine, microsoftApprovingLine, MICROSOFT_PAGE_INTRO, MICROSOFT_PAGE_TITLE, primaryEmailOf } from "@/lib/addinApprove";

type Status = "idle" | "approving" | "approved" | "error";

export function MicrosoftApprove({ code }: { code: string }) {
  const { authReady, getAuthToken, queryScope } = useApp();
  const { user } = useUser();
  const email = primaryEmailOf(user);
  const query = useQuery({ ...plainPageSectionQuery<AddinPairInfo>(PLAIN_LOAD_PATHS.addinPairInfo, queryScope, { code }, getAuthToken), enabled: authReady && !!code, staleTime: 0, gcTime: 0 });
  const info = query.data?.kind === "microsoft" ? query.data : null;
  const loadError = query.error ? query.error instanceof ApiError && [404, 410].includes(query.error.status) ? "This pairing code has expired. Start over in Teams or Outlook." : query.error.message : query.data && !info ? "This code isn't for the Teams or Outlook add-ins. Open this page from the link the add-in showed you." : null;
  const [picked, setPicked] = useState<string | null>(null);
  const [status, setStatus] = useState<Status>("idle");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => { if (info?.workspaces.length === 1) setPicked(info.workspaces[0].account_id); }, [info]);

  const chosen = info?.workspaces.find((w) => w.account_id === picked) ?? null;

  const onApprove = async () => {
    const token = authReady ? getAuthToken() : null;
    if (!token || !picked) { setStatus("error"); setError("Your session isn't ready yet — reload and try again."); return; }
    setStatus("approving"); setError(null);
    try { await api.addin.approveMicrosoft(token, code, picked); setStatus("approved"); }
    catch (e) { setStatus("error"); setError(e instanceof ApiError ? e.message : "Couldn't approve this code."); }
  };

  return (
    <>
      <PageHead title={MICROSOFT_PAGE_TITLE}>{MICROSOFT_PAGE_INTRO}</PageHead>
      <Card className="max-w-[460px]">
        {!code ? <ErrorNote>No pairing code was given. Open this page from the link the Teams or Outlook add-in showed you, not directly.</ErrorNote> : (
          <>
            <Mono className="mb-[6px] block text-ink-3">pairing code</Mono>
            <div className="mb-[14px] flex items-center justify-center rounded-control border border-line-2 bg-bg-2 py-[16px]">
              <span className="font-mono text-[22px] tracking-[0.18em] text-ink">{code}</span>
            </div>
            {loadError ? <ErrorNote>{loadError}</ErrorNote> : !info ? <Loading what="loading" /> : status === "approved" && chosen ? (
              <div className="rounded-control border px-[14px] py-[11px] text-[13px] leading-[1.55] text-ink"
                style={{ borderColor: "var(--ok)", background: "color-mix(in srgb, var(--ok) 8%, var(--bg))" }}>
                ✓ {microsoftApprovedLine(email, chosen.name)}
              </div>
            ) : (
              <>
                <fieldset className="mb-[14px]">
                  <legend className="mb-[6px] text-[13px] text-ink-2">Which workspace should the add-ins use? You can change it later from the add-in menu.</legend>
                  {info.workspaces.map((workspace) => (
                    <label key={workspace.account_id} className="mb-[6px] flex cursor-pointer items-center gap-[10px] rounded-control border border-line-2 bg-bg-2 px-[12px] py-[10px] text-[13.5px] text-ink">
                      <input type="radio" name="workspace" value={workspace.account_id} checked={picked === workspace.account_id} onChange={() => setPicked(workspace.account_id)} />
                      <span className="flex-1">{workspace.name}</span>
                      <span className="text-[12px] text-ink-3">{workspace.kind === "org" ? "Organization" : "Personal"}</span>
                    </label>
                  ))}
                </fieldset>
                <div className="mb-[14px] rounded-control border border-line-2 bg-bg-2 px-[12px] py-[10px]">
                  <span className="block break-all text-[13px] leading-[1.5] text-ink">{microsoftApprovingLine(email, chosen?.name ?? null)}</span>
                  <span className="mt-[5px] block text-[12px] leading-[1.5] text-ink-3">Only approve a code you just saw in Teams or Outlook. Not the right account? Sign out and sign back in as the one whose workspace these belong in.</span>
                </div>
                {error && <ErrorNote className="mb-[12px]">{error}</ErrorNote>}
                <Button variant="brand" full disabled={status === "approving" || !picked} onClick={onApprove}>{status === "approving" ? "Approving…" : "Approve"}</Button>
              </>
            )}
          </>
        )}
      </Card>
      <Mono className="mt-[14px] block">Didn&rsquo;t open this from Teams or Outlook? You can close this tab.</Mono>
    </>
  );
}
