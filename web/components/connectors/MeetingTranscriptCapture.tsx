"use client";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useAuth, useOrganization } from "@clerk/nextjs";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "@/lib/api";
import { useApp } from "@/lib/providers";
import { meetingCapturesQueryOptions } from "@/lib/appQueries";
import { Button, Card, ErrorNote, Input, Loading } from "@/components/ui";
import { explainMeetingError } from "@/lib/addin/copy";
import { meetingCapturePath, nativeMeetingCapturePath, validateMeetingCaptureQuery } from "@/lib/teams/meetingCapture";

function failureMessage(cause: unknown): string {
  if (cause instanceof ApiError) {
    if (cause.code === "occurrence_unverified") return "Microsoft could not confirm this meeting yet. Try again in a minute; no transcript capture was enabled.";
    if (cause.code === "meeting_context_unavailable") return "Add CoralSwarm to this meeting first, then reopen its meeting tab. The installed meeting context could not be verified.";
    if (cause.code === "microsoft_identity_mismatch") return "Use the Microsoft account that added CoralSwarm to this meeting. Capture was not enabled for a different account.";
    if (cause.code === "consumer_identity_unsupported") return explainMeetingError(cause.code);
    // A server answer we do not recognize is never echoed to the user.
    return "Unable to verify this meeting action.";
  }
  return cause instanceof Error ? cause.message : "Unable to verify this meeting action.";
}

type Capture = { capture_id: string; meeting_id: string; created_at: string; status: "active" };
export function MeetingTranscriptCapture() {
  const query = useSearchParams();
  const { accountId, resolvedOrgId, ready, authReady, isPersonalOrg, getAuthToken } = useApp();
  const { userId, sessionId, orgId, isLoaded } = useAuth();
  const { organization } = useOrganization();
  if (!ready || !authReady || !isLoaded || !accountId || resolvedOrgId !== (orgId ?? null)) return <Loading what="meeting capture" />;
  try { validateMeetingCaptureQuery(query); }
  catch (cause) { return <Card><ErrorNote>{cause instanceof Error ? cause.message : "Invalid meeting link."}</ErrorNote></Card>; }
  const personal = !orgId || isPersonalOrg;
  return <MemberMeetingCapture key={`${accountId}:${orgId ?? "personal"}:${userId}:${sessionId}:${query.get("join_url") ?? ""}:${query.get("meeting_ticket") ?? ""}:${query.get("teams_meeting_id") ?? ""}:${query.get("teams_chat_id") ?? ""}`} getAuthToken={getAuthToken} scope={`${accountId}:${orgId ?? "personal"}:${userId}:${sessionId}`} candidate={query.get("join_url") ?? ""} ticket={query.get("meeting_ticket")} nativeMeetingId={query.get("teams_meeting_id")} nativeChatId={query.get("teams_chat_id")} personal={personal} workspace={personal ? "Personal workspace" : organization?.name ?? "Selected organization"} />;
}
function MemberMeetingCapture({ getAuthToken, scope, candidate, ticket, nativeMeetingId, nativeChatId, personal, workspace }: { getAuthToken: () => string | null; scope: string; candidate: string; ticket: string | null; nativeMeetingId: string | null; nativeChatId: string | null; personal: boolean; workspace: string }) {
  const [resolvedMeeting, setResolvedMeeting] = useState<string | null>(null);
  const [resolveAttempt, setResolveAttempt] = useState(0);
  const [connectRequiredByAction, setConnectRequired] = useState(false);
  const [joinUrl, setJoinUrl] = useState(candidate);
  const qc = useQueryClient();
  // Keyed on the member identity the parent also remounts on.
  const capturesOptions = meetingCapturesQueryOptions({ getAuthToken, scope });
  const capturesQuery = useQuery(capturesOptions);
  // An action receipt may mask a stopped selection locally. Never edit the
  // payload while retaining a Stored certificate from an older worker read.
  const [revoked, setRevoked] = useState<Set<string>>(new Set());
  const captures: Capture[] = (capturesQuery.data ?? []).filter((capture) => !revoked.has(capture.capture_id));
  const [actionError, setError] = useState<string | null>(null);
  const error = actionError ?? (capturesQuery.error ? failureMessage(capturesQuery.error) : null);
  const connectRequired = connectRequiredByAction || (capturesQuery.error instanceof ApiError && capturesQuery.error.code === "connect_required");
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const alive = useRef(true);
  const locked = useRef(false);
  useEffect(() => {
    alive.current = true;
    return () => { alive.current = false; };
  }, []);
  useEffect(() => {
    if (!nativeMeetingId) return;
    let current = true;
    const controller = new AbortController();
    setResolvedMeeting(null);
    if (!nativeChatId) { setError("This meeting link is missing its conversation context. Open the CoralSwarm meeting tab again."); return; }
    const auth = getAuthToken();
    // eslint-disable-next-line no-restricted-syntax -- a POST that has Microsoft verify this tab's native meeting context; the resolution the page exists to perform, not a cacheable read
    if (auth) void api.msTeams.meetingContext(auth, { teams_meeting_id: nativeMeetingId, chat_id: nativeChatId }, controller.signal).then((result) => {
      if (!result.meeting_id) throw new Error("Microsoft did not verify this meeting.");
      if (current) { setResolvedMeeting(result.meeting_id); setError(null); setConnectRequired(false); }
    }).catch((cause: unknown) => {
      if (!current) return;
      if (cause instanceof ApiError && cause.code === "connect_required") setConnectRequired(true);
      setError(failureMessage(cause));
    });
    return () => { current = false; controller.abort(); };
  }, [nativeMeetingId, nativeChatId, getAuthToken, resolveAttempt]);
  async function act(action: (auth: string) => Promise<string>) {
    if (locked.current) return;
    locked.current = true; setBusy(true); setError(null); setStatus(null);
    try {
      const auth = getAuthToken(); if (!auth) throw new Error("Your session isn't ready. Try again.");
      const message = await action(auth); if (alive.current) setStatus(message);
    } catch (cause) { if (alive.current) { if (cause instanceof ApiError && cause.code === "connect_required") setConnectRequired(true); setError(failureMessage(cause)); } }
    finally { if (alive.current) { locked.current = false; setBusy(false); } }
  }
  return <main className="mx-auto max-w-3xl p-6">
    <Link href="/connectors/microsoft-teams" className="text-sm">← Microsoft Teams</Link>
    <h1 className="mt-4 mb-2 text-2xl font-semibold">Meeting transcript capture</h1>
    <p className="mb-2 text-sm">Workspace: <strong>{workspace}</strong>. {personal ? "Capture is private to you as the owner of this workspace." : "Capture is private to your signed-in member."}</p>
    <p className="mb-5 text-sm">Only meetings you explicitly select are captured. Recording or transcription alone is not permission. Microsoft must also allow your account to read the transcript{personal ? ", which it only does for work or school accounts" : ""}.</p>
    {error && <ErrorNote>{error}</ErrorNote>}
    {status && <p role="status" className="my-3 text-sm">{status}</p>}
    {nativeMeetingId && !resolvedMeeting && <Button disabled={busy} onClick={() => setResolveAttempt((value) => value + 1)}>Verify this meeting again</Button>}
    {connectRequired && <Card>
      <p className="mb-3 text-sm">Connect your own Microsoft account. You will return to this meeting selection and confirm capture; connecting alone does not enable it.</p>
      <Button disabled={busy} onClick={() => void act(async (auth) => {
        const returnPath = ticket
          ? `/connectors/microsoft-teams/meeting-capture?meeting_ticket=${encodeURIComponent(ticket)}`
          : nativeMeetingId && nativeChatId
            ? nativeMeetingCapturePath(nativeMeetingId, nativeChatId)
            : meetingCapturePath(joinUrl.trim());
        const result = await api.msTeams.connectMeeting(auth, returnPath);
        const destination = new URL(result.authorization_url);
        if (destination.protocol !== "https:" || destination.hostname !== "login.microsoftonline.com" || destination.username || destination.password || destination.port)
          throw new Error("Microsoft sign-in could not be verified.");
        if (alive.current) window.location.assign(destination.href);
        return "Opening Microsoft sign-in. Capture still requires your confirmation afterward.";
      })}>Connect Microsoft</Button>
    </Card>}
    <Card>
      {!ticket && !nativeMeetingId && <Input type="url" label="Meeting join link" value={joinUrl} onChange={(event) => setJoinUrl(event.target.value)} disabled={busy} wrapperClassName="my-3" />}
      <Button variant="primary" disabled={busy || (nativeMeetingId ? !resolvedMeeting : !ticket && !joinUrl.trim())} onClick={() => void act(async (auth) => {
        if (!ticket && !nativeMeetingId) meetingCapturePath(joinUrl.trim());
        const result = await api.msTeams.captureMeeting(auth, ticket ? { meeting_ticket: ticket } : resolvedMeeting ? { meeting_id: resolvedMeeting } : { join_url: joinUrl.trim() });
        if (result.status !== "waiting_for_transcript" || !result.meeting_id || !result.capture_id) throw new Error("Capture could not be verified.");
        if (alive.current) {
          setConnectRequired(false);
          setRevoked((previous) => { const next = new Set(previous); next.delete(result.capture_id); return next; });
          void qc.fetchQuery({ ...capturesOptions, staleTime: 0 })
            .catch(() => { if (alive.current) setError("Capture was enabled, but the selections list could not refresh. Reload this page to manage it."); });
        }
        return "Capture enabled for this meeting only. Waiting for a transcript; nothing has been fetched by this action.";
      })}>Enable capture for this meeting</Button>
      <p className="mt-3 text-sm">Stopping capture prevents future transcript reads; it does not delete transcripts already saved.</p>
    </Card>
    <h2 className="mt-6 mb-3 text-lg font-semibold">Your selected meetings</h2>
    {!captures.length && <p className="text-sm">No verified meeting selections loaded.</p>}
    {captures.map((capture) => <Card key={capture.capture_id} className="mb-3">
      <p className="mb-2 break-all text-sm">Meeting {capture.meeting_id}</p>
      <p className="mb-3 text-sm">Capture enabled. CoralSwarm checks for available transcripts automatically.</p>
      <div className="flex gap-3">
        <Button disabled={busy} onClick={() => void act(async (auth) => { const result = await api.msTeams.syncMeeting(auth, { meeting_id: capture.meeting_id }); return `Fetched ${result.fetched} transcripts from this meeting; saved ${result.ingested}. Capture remains enabled.`; })}>Sync this meeting</Button>
        <Button disabled={busy} onClick={() => void act(async (auth) => { const result = await api.msTeams.stopMeetingCapture(auth, capture.meeting_id); if (result.status !== "revoked") throw new Error("Stopping capture could not be verified."); if (alive.current) { setRevoked((previous) => new Set(previous).add(capture.capture_id)); void qc.invalidateQueries({ queryKey: capturesOptions.queryKey, exact: true }); } return "Future transcript capture stopped for this meeting."; })}>Stop capture</Button>
      </div>
    </Card>)}
  </main>;
}
