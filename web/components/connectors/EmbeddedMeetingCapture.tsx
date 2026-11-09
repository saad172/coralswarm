"use client";

import { useEffect, useRef, useState } from "react";
import { SignIn, useAuth, useOrganization, useOrganizationList, useUser } from "@clerk/nextjs";
import { api, ApiError } from "@/lib/api";
import { useApp } from "@/lib/providers";
import { Button, ErrorNote } from "@/components/ui";
import { explainMeetingError } from "@/lib/addin/copy";

export type MeetingSelector = { join_url: string } | { teams_meeting_id: string; chat_id: string };
type Snapshot = Awaited<ReturnType<typeof api.msTeams.meetingStatus>>;

/** Select value for the personal workspace. Clerk org ids never take this form, so it cannot collide with one. */
const PERSONAL_WORKSPACE = "__personal__";

function explain(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.code === "availability_window_ended") return "The transcript capture window has ended for this meeting.";
    if (error.code === "occurrence_unverified") return "Microsoft could not confirm this meeting yet. Try again in a minute.";
    if (error.code === "microsoft_identity_mismatch") return "Connect the Microsoft account that added CoralSwarm to this meeting.";
    if (error.code === "meeting_context_unavailable") return "Add CoralSwarm to this meeting, then reopen this pane.";
    if (error.code === "consumer_identity_unsupported") return explainMeetingError(error.code);
  }
  return "This meeting could not be checked. Try again; capture has not changed.";
}

/** No app shell, external capture page, or client-selected ocean. */
export function EmbeddedMeetingCapture({ selector, openAuthentication, connectionReturnPath }: {
  selector: MeetingSelector;
  openAuthentication: (url: string) => Promise<void>;
  connectionReturnPath: "/teams/meeting/connected" | "/outlook/meeting/connected";
}) {
  const { isLoaded, isSignedIn, userId, sessionId, orgId, getToken } = useAuth();
  const { user } = useUser();
  const { organization } = useOrganization();
  const { isLoaded: organizationsLoaded, userMemberships, setActive } = useOrganizationList({ userMemberships: { infinite: true } });
  const { accountId, resolvedOrgId, ready, authReady, isPersonalOrg, bootError, retryBoot } = useApp();
  const [switching, setSwitching] = useState(false);
  const [switchError, setSwitchError] = useState(false);
  const { hasNextPage, isFetching, fetchNext } = userMemberships;
  useEffect(() => {
    // eslint-disable-next-line no-restricted-syntax -- Clerk SDK pagination of the caller's org memberships for the workspace picker, not an app API read
    if (hasNextPage && !isFetching) void fetchNext?.();
  }, [hasNextPage, isFetching, fetchNext]);
  const identity = `${accountId}:${userId}:${sessionId}:${orgId ?? "personal"}`;
  const selection = JSON.stringify(selector);
  const personal = !orgId || isPersonalOrg;
  if (!isLoaded) return <p role="status" className="p-5 text-sm">Loading sign-in…</p>;
  if (!isSignedIn) return <section className="p-5"><h1 className="mb-3 font-display text-2xl">Your meeting, your workspace.</h1><p className="mb-4 text-sm text-ink-2">Sign in here to choose your workspace and manage this meeting’s private transcript capture.</p><SignIn appearance={{ elements: { rootBox: "w-full max-w-full", cardBox: "!w-full !max-w-full !shadow-none", card: "!w-full !max-w-full !bg-bg !border !border-line !shadow-none !rounded-frame !px-4 !py-5" } }} routing="hash" oauthFlow="popup" forceRedirectUrl={typeof window === "undefined" ? undefined : window.location.href} signUpForceRedirectUrl={typeof window === "undefined" ? undefined : window.location.href} /></section>;
  return <main className="mx-auto max-w-lg p-5">
    <p className="mb-2 font-mono text-[10px] uppercase tracking-[.16em] text-ink-3">CoralSwarm · private capture</p>
    <h1 className="mb-4 font-display text-3xl">This meeting.</h1>
    <div className="mb-5 border-b border-line pb-4">
      <p className="mb-2 truncate text-xs text-ink-2">Signed in as {user?.primaryEmailAddress?.emailAddress ?? user?.fullName ?? "your account"}</p>
      <label className="block text-xs text-ink-2">Workspace
        <select aria-label="Workspace" className="mt-1 w-full rounded-row border border-line bg-bg px-3 py-2 text-sm text-ink" value={orgId ?? PERSONAL_WORKSPACE} disabled={switching || !organizationsLoaded} onChange={async (event) => {
          setSwitching(true); setSwitchError(false);
          // Clerk's personal workspace is the absence of an active organization: null, never a string.
          try { if (!setActive) throw new Error("Workspace switch unavailable"); await setActive({ organization: event.target.value === PERSONAL_WORKSPACE ? null : event.target.value }); }
          catch { setSwitchError(true); }
          finally { setSwitching(false); }
        }}><option value={PERSONAL_WORKSPACE}>Personal workspace</option>{(userMemberships.data ?? []).map(member => <option key={member.organization.id} value={member.organization.id}>{member.organization.name}</option>)}</select>
      </label>
      {switchError && <ErrorNote>Workspace could not change. Try again.</ErrorNote>}
    </div>
    {bootError ? <><ErrorNote>Your workspace could not load.</ErrorNote><Button onClick={retryBoot}>Retry workspace</Button></> : switching || !ready || !authReady || resolvedOrgId !== (orgId ?? null) || !accountId ? <p role="status" className="text-sm">Checking your workspace…</p> :
      <CurrentMeetingCapture key={`${identity}:${selection}`} selector={selector} identity={identity} getToken={getToken} personal={personal} workspace={personal ? "Personal workspace" : organization?.name ?? "Selected workspace"} openAuthentication={openAuthentication} connectionReturnPath={connectionReturnPath} />}
  </main>;
}

/** Identity and selector remount this controller; late reads and writes stay inert. */
export function CurrentMeetingCapture({ selector, identity, getToken, workspace, personal = false, openAuthentication, connectionReturnPath }: {
  selector: MeetingSelector; identity: string; getToken: () => Promise<string | null>; workspace: string; personal?: boolean;
  openAuthentication: (url: string) => Promise<void>; connectionReturnPath: string;
}) {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [connectRequired, setConnectRequired] = useState(false);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);
  const snapshotRef = useRef<Snapshot | null>(null);
  const alive = useRef(false);
  const locked = useRef(false);
  const sequence = useRef(0);
  /** A refused action's message outlives the status refresh that follows it, until the next action. */
  const actionError = useRef(false);
  const selectionKey = JSON.stringify(selector);
  const currentKey = `${identity}:${selectionKey}`;
  const liveKey = useRef(currentKey); liveKey.current = currentKey;
  const tokenGetter = useRef(getToken); tokenGetter.current = getToken;
  const current = () => alive.current && liveKey.current === currentKey;

  useEffect(() => {
    alive.current = true;
    let pending = false;
    const controller = new AbortController();
    async function read(initial = false) {
      if (!initial && (snapshotRef.current?.capture?.status !== "active" || !snapshotRef.current.availability_window_open)) return;
      if (pending || locked.current || document.visibilityState === "hidden") return;
      pending = true;
      const request = ++sequence.current;
      try {
        const token = await tokenGetter.current();
        if (!current() || controller.signal.aborted) return;
        if (!token) throw new Error("Session expired");
        const previous = snapshotRef.current;
        const target = previous?.capture ? { capture_id: previous.capture.capture_id } : selector;
        // eslint-disable-next-line no-restricted-syntax -- live meeting-status poll (15s, visibility-gated, paused while an action holds the lock) on an embedded Teams/Outlook pane; each snapshot is validated and fenced to this identity and selector
        const result = await api.msTeams.meetingStatus(token, target, controller.signal);
        if (previous && result.meeting_id !== previous.meeting_id) throw new Error("Meeting changed");
        if (!result.title && previous?.title) result.title = previous.title;
        if (!result.meeting_id || typeof result.availability_window_open !== "boolean" || typeof result.connection_required !== "boolean"
          || (result.capture && (!result.capture.capture_id || !["active", "stopped"].includes(result.capture.status)
            || !Number.isInteger(result.capture.retrieved_transcript_count) || result.capture.retrieved_transcript_count < 0)))
          throw new Error("Missing meeting status");
        if (current() && request === sequence.current) { snapshotRef.current = result; setSnapshot(result); if (!actionError.current) setError(null); setConnectRequired(result.connection_required); }
      } catch (cause) {
        if (current() && !controller.signal.aborted && request === sequence.current) {
          setSnapshot(null); setConnectRequired(cause instanceof ApiError && cause.code === "connect_required"); setError(explain(cause));
        }
      } finally { pending = false; }
    }
    void read(true);
    const timer = setInterval(() => void read(), 15_000);
    const visible = () => { if (document.visibilityState !== "hidden") void read(true); };
    document.addEventListener("visibilitychange", visible);
    window.addEventListener("focus", visible);
    return () => { alive.current = false; controller.abort(); clearInterval(timer); document.removeEventListener("visibilitychange", visible); window.removeEventListener("focus", visible); };
    // getToken rotates without restarting selection; selector is immutable under the keyed gate.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentKey, refresh]);

  async function act(operation: (token: string, selected: Snapshot) => Promise<string>) {
    if (locked.current || !snapshot) return;
    locked.current = true; ++sequence.current; actionError.current = false; setBusy(true); setError(null); setNotice(null);
    const selected = snapshot;
    try {
      const token = await tokenGetter.current();
      if (!current()) return;
      if (!token) throw new Error("Session expired");
      const message = await operation(token, selected);
      if (current()) { setNotice(message); setSnapshot(null); setRefresh(value => value + 1); }
    } catch (cause) { if (current()) { actionError.current = true; setError(explain(cause)); setRefresh(value => value + 1); } }
    finally { locked.current = false; if (current()) setBusy(false); }
  }
  const active = snapshot?.capture?.status === "active";
  const windowEnded = snapshot?.availability_window_open === false;
  const retrieved = snapshot?.capture?.last_retrieved_at;
  const hasTranscript = Boolean(retrieved && snapshot?.capture?.retrieved_transcript_count);
  return <section aria-label="Current meeting capture">
    <p className="mb-3 text-sm text-ink-2">{personal
      ? <>Only this meeting is selected. Transcripts go to your private workspace in your <strong>{workspace}</strong>, visible only to you. Microsoft shares transcripts for work or school accounts only.</>
      : <>Only this meeting is selected. Transcripts go to your member-owned private workspace in <strong>{workspace}</strong>.</>}</p>
    {snapshot && <h2 className="mb-4 break-words text-lg font-medium">{snapshot.title || "Selected meeting"}</h2>}
    {error && <ErrorNote>{error}</ErrorNote>}
    {notice && <p role="status" className="mb-3 text-sm">{notice}</p>}
    {connectRequired && (!snapshot || !windowEnded) && <><p className="mb-3 text-sm">Connect your own Microsoft account, then return to this pane. Connecting does not enable capture.</p><Button disabled={busy} onClick={async () => {
      if (locked.current) return;
      locked.current = true; setBusy(true);
      try {
        const token = await tokenGetter.current(); if (!current() || !token) return;
        const result = await api.msTeams.connectMeeting(token, connectionReturnPath);
        const url = new URL(result.authorization_url);
        if (url.protocol !== "https:" || url.hostname !== "login.microsoftonline.com" || url.port || url.username || url.password) throw new Error("Invalid sign-in destination");
        if (current()) await openAuthentication(url.href);
      } catch { if (current()) setError("Microsoft sign-in could not open. Try again."); }
      finally { locked.current = false; if (current()) setBusy(false); }
    }}>Connect Microsoft</Button><Button className="ml-2" disabled={busy} onClick={() => setRefresh(value => value + 1)}>Check connection</Button></>}
    {!snapshot ? connectRequired ? null : <><p role="status" className="mb-3 text-sm">{error ? "Capture status is unavailable." : "Verifying this meeting…"}</p>{error && <Button onClick={() => setRefresh(value => value + 1)}>Check again</Button>}</> : <>
      <div className="mb-4 border-l-2 border-brand bg-bg-2 px-4 py-3" aria-live="polite">
        <p className="font-medium">{windowEnded ? "Capture window ended" : active && connectRequired ? "Microsoft connection needed" : active ? hasTranscript ? "Transcript retrieved" : "Waiting for transcript" : snapshot.capture ? "Capture stopped" : "Capture is off"}</p>
        <p className="mt-1 text-xs text-ink-2">{windowEnded ? "This meeting’s transcript retrieval window has closed. Existing transcripts remain in your private workspace." : active && connectRequired ? "Capture is selected, but transcript retrieval is paused until you reconnect." : active ? "CoralSwarm checks for available transcripts automatically." : "Enable capture to allow transcript retrieval for this meeting."}</p>
        {retrieved && <p className="mt-2 text-xs">{hasTranscript ? <>Last successful check retrieved {snapshot.capture?.retrieved_transcript_count} transcript(s) from Microsoft. Processing into your private workspace can take a moment.</> : "No transcript was available at the last successful check."}</p>}
        {snapshot.capture?.last_checked_at && <p className="mt-2 text-xs text-ink-3">Last checked: {new Date(snapshot.capture.last_checked_at).toLocaleString()}</p>}
        {snapshot.capture?.last_error_code && <p role="status" className="mt-2 text-xs">{!active || windowEnded ? "The last transcript check did not complete." : connectRequired ? "Reconnect Microsoft to resume transcript checks." : "The last transcript check did not complete. CoralSwarm will retry while capture is enabled."}</p>}
      </div>
      <Button variant={active ? "secondary" : "primary"} disabled={busy || (!active && (windowEnded || connectRequired))} onClick={() => void act(async (token, selected) => {
        if (active) {
          if (!selected.capture) throw new Error("No verified capture");
          const result = await api.msTeams.stopMeetingSelection(token, selected.capture.capture_id);
          if (result.status !== "revoked") throw new Error("Stop unverified");
          return "Capture stopped. Transcripts already saved remain in your private workspace.";
        }
        const result = await api.msTeams.captureMeeting(token, { meeting_id: selected.meeting_id });
        if (result.meeting_id !== selected.meeting_id || !result.capture_id || result.status !== "waiting_for_transcript") throw new Error("Capture unverified");
        if (current()) snapshotRef.current = { ...selected, capture: { capture_id: result.capture_id, status: "active", last_checked_at: null, last_retrieved_at: null, retrieved_transcript_count: 0, last_error_code: null } };
        return "Capture enabled for this meeting. CoralSwarm checks for its transcript automatically.";
      })}>{busy ? "Updating…" : active ? "Stop capture" : "Enable capture"}</Button>
    </>}
    <p className="mt-4 border-t border-line pt-4 text-xs leading-relaxed text-ink-3">Capture retrieves a transcript that Microsoft makes available. Use Teams’ transcription controls to create it. Stop prevents future retrieval and keeps transcripts already saved.</p>
  </section>;
}
