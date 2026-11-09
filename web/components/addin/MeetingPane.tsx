"use client";

/* The meeting surface for Teams (side panel) and Outlook (appointment pane):
 * title row, status line, one primary button. Organization and personal workspaces
 * both reach the same explicit consent; a personal workspace's transcript stays
 * private to its owner, and Microsoft only shares transcripts for work or school accounts.
 *
 * Controller semantics are the original embedded pane's (EmbeddedMeetingCapture
 * CurrentMeetingCapture): the host selector is only a selector, the backend
 * verifies the meeting before anything can be saved, status is read on open and
 * every 15 s while a save is active and visible, a workspace/selector change
 * remounts the controller so late reads and writes stay inert, and Stop keeps
 * transcripts already saved. What changed: the credential is the add-in session
 * (client.withToken re-trades silently on 401), and the words are Gmail's. */

import { useEffect, useRef, useState } from "react";
import { api, ApiError } from "@/lib/api";
import { copy, explainMeetingError } from "@/lib/addin/copy";
import { ReconnectRequiredError } from "@/lib/addin/microsoftSso";
import { FilledButton, Footer, OutlineButton, Row, Section } from "./kit";
import type { ConnectedContext } from "./AddinApp";
import { TranscribePrompt } from "./TranscribePrompt";
import { meetingStatusView, type MeetingSnapshot } from "./meetingModel";

export type MeetingSelector = { join_url: string } | { teams_meeting_id: string; chat_id: string };

export const POLL_MS = 15_000;

function explain(error: unknown): string {
  return explainMeetingError(error instanceof ApiError ? error.code : null);
}

export function MeetingPane(props: {
  context: ConnectedContext;
  selector: MeetingSelector;
  host: "teams" | "outlook";
  /** Opens a login.microsoftonline.com URL in the host's browser. */
  openAuthentication: (url: string) => Promise<void>;
  connectionReturnPath: "/teams/meeting/connected" | "/outlook/meeting/connected";
}) {
  const { context, selector } = props;
  // Personal and organization workspaces both remount on a workspace or meeting change.
  const identity = `${context.session.workspace.kind}:${context.session.workspace.account_id}:${JSON.stringify(selector)}`;
  return <MeetingController key={identity} {...props} />;
}

function MeetingController({ context, selector, host, openAuthentication, connectionReturnPath }: {
  context: ConnectedContext; selector: MeetingSelector; host: "teams" | "outlook";
  openAuthentication: (url: string) => Promise<void>; connectionReturnPath: string;
}) {
  const { client, toast } = context;
  const [snapshot, setSnapshot] = useState<MeetingSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [connectRequired, setConnectRequired] = useState(false);
  const [busy, setBusy] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const snapshotRef = useRef<MeetingSnapshot | null>(null);
  const alive = useRef(false);
  const locked = useRef(false);
  const sequence = useRef(0);
  /** A refused action's message outlives the status refresh that follows it, until the next action. */
  const actionError = useRef(false);
  const reconnect = useRef(context.reconnectRequired); reconnect.current = context.reconnectRequired;

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
        const previous = snapshotRef.current;
        const target = previous?.capture ? { capture_id: previous.capture.capture_id } : selector;
        // eslint-disable-next-line no-restricted-syntax -- live meeting-status poll on an Office/Teams add-in pane with its own host token client; validated, fenced snapshots drive a state machine, not a cacheable read
        const result = await client.withToken((token) => api.msTeams.meetingStatus(token, target, controller.signal));
        if (!alive.current || controller.signal.aborted) return;
        if (previous && result.meeting_id !== previous.meeting_id) throw new Error("Meeting changed");
        if (!result.title && previous?.title) result.title = previous.title;
        if (!result.meeting_id || typeof result.availability_window_open !== "boolean" || typeof result.connection_required !== "boolean"
          || (result.capture && (!result.capture.capture_id || !["active", "stopped"].includes(result.capture.status)
            || !Number.isInteger(result.capture.retrieved_transcript_count) || result.capture.retrieved_transcript_count < 0)))
          throw new Error("Missing meeting status");
        if (request === sequence.current) { snapshotRef.current = result; setSnapshot(result); if (!actionError.current) setError(null); setConnectRequired(result.connection_required); }
      } catch (cause) {
        if (!alive.current || controller.signal.aborted) return;
        if (cause instanceof ReconnectRequiredError) { reconnect.current(); return; }
        if (request === sequence.current) {
          setSnapshot(null); setConnectRequired(cause instanceof ApiError && cause.code === "connect_required"); setError(explain(cause));
        }
      } finally { pending = false; }
    }
    void read(true);
    const timer = setInterval(() => void read(), POLL_MS);
    const visible = () => { if (document.visibilityState !== "hidden") void read(true); };
    document.addEventListener("visibilitychange", visible);
    window.addEventListener("focus", visible);
    return () => { alive.current = false; controller.abort(); clearInterval(timer); document.removeEventListener("visibilitychange", visible); window.removeEventListener("focus", visible); };
    // The client rotates tokens without restarting selection; the selector is immutable under the keyed gate.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refresh]);

  async function act(operation: (token: string, selected: MeetingSnapshot) => Promise<string>) {
    if (locked.current || !snapshot) return;
    locked.current = true; ++sequence.current; actionError.current = false; setBusy(true); setError(null);
    const selected = snapshot;
    try {
      const message = await client.withToken((token) => {
        // A workspace or meeting change while the token was being minted must not send the write.
        if (!alive.current) throw new DOMException("Cancelled", "AbortError");
        return operation(token, selected);
      });
      if (alive.current) { toast(message); setSnapshot(null); setRefresh((value) => value + 1); }
    } catch (cause) {
      if (!alive.current) return;
      if (cause instanceof ReconnectRequiredError) { reconnect.current(); return; }
      actionError.current = true; setError(explain(cause)); setRefresh((value) => value + 1);
    } finally { locked.current = false; if (alive.current) setBusy(false); }
  }

  async function connectMicrosoft() {
    if (locked.current) return;
    locked.current = true; setBusy(true);
    try {
      const result = await client.withToken((token) => {
        if (!alive.current) throw new DOMException("Cancelled", "AbortError");
        return api.msTeams.connectMeeting(token, connectionReturnPath);
      });
      const url = new URL(result.authorization_url);
      if (url.protocol !== "https:" || url.hostname !== "login.microsoftonline.com" || url.port || url.username || url.password) throw new Error("Invalid sign-in destination");
      if (alive.current) await openAuthentication(url.href);
    } catch (cause) {
      if (!alive.current) return;
      if (cause instanceof ReconnectRequiredError) reconnect.current();
      else setError(copy.meeting.microsoftCouldNotOpen);
    } finally { locked.current = false; if (alive.current) setBusy(false); }
  }

  const view = snapshot ? meetingStatusView(snapshot, connectRequired) : null;
  const showConnect = connectRequired && (!snapshot || !view?.windowEnded);
  const saveLabel = copy.meeting.save[host];
  const workspaceName = context.session.workspace.name;

  return (
    <>
      <Section>
        <Row icon="subject" hue="amber" text={<span className="break-words">{snapshot?.title || copy.meeting.fallbackTitle}</span>} bottom={workspaceName} />
        {view ? (
          <div aria-live="polite">
            <Row icon={view.state === "saved" || view.state === "waiting" ? "check" : view.state === "connect_needed" || view.state === "connect_first" ? "key" : "sync"}
              hue={view.state === "saved" || view.state === "waiting" ? "ok" : view.state === "connect_needed" || view.state === "connect_first" ? "ink" : "teal"}
              text={<b className="font-medium">{view.line}</b>}
              bottom={<><span className="block">{view.hint}</span>{view.details.map((line) => <span key={line} className="mt-1 block">{line}</span>)}</>} />
          </div>
        ) : (
          <Row icon="sync" hue="teal" text={<span role="status">{error ? copy.meeting.unavailable : copy.meeting.verifying}</span>} />
        )}
        {error ? <p role="alert" className="px-3 py-2.5 text-ink-2">{error}</p> : null}
        {context.session.workspace.kind === "personal" ? <p className="px-3 py-2.5 text-pane-xs text-ink-3">{copy.meeting.personalNote}</p> : null}
        {/* Transcription only exists if Teams records it: say so wherever the user turns saving on (Outlook; Teams has its own pane). */}
        {host === "outlook" && snapshot && !view?.windowEnded && view?.state !== "stopped" && view?.state !== "connect_first"
          ? <TranscribePrompt kind="saved" auto={snapshot.auto_transcription ?? null} /> : null}
      </Section>
      <Footer>
        {showConnect ? (
          <>
            <OutlineButton disabled={busy} onClick={() => setRefresh((value) => value + 1)}>{copy.meeting.checkConnection}</OutlineButton>
            <FilledButton disabled={busy} onClick={() => void connectMicrosoft()}>{copy.meeting.connectMicrosoft}</FilledButton>
          </>
        ) : !snapshot ? (
          error ? <OutlineButton onClick={() => setRefresh((value) => value + 1)}>{copy.meeting.checkAgain}</OutlineButton> : null
        ) : view?.primary === "stop" ? (
          <OutlineButton disabled={busy} onClick={() => void act(async (token, selected) => {
            if (!selected.capture) throw new Error("No verified capture");
            const result = await api.msTeams.stopMeetingSelection(token, selected.capture.capture_id);
            if (result.status !== "revoked") throw new Error("Stop unverified");
            return copy.meeting.stoppedToast;
          })}>{busy ? copy.meeting.updating : copy.meeting.stop}</OutlineButton>
        ) : (
          <FilledButton disabled={busy || Boolean(view?.primaryDisabled)} onClick={() => void act(async (token, selected) => {
            const result = await api.msTeams.captureMeeting(token, { meeting_id: selected.meeting_id });
            if (result.meeting_id !== selected.meeting_id || !result.capture_id || result.status !== "waiting_for_transcript") throw new Error("Capture unverified");
            if (alive.current) snapshotRef.current = { ...selected, capture: { capture_id: result.capture_id, status: "active", last_checked_at: null, last_retrieved_at: null, retrieved_transcript_count: 0, last_error_code: null } };
            return copy.meeting.savedToast;
          })}>{busy ? copy.meeting.updating : saveLabel}</FilledButton>
        )}
      </Footer>
    </>
  );
}
