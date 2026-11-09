"use client";

/* The connected Teams panel: read this meeting, then one of three states in one
 * card: Off (recording/transcription not on, no button; re-checked every 5 s),
 * Ready (one Add button) and Added. No status row, no Stop. Meeting identity is verified by the backend (the
 * selector is only a selector), exactly as in MeetingPane; this component just
 * shows less. Microsoft "connect" is folded into the Log in screen. */

import { useEffect, useRef, useState } from "react";
import { api, ApiError } from "@/lib/api";
import { copy, explainMeetingError } from "@/lib/addin/copy";
import { ReconnectRequiredError } from "@/lib/addin/microsoftSso";
import { FilledButton, Footer, PlusRow, Section } from "./kit";
import type { ConnectedContext } from "./AddinApp";
import type { MeetingSnapshot } from "./meetingModel";
import type { MeetingSelector } from "./MeetingPane";
import { HeroArt, HeroCard, LogInCard } from "./TeamsSimpleApp";

const POLL_MS = 5_000;

type Phase = "reading" | "off" | "ready" | "added" | "ended" | "login" | "read_failed" | "unsupported";

const isConsumerIdentity = (cause: unknown) => cause instanceof ApiError && cause.code === "consumer_identity_unsupported";

function addFailureMessage(cause: unknown): string {
  const known = cause instanceof ApiError && cause.code ? explainMeetingError(cause.code) : null;
  return known && known !== copy.meeting.explain.generic ? known : copy.teams.addFailed;
}

export function TeamsAddMeeting({ context, selector, connectionReturnPath, openAuthentication }: {
  context: ConnectedContext;
  selector: MeetingSelector;
  connectionReturnPath: "/teams/meeting/connected";
  openAuthentication: (url: string) => Promise<void>;
}) {
  // Personal and organization workspaces are both valid; the workspace and the meeting remount the controller,
  // so nothing started for one destination can finish for another.
  const { account_id, kind } = context.session.workspace;
  const identity = `${kind}:${account_id}:${JSON.stringify(selector)}`;
  return <Controller key={identity} context={context} selector={selector} personal={kind === "personal"} connectionReturnPath={connectionReturnPath} openAuthentication={openAuthentication} />;
}

function Controller({ context, selector, personal, connectionReturnPath, openAuthentication }: {
  context: ConnectedContext; selector: MeetingSelector; personal: boolean; connectionReturnPath: string; openAuthentication: (url: string) => Promise<void>;
}) {
  const { client } = context;
  const [phase, setPhase] = useState<Phase>("reading");
  const [snapshot, setSnapshot] = useState<MeetingSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const alive = useRef(false);
  const locked = useRef(false);
  const reconnect = useRef(context.reconnectRequired); reconnect.current = context.reconnectRequired;
  const phaseRef = useRef(phase); phaseRef.current = phase;

  // One read on open, plus one when the pane regains focus while Microsoft is still being connected.
  useEffect(() => {
    alive.current = true;
    const controller = new AbortController();
    (async () => {
      try {
        // eslint-disable-next-line no-restricted-syntax -- live meeting-status poll on an Office/Teams add-in pane with its own host token client; validated, fenced snapshots drive a state machine, not a cacheable read
        const result = await client.withToken((token) => api.msTeams.meetingStatus(token, selector, controller.signal));
        if (!alive.current || controller.signal.aborted) return;
        if (!result.meeting_id || typeof result.connection_required !== "boolean") throw new Error("Missing meeting status");
        setSnapshot(result); setError(null);
        const captured = result.capture?.status === "active";
        setPhase(captured ? "added" : result.availability_window_open === false ? "ended" : result.connection_required ? "login" : result.recording_active === true ? "ready" : "off");
      } catch (cause) {
        if (!alive.current || controller.signal.aborted) return;
        if (cause instanceof ReconnectRequiredError) { reconnect.current(); return; }
        if (cause instanceof ApiError && cause.code === "connect_required") { setPhase("login"); return; }
        if (isConsumerIdentity(cause)) { setError(explainMeetingError("consumer_identity_unsupported")); setPhase("unsupported"); return; }
        setError(copy.teams.checkFailed); setPhase("read_failed");
      }
    })();
    const onFocus = () => { if (document.visibilityState !== "hidden" && phaseRef.current === "login") setRefresh((value) => value + 1); };
    document.addEventListener("visibilitychange", onFocus);
    window.addEventListener("focus", onFocus);
    return () => { alive.current = false; controller.abort(); document.removeEventListener("visibilitychange", onFocus); window.removeEventListener("focus", onFocus); };
    // The selector is immutable under the keyed gate; the client rotates tokens on its own.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refresh]);

  // Off: ask again every 5 s while the pane is visible; flip to Ready when recording turns on.
  useEffect(() => {
    if (phase !== "off") return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setInterval> | null = null;
    let inFlight = false;
    const poll = async () => {
      if (inFlight || document.visibilityState === "hidden") return;
      inFlight = true;
      try {
        // eslint-disable-next-line no-restricted-syntax -- polls every 5s while the meeting is not recording, to flip the pane to Ready; same add-in state machine as above
        const result = await client.withToken((token) => api.msTeams.meetingStatus(token, selector, controller.signal));
        if (!alive.current || controller.signal.aborted || phaseRef.current !== "off") return;
        if (result.capture?.status === "active") setPhase("added");
        else if (result.recording_active === true) setPhase("ready");
      } catch { /* a failed check keeps the Off state; the next tick retries */ }
      finally { inFlight = false; }
    };
    const start = () => { if (!timer) timer = setInterval(() => void poll(), POLL_MS); };
    const stop = () => { if (timer) { clearInterval(timer); timer = null; } };
    const onVisibility = () => { if (document.visibilityState === "hidden") stop(); else { start(); void poll(); } };
    const onFocus = () => { if (document.visibilityState !== "hidden") { start(); void poll(); } };
    if (document.visibilityState !== "hidden") start();
    document.addEventListener("visibilitychange", onVisibility);
    window.addEventListener("focus", onFocus);
    return () => { controller.abort(); stop(); document.removeEventListener("visibilitychange", onVisibility); window.removeEventListener("focus", onFocus); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [phase]);

  async function addMeeting() {
    if (locked.current || !snapshot) return;
    locked.current = true; setBusy(true); setError(null);
    try {
      const result = await client.withToken((token) => {
        // A workspace or meeting change while the token was being minted must not send the write.
        if (!alive.current) throw new DOMException("Cancelled", "AbortError");
        return api.msTeams.captureMeeting(token, { meeting_id: snapshot.meeting_id, ..."chat_id" in selector ? { chat_id: selector.chat_id } : {} });
      });
      if (result.meeting_id !== snapshot.meeting_id || !result.capture_id || result.status !== "waiting_for_transcript") throw new Error("Capture unverified");
      if (alive.current) setPhase("added");
    } catch (cause) {
      if (!alive.current) return;
      if (cause instanceof ReconnectRequiredError) { reconnect.current(); return; }
      if (cause instanceof ApiError && cause.code === "connect_required") setPhase("login");
      else if (cause instanceof ApiError && cause.code === "recording_off") setPhase("off");
      else if (isConsumerIdentity(cause)) { setError(explainMeetingError("consumer_identity_unsupported")); setPhase("unsupported"); }
      else setError(addFailureMessage(cause));
    } finally { locked.current = false; if (alive.current) setBusy(false); }
  }

  async function logInToMicrosoft() {
    if (locked.current) return;
    locked.current = true; setBusy(true); setError(null);
    try {
      const result = await client.withToken((token) => api.msTeams.connectMeeting(token, connectionReturnPath));
      const url = new URL(result.authorization_url);
      if (url.protocol !== "https:" || url.hostname !== "login.microsoftonline.com" || url.port || url.username || url.password) throw new Error("Invalid sign-in destination");
      if (alive.current) await openAuthentication(url.href);
    } catch (cause) {
      if (!alive.current) return;
      if (cause instanceof ReconnectRequiredError) reconnect.current();
      else setError(copy.meeting.microsoftCouldNotOpen);
    } finally { locked.current = false; if (alive.current) setBusy(false); }
  }

  const errorLine = error ? <p role="alert" className="rounded-card border border-line bg-bg px-3 py-3 text-ink-2">{error}</p> : null;

  if (phase === "reading") return <p role="status" className="px-1 py-6 text-center text-ink-3">{copy.meeting.verifying}</p>;
  if (phase === "login") {
    return (<>
      <LogInCard busy={busy} onLogIn={() => void logInToMicrosoft()} hint={copy.teams.microsoftHint} />
      {errorLine}
    </>);
  }
  if (phase === "read_failed") {
    return (<>
      {errorLine}
      <Footer><FilledButton full onClick={() => { setPhase("reading"); setError(null); setRefresh((value) => value + 1); }}>{copy.retry}</FilledButton></Footer>
    </>);
  }
  if (phase === "unsupported") return <Section><PlusRow muted text={error ?? explainMeetingError("consumer_identity_unsupported")} /></Section>;
  if (phase === "ended") return <Section><PlusRow muted text={copy.teams.windowEnded} /></Section>;
  // Off, Ready, Adding, Add failed and Added are one card: same art circle, title and position throughout.
  const title = snapshot?.title || copy.meeting.fallbackTitle;
  const added = phase === "added";
  return (
    <HeroCard art={<HeroArt glyph={added ? "check" : "video"} hue={added ? "ok" : "coral"} />} title={title} titleAttr={title}>
      {added ? (
        <p aria-live="polite" className="text-center font-medium text-ink">{copy.teams.added}</p>
      ) : phase === "off" ? (
        <div className="text-center">
          <p className="font-bold text-ink">{copy.teams.recordingOffTitle}</p>
          <p className="mt-1 text-pane-xs text-ink-3">{copy.teams.recordingOffSteps}</p>
        </div>
      ) : (<>
        {error ? <p role="alert" className="text-center text-ink-2">{error}</p> : null}
        <FilledButton full disabled={busy} onClick={() => void addMeeting()}>{busy ? copy.teams.adding : copy.teams.addMeeting}</FilledButton>
        {personal ? <p className="text-center text-pane-xs text-ink-3">{copy.teams.personalNote}</p> : null}
      </>)}
    </HeroCard>
  );
}
