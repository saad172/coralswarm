"use client";

/* The Google Meet side panel ("Add to CoralSwarm", inside the meeting).
 *
 * Founder decision 2026-10-06: a Meet transcript is saved only for a meeting the
 * member explicitly approves, and approval lives in Google Calendar (the Gmail
 * add-on's Calendar card) and HERE, inside the meeting. There is no approval
 * screen in the web app.
 *
 * One screen (mock: "MeetPanel" artboard): CoralSwarm header, "Save this meeting
 * to your ocean?", this meeting's code, the transcripts hint, then one of:
 *   signed out     -> Sign in to CoralSwarm (popup, Clerk OAuth, Meet's own client)
 *   ready          -> Add to CoralSwarm  (POST captures {space})
 *   added          -> ✓ Added + Undo      (DELETE captures/{id})
 *   not connected  -> Connect Google Meet (opens /connectors in a new tab)
 *   org disabled   -> says so
 * and the account footer (email, workspace, ⋮ Change workspace / Sign out).
 *
 * The approval is for the member's OWN copy only; the sweep writes nothing for
 * anyone else (services::google_meet_capture). On mount the panel resumes the
 * sign-in the client persisted from an earlier meeting (meetClient.ts), so the
 * sign-in card shows only when there is none. */

import { useCallback, useEffect, useRef, useState } from "react";
import { copy } from "@/lib/addin/copy";
import { DialogError, openSignInPopup, type PopupHandle } from "@/lib/addin/authDialog";
import {
  MeetApiError, MeetSignedOutError, captureForMeeting, isSignInCancelled,
  type MeetAccount, type MeetCapture, type MeetClient,
} from "@/lib/addin/meetClient";
import type { MeetingInfo } from "@/lib/addin/meetSdk";
import { FilledButton, OutlineButton, OverflowMenu } from "./kit";

type MeetingState = { status: "loading" } | { status: "none" } | { status: "ok"; info: MeetingInfo };
type Notice = "cancelled" | "failed" | "blocked" | "unavailable" | null;
type Phase =
  | { kind: "checking" }
  | { kind: "ready" }
  | { kind: "added"; capture: MeetCapture }
  | { kind: "not_connected" }
  | { kind: "org_disabled" }
  | { kind: "forbidden" }
  | { kind: "check_failed" };

export type MeetPanelClient = Pick<MeetClient, "enabled" | "resume" | "signIn" | "signOut" | "listCaptures" | "addCapture" | "removeCapture">;

export interface MeetSidePanelProps {
  client: MeetPanelClient;
  readMeeting: () => Promise<MeetingInfo>;
  /** Test seam; defaults to window.open("about:blank") in the click. */
  openPopup?: () => PopupHandle | null;
}

/** Map an API refusal that is about the workspace/connection, not this one action. */
function phaseForRefusal(cause: unknown): Phase | null {
  if (!(cause instanceof MeetApiError)) return null;
  if (cause.status === 409 && cause.code === "google_meet_not_connected") return { kind: "not_connected" };
  if (cause.status === 403 && cause.code === "org_google_meet_disabled") return { kind: "org_disabled" };
  if (cause.status === 403) return { kind: "forbidden" };
  return null;
}

export function MeetSidePanel({ client, readMeeting, openPopup = openSignInPopup }: MeetSidePanelProps) {
  const [meeting, setMeeting] = useState<MeetingState>({ status: "loading" });
  const [account, setAccount] = useState<MeetAccount | null>(null);
  const [resuming, setResuming] = useState(true);
  const [notice, setNotice] = useState<Notice>(null);
  const [signingIn, setSigningIn] = useState(false);
  const [phase, setPhase] = useState<Phase>({ kind: "checking" });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [checkKey, setCheckKey] = useState(0);
  const alive = useRef(true);
  const phaseRef = useRef(phase); phaseRef.current = phase;

  useEffect(() => {
    alive.current = true;
    readMeeting().then(
      (info) => { if (alive.current) setMeeting({ status: "ok", info }); },
      () => { if (alive.current) setMeeting({ status: "none" }); },
    );
    return () => { alive.current = false; };
  }, [readMeeting]);

  useEffect(() => {
    let current = true;
    client.resume().then(
      (next) => { if (current && next) setAccount(next); },
      () => {},
    ).finally(() => { if (current) setResuming(false); });
    return () => { current = false; };
  }, [client]);

  const signedOut = useCallback(() => { client.signOut(); setAccount(null); setNotice(null); setError(null); }, [client]);

  // Signed in with a meeting: read this member's existing approvals and find this meeting's.
  const info = meeting.status === "ok" ? meeting.info : null;
  useEffect(() => {
    if (!account || !info) return;
    const controller = new AbortController();
    setPhase({ kind: "checking" }); setError(null);
    client.listCaptures(controller.signal).then(
      (body) => {
        if (controller.signal.aborted) return;
        const existing = captureForMeeting(Array.isArray(body?.captures) ? body.captures : [], info);
        setPhase(existing ? { kind: "added", capture: existing } : { kind: "ready" });
      },
      (cause) => {
        if (controller.signal.aborted) return;
        if (cause instanceof MeetSignedOutError) { signedOut(); return; }
        setPhase(phaseForRefusal(cause) ?? { kind: "check_failed" });
      },
    );
    return () => controller.abort();
  }, [account, info, checkKey, client, signedOut]);

  // Back from connecting Google Meet in the other tab: check again.
  useEffect(() => {
    if (!account) return;
    const onFocus = () => {
      if (document.visibilityState === "hidden") return;
      if (phaseRef.current.kind === "not_connected") setCheckKey((k) => k + 1);
    };
    window.addEventListener("focus", onFocus);
    document.addEventListener("visibilitychange", onFocus);
    return () => { window.removeEventListener("focus", onFocus); document.removeEventListener("visibilitychange", onFocus); };
  }, [account]);

  /** MUST run synchronously inside the click: the popup is opened before any await. */
  function startSignIn(prompt?: "consent") {
    if (signingIn) return;
    if (!client.enabled) { setNotice("unavailable"); return; }
    const popup = openPopup();
    if (!popup) { setNotice("blocked"); return; }
    setSigningIn(true); setNotice(null);
    client.signIn(popup, prompt ? { prompt } : {}).then(
      (next) => { if (!alive.current) return; setAccount(next); setError(null); setCheckKey((k) => k + 1); },
      (cause) => {
        if (!alive.current) return;
        // Changing workspace and backing out keeps the current sign-in.
        if (isSignInCancelled(cause)) setNotice("cancelled");
        else if (cause instanceof DialogError && cause.code === "popup_blocked") setNotice("blocked");
        else setNotice("failed");
      },
    ).finally(() => { if (alive.current) setSigningIn(false); });
  }

  async function add() {
    if (busy || !info) return;
    setBusy(true); setError(null);
    try {
      const result = await client.addCapture(info.meetingId);
      if (!result?.capture?.id) throw new Error("No capture id");
      if (alive.current) setPhase({ kind: "added", capture: result.capture });
    } catch (cause) {
      if (!alive.current) return;
      if (cause instanceof MeetSignedOutError) { signedOut(); return; }
      const refusal = phaseForRefusal(cause);
      if (refusal) setPhase(refusal); else setError(copy.meetPanel.addFailed);
    } finally { if (alive.current) setBusy(false); }
  }

  async function undo(capture: MeetCapture) {
    if (busy) return;
    setBusy(true); setError(null);
    try {
      await client.removeCapture(capture.id);
      if (alive.current) setPhase({ kind: "ready" });
    } catch (cause) {
      if (!alive.current) return;
      if (cause instanceof MeetSignedOutError) { signedOut(); return; }
      if (cause instanceof MeetApiError && cause.status === 404) { setPhase({ kind: "ready" }); return; }
      if (cause instanceof MeetApiError && cause.status === 409 && cause.code === "already_ingested") { setError(copy.meetPanel.alreadySaved); return; }
      const refusal = phaseForRefusal(cause);
      if (refusal) setPhase(refusal); else setError(copy.meetPanel.undoFailed);
    } finally { if (alive.current) setBusy(false); }
  }

  const t = copy.meetPanel;
  const errorLine = (text: string | null) => (text ? <p role="alert" className="text-pane-sm text-bad">{text}</p> : null);
  const noticeText = notice === "cancelled" ? copy.signIn.cancelled
    : notice === "failed" ? copy.signIn.failed
    : notice === "blocked" ? t.popupBlocked
    : notice === "unavailable" ? t.signInUnavailable : null;

  let body: React.ReactNode;
  if (meeting.status === "loading") {
    body = <p role="status" className="text-ink-3">{t.loadingMeeting}</p>;
  } else if (meeting.status === "none") {
    body = <p role="alert" className="text-ink-2">{t.notInMeeting}</p>;
  } else {
    body = (
      <>
        <div className="flex flex-col gap-1">
          <span data-testid="meeting-code" className="font-mono text-pane-2xs text-ink-3">{t.thisMeeting(meeting.info.meetingCode ?? meeting.info.meetingId)}</span>
          <h1 className="font-display text-pane-title font-normal text-ink">{t.question} <em className="text-brand-2">{t.questionOcean}</em>?</h1>
        </div>
        {phase.kind !== "added" || !account ? (
          <div className="flex items-start gap-2.5 rounded-card border border-line bg-hue-amber/10 p-3.5">
            <span aria-hidden="true" className="mt-1.5 h-2 w-2 shrink-0 rounded-pill bg-warn" />
            <div className="flex flex-col gap-1">
              <span className="font-medium text-ink">{t.transcriptsTitle}</span>
              <span className="text-pane-sm text-ink-2">{t.transcriptsHint}</span>
            </div>
          </div>
        ) : null}
        {!account && resuming ? (
          <p role="status" className="text-ink-3">{t.resuming}</p>
        ) : !account ? (
          <>
            {noticeText ? <p role="alert" className="text-pane-sm text-ink-2">{noticeText}</p> : <p className="text-pane-sm text-ink-2">{t.signInHint}</p>}
            <FilledButton full disabled={signingIn} onClick={() => startSignIn()}>{notice === "cancelled" || notice === "failed" ? copy.retry : copy.signIn.button}</FilledButton>
            <p className="text-pane-xs text-ink-3">{t.ownCopy}</p>
          </>
        ) : (<>
          {noticeText ? <p role="status" className="text-pane-sm text-ink-2">{noticeText}</p> : null}
          <PhaseBody phase={phase} busy={busy} error={error} workspace={account.workspace.name} errorLine={errorLine}
            onAdd={() => void add()} onUndo={(c) => void undo(c)} onRetry={() => setCheckKey((k) => k + 1)} />
        </>)}
      </>
    );
  }

  return (
    <main aria-label={t.label} className="flex min-h-screen flex-col bg-bg font-sans text-pane text-ink">
      <header className="flex items-center justify-between border-b border-line px-4 py-4">
        <span className="text-lg font-semibold tracking-tight">coralswarm</span>
        <span className="font-mono text-pane-label uppercase tracking-pane-label text-ink-3">{t.hostTag}</span>
      </header>
      <div className="flex flex-1 flex-col gap-4 px-4 py-5">{body}</div>
      {account ? (
        <footer className="flex items-center gap-2.5 border-t border-line px-4 py-3.5">
          <span aria-hidden="true" className="flex h-7 w-7 shrink-0 items-center justify-center rounded-pill border border-hue-pink/40 bg-hue-pink/20 text-pane-xs font-semibold">
            {(account.email || account.workspace.name || "?").slice(0, 1).toUpperCase()}
          </span>
          <div className="flex min-w-0 flex-1 flex-col">
            <span className="truncate text-pane-sm">{account.email}</span>
            <span data-testid="workspace-name" className="truncate font-mono text-pane-2xs text-ink-3">{account.workspace.name}</span>
          </div>
          <OverflowMenu label={copy.menu.label} header={account.email || undefined} subheader={copy.account.workspace(account.workspace.name)}
            items={[
              { label: copy.menu.changeWorkspace, onSelect: () => startSignIn("consent") },
              { label: t.signOut, onSelect: signedOut, destructive: true },
            ]} />
        </footer>
      ) : null}
    </main>
  );
}

function PhaseBody({ phase, busy, error, workspace, errorLine, onAdd, onUndo, onRetry }: {
  phase: Phase; busy: boolean; error: string | null; workspace: string;
  errorLine: (text: string | null) => React.ReactNode;
  onAdd: () => void; onUndo: (capture: MeetCapture) => void; onRetry: () => void;
}) {
  const t = copy.meetPanel;
  switch (phase.kind) {
    case "checking":
      return <p role="status" className="text-ink-3">{t.checking}</p>;
    case "ready":
      return (
        <>
          {errorLine(error)}
          <FilledButton full disabled={busy} onClick={onAdd}>{busy ? t.adding : t.add}</FilledButton>
          <p className="text-pane-xs text-ink-3">{t.ownCopy}</p>
        </>
      );
    case "added":
      return (
        <>
          <div className="flex flex-col gap-1 rounded-card border border-ok/40 bg-ok/5 p-3.5" aria-live="polite">
            <span className="font-medium text-ok">{t.added}</span>
            <span className="text-pane-sm text-ink-2">{t.addedHint}</span>
          </div>
          {errorLine(error)}
          <div className="flex self-start"><OutlineButton disabled={busy} onClick={() => onUndo(phase.capture)}>{busy ? t.undoing : t.undo}</OutlineButton></div>
          <p className="text-pane-xs text-ink-3">{t.ownCopy}</p>
        </>
      );
    case "not_connected":
      return (
        <>
          <p role="alert" className="text-ink-2">{t.notConnected}</p>
          <a href="/connectors" target="_blank" rel="noopener noreferrer"
            className="w-full rounded-control bg-brand px-4 py-2.5 text-center text-pane font-medium text-on-brand transition hover:bg-brand-2">{t.connect}</a>
          <button type="button" onClick={onRetry} className="self-start rounded-control px-1 py-1 text-pane-sm text-ink-2 underline">{copy.retry}</button>
        </>
      );
    case "org_disabled":
      return <p role="alert" className="text-ink-2">{t.orgDisabled(workspace)}</p>;
    case "forbidden":
      return <p role="alert" className="text-ink-2">{t.forbidden}</p>;
    case "check_failed":
      return (
        <>
          <p role="alert" className="text-ink-2">{t.checkFailed}</p>
          <FilledButton full onClick={onRetry}>{copy.retry}</FilledButton>
        </>
      );
  }
}
