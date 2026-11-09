"use client";

/* Outlook, unsaved Teams meeting (compose / organizer mode).
 *
 * Turning it on creates an intent on the server and writes its nonce into the
 * draft's CustomProperties. Nothing else about the draft changes: Outlook still
 * owns Save and Send. After the user saves or sends, the backend finds the nonce
 * on the saved event; it follows the event if it is moved and covers every
 * occurrence of a recurring meeting. Copy never names a time for that reason.
 *
 * Reopening the same draft reads the marker back, so the On state survives. */

import { useEffect, useMemo, useRef, useState } from "react";
import { api, ApiError, type OutlookIntentStatus } from "@/lib/api";
import { copy } from "@/lib/addin/copy";
import { ComposeIntentError, createComposeAdapter, composeSupported, readComposeRecurring, type ComposeAdapter } from "@/lib/addin/composeIntent";
import { mark } from "@/lib/addin/perf";
import { formatDeadline, formatMeetingWhen } from "@/lib/addin/meetingTime";
import { ReconnectRequiredError } from "@/lib/addin/microsoftSso";
import type { OfficeApi } from "@/lib/addin/office";
import type { ConnectedContext } from "./AddinApp";
import { FilledButton, Footer, OutlineButton, PlusRow, Row, Section } from "./kit";
import { TranscribePrompt } from "./TranscribePrompt";

export const COMPOSE_POLL_MS = 10_000;
export const COMPOSE_ACTIVE_POLL_MS = 15_000;
/** A status (or lookup) request that has not answered by now is reported instead of waiting silently. */
export const COMPOSE_STATUS_TIMEOUT_MS = 15_000;

const GONE: OutlookIntentStatus = { status: "error", error_code: "generic", recurring: false, next_occurrence_start: null, auto_transcription: null, manual_reason: null };

type Marker = { nonce: string; intentId: string | null; expiresAt: string | null };

export function OutlookComposePane({ context, office, openAuthentication }: {
  context: ConnectedContext; office: OfficeApi;
  /** Opens a login.microsoftonline.com URL in the host's browser. */
  openAuthentication: (url: string) => Promise<void>;
}) {
  if (context.session.workspace.kind !== "org") {
    return (<>
      <Section><PlusRow muted text={copy.meeting.needsOrg} /></Section>
      <Footer><FilledButton full onClick={context.openWorkspacePicker}>{copy.meeting.changeWorkspace}</FilledButton></Footer>
    </>);
  }
  if (!composeSupported(office.context.mailbox.item)) {
    return (<Section>
      <PlusRow muted text={copy.compose.unsupported} />
      <p role="status" className="px-3 py-2.5 text-ink-2">{copy.compose.unsupportedFallback}</p>
    </Section>);
  }
  return <Controller key={context.session.workspace.account_id} context={context} office={office} openAuthentication={openAuthentication} />;
}

function Controller({ context, office, openAuthentication }: { context: ConnectedContext; office: OfficeApi; openAuthentication: (url: string) => Promise<void> }) {
  const { client } = context;
  const adapter = useMemo<ComposeAdapter>(() => createComposeAdapter(office), [office]);
  const [marker, setMarker] = useState<Marker | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [status, setStatus] = useState<OutlookIntentStatus | null>(null);
  const [draftRecurring, setDraftRecurring] = useState(false);
  const [busy, setBusy] = useState<"on" | "off" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [statusError, setStatusError] = useState(false);
  const [connectRequired, setConnectRequired] = useState(false);
  const alive = useRef(false);
  const locked = useRef(false);
  const reconnect = useRef(context.reconnectRequired); reconnect.current = context.reconnectRequired;

  useEffect(() => {
    alive.current = true;
    adapter.read().then((found) => { if (alive.current) { setMarker(found); setLoaded(true); } })
      .catch(() => { if (alive.current) { setError(copy.compose.failed); setLoaded(true); } });
    void readComposeRecurring(office).then((value) => { if (alive.current) setDraftRecurring(value); });
    return () => { alive.current = false; };
  }, [adapter, office]);

  // A marker with a nonce but no intent id (written elsewhere): the nonce finds the intent. A 404 means the
  // server has no such intent; any other failure is shown and retried, never silently treated as local-only.
  const needsLookup = Boolean(marker && !marker.intentId);
  const markerNonce = marker?.nonce ?? null;
  useEffect(() => {
    if (!needsLookup || !markerNonce) return;
    let live = true; let timer: ReturnType<typeof setTimeout>; let attempts = 0;
    const nonce = markerNonce;
    const retryLater = () => { clearTimeout(timer); timer = setTimeout(() => void lookup(), COMPOSE_POLL_MS); };
    async function lookup() {
      // Each call is one logical attempt. The deadline ends it (and schedules the next); anything an older
      // attempt returns afterwards is ignored, so a late answer can't resurrect or overwrite newer state.
      const attempt = ++attempts;
      const slow = setTimeout(() => { if (live && attempt === attempts) { setStatusError(true); retryLater(); } }, COMPOSE_STATUS_TIMEOUT_MS);
      try {
        // eslint-disable-next-line no-restricted-syntax -- polls for the compose item the user is saving in Outlook (deadline + retry), on an add-in pane with its own host token client
        const found = await client.withToken((token) => api.outlookCaptureSchedule.byNonce(token, nonce));
        clearTimeout(slow);
        if (!live || attempt !== attempts) return;
        clearTimeout(timer);
        setStatusError(false); setMarker((previous) => ({ nonce, intentId: found.intent_id, expiresAt: previous?.expiresAt ?? null }));
      } catch (cause) {
        clearTimeout(slow);
        if (!live || attempt !== attempts) return;
        if (cause instanceof ReconnectRequiredError) { reconnect.current(); return; }
        if (cause instanceof ApiError && cause.status === 404) { clearTimeout(timer); setStatus(GONE); setStatusError(false); return; }
        setStatusError(true);
        retryLater();
      }
    }
    void lookup();
    return () => { live = false; clearTimeout(timer); };
    // The client rotates tokens without restarting the lookup.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [needsLookup, markerNonce]);

  // Status: fast while waiting for the user to save, slower once it is on.
  const intentId = marker?.intentId ?? null;
  const statusRef = useRef<OutlookIntentStatus | null>(null);
  /** The saved event was found but had no Teams link at some point: an expiry then is about the link, not the save. */
  const sawLinkMissing = useRef(false);
  useEffect(() => {
    if (!intentId) return;
    const id: string = intentId;
    let live = true, pending = false, attempts = 0;
    const controller = new AbortController();
    async function read() {
      const current = statusRef.current?.status;
      // Hidden tabs skip polling, but never the first answer: a pane that has not yet shown a status must ask,
      // whatever the host reports for visibility (an Outlook task pane is not guaranteed to say "visible").
      const hidden = document.visibilityState === "hidden" && statusRef.current !== null;
      if (pending || hidden || current === "stopped" || current === "expired" || current === "error") return;
      pending = true;
      mark("status-start");
      const attempt = ++attempts;
      // A request stuck in the token exchange or the network must not wedge polling or hide behind "On":
      // after the deadline say so, and let the next poll try again.
      const slow = setTimeout(() => { if (live && attempt === attempts) { pending = false; setStatusError(true); } }, COMPOSE_STATUS_TIMEOUT_MS);
      try {
        // eslint-disable-next-line no-restricted-syntax -- live capture-intent status poll (fast while pending save, slower once on) on an add-in pane; drives a state machine, not a cacheable read
        const next = await client.withToken((token) => api.outlookCaptureSchedule.status(token, id, controller.signal));
        clearTimeout(slow);
        if (!live || attempt !== attempts) return;
        mark("status-done");
        if (next.status === "pending_save" && next.error_code === "teams_link_missing") sawLinkMissing.current = true;
        statusRef.current = next; setStatus(next); setStatusError(false);
      } catch (cause) {
        clearTimeout(slow);
        if (!live || controller.signal.aborted || attempt !== attempts) return;
        if (cause instanceof ReconnectRequiredError) { reconnect.current(); return; }
        if (cause instanceof ApiError && cause.status === 404) {
          // This member has no such intent (deleted, or not theirs): a stale marker, not a network blip.
          statusRef.current = GONE; setStatus(GONE); return;
        }
        setStatusError(true);
      } finally { if (attempt === attempts) pending = false; }
    }
    void read();
    let timer: ReturnType<typeof setTimeout>;
    const schedule = () => {
      timer = setTimeout(() => { void read().finally(() => { if (live) schedule(); }); },
        statusRef.current?.status === "active" ? COMPOSE_ACTIVE_POLL_MS : COMPOSE_POLL_MS);
    };
    schedule();
    const visible = () => { if (document.visibilityState !== "hidden") void read(); };
    document.addEventListener("visibilitychange", visible);
    return () => { live = false; controller.abort(); clearTimeout(timer); document.removeEventListener("visibilitychange", visible); statusRef.current = null; };
    // The client rotates tokens without restarting polling.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intentId]);

  /** `again`: the current intent expired, so a new intent replaces its marker on the draft. */
  async function turnOn(again = false) {
    if (locked.current) return;
    locked.current = true; setBusy("on"); setError(null);
    let created: { intent_id: string; nonce: string; expires_at: string } | null = null;
    try {
      setConnectRequired(false);
      created = await client.withToken((token) => api.outlookCaptureSchedule.create(token));
      await adapter.stage(created.nonce, created.intent_id, { expiresAt: created.expires_at, replace: again });
      if (alive.current) { sawLinkMissing.current = false; statusRef.current = null; setStatus(null); setMarker({ nonce: created.nonce, intentId: created.intent_id, expiresAt: created.expires_at ?? null }); }
    } catch (cause) {
      if (cause instanceof ReconnectRequiredError) { reconnect.current(); return; }
      if (created) {
        const made = created;
        // Outlook never confirmed (timeout) but the write may still land: cancelling now could leave a
        // nonce on the draft for a cancelled intent. Only a definite refusal, or a failure before any
        // write, cancels. Otherwise keep the intent, trust the marker if it can be read, and say so.
        const ambiguous = cause instanceof ComposeIntentError && !cause.definitive && (cause.message === "timeout" || cause.message === "item_changed");
        if (ambiguous) {
          const landed = await adapter.read().catch(() => null);
          if (alive.current) {
            sawLinkMissing.current = false; statusRef.current = null; setStatus(null);
            setMarker({ nonce: made.nonce, intentId: made.intent_id, expiresAt: made.expires_at ?? null });
            if (landed?.nonce !== made.nonce) setError(copy.compose.unconfirmed);
          }
          return;
        }
        void client.withToken((token) => api.outlookCaptureSchedule.cancel(token, made.intent_id)).catch(() => {});
      }
      if (alive.current) {
        const code = !created && cause instanceof ApiError ? cause.code : null;
        if (code === "connect_required") { setConnectRequired(true); setError(null); }
        else setError((code && Object.prototype.hasOwnProperty.call(copy.compose.createFailed, code) && copy.compose.createFailed[code]) || copy.compose.failed);
      }
    } finally { locked.current = false; if (alive.current) setBusy(null); }
  }

  async function connectMicrosoft() {
    if (locked.current) return;
    locked.current = true; setBusy("on");
    try {
      const result = await client.withToken((token) => api.msTeams.connectMeeting(token, "/outlook/meeting/connected"));
      const url = new URL(result.authorization_url);
      if (url.protocol !== "https:" || url.hostname !== "login.microsoftonline.com" || url.port || url.username || url.password) throw new Error("Invalid sign-in destination");
      if (alive.current) await openAuthentication(url.href);
    } catch (cause) {
      if (cause instanceof ReconnectRequiredError) reconnect.current();
      else if (alive.current) setError(copy.meeting.microsoftCouldNotOpen);
    } finally { locked.current = false; if (alive.current) setBusy(null); }
  }

  async function turnOff() {
    if (locked.current || !marker) return;
    locked.current = true; setBusy("off"); setError(null);
    try {
      let id = marker.intentId;
      if (!id) {
        // Never drop to local-only: find the server intent first. 404 = the server has none.
        const nonce = marker.nonce;
        try { id = (await client.withToken((token) => api.outlookCaptureSchedule.byNonce(token, nonce))).intent_id; }
        catch (cause) { if (!(cause instanceof ApiError && cause.status === 404)) throw cause; }
      }
      if (id) {
        const target = id;
        try { await client.withToken((token) => api.outlookCaptureSchedule.cancel(token, target)); }
        catch (cause) { if (!(cause instanceof ApiError && cause.status === 404)) throw cause; }
      }
      await adapter.remove();
      if (alive.current) { sawLinkMissing.current = false; statusRef.current = null; setStatus(null); setMarker(null); setStatusError(false); }
    } catch (cause) {
      if (cause instanceof ReconnectRequiredError) { reconnect.current(); return; }
      if (alive.current) setError(copy.compose.offFailed);
    } finally { locked.current = false; if (alive.current) setBusy(null); }
  }

  if (!loaded) return <Section><Row icon="sync" hue="teal" text={<span role="status">{copy.compose.resumed}</span>} /></Section>;

  const recurring = status ? status.recurring : draftRecurring;
  const kind = recurring ? "series" : "compose";
  const workspaceName = context.session.workspace.name;

  if (!marker) {
    return (<>
      <Section>
        <Row icon="subject" hue="amber" text={<span>{copy.compose.save}</span>} bottom={workspaceName} />
        {recurring ? <Row icon="sync" hue="teal" text={<b className="font-medium">{copy.compose.seriesTitle}</b>} /> : null}
        <p className="px-3 py-2.5 text-ink-2">{copy.compose.saveHint}</p>
        <TranscribePrompt kind={kind} auto={null} />
        {connectRequired ? <p role="status" className="px-3 py-2.5 text-ink-2">{copy.compose.connectFirst}</p> : null}
        {error ? <p role="alert" className="px-3 py-2.5 text-ink-2">{error}</p> : null}
      </Section>
      <Footer>
        {connectRequired ? (<>
          <OutlineButton disabled={busy !== null} onClick={() => void turnOn()}>{copy.meeting.checkConnection}</OutlineButton>
          <FilledButton disabled={busy !== null} onClick={() => void connectMicrosoft()}>{copy.meeting.connectMicrosoft}</FilledButton>
        </>) : <FilledButton full disabled={busy !== null} onClick={() => void turnOn()}>{busy === "on" ? copy.compose.saving : copy.compose.save}</FilledButton>}
      </Footer>
    </>);
  }

  const phase = status?.status ?? "pending_save";
  // pending_save + teams_link_missing: the saved event has no Teams meeting yet. Still waiting, but it needs the user.
  const linkMissing = phase === "pending_save" && status?.error_code === "teams_link_missing";
  const line = phase === "active" ? (recurring ? copy.compose.activeLineSeries : copy.compose.activeLine)
    : phase === "stopped" ? copy.compose.stopped
    : phase === "expired" ? (sawLinkMissing.current ? copy.compose.expiredNoLink : copy.compose.expired)
    : linkMissing ? copy.compose.waitingForLink.title
    : phase === "error" ? copy.compose.intentError
    : recurring ? copy.compose.onSeries : copy.compose.on;
  const live = phase === "pending_save" || phase === "active";
  const code = status?.error_code ?? null;
  const table = (t: Record<string, string>) => (code && Object.prototype.hasOwnProperty.call(t, code) ? t[code] : null);
  const detail = phase === "error" ? (table(copy.compose.errors) ?? copy.compose.errors.generic)
    : linkMissing ? copy.compose.waitingForLink.body(formatDeadline(status?.expires_at ?? marker.expiresAt))
    : phase === "pending_save" ? table(copy.compose.errors)
    : phase === "stopped" ? table(copy.compose.stoppedBecause) : null;
  const next = recurring && status?.next_occurrence_start ? formatMeetingWhen(status.next_occurrence_start, status.next_occurrence_start) : null;
  return (<>
    <Section>
      <div aria-live="polite">
        <Row icon={linkMissing ? "subject" : live ? "check" : "sync"} hue={linkMissing ? "amber" : live ? "ok" : "teal"} text={<b className="font-medium">{line}</b>} bottom={workspaceName} />
        {recurring && live ? <Row icon="sync" hue="teal" text={<span>{copy.compose.seriesTitle}</span>} bottom={next ? copy.compose.nextMeeting(next) : undefined} /> : null}
      </div>
      {connectRequired && phase === "expired" ? <p role="status" className="px-3 py-2.5 text-ink-2">{copy.compose.connectFirst}</p> : null}
      {detail ? <p role="status" className={`px-3 py-2.5 text-ink-2${linkMissing ? " border-l-2 border-amber bg-bg-2" : ""}`}>{detail}</p> : null}
      {live ? <TranscribePrompt kind={kind} auto={status?.auto_transcription ?? null} reason={status?.manual_reason ?? null} /> : null}
      {statusError && !error ? <p role="status" className="px-3 py-2.5 text-ink-3">{copy.compose.statusFailed}</p> : null}
      {error ? <p role="alert" className="px-3 py-2.5 text-ink-2">{error}</p> : null}
    </Section>
    <Footer>
      <OutlineButton disabled={busy !== null} onClick={() => void turnOff()}>{busy === "off" ? copy.compose.turningOff : copy.compose.turnOff}</OutlineButton>
      {phase === "expired" && connectRequired ? (<>
        <OutlineButton disabled={busy !== null} onClick={() => void turnOn(true)}>{copy.meeting.checkConnection}</OutlineButton>
        <FilledButton disabled={busy !== null} onClick={() => void connectMicrosoft()}>{copy.meeting.connectMicrosoft}</FilledButton>
      </>) : phase === "expired" ? <FilledButton disabled={busy !== null} onClick={() => void turnOn(true)}>{busy === "on" ? copy.compose.saving : copy.compose.turnOnAgain}</FilledButton> : null}
    </Footer>
  </>);
}
