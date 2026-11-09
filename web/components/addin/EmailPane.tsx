"use client";

/* The open-message surface of the Outlook add-in: save this thread to your ocean.
 * Same flow, rows and copy as the Gmail add-on's message card (Card.gs
 * buildConnectedCard_, Ingest.gs), over the Microsoft routes:
 *
 *   GET  /v1/connectors/microsoft/watched/{conversationId}   → {following}
 *   PUT  /v1/connectors/microsoft/watched/{conversationId}   {follow} → {following}
 *   POST /v1/connectors/microsoft/ingest  {item_ids, scope?, follow?} → {ingested}
 *
 * Only the open message's id is sent, never the mailbox; the server expands it to
 * the conversation (scope "thread", the default) and registers it for reply
 * auto-save (follow, default on).
 *
 * THE RULE (shared with Gmail): a save field is sent iff its control is rendered.
 *   checking   the watched read is in flight, no knob rendered → neither field
 *   fresh      radio + switch rendered                          → scope and follow
 *   following  saved row + LIVE switch rendered                 → follow only
 * The component is remounted (keyed) for every message/workspace, so a late
 * answer from an old item cannot touch the new one.
 *
 * The transport is injected (`EmailApi`): the add-in session in the SSO path, a
 * Clerk bearer in the fallback path. */

import { useEffect, useMemo, useRef, useState } from "react";
import { copy } from "@/lib/addin/copy";
import type { ApiResult, EmailApi } from "@/lib/addin/emailTransport";
import { FilledButton, Footer, PlusRow, RadioRows, Row, Section, Switch } from "./kit";

export interface EmailItem { itemId: string; conversationId: string | null; subject: string | null; from: string | null }

/** How long the on-open "are we following this?" read may take before the pane stops waiting. */
export const WATCHED_CHECK_TIMEOUT_MS = 8000;

type Mode = "checking" | "fresh" | "following";
type Scope = "thread" | "message";

const watchedPath = (conversationId: string) => `/v1/connectors/microsoft/watched/${encodeURIComponent(conversationId)}`;
const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null;

/** The body the Save action sends, derived from the mode alone: fields only for controls on screen. */
export function ingestBody(restId: string, mode: Mode, scope: Scope, follow: boolean): { item_ids: string[]; scope?: Scope; follow?: boolean } {
  const body: { item_ids: string[]; scope?: Scope; follow?: boolean } = { item_ids: [restId] };
  if (mode === "fresh") body.scope = scope;
  if (mode === "fresh" || mode === "following") body.follow = follow;
  return body;
}

export function EmailPane({ item, api, workspaceName, toast, onUnauthorized, toRestId, openToday }: {
  item: EmailItem;
  api: EmailApi;
  workspaceName: string;
  toast: (message: string) => void;
  /** Any answer showing the credential is gone: parent returns to the start card. */
  onUnauthorized: () => void;
  toRestId: (ewsId: string) => string;
  openToday: () => void;
}) {
  const [mode, setMode] = useState<Mode>("checking");
  const [following, setFollowing] = useState(false);
  const [savedCount, setSavedCount] = useState<number | null>(null);
  const [scope, setScope] = useState<Scope>("thread");
  const [follow, setFollow] = useState(true);
  const [busy, setBusy] = useState(false);
  const alive = useRef(true);
  const modeRef = useRef<Mode>("checking"); modeRef.current = mode;
  const lock = useRef(false);

  const conversationId = useMemo(() => {
    if (!item.conversationId) return null;
    try { return toRestId(item.conversationId); } catch { return null; }
  }, [item.conversationId, toRestId]);

  useEffect(() => {
    alive.current = true;
    return () => { alive.current = false; };
  }, []);

  // On open: is this conversation already followed? Every failure lands on "fresh".
  useEffect(() => {
    let live = true;
    (async () => {
      let isFollowing = false;
      if (conversationId) {
        try {
          const result = await api("GET", watchedPath(conversationId), undefined, WATCHED_CHECK_TIMEOUT_MS);
          isFollowing = result.ok && isRecord(result.data) && result.data.following === true;
        } catch { isFollowing = false; }
      }
      // A save tapped inside the checking window already moved the mode on.
      if (!live || modeRef.current !== "checking") return;
      setMode(isFollowing ? "following" : "fresh");
      setFollowing(isFollowing);
      if (isFollowing) setFollow(true);
    })();
    return () => { live = false; };
    // One read per mounted (keyed) item.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function onSave() {
    if (lock.current) return;
    let restId: string;
    try { restId = toRestId(item.itemId); } catch { toast(copy.email.couldNotRead); return; }
    // In "following" the live switch IS the state (`following`); the knob state only exists while fresh.
    const body = ingestBody(restId, mode, scope, mode === "following" ? following : follow);
    lock.current = true; setBusy(true);
    let result: ApiResult;
    try { result = await api("POST", "/v1/connectors/microsoft/ingest", body); }
    catch { if (alive.current) { lock.current = false; setBusy(false); toast(copy.email.unreachable); } return; }
    if (!alive.current) return;
    lock.current = false; setBusy(false);
    if (result.status === 401 || result.unauthenticated) { onUnauthorized(); return; }
    // 403/404 here is the tenancy gate (token and workspace disagree), not the connector.
    if (result.status === 403 || result.status === 404) { toast(copy.email.refused(workspaceName)); return; }
    if (!result.ok) { toast(copy.email.couldNotSave(result.status)); return; }
    const ingested = isRecord(result.data) ? result.data.ingested : undefined;
    if (typeof ingested !== "number" || !Number.isInteger(ingested) || ingested < 0) { toast(copy.email.unconfirmed); return; }
    // `ingested` counts messages newly stored: an already-saved thread reports 0, which is not an error.
    toast(ingested >= 1 ? copy.email.savedToast(ingested) : copy.email.alreadySaved);
    // The thread is in the ocean now, so the switch turns live. Without a conversation id there is nothing to PUT to.
    if (conversationId) {
      setMode("following");
      setFollowing(body.follow === undefined ? true : body.follow);
      setSavedCount(ingested);
    }
  }

  async function onFollowToggle(next: boolean) {
    if (mode === "fresh") { setFollow(next); return; } // a knob: nothing is sent until Save
    if (mode !== "following" || lock.current || !conversationId) return;
    lock.current = true; setBusy(true);
    let result: ApiResult | null;
    try { result = await api("PUT", watchedPath(conversationId), { follow: next }); } catch { result = null; }
    if (!alive.current) return;
    lock.current = false; setBusy(false);
    if (result && result.ok && isRecord(result.data) && typeof result.data.following === "boolean") {
      setFollowing(result.data.following);
      toast(result.data.following ? copy.email.followOn : copy.email.followOff);
      return;
    }
    if (result && result.status === 404) { setMode("fresh"); setFollowing(false); setSavedCount(null); setFollow(true); toast(copy.email.notInOcean); return; }
    if (result && (result.status === 401 || result.unauthenticated)) { onUnauthorized(); return; }
    toast(result ? copy.email.couldNotFollow(result.status) : copy.email.unreachable);
  }

  const subject = item.subject || copy.email.fallbackSubject;
  const meta = copy.email.meta(null, item.from);
  return (
    <>
      <Section>
        {mode === "following" ? (
          <>
            <Row icon="check" hue="ok" text={subject} bottom={copy.email.saved(following, savedCount ?? 0)} trailing={<span className="mt-0.5 shrink-0"><OpenGlyph /></span>} onClick={openToday} />
            <Row icon="sync" hue="teal" text={copy.email.follow} trailing={<Switch checked={following} disabled={busy} onChange={(next) => void onFollowToggle(next)} label={copy.email.follow} />} />
          </>
        ) : (
          <>
            <PlusRow text={copy.email.plusRow} onClick={() => void onSave()} disabled={busy} />
            <Row icon="thread" text={subject} bottom={meta || copy.email.fallbackMeta} />
            {mode === "fresh" ? (
              <>
                <RadioRows name="scope" value={scope} onChange={setScope} disabled={busy} options={[{ value: "thread", label: copy.email.wholeThread(null) }, { value: "message", label: copy.email.justThis }]} />
                <Row icon="sync" hue="teal" text={copy.email.follow} trailing={<Switch checked={follow} disabled={busy} onChange={(next) => void onFollowToggle(next)} label={copy.email.follow} />} />
              </>
            ) : null}
          </>
        )}
      </Section>
      <Footer><FilledButton full disabled={busy} onClick={() => void onSave()}>{busy ? copy.email.saving : copy.email.save}</FilledButton></Footer>
    </>
  );
}

function OpenGlyph() {
  // eslint-disable-next-line @next/next/no-img-element -- same hosted glyph the Gmail card uses
  return <img src="/addin/icons/open--ink.png?v=2" alt="" width={16} height={16} aria-hidden="true" />;
}
