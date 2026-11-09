/* The compose-mode marker for an UNSAVED Outlook appointment.
 *
 * Only the item's CustomProperties are touched, and only through their own
 * saveAsync. Never item.saveAsync, never send: Outlook owns saving and sending
 * the meeting. The marker is a public correlation value (a random nonce the
 * backend issued), not a credential. When the user saves or sends the meeting,
 * the backend finds the nonce on the saved event and starts saving the
 * transcript; that is also what keeps it working if the meeting is moved.
 *
 * The intent id is stored next to the nonce so that reopening the same draft can
 * read the intent's status. It is an identifier, authorised server-side per
 * session, so it grants nothing by itself. */

import type { OfficeApi, OfficeAsyncResult, OfficeCustomProperties, OfficeMailItem } from "./office";

export const NONCE_PROPERTY = "coralswarmCaptureIntent";
export const INTENT_ID_PROPERTY = "coralswarmCaptureIntentId";
/** When the intent stops waiting (the create response's `expires_at`). A public timestamp, kept so the
 *  pane can say "CoralSwarm keeps waiting until ..." on reopen; the status route does not return it. */
export const EXPIRES_PROPERTY = "coralswarmCaptureIntentExpires";

const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
export const validNonce = (value: unknown): value is string => typeof value === "string" && UUID.test(value);

export class ComposeIntentError extends Error {
  /** Outlook answered "failed": the write definitely did not persist. A timeout is not definitive. */
  constructor(message: string, readonly definitive = false) { super(message); }
}

export interface ComposeMarker { nonce: string; intentId: string | null; expiresAt: string | null }

export interface StageOptions {
  /** RFC 3339 `expires_at` from the create response. Ignored when unparseable. */
  expiresAt?: string | null;
  /** Overwrite an existing, different marker (turning on again after an intent expired). */
  replace?: boolean;
}

const validExpiry = (value: unknown): value is string => typeof value === "string" && value.length <= 40 && !Number.isNaN(Date.parse(value));

export interface ComposeAdapter {
  read(): Promise<ComposeMarker | null>;
  stage(nonce: string, intentId: string, options?: StageOptions): Promise<void>;
  remove(): Promise<void>;
}

/** An organizer draft exposes `subject.getAsync`; a saved appointment exposes a string. */
export function isComposeAppointment(item: OfficeMailItem | null | undefined): item is OfficeMailItem {
  return Boolean(item) && typeof item?.subject === "object" && item?.subject !== null && typeof item.subject.getAsync === "function";
}

/** Can this Outlook client persist a marker on an unsaved draft? */
export function composeSupported(item: OfficeMailItem | null | undefined): boolean {
  return isComposeAppointment(item) && typeof item.loadCustomPropertiesAsync === "function";
}

const TIMEOUT_MS = 30_000;

export function createComposeAdapter(office: OfficeApi, timeoutMs = TIMEOUT_MS): ComposeAdapter {
  let changing = false;
  function context() {
    const item = office.context.mailbox.item;
    if (!composeSupported(item) || !item?.loadCustomPropertiesAsync) throw new ComposeIntentError("unsupported");
    const load = item.loadCustomPropertiesAsync.bind(item);
    return { item, load, current: () => office.context.mailbox.item === item };
  }
  /** Outlook callbacks that have not fired yet. A timeout rejects the caller, but the write it
   *  started can still land, so `busy` must outlive the timeout until the callback settles. */
  let outstanding = 0;
  function call<T>(invoke: (done: (result: OfficeAsyncResult<T>) => void) => void): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      outstanding++;
      let counted = true;
      const settle = () => { if (counted) { counted = false; outstanding--; } };
      const timer = setTimeout(() => reject(new ComposeIntentError("timeout")), timeoutMs);
      try {
        invoke((result) => {
          clearTimeout(timer); settle();
          if (result.status !== office.AsyncResultStatus.Succeeded) reject(new ComposeIntentError("failed", true));
          else resolve(result.value);
        });
      } catch (cause) { clearTimeout(timer); settle(); reject(cause); }
    });
  }
  const busy = () => changing || outstanding > 0;
  async function change(apply: (props: OfficeCustomProperties) => void, restore: (props: OfficeCustomProperties) => void) {
    if (busy()) throw new ComposeIntentError("busy");
    const selected = context(); changing = true;
    try {
      const props = await call<OfficeCustomProperties>((done) => selected.load(done));
      if (!selected.current()) throw new ComposeIntentError("item_changed");
      apply(props);
      try { await call((done) => props.saveAsync(done)); }
      // Roll the local bag back only when Outlook definitely refused; after a timeout the save may still land.
      catch (cause) { if (cause instanceof ComposeIntentError && cause.definitive) restore(props); throw cause; }
      if (!selected.current()) throw new ComposeIntentError("item_changed");
    } finally { changing = false; }
  }
  return {
    async read() {
      // A write may still be in flight (or timed out but not yet settled): a fresh bag could miss it.
      if (busy()) throw new ComposeIntentError("busy");
      const selected = context();
      const props = await call<OfficeCustomProperties>((done) => selected.load(done));
      if (!selected.current()) throw new ComposeIntentError("item_changed");
      const nonce = props.get(NONCE_PROPERTY);
      if (!nonce) return null;
      if (!validNonce(nonce)) throw new ComposeIntentError("unsupported_marker");
      const id = props.get(INTENT_ID_PROPERTY);
      const expires = props.get(EXPIRES_PROPERTY);
      return { nonce, intentId: validNonce(id) ? id : null, expiresAt: validExpiry(expires) ? expires : null };
    },
    stage(nonce, intentId, options = {}) {
      if (!validNonce(nonce) || !validNonce(intentId)) return Promise.reject(new ComposeIntentError("invalid_intent"));
      let before: { nonce: unknown; id: unknown; expires: unknown } = { nonce: undefined, id: undefined, expires: undefined };
      return change((props) => {
        before = { nonce: props.get(NONCE_PROPERTY), id: props.get(INTENT_ID_PROPERTY), expires: props.get(EXPIRES_PROPERTY) };
        if (before.nonce && before.nonce !== nonce && !options.replace) throw new ComposeIntentError("different_marker");
        props.set(NONCE_PROPERTY, nonce); props.set(INTENT_ID_PROPERTY, intentId);
        if (validExpiry(options.expiresAt)) props.set(EXPIRES_PROPERTY, options.expiresAt); else props.remove(EXPIRES_PROPERTY);
      }, (props) => {
        // Undo the local bag when persistence failed.
        if (before.nonce) props.set(NONCE_PROPERTY, String(before.nonce)); else props.remove(NONCE_PROPERTY);
        if (before.id) props.set(INTENT_ID_PROPERTY, String(before.id)); else props.remove(INTENT_ID_PROPERTY);
        if (before.expires) props.set(EXPIRES_PROPERTY, String(before.expires)); else props.remove(EXPIRES_PROPERTY);
      });
    },
    remove() {
      let before: { nonce: unknown; id: unknown; expires: unknown } = { nonce: undefined, id: undefined, expires: undefined };
      return change((props) => {
        before = { nonce: props.get(NONCE_PROPERTY), id: props.get(INTENT_ID_PROPERTY), expires: props.get(EXPIRES_PROPERTY) };
        props.remove(NONCE_PROPERTY); props.remove(INTENT_ID_PROPERTY); props.remove(EXPIRES_PROPERTY);
      }, (props) => {
        if (before.nonce) props.set(NONCE_PROPERTY, String(before.nonce));
        if (before.id) props.set(INTENT_ID_PROPERTY, String(before.id));
        if (before.expires) props.set(EXPIRES_PROPERTY, String(before.expires));
      });
    },
  };
}

/** Best effort: does this draft already repeat? (Mailbox 1.7 `recurrence.getAsync`; null = one-off.) The
 *  server's `recurring` answer replaces this once an intent exists. */
export function readComposeRecurring(office: OfficeApi): Promise<boolean> {
  const item = office.context.mailbox.item;
  const getAsync = item?.recurrence?.getAsync;
  if (!item || typeof getAsync !== "function") return Promise.resolve(false);
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), 5_000);
    try {
      getAsync.call(item.recurrence, (result) => { clearTimeout(timer); resolve(result.status === office.AsyncResultStatus.Succeeded && result.value != null); });
    } catch { clearTimeout(timer); resolve(false); }
  });
}
