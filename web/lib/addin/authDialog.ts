/* The host sign-in dialog: Office `displayDialogAsync` for Outlook,
 * `authentication.authenticate` for Teams, a plain popup for the Google Meet side
 * panel (Meet has no dialog API; the panel is our page in an iframe). The dialog opens OUR start page, which
 * redirects to Clerk; Clerk redirects to OUR callback page, which hands
 * `{code, state}` back to this pane. Nothing secret is ever in a URL we open: the
 * PKCE verifier stays in this module's memory, only the challenge and state travel.
 *
 * Must be called from a user click: Office only opens a dialog from a gesture.
 * `promptBeforeOpen: false` removes Outlook on the web's "wants to display a new
 * window - Allow" prompt (verified live). */

import { buildStartUrl, createPkce, createState, redirectUriFor, type AddinHostKind } from "./clerkOAuth";
import type { OfficeApi } from "./office";

/** The parts of a popup window the Meet dialog uses. */
export interface PopupHandle {
  closed: boolean;
  location: { href: string };
  close(): void;
}

/** Where the Meet dialog listens for the callback's postMessage (the panel's own window). */
export interface MessageSource {
  addEventListener(type: "message", listener: (event: MessageEvent) => void): void;
  removeEventListener(type: "message", listener: (event: MessageEvent) => void): void;
}

export type DialogTarget =
  | { kind: "outlook"; office: () => Promise<OfficeApi> }
  | { kind: "teams"; authenticate?: (params: { url: string; width: number; height: number }) => Promise<string> }
  /** `popup` MUST come from `openSignInPopup()` called synchronously in the click handler: the browser
   *  only allows window.open during the user gesture, and the PKCE work before navigating is async. */
  | { kind: "meet"; popup: PopupHandle | null; events?: MessageSource; pollMs?: number };

/** Open the (blank) sign-in popup. Call synchronously from a click handler; null when blocked. */
export function openSignInPopup(win: Pick<Window, "open"> = window): Window | null {
  try { return win.open("about:blank", "coralswarm-signin", "popup,width=520,height=640"); } catch { return null; }
}

/** The user closed the dialog or declined consent. The pane shows "Sign-in was cancelled." */
export class DialogCancelledError extends Error { constructor() { super("Sign-in was cancelled"); this.name = "DialogCancelledError"; } }
/** The dialog could not open, or came back with something we do not accept. */
export class DialogError extends Error {
  code: string;
  constructor(code: string) { super(code); this.name = "DialogError"; this.code = code; }
}

export interface DialogResult { code: string; verifier: string; redirectUri: string }

export interface RunDialogOptions {
  target: DialogTarget;
  /** Re-show the consent screen (and its organization picker): "Change workspace". */
  prompt?: "consent";
  /** This page's origin; defaults to window.location.origin. */
  origin?: string;
}

/** Office reports a user-closed dialog as DialogEventReceived code 12006. */
const OFFICE_DIALOG_CLOSED = 12006;

/** Parse the callback's message and check it against the state this pane generated. The state is
 *  compared once, here; a mismatch (or any malformed message) never yields a code. */
export function interpretCallbackMessage(raw: unknown, expectedState: string): string {
  let message: unknown;
  try { message = typeof raw === "string" ? JSON.parse(raw) : raw; } catch { throw new DialogError("bad_message"); }
  if (typeof message !== "object" || message === null) throw new DialogError("bad_message");
  const { code, state, error } = message as { code?: unknown; state?: unknown; error?: unknown };
  if (typeof state !== "string" || state !== expectedState) throw new DialogError("state_mismatch");
  if (typeof error === "string" && error) {
    if (error === "access_denied") throw new DialogCancelledError();
    throw new DialogError("authorize_error");
  }
  if (typeof code !== "string" || !code) throw new DialogError("no_code");
  return code;
}

function openOfficeDialog(office: OfficeApi, url: string, expectedState: string, origin: string): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const ui = office.context.ui;
    const messageEvent = office.EventType.DialogMessageReceived;
    const closeEvent = office.EventType.DialogEventReceived;
    if (!ui?.displayDialogAsync || !messageEvent || !closeEvent) { reject(new DialogError("unsupported")); return; }
    let settled = false;
    let dialog: { close(): void } | null = null;
    const settle = (fn: () => void) => {
      if (settled) return;
      settled = true;
      try { dialog?.close(); } catch { /* already closed */ }
      fn();
    };
    ui.displayDialogAsync(url, { height: 60, width: 30, promptBeforeOpen: false }, (result) => {
      if (result.status !== office.AsyncResultStatus.Succeeded) { settled = true; reject(new DialogError(`open_failed_${result.error?.code ?? "unknown"}`)); return; }
      dialog = result.value;
      result.value.addEventHandler(messageEvent, (arg) => {
        // A message from any origin but ours is ignored (the dialog keeps waiting): only our own
        // callback page may answer. Some clients omit the origin; then only a valid result settles.
        if (arg.origin && arg.origin !== origin) return;
        // With no origin to vouch for the sender, only a message that passes the shape and state check may
        // settle; anything else is ignored and the dialog keeps waiting for the real result.
        if (!arg.origin) {
          try { interpretCallbackMessage(arg.message, expectedState); }
          catch (cause) { if (cause instanceof DialogError && (cause.code === "bad_message" || cause.code === "state_mismatch")) return; }
        }
        settle(() => { try { resolve(interpretCallbackMessage(arg.message, expectedState)); } catch (cause) { reject(cause); } });
      });
      result.value.addEventHandler(closeEvent, (arg) => {
        settle(() => reject(arg.error === OFFICE_DIALOG_CLOSED ? new DialogCancelledError() : new DialogError(`dialog_event_${arg.error ?? "unknown"}`)));
      });
    });
  });
}

async function openTeamsDialog(target: Extract<DialogTarget, { kind: "teams" }>, url: string, expectedState: string): Promise<string> {
  const authenticate = target.authenticate ?? (async (params) => (await import("@microsoft/teams-js")).authentication.authenticate(params));
  let result: string;
  try { result = await authenticate({ url, width: 600, height: 560 }); }
  catch (cause) {
    const reason = cause instanceof Error ? cause.message : typeof cause === "string" ? cause : "";
    // The callback reports a declined consent as `access_denied`; Teams reports a closed popup as CancelledByUser.
    if (/cancel|access_denied/i.test(reason)) throw new DialogCancelledError();
    throw new DialogError("authenticate_failed");
  }
  return interpretCallbackMessage(result, expectedState);
}

/** How long after the popup reports closed a late-arriving message may still settle (the callback posts, then closes). */
const POPUP_CLOSE_GRACE_MS = 400;

/** The Meet panel's popup. A message counts only when it comes from OUR origin, from THIS popup, and
 *  carries the state we generated; anything else is ignored and the popup keeps waiting. The user
 *  closing the popup is a cancel. */
function openPopupDialog(target: Extract<DialogTarget, { kind: "meet" }>, url: string, expectedState: string, origin: string): Promise<string> {
  const popup = target.popup;
  if (!popup) return Promise.reject(new DialogError("popup_blocked"));
  if (popup.closed) return Promise.reject(new DialogCancelledError());
  const events: MessageSource = target.events ?? window;
  return new Promise<string>((resolve, reject) => {
    let settled = false;
    let poll: ReturnType<typeof setInterval> | null = null;
    let grace: ReturnType<typeof setTimeout> | null = null;
    const finish = (fn: () => void) => {
      if (settled) return;
      settled = true;
      events.removeEventListener("message", onMessage);
      if (poll) clearInterval(poll);
      if (grace) clearTimeout(grace);
      try { if (!popup.closed) popup.close(); } catch { /* already gone */ }
      fn();
    };
    function onMessage(event: MessageEvent) {
      if (event.origin !== origin || event.source !== (popup as unknown)) return;
      let code: string;
      try { code = interpretCallbackMessage(event.data, expectedState); }
      catch (cause) {
        if (cause instanceof DialogError && (cause.code === "bad_message" || cause.code === "state_mismatch")) return;
        finish(() => reject(cause));
        return;
      }
      finish(() => resolve(code));
    }
    events.addEventListener("message", onMessage);
    poll = setInterval(() => {
      if (!popup.closed || grace) return;
      grace = setTimeout(() => finish(() => reject(new DialogCancelledError())), POPUP_CLOSE_GRACE_MS);
    }, target.pollMs ?? 500);
    try { popup.location.href = url; }
    catch { finish(() => reject(new DialogError("popup_navigation_failed"))); }
  });
}

/** Open the sign-in dialog and resolve with the authorization code plus the verifier to redeem it. */
export async function runAuthDialog(options: RunDialogOptions): Promise<DialogResult> {
  const origin = options.origin ?? window.location.origin;
  const host: AddinHostKind = options.target.kind;
  const { verifier, challenge } = await createPkce();
  const state = createState(host);
  const url = buildStartUrl(origin, { cc: challenge, state, host, prompt: options.prompt });
  const target = options.target;
  const code = target.kind === "outlook"
    ? await openOfficeDialog(await target.office(), url, state, origin)
    : target.kind === "meet"
      ? await openPopupDialog(target, url, state, origin)
      : await openTeamsDialog(target, url, state);
  return { code, verifier, redirectUri: redirectUriFor(origin) };
}
