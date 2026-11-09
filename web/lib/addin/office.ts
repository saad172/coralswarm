/* The slice of Office.js the Outlook panes use, typed locally (the project does
 * not depend on @types/office-js) and reachable through one injectable seam so
 * every Office interaction is testable without Outlook.
 *
 * The page loads office.js itself (`loadOffice`) instead of the layout doing it:
 * only the Outlook panes pay for it, and a test can hand in a fake. */

import { mark } from "./perf";
import { restoreHistory } from "./history";

export interface OfficeAsyncResult<T> { status: string; value: T; error?: { message?: string } }

/** The per-item property bag (Mailbox 1.1+). Not a credential store: only a public correlation marker goes in it. */
export interface OfficeCustomProperties {
  get(name: string): unknown;
  set(name: string, value: string): void;
  remove(name: string): void;
  saveAsync(callback: (result: OfficeAsyncResult<unknown>) => void): void;
}

export interface OfficeMailItem {
  itemType?: string;
  itemId?: string;
  conversationId?: string;
  /** A string when reading; an object with `getAsync` when composing. */
  subject?: string | { getAsync?: unknown };
  /** Present in compose mode: the series pattern, `null` for a one-off meeting. */
  recurrence?: { getAsync?(callback: (result: OfficeAsyncResult<unknown>) => void): void };
  loadCustomPropertiesAsync?(callback: (result: OfficeAsyncResult<OfficeCustomProperties>) => void): void;
  from?: { displayName?: string; emailAddress?: string };
  body?: { getAsync(coercion: string, callback: (result: OfficeAsyncResult<string>) => void): void };
}

/** The slice of Office.js's dialog API the add-in sign-in uses (displayDialogAsync). */
export interface OfficeDialog {
  addEventHandler(event: string, handler: (arg: { message?: string; origin?: string; error?: number }) => void): void;
  close(): void;
}

export interface OfficeApi {
  onReady(callback: (info: { host?: string }) => void): Promise<unknown> | void;
  HostType?: { Outlook?: string };
  EventType: { ItemChanged: string; DialogMessageReceived?: string; DialogEventReceived?: string };
  CoercionType: { Html: string };
  AsyncResultStatus: { Succeeded: string; Failed?: string };
  MailboxEnums: { ItemType: { Message: string; Appointment: string }; RestVersion: { v2_0: string } };
  auth?: { getAccessToken(options?: Record<string, unknown>): Promise<string> };
  context: {
    mailbox: {
      item: OfficeMailItem | null;
      convertToRestId(id: string, version: string): string;
      addHandlerAsync?(event: string, handler: () => void, callback?: (result: OfficeAsyncResult<unknown>) => void): void;
      removeHandlerAsync?(event: string, handler: () => void, callback?: (result: OfficeAsyncResult<unknown>) => void): void;
    };
    ui?: {
      openBrowserWindow?(url: string): void;
      /** Inside a dialog: send a message to the pane that opened it. */
      messageParent?(message: string, options?: { targetOrigin?: string }): void;
      displayDialogAsync?(url: string, options: Record<string, unknown>, callback: (result: { status: string; value: OfficeDialog; error?: { code?: number; message?: string } }) => void): void;
    };
    requirements: { isSetSupported(name: string, version?: string): boolean };
  };
}

export const OFFICE_SRC = "https://appsforoffice.microsoft.com/lib/1/hosted/office.js";

declare global {
  interface Window { Office?: OfficeApi }
}

let loading: Promise<OfficeApi> | null = null;

/** Load office.js once and resolve when `Office.onReady` fires. Rejects outside an Office host. */
export function loadOffice(): Promise<OfficeApi> {
  if (loading) return loading;
  loading = new Promise<OfficeApi>((resolve, reject) => {
    const ready = () => {
      mark("office-script");
      restoreHistory();
      const office = window.Office;
      if (!office) { reject(new Error("Office.js did not load")); return; }
      // Outside an Office host onReady still fires, with no host: that is not usable.
      Promise.resolve(office.onReady((info) => {
        if (!info?.host) { reject(new Error("Not running in an Office host")); return; }
        mark("office-ready");
        restoreHistory();
        resolve(office);
      })).catch(reject);
    };
    if (window.Office) { ready(); return; }
    const script = document.createElement("script");
    script.src = OFFICE_SRC;
    script.async = true;
    script.onload = ready;
    script.onerror = () => reject(new Error("Office.js failed to load"));
    document.head.appendChild(script);
  });
  loading.catch(() => { loading = null; });
  return loading;
}

/** Test seam: forget the cached load. */
export function resetOfficeLoaderForTests(): void { loading = null; }

export function openInBrowser(office: Pick<OfficeApi, "context"> | null, url: string): Promise<void> {
  const open = office?.context.ui?.openBrowserWindow;
  if (open && office?.context.requirements.isSetSupported("OpenBrowserWindowApi", "1.1")) { open.call(office.context.ui, url); return Promise.resolve(); }
  window.open(url, "_blank", "noopener,noreferrer");
  return Promise.resolve();
}
