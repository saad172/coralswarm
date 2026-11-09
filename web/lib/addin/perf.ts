/* Startup timing for the Outlook panes. Each phase of "Connecting to CoralSwarm…"
 * leaves a User Timing mark (`cs:<phase>`) that shows up in the browser's
 * Performance panel, so the cost can be read from a real Outlook host instead of
 * guessed. Nothing leaves the page: no network, no storage.
 *
 * To read it in Outlook on the web, open the pane's DevTools and run
 *   window.__csTimings()
 * which prints each phase as milliseconds since the pane's navigation start. */

export type Phase =
  | "pane-module"        // pane JS evaluated (hydration started)
  | "office-script"      // office.js <script> loaded
  | "office-ready"       // Office.onReady fired
  | "msal-loaded"        // @azure/msal-browser chunk imported
  | "nested-ready"       // createNestablePublicClientApplication resolved
  | "token-silent"       // acquireTokenSilent resolved (Microsoft token in hand)
  | "token-fallback"     // Office.auth.getAccessToken resolved (NAA not used)
  | "exchange-start"     // POST /v1/addin/microsoft/session sent
  | "exchange-done"      // ... answered
  | "connected"          // the pane has a CoralSwarm session
  | "item-ready"         // the Outlook item snapshot was read
  | "status-start"       // compose pane: first intent status request sent
  | "status-done";       // ... answered        // the Outlook item snapshot was read

const PREFIX = "cs:";

/** Record a phase once per page load. Safe anywhere (SSR, tests, hosts without User Timing). */
export function mark(phase: Phase): void {
  try {
    if (typeof performance === "undefined" || typeof performance.mark !== "function") return;
    if (performance.getEntriesByName(PREFIX + phase, "mark").length > 0) return;
    performance.mark(PREFIX + phase);
  } catch { /* timing must never affect the pane */ }
}

/** Every recorded phase, in order, as ms since navigation start. */
export function timings(): Array<{ phase: string; ms: number; sincePrevious: number }> {
  if (typeof performance === "undefined" || typeof performance.getEntriesByType !== "function") return [];
  const marks = performance.getEntriesByType("mark").filter((entry) => entry.name.startsWith(PREFIX)).sort((a, b) => a.startTime - b.startTime);
  let previous = 0;
  return marks.map((entry) => {
    const row = { phase: entry.name.slice(PREFIX.length), ms: Math.round(entry.startTime), sincePrevious: Math.round(entry.startTime - previous) };
    previous = entry.startTime;
    return row;
  });
}

declare global {
  interface Window { __csTimings?: () => ReturnType<typeof timings> }
}

if (typeof window !== "undefined") window.__csTimings = () => { const rows = timings(); console.table(rows); return rows; };
