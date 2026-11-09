/**
 * First-touch acquisition attribution.
 *
 * Stashes how a visitor first arrived (UTM params, referrer, landing path)
 * in localStorage the first time captureFirstTouch() runs on a device that
 * doesn't already have a stash. Later — possibly much later, and after
 * they've browsed the site or left and come back — EmailForm reads the
 * stash and sends it with the signup. That's the point of *first*-touch:
 * a visitor who lands via a Hacker News link, pokes around, and signs up
 * from a page they navigated to internally should still credit Hacker
 * News, not whatever page (or lack of referrer) happened to be current
 * when they filled out the form.
 *
 * All storage access is wrapped in try/catch: Safari private browsing (and
 * some locked-down browser configs) throw synchronously on
 * localStorage.getItem/setItem rather than silently no-op, so a failure
 * here must never be allowed to break page load or the signup form.
 */

const STORAGE_KEY = "cs:first-touch";

export interface FirstTouch {
  utm_source: string | null;
  utm_medium: string | null;
  utm_campaign: string | null;
  referrer: string | null;
  landing_path: string | null;
  first_seen: string;
}

/**
 * Reduces a raw `document.referrer` to just its origin and path, or null.
 *
 * We deliberately DROP the query string and fragment. Attribution only needs
 * to know which site (and roughly which page) sent someone here — but a
 * referring URL's query string is arbitrary third-party data we did not
 * choose and cannot audit: search queries, session identifiers, occasionally
 * an email address or token. Persisting it verbatim to localStorage and then
 * into our database would mean collecting personal data we never asked for
 * and have no use for. Origin + pathname answers the question; the rest is
 * liability.
 *
 * Returns null for an empty referrer, a same-origin referrer (an internal
 * navigation says nothing about acquisition), and anything unparseable —
 * if we can't safely reduce it, we store nothing rather than the raw string.
 *
 * Exported so components/VisitPing.tsx can apply the exact same reduction
 * to the per-visit pageview ping — the privacy reasoning above applies
 * identically there, so the logic lives in exactly one place.
 */
export function sanitizeReferrer(rawReferrer: string): string | null {
  if (!rawReferrer) return null;
  try {
    const url = new URL(rawReferrer);
    if (url.origin === window.location.origin) return null;
    // origin + pathname only — no `search`, no `hash`.
    return `${url.origin}${url.pathname}`;
  } catch {
    return null;
  }
}

/**
 * Idempotent — writes the stash only if one isn't already present. Safe
 * (and intended) to call on every page view.
 */
export function captureFirstTouch(): void {
  try {
    if (window.localStorage.getItem(STORAGE_KEY)) return;

    const params = new URLSearchParams(window.location.search);
    const rawReferrer = document.referrer || "";

    const record: FirstTouch = {
      utm_source: params.get("utm_source"),
      utm_medium: params.get("utm_medium"),
      utm_campaign: params.get("utm_campaign"),
      referrer: sanitizeReferrer(rawReferrer),
      landing_path: window.location.pathname,
      first_seen: new Date().toISOString(),
    };

    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(record));
  } catch {
    // Storage unavailable (Safari private mode, a locked-down browser
    // profile, etc.) — first-touch attribution is a nice-to-have, never
    // worth breaking the page over.
  }
}

/** Best-effort read of the stashed first-touch record, if any. */
export function readFirstTouch(): FirstTouch | null {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    return JSON.parse(raw) as FirstTouch;
  } catch {
    return null;
  }
}
