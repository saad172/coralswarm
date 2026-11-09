"use client";

import { useEffect } from "react";
import { readFirstTouch, sanitizeReferrer } from "./firstTouch";

/**
 * First-party, cookieless pageview ping.
 *
 * PostHog is on hold and Cloudflare Web Analytics (the beacon in
 * app/layout.tsx, gated on NEXT_PUBLIC_CF_BEACON_TOKEN) can't break traffic
 * down by UTM param. This posts one small, unauthenticated, fire-and-forget
 * event per tab session to POST /api/waves (see worker/index.ts) so the
 * founder can see which channel/campaign is actually driving visits, not
 * just which one drove a form submission.
 *
 * Deliberately reads UTM params from the CURRENT URL, not the localStorage
 * first-touch stash (components/firstTouch.ts) — that stash answers "what
 * got this visitor here the first time, ever" for crediting a signup;
 * per-visit attribution here answers a different question ("what's driving
 * traffic to the site right now") and needs this visit's own params.
 *
 * Mounted once from app/layout.tsx, ahead of {children} in the JSX — so its
 * effect (a leaf, no children of its own) always commits before any
 * deeper-in-the-tree effect that might touch the first-touch stash (namely
 * SectionContact's captureFirstTouch() call). That ordering is what makes
 * the "did the stash exist before this load" check below correct instead
 * of racy: reading it any later risks reading a stash this same page load
 * just wrote, which would misreport a first-ever visit as a repeat one.
 */
const SESSION_FLAG_KEY = "cs:visit-pinged";
const ENDPOINT = "/api/waves";

export function VisitPing() {
  useEffect(() => {
    // Doesn't stop a determined bot, but Playwright/Puppeteer/Selenium all
    // set this by default, and there's no reason to spend a D1 write (or
    // muddy the UTM numbers) on traffic that identifies itself this
    // plainly.
    if (typeof navigator !== "undefined" && navigator.webdriver === true) return;

    // Dedupe to one ping per tab session. Wrapped in try/catch like every
    // other storage access in this codebase (see firstTouch.ts) — Safari
    // private browsing and some locked-down profiles throw synchronously on
    // sessionStorage access rather than no-op. If storage isn't available
    // we can't dedupe, but that's a smaller problem than never counting the
    // visit at all, so we fall through and ping anyway.
    try {
      if (window.sessionStorage.getItem(SESSION_FLAG_KEY)) return;
    } catch {
      // fall through — ping once, best-effort dedupe only.
    }

    // Must be read BEFORE anything in this page load writes the stash —
    // see the ordering note above.
    const isFirstVisit = readFirstTouch() === null;

    const params = new URLSearchParams(window.location.search);
    const payload = {
      path: window.location.pathname,
      utm_source: params.get("utm_source"),
      utm_medium: params.get("utm_medium"),
      utm_campaign: params.get("utm_campaign"),
      referrer: sanitizeReferrer(document.referrer || ""),
      is_first_visit: isFirstVisit,
    };

    try {
      // keepalive lets the request outlive a same-tick navigation (a
      // visitor clicking a link before the browser would otherwise have
      // sent the ping). Errors are swallowed — a dropped pageview log entry
      // must never surface to the visitor or affect page behavior.
      fetch(ENDPOINT, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        keepalive: true,
        body: JSON.stringify(payload),
      }).catch(() => {});
    } catch {
      // fetch can throw synchronously in a handful of locked-down
      // environments — same reasoning as above.
    }

    try {
      window.sessionStorage.setItem(SESSION_FLAG_KEY, "1");
    } catch {
      // Best-effort only — see the read above.
    }
  }, []);

  return null;
}
