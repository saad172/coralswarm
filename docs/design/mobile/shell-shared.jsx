/* Shared atoms, icons, nav data and the sample "Ask" content used by all three
   shell directions. Everything is exported to window at the bottom so the other
   babel scripts can read it (separate babel scripts don't share scope). */
const { useState } = React;

/* ── Icons (1.7 stroke, 24 grid) — refined line set matching the product ── */
const ic = (p) => ({
  width: 18, height: 18, viewBox: "0 0 24 24", fill: "none",
  stroke: "currentColor", strokeWidth: 1.7, strokeLinecap: "round", strokeLinejoin: "round", ...p,
});
const IconAsk = (p) => <svg {...ic(p)}><circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" /></svg>;
const IconReefs = (p) => <svg {...ic(p)}><circle cx="6" cy="6" r="2.4" /><circle cx="18" cy="7" r="2.4" /><circle cx="12" cy="17" r="2.4" /><path d="M7.7 7.4 10.5 15M16.5 8.6 13.4 15.4M8 6.4h8" /></svg>;
const IconReview = (p) => <svg {...ic(p)}><circle cx="12" cy="12" r="9" /><path d="M8 12.3 11 15l5-6" /></svg>;
const IconCurrents = (p) => <svg {...ic(p)}><path d="M3 14c3-4 6 4 9 0s6-4 9 0" /><path d="M3 9c3-4 6 4 9 0s6-4 9 0" /></svg>;
const IconTeam = (p) => <svg {...ic(p)}><circle cx="9" cy="8" r="3" /><path d="M3.5 19a5.5 5.5 0 0 1 11 0" /><path d="M16 6a3 3 0 0 1 0 6M20.5 19a5.5 5.5 0 0 0-3.5-5.1" /></svg>;
const IconAdd = (p) => <svg {...ic(p)}><path d="M12 5v14M5 12h14" /></svg>;
const IconConnectors = (p) => <svg {...ic(p)}><rect x="4" y="4" width="6" height="6" rx="1.4" /><rect x="14" y="4" width="6" height="6" rx="1.4" /><rect x="4" y="14" width="6" height="6" rx="1.4" /><path d="M14 17h6" /><path d="M17 14v6" /></svg>;
const IconWorkflows = (p) => <svg {...ic(p)}><circle cx="6" cy="6" r="2.2" /><circle cx="18" cy="18" r="2.2" /><path d="M8 6h6a4 4 0 0 1 4 4v6" /></svg>;
const IconTeams = (p) => <svg {...ic(p)}><rect x="4" y="4" width="16" height="16" rx="3" /><path d="M9 9h6M9 13h4" /></svg>;
const IconSettings = (p) => <svg {...ic(p)}><circle cx="12" cy="12" r="3" /><path d="M19 12a7 7 0 0 0-.1-1.3l1.8-1.4-2-3.4-2.1.8a7 7 0 0 0-2.2-1.3L12 2h-4l-.4 2.1a7 7 0 0 0-2.2 1.3l-2.1-.8-2 3.4 1.8 1.4A7 7 0 0 0 3 12" /></svg>;
const IconLogout = (p) => <svg {...ic(p)}><path d="M14 4h4a1 1 0 0 1 1 1v14a1 1 0 0 1-1 1h-4" /><path d="M10 12H3M6 8l-3 4 3 4" /></svg>;
const IconUp = (p) => <svg {...ic(p)}><path d="M12 19V6M6 12l6-6 6 6" /></svg>;
const IconKey = (p) => <svg {...ic(p)}><path d="M4 4l16 16" /></svg>;

/* ── Nav model (from the real Shell) ── */
const BROWSE = [
  { label: "Ask", hue: "coral", Icon: IconAsk, active: true },
  { label: "Reefs", hue: "teal", Icon: IconReefs },
  { label: "Review", hue: "amber", Icon: IconReview, count: 3 },
  { label: "Currents", hue: "plum", Icon: IconCurrents },
];
const WORKSPACE = [
  { label: "Team & Offboarding", hue: "pink", Icon: IconTeam },
  { label: "Add corals", hue: "teal", Icon: IconAdd },
  { label: "Connectors", hue: "amber", Icon: IconConnectors },
  { label: "Workflows", hue: "coral", Icon: IconWorkflows },
  { label: "Teams", hue: "teal", Icon: IconTeams },
  { label: "Settings", hue: "plum", Icon: IconSettings },
];
const OCEANS = [
  { name: "Henderson", badge: "primary", hue: "coral", active: true },
  { name: "Core Platform", badge: "team", hue: "teal" },
  { name: "Acme", badge: "org", hue: "plum" },
  { name: "Scratch", badge: "personal", hue: "amber" },
];

const SUGGESTIONS = [
  "What did we decide about auth?",
  "How did we speed up embeddings?",
  "Summarize the Henderson reef",
  "Recent decisions this week",
];

/* The sample answered turn — realistic CoralSwarm content with provenance. */
const QUESTION = "What did we decide about auth?";
const ANSWER = [
  { t: "We standardized on " }, { b: "Clerk" },
  { t: " for both the web app and the desktop build, with org-scoped sessions so an ocean's membership maps straight onto access" }, { c: 1 },
  { t: ". The team ruled out rolling our own after the March security review" }, { c: 2 },
  { t: ", and desktop auth now flows through Clerk's device-code grant rather than an embedded webview" }, { c: 3 },
  { t: "." },
];
const CITES = [
  { n: 1, hue: "coral", kind: "decision", label: "Adopt Clerk for all auth surfaces", meta: "decision · 2026-03-14", ocean: "Henderson", oceanHue: "coral" },
  { n: 2, hue: "teal", kind: "meeting", label: "Security review — auth options", meta: "zoom · Alex, lena · 2026-03-09", ocean: "Henderson", oceanHue: "coral" },
  { n: 3, hue: "amber", kind: "slack", label: "desktop should use device-code, not a webview", meta: "slack · in #eng-desktop · 2026-03-11", ocean: "Core Platform", oceanHue: "teal" },
];

/* ── Small shared atoms ── */
function HueDot({ hue, size = 8, style }) {
  return <span style={{ width: size, height: size, borderRadius: "50%", background: `var(--hue-${hue})`, flex: "none", ...style }} />;
}

function Avatar({ children = "SK", hue = "coral", size = 28 }) {
  return (
    <span style={{
      width: size, height: size, borderRadius: "50%", flex: "none",
      display: "grid", placeItems: "center",
      fontFamily: "var(--mono)", fontSize: size * 0.36, color: "var(--ink)",
      background: `color-mix(in srgb, var(--hue-${hue}) 22%, var(--bg))`,
      border: `1px solid color-mix(in srgb, var(--hue-${hue}) 42%, transparent)`,
    }}>{children}</span>
  );
}

/* Inline citation chip that heats on hover and lights its row via shared state */
function Cite({ n, hot, setHot }) {
  const on = hot === n;
  return (
    <span
      onMouseEnter={() => setHot(n)} onMouseLeave={() => setHot(null)}
      style={{
        display: "inline-flex", alignItems: "center", justifyContent: "center",
        minWidth: 17, height: 17, padding: "0 5px", margin: "0 1px",
        borderRadius: 5, fontFamily: "var(--mono)", fontSize: 10.5, verticalAlign: "1px",
        cursor: "pointer", transition: "all var(--dur) var(--ease)",
        background: on ? "var(--brand)" : "var(--brand-3)",
        color: on ? "#fff" : "var(--brand-2)",
      }}>{n}</span>
  );
}

function Answer({ hot, setHot, size = 16 }) {
  return (
    <span style={{ fontFamily: "var(--sans)", fontSize: size, lineHeight: 1.65, color: "var(--ink)" }}>
      {ANSWER.map((p, i) =>
        p.c != null ? <Cite key={i} n={p.c} hot={hot} setHot={setHot} />
        : p.b ? <strong key={i} style={{ fontWeight: 600 }}>{p.b}</strong>
        : <span key={i}>{p.t}</span>
      )}
    </span>
  );
}

/* Reusable suggestion chip */
function Suggestion({ children, soft }) {
  return (
    <span style={{
      display: "inline-flex", alignItems: "center", padding: "7px 14px", borderRadius: "var(--radius-pill)",
      fontFamily: "var(--mono)", fontSize: 12, letterSpacing: "0.02em", color: "var(--ink-2)",
      cursor: "pointer", whiteSpace: "nowrap", transition: "all var(--dur) var(--ease)",
      background: soft ? "var(--bg)" : "transparent",
      border: "1px solid var(--line)",
    }}>{children}</span>
  );
}

Object.assign(window, {
  IconAsk, IconReefs, IconReview, IconCurrents, IconTeam, IconAdd,
  IconConnectors, IconWorkflows, IconTeams, IconSettings, IconLogout, IconUp, IconKey,
  BROWSE, WORKSPACE, OCEANS, SUGGESTIONS, QUESTION, ANSWER, CITES,
  HueDot, Avatar, Cite, Answer, Suggestion,
});
