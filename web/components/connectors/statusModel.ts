import type { ButtonVariant, VerdictTone } from "@/components/ui";
import type { ConnectorStatus, ConnectorSummary, Problem } from "@/lib/connectorsApi";

/* The connector card's words, derived from `GET /v1/connectors`.

   ONE place that turns the backend's status enum, counts and problem codes
   into what the card says. The backend ships codes and params, never
   sentences (web/lib/api.ts, "backend ships no sentences"; PLAN.md decision 8),
   so every sentence a member reads on a connector is written here.

   Pure on purpose: clocks come in as a parameter so the tests are exact. */

// ── Status → pill, glyph, action ────────────────────────────────────────

export type PrimaryActionKind = "connect" | "continue_setup" | "manage" | "reconnect";

export interface PrimaryAction {
  kind: PrimaryActionKind;
  label: string;
  variant: ButtonVariant;
}

export interface StatusView {
  label: string;
  tone: VerdictTone;
  glyph: string;
  action: PrimaryAction;
}

export const STATUS_VIEW: Record<ConnectorStatus, StatusView> = {
  not_connected: {
    label: "Not connected",
    tone: "muted",
    glyph: "○",
    action: { kind: "connect", label: "Connect", variant: "secondary" },
  },
  setting_up: {
    label: "Setting up",
    tone: "muted",
    glyph: "◐",
    action: { kind: "continue_setup", label: "Continue setup", variant: "primary" },
  },
  active: {
    label: "Active",
    tone: "ok",
    glyph: "✓",
    action: { kind: "manage", label: "Manage", variant: "secondary" },
  },
  importing: {
    label: "Importing history",
    tone: "ok",
    glyph: "↓",
    action: { kind: "manage", label: "Manage", variant: "secondary" },
  },
  needs_attention: {
    label: "Needs attention",
    tone: "warn",
    glyph: "!",
    action: { kind: "manage", label: "Manage", variant: "secondary" },
  },
  reconnect_needed: {
    label: "Reconnect needed",
    tone: "bad",
    glyph: "×",
    action: { kind: "reconnect", label: "Reconnect", variant: "primary" },
  },
};

/** The status view, falling back to "not connected" for a status this build
 *  does not know (a newer backend), rather than throwing mid-render. */
export function statusView(status: ConnectorStatus): StatusView {
  return STATUS_VIEW[status] ?? STATUS_VIEW.not_connected;
}

/** The card's primary action. A recommended (onboarding "start here") card
 *  that is not yet connected takes the brand button instead of the secondary. */
export function primaryAction(status: ConnectorStatus, recommended = false): PrimaryAction {
  const a = statusView(status).action;
  return recommended && a.kind === "connect" ? { ...a, variant: "brand" } : a;
}

/** The two unhealthy statuses show the problem sentence in place of the
 *  freshness line. */
export function isUnhealthy(status: ConnectorStatus): boolean {
  return status === "needs_attention" || status === "reconnect_needed";
}

// ── Time ────────────────────────────────────────────────────────────────

/** "just now", "3 min ago", "2 h ago", "5 days ago", or a short date past a
 *  month. A future timestamp (clock skew) reads "just now". */
export function relativeTime(iso: string, now: Date): string {
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return "a while ago";
  const secs = Math.max(0, Math.floor((now.getTime() - t) / 1000));
  if (secs < 60) return "just now";
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins} min ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours} h ago`;
  const days = Math.floor(hours / 24);
  if (days < 31) return `${days} ${days === 1 ? "day" : "days"} ago`;
  return `on ${shortDate(iso)}`;
}

/** "Oct 27". */
export function shortDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "recently";
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

// ── Freshness line ──────────────────────────────────────────────────────

/** How much of the connector is on: "3 of 5 sources", or for a one-source
 *  connector what it is scoped to ("14 channels", "Everything I can see"). */
function scopePart(c: ConnectorSummary): string {
  if (c.sources_total === 1) {
    const s = c.sources[0];
    const scope = s?.scope;
    if (scope?.mode === "everything") return "Everything I can see";
    if (scope && scope.count !== null && scope.noun) {
      return `${scope.count} ${scope.count === 1 ? singular(scope.noun) : scope.noun}`;
    }
    return "1 source";
  }
  return `${c.sources_on} of ${c.sources_total} sources`;
}

function singular(noun: string): string {
  return noun.endsWith("s") ? noun.slice(0, -1) : noun;
}

/** Where a half-finished setup stands, in the mono line's voice. */
function setupPart(c: ConnectorSummary): string {
  switch (c.resume_step) {
    case "sources":
      return "no sources chosen yet · nothing captured";
    case "scope":
      return "scope not set yet";
    case "history":
      return "history not chosen yet";
    default:
      return "setup not finished";
  }
}

/** The mono line under a healthy card: "4 of 5 sources · last captured 3 min
 *  ago". Null when there is nothing to say (not connected, or unhealthy: the
 *  problem sentence takes its place). */
export function freshnessLine(c: ConnectorSummary, now: Date): string | null {
  switch (c.status) {
    case "not_connected":
    case "needs_attention":
    case "reconnect_needed":
      return null;
    case "setting_up":
      return c.sources_total === 1 ? setupPart(c) : `${c.sources_on} of ${c.sources_total} sources chosen · ${setupPart(c)}`;
    case "importing": {
      const i = c.importing;
      const window = !i || i.window_days === 0 ? "importing history" : `importing ${i.window_days} days`;
      const pct = i && i.percent !== null ? ` · ${i.percent}%` : "";
      return `${scopePart(c)} · ${window}${pct}`;
    }
    case "active":
      return `${scopePart(c)} · ${
        c.last_captured_at ? `last captured ${relativeTime(c.last_captured_at, now)}` : "nothing captured yet"
      }`;
    default:
      return null;
  }
}

/** Importing progress for the card's Meter: a percent, or null for indeterminate. */
export function importProgress(c: ConnectorSummary): { percent: number | null } | null {
  if (c.status !== "importing") return null;
  return { percent: c.importing?.percent ?? null };
}

// ── Problem code → sentence ─────────────────────────────────────────────

const REASON_SENTENCE: Record<string, (name: string, since: string) => string> = {
  ms_password_changed_or_admin_reset: (n, d) =>
    `Your ${n} password was changed or reset on ${d}, which ended the sign-in. Reconnect to resume.`,
  ms_account_disabled: (n) => `Your ${n} account is disabled. Ask your administrator, then reconnect.`,
  ms_account_deleted: (n) => `Your ${n} account no longer exists. Connect a different account.`,
  slack_token_revoked: (n, d) => `${n} revoked access on ${d}. Reconnect to resume.`,
  slack_tokens_revoked_event: (n, d) => `${n} revoked access on ${d}. Reconnect to resume.`,
  slack_account_inactive: (n) => `Your ${n} account is deactivated. Ask a workspace admin, then reconnect.`,
};

function expired(name: string, since: string): string {
  return `Your ${name} sign-in expired on ${since}. Reconnect to resume.`;
}

/** The sentence for a problem. `name` is the connector's display name. An
 *  unknown code gets a generic sentence naming the connector and the date,
 *  never the raw code. */
export function problemSentence(p: Problem, name: string): string {
  const since = shortDate(p.since);
  switch (p.code) {
    case "credential_invalid": {
      const reason = typeof p.params.reason === "string" ? p.params.reason : "";
      const specific = REASON_SENTENCE[reason];
      return specific ? specific(name, since) : expired(name, since);
    }
    case "sign_in_expired":
      return expired(name, since);
    case "access_revoked":
      return `${name} access was revoked on ${since}. Reconnect to resume.`;
    case "missing_scopes": {
      const n = typeof p.params.count === "number" ? p.params.count : null;
      const what = n === null ? "some permissions" : n === 1 ? "1 permission" : `${n} permissions`;
      return `${name} is missing ${what} CoralSwarm needs. Reconnect and approve them to resume.`;
    }
    case "app_not_installed":
      return `The CoralSwarm app isn't installed in your ${name} workspace yet. Reconnect to install it.`;
    case "tenant_consent_required":
      return `Your ${name} organization hasn't approved CoralSwarm. Ask an admin to approve it in ${name}, then retry.`;
    case "installation_suspended":
      return `The CoralSwarm installation on ${name} was suspended on ${since}. Unsuspend it in ${name}, then retry.`;
    case "capture_unavailable":
      return `${name} is connected, but capture isn't working right now. Open Manage to see what to do.`;
    case "destination_unreachable":
      return `CoralSwarm couldn't reach some of your ${name} repositories on ${since}. Check they still exist and that access is granted.`;
    case "sync_failed":
      return `Syncing ${name} failed on ${since}. Check Manage for current status. Checking status does not restart capture.`;
    default:
      return `${name} needs attention since ${since}. Open Manage to see what happened.`;
  }
}

/** What the card says for an unhealthy connector: the problem sentence, or the
 *  generic one if the backend named a status but sent no problem. */
export function problemLine(c: ConnectorSummary): string | null {
  if (!isUnhealthy(c.status)) return null;
  if (c.problem) return problemSentence(c.problem, c.name);
  return c.status === "reconnect_needed"
    ? `Your ${c.name} sign-in stopped working. Reconnect to resume.`
    : `${c.name} needs attention. Open Manage to see what happened.`;
}

export function sourceStatusView(status: string): Pick<StatusView, "label" | "tone" | "glyph"> {
  if (status === "paused") return { label: "Paused", tone: "muted", glyph: "‖" };
  if (status === "stopped") return { label: "Stopped", tone: "bad", glyph: "×" };
  if (status === "off") return { label: "Off", tone: "muted", glyph: "○" };
  return statusView(status as ConnectorStatus);
}
