import type {
  ConnectorHealthItem,
  ConnectorHealthResponse,
  DirectoryAttribution,
} from "@/lib/api";
import { DIRECTORY_READONLY_SCOPE, scopeDescription, scopeLabel } from "./reconsentModel";

/* Connector-health copy derivation — CS-446 (org-onboarding-from-the-invite.md
 * §3 item 16, D16).
 *
 * D16's build note: "if the member whose token backs the directory leaves,
 * the whole org silently drops from rung 1 to rung 2 unless that raises a
 * `connector_health` warning." The backend records that
 * (`connector_health`, migration 0243) and serves it as three
 * machine-readable keys — provider, capability, condition — and no prose.
 * This module is the ONE place those keys become words, the same "shared
 * derivation, not two that could drift" split `reconsentModel.ts` makes
 * against `missing_scopes`.
 *
 * ── Three sentences, always in this order ──
 * WHAT stopped working, what it MEANS, what FIXES it. A notice that says
 * only the first is a fact an admin can do nothing with; one that says only
 * the third is an instruction with no reason attached. The degradation this
 * ships with is invisible by nature — nothing breaks, no page errors, an
 * auto-add decision is simply made on weaker evidence — so "what it means"
 * is doing most of the work.
 *
 * ── Never guess ──
 * An unrecognised (provider, capability, condition) triple still RENDERS,
 * naming the keys it was given, rather than being silently dropped or
 * described with an invented meaning. Same posture `scopeDescription`'s
 * fallback takes for an unknown scope: say plainly that something is
 * degraded and that we cannot say more, because a connector-health surface
 * that quietly hides the conditions it does not recognise is the exact
 * failure this item exists to prevent, one level up. */

/** The one condition migration 0243's CHECK admits today. */
export const BACKING_MEMBER_DEPARTED = "backing_member_departed";
/** D16's rung 1 — the org's own Google Workspace directory. */
export const DOMAIN_DIRECTORY = "domain_directory";
export const GOOGLE_CHAT = "google_chat";

export interface ConnectorHealthCopy {
  /** Short, plain, specific — what stopped working. */
  what: string;
  /** Why an admin should care: the consequence, in product terms. */
  means: string;
  /** What restores it. Always something the reader can actually do. */
  remedy: string;
  /** Whose credential it was, when the record names one. Rendered as its
   *  own line so it reads as evidence rather than as blame folded into a
   *  sentence. `null` when the row carries no address (a user row deleted
   *  since — 0243 keeps the record and drops the link). */
  backingEmail: string | null;
  /** True only for a triple this module actually recognises. The component
   *  uses it to decide whether to offer the connect action: offering a
   *  specific remedy for a condition we cannot name would be a guess. */
  recognised: boolean;
}

/** Turn one degradation into the three sentences a notice shows.
 *
 *  Total — every input produces copy. See this module's doc comment on why
 *  an unrecognised triple renders rather than disappearing. */
export function connectorHealthCopy(item: ConnectorHealthItem): ConnectorHealthCopy {
  if (
    item.provider === GOOGLE_CHAT &&
    item.capability === DOMAIN_DIRECTORY &&
    item.condition === BACKING_MEMBER_DEPARTED
  ) {
    return {
      what: "CoralSwarm can no longer read your Google Workspace directory.",
      // Deliberately concrete about the consequence rather than abstract
      // about "rung 1": an admin has never read D16 and should not have to.
      // "the weaker check" is the plainest true description of dropping
      // from "your own directory lists them" to "full workspace member on a
      // verified domain".
      means:
        "When someone asks to join this organization, CoralSwarm now decides on the weaker " +
        "check — full membership of your chat workspace plus a verified email domain — instead " +
        "of asking your own directory whether they work here.",
      remedy:
        "Any member with a Google Workspace account can restore it: connect Google Chat and " +
        "allow directory access when Google asks.",
      backingEmail: item.backing_email,
      recognised: true,
    };
  }
  if (
    item.provider === "microsoft" &&
    item.capability === "teams_container_push" &&
    item.condition === "no_eligible_backer"
  ) {
    const fromDetail = item.detail.remedy;
    const remedy =
      typeof fromDetail === "string" && fromDetail.length > 0
        ? fromDetail
        : "any member of this channel can connect";
    return {
      what: "Teams capture for a channel is waiting on someone to reconnect.",
      means:
        "The shared push subscription has nobody left who can renew it, so new messages are delayed until polling catches them.",
      remedy,
      // The hardcoded "no longer in this organization" line is for directory
      // departure only. The last owner's name, when they can still reconnect,
      // is already inside `remedy`.
      backingEmail: null,
      recognised: false,
    };
  }
  return {
    what: `A connector capability stopped working: ${item.provider} · ${item.capability}.`,
    means: `This version of the app does not have a description for “${item.condition}”.`,
    remedy: "Reconnecting the affected connector is the usual fix.",
    backingEmail: item.backing_email,
    recognised: false,
  };
}

/** The degradations worth putting in front of an admin right now.
 *
 *  A pass-through today (the backend already returns only CURRENT rows, and
 *  re-derives "current" on every read rather than trusting a stored flag),
 *  named anyway so the component has no list logic of its own and a future
 *  ordering or severity rule has one obvious home. `null` in, `[]` out —
 *  the not-loaded and the nothing-wrong cases are the same to a caller that
 *  renders nothing for both. */
export function visibleDegradations(
  payload: ConnectorHealthResponse | null,
): ConnectorHealthItem[] {
  return payload?.degradations ?? [];
}

/** A stable key for one degradation — the same three columns that form
 *  migration 0243's PRIMARY KEY, so two list entries can never collide the
 *  way an index key would across a refetch. */
export function degradationKey(item: ConnectorHealthItem): string {
  const base = `${item.provider}:${item.capability}:${item.condition}`;
  const instance = item.detail.instance_key;
  if (typeof instance === "string" && instance.length > 0) {
    return `${base}:${instance}`;
  }
  return base;
}

/* ── CS-448: attribution, the third thing this module knows how to say ──────
 *
 * Item 16's notice above answers "what stopped working". Item 49's members
 * page answers "who hasn't connected". Neither says those are the same
 * sentence, and an admin holding both halves still cannot see that join
 * requests are being decided on the weaker rung BECAUSE of a specific
 * person. This is that sentence.
 *
 * ── The one claim this copy must never make ──
 * It does NOT say the named member's grant was the one carrying
 * `directory.readonly`. Disconnecting deletes the stored grant, which is
 * the only place the granted scopes ever lived, so nobody can know that
 * after the fact — see `services::connector_health::directory_backing`'s
 * own doc comment. What the backend can prove, and all this says, is: no
 * current member grants directory access, and THIS member, who is still
 * here, disconnected Google Chat on THIS date. Two true facts, put
 * together. The remedy works either way, and it is pointed at a person who
 * is still around to press the button — which is the whole difference from
 * a departure. */

/** A notice that names people. Same three-sentence spine as
 *  {@link ConnectorHealthCopy} — what, means, remedy — so the two notices
 *  read as one voice when both are on screen. */
export interface DirectoryAttributionCopy {
  what: string;
  means: string;
  remedy: string;
  /** Newest disconnect first, as the backend ordered them. Never empty. */
  members: { email: string; disconnectedAt: string }[];
  /** What the missing scope unlocks, taken from CS-431's own `SCOPE_COPY`
   *  rather than restated here — one description of `directory.readonly`
   *  in the app, shown from the member's side by `ReconsentNotice` and from
   *  the org's side here. */
  scopeLabel: string;
  scopeDescription: string;
}

/** Turn the read-time attribution into copy, or `null` for nothing to say.
 *
 *  `null` for an absent payload AND for a present-but-empty `members` list.
 *  The backend already refuses to send an empty one, so the second check is
 *  belt-and-braces against a notice that names nobody — which would read
 *  as "your directory is broken, we won't say why", the exact shape of
 *  alarm this whole area exists to avoid. */
export function directoryAttributionCopy(
  attribution: DirectoryAttribution | null | undefined,
): DirectoryAttributionCopy | null {
  if (!attribution || attribution.members.length === 0) return null;
  const many = attribution.members.length > 1;
  return {
    what: many
      ? "No one in this organization is giving CoralSwarm access to your Google Workspace directory — these members disconnected Google Chat and are still here."
      : "No one in this organization is giving CoralSwarm access to your Google Workspace directory — this member disconnected Google Chat and is still here.",
    // Concrete about the consequence rather than abstract about "rung 1":
    // an admin has never read D16 and should not have to. Deliberately the
    // same sentence `connectorHealthCopy` uses for the departure case,
    // because the consequence IS the same — only the cause and the remedy
    // differ, and repeating one truth in two voices is how a surface starts
    // sounding like it is describing two problems.
    means:
      "Join requests are being decided on the weaker check — full membership of your chat " +
      "workspace plus a verified email domain — instead of asking your own directory whether " +
      "someone works here.",
    remedy: many
      ? "Any of them can restore it by reconnecting Google Chat and allowing directory access."
      : "They can restore it by reconnecting Google Chat and allowing directory access — so can " +
        "any other member with a Google Workspace account.",
    members: attribution.members.map((m) => ({
      email: m.email,
      disconnectedAt: m.disconnected_at,
    })),
    scopeLabel: scopeLabel(DIRECTORY_READONLY_SCOPE),
    scopeDescription: scopeDescription(DIRECTORY_READONLY_SCOPE),
  };
}
