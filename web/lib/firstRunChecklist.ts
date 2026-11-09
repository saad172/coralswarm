import { ApiError, type ConversationSummary, type OAuthConnection, type OrgDirectoryMember } from "@/lib/api";

/* The first-run checklist on an org's empty Ask: connect your sources, invite
   teammates, ask your first question. Every item is derived from server state
   on each load (never from a local "done" flag), so it cannot claim progress
   the member has not made, and it disappears by itself once the work is done.
   The only local state is a snooze, which expires. */

export const FIRST_RUN_SNOOZE_MS = 3 * 24 * 60 * 60 * 1000;

export function firstRunSnoozeKey(accountId: string, userId: string): string {
  return `cs_first_run_snooze:${accountId}:${userId}`;
}

/** `unavailable`: the step does not apply to this caller here (no connector is
 *  switched on for this org; inviting is an admin's job), so it neither blocks
 *  completion nor shows as to-do. */
export type ItemState = "done" | "todo" | "unknown" | "unavailable";

export interface ConnectItem {
  id: "connect";
  state: ItemState;
  /** Sources this member connected with their own grant. */
  connected: string[];
  /** Account-wide connections someone else made. Shown, never counted (D21). */
  shared: string[];
  /** The member's own grants that were revoked and need reconnecting. */
  needsReconnect: string[];
}

export interface InviteItem {
  id: "invite";
  state: ItemState;
  /** Other people in the org, signed in or invited. */
  teammates: number;
}

export interface AskItem {
  id: "ask";
  state: ItemState;
}

export interface FirstRunChecklist {
  visible: boolean;
  complete: boolean;
  snoozed: boolean;
  items: [ConnectItem, InviteItem, AskItem];
}

export interface FirstRunInput {
  hasOrg: boolean;
  /** `null` while loading; `"error"` when the read failed; `"unavailable"`
   *  when the org has no connector switched on ([`connectorsOffInOrg`]). */
  connections: OAuthConnection[] | null | "error" | "unavailable";
  /** How many OTHER people hold a place in the org (members plus pending
   *  invites, the caller excluded). `"unavailable"` for a non-admin, who
   *  cannot invite, or when member management is off for the org. */
  teammates: number | null | "error" | "unavailable";
  /** The caller's own conversations (not ones shared into the ocean). */
  conversationCount: number | null | "error";
  snoozedUntil: number | null;
  now: number;
}

const PROVIDER_LABELS: Record<string, string> = {
  slack: "Slack",
  slack_user: "Slack",
  google: "Google Calendar",
  google_gmail: "Gmail",
  google_chat: "Google Chat",
  google_meet: "Google Meet",
  microsoft: "Microsoft",
  github: "GitHub",
  linear: "Linear",
  jira: "Jira",
};

export function providerLabel(provider: string): string {
  return PROVIDER_LABELS[provider] ?? provider;
}

function labels(rows: OAuthConnection[]): string[] {
  return [...new Set(rows.map((r) => providerLabel(r.provider)))];
}

/** `routes/oauth.rs` `ORG_CONNECTORS_OFF_CODE`: `GET /v1/oauth/connections`
 *  answers 403 with this code when no connector flag is on for the org, which
 *  is every new self-serve org. */
export const ORG_CONNECTORS_OFF_CODE = "org_connectors_off";

// A backend deployed before the code existed sends only the sentence. Web and
// backend deploy separately, so the sentence stays a fallback for that window.
const CONNECTORS_OFF_IN_ORG_SENTENCE = "connector authorization is available in your personal workspace only";

export function connectorsOffInOrg(err: unknown): boolean {
  if (!(err instanceof ApiError) || err.status !== 403) return false;
  if (err.code) return err.code === ORG_CONNECTORS_OFF_CODE;
  return err.message.startsWith(CONNECTORS_OFF_IN_ORG_SENTENCE);
}

/** The caller's own chats: the list also carries chats shared into an ocean
 *  they can read, which are not "you asked a question". */
export function ownConversationCount(conversations: readonly ConversationSummary[]): number {
  return conversations.filter((c) => c.mine).length;
}

/** Everyone in the directory except the caller, pending invites included: an
 *  invite sent is the step done, whether or not they have signed in yet. */
export function teammateCount(directory: readonly OrgDirectoryMember[], selfId: string | null): number {
  return directory.filter((m) => m.user_id !== selfId).length;
}

function connectItem(connections: FirstRunInput["connections"]): ConnectItem {
  if (connections === "unavailable") {
    return { id: "connect", state: "unavailable", connected: [], shared: [], needsReconnect: [] };
  }
  if (connections === null || connections === "error") {
    return { id: "connect", state: "unknown", connected: [], shared: [], needsReconnect: [] };
  }
  // `mine` is the member's own grant (`owner_user_id` = caller). The same
  // predicate the roster's `connected_providers` and the member-connect
  // nudge use; `undefined` (an older backend) is not proof of a grant.
  const own = connections.filter((c) => c.mine === true);
  const live = own.filter((c) => !c.credential_invalid_at);
  const dead = own.filter((c) => c.credential_invalid_at);
  const ownLabels = new Set(labels(own));
  const liveLabels = labels(live);
  return {
    id: "connect",
    state: live.length > 0 ? "done" : "todo",
    connected: liveLabels,
    shared: labels(connections.filter((c) => c.mine !== true)).filter((l) => !ownLabels.has(l)),
    needsReconnect: labels(dead).filter((l) => !liveLabels.includes(l)),
  };
}

function inviteItem(teammates: FirstRunInput["teammates"]): InviteItem {
  if (teammates === "unavailable") return { id: "invite", state: "unavailable", teammates: 0 };
  if (teammates === null || teammates === "error") return { id: "invite", state: "unknown", teammates: 0 };
  return { id: "invite", state: teammates > 0 ? "done" : "todo", teammates };
}

export function deriveFirstRunChecklist(input: FirstRunInput): FirstRunChecklist {
  const connect = connectItem(input.connections);
  const invite = inviteItem(input.teammates);
  const count = input.conversationCount;
  const ask: AskItem = {
    id: "ask",
    state: count === null || count === "error" ? "unknown" : count > 0 ? "done" : "todo",
  };
  const items: [ConnectItem, InviteItem, AskItem] = [connect, invite, ask];
  const complete = items.every((i) => i.state === "done" || i.state === "unavailable");
  // A snooze can be at most one window long from now, so a bad or tampered
  // stored value cannot hide real incompleteness indefinitely.
  const snoozed =
    input.snoozedUntil !== null &&
    input.snoozedUntil > input.now &&
    input.snoozedUntil <= input.now + FIRST_RUN_SNOOZE_MS;
  const loading = input.connections === null || input.teammates === null || input.conversationCount === null;
  // Only render when something is KNOWN to be left: done + unknown is not
  // evidence of incompleteness.
  const anyTodo = items.some((i) => i.state === "todo");
  return {
    visible: input.hasOrg && !loading && !complete && anyTodo && !snoozed,
    complete,
    snoozed,
    items,
  };
}
